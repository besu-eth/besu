/*
 * Copyright contributors to Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.mainnet.parallelization.prefetch;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListAccountLookup;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mechanism for prefetching world state data based on Block Access List (BAL).
 *
 * <p>This class handles the prefetching of account and storage data to populate the cache before
 * transaction execution, improving parallel processing performance.
 */
@SuppressWarnings("rawtypes")
public class BalPrefetcher {

  private static final Logger LOG = LoggerFactory.getLogger(BalPrefetcher.class);

  private static final Comparator<byte[]> STORAGE_KEY_COMPARATOR = Arrays::compareUnsigned;
  private final boolean isSortingEnabled;
  private final int batchSize;

  /**
   * Creates a new prefetch mechanism.
   *
   * @param isSortingEnabled whether to sort keys before prefetching (may improve DB locality)
   * @param batchSize the batch size for prefetch operations (0 or negative = no batching, fetch all
   *     at once)
   */
  public BalPrefetcher(final boolean isSortingEnabled, final int batchSize) {
    this.isSortingEnabled = isSortingEnabled;
    this.batchSize = batchSize;
  }

  /**
   * Prefetch world state data based on the block access list.
   *
   * @param worldState the world state to prefetch data into
   * @param accountLookup the block access list's lookup, which keeps the accounts read for the
   *     block's transactions
   * @param orchestrationExecutor the executor that runs the prefetch orchestration task
   * @param fetchExecutor the executor for fetch operations
   * @return a completable future that completes when prefetching is done
   */
  public CompletableFuture<Void> prefetch(
      final BonsaiWorldState worldState,
      final BlockAccessListAccountLookup accountLookup,
      final Executor orchestrationExecutor,
      final Executor fetchExecutor) {

    return CompletableFuture.supplyAsync(
            () -> {
              worldState.disableCacheMerkleTrieLoader();

              // Collect all keys to prefetch
              final PrefetchKeys keys = collectKeys(accountLookup);

              LOG.debug(
                  "Prefetch: collected {} account keys and {} storage keys",
                  keys.accountKeys.size(),
                  keys.storageKeys.size());

              return keys;
            },
            orchestrationExecutor)
        .thenCompose(
            keys ->
                fetchKeysAsync(worldState, accountLookup, keys, fetchExecutor)
                    .thenRun(
                        () ->
                            LOG.debug(
                                "Prefetch completed: {} accounts + {} storage slots{}",
                                keys.accountKeys.size(),
                                keys.storageKeys.size(),
                                shouldBatch()
                                    ? " in batches of " + batchSize
                                    : " in single batch")))
        .whenComplete(
            (result, ex) -> {
              if (ex != null) {
                LOG.error("Error during prefetch", ex);
              }
            });
  }

  /** Collect all account and storage keys from the block access list. */
  private PrefetchKeys collectKeys(final BlockAccessListAccountLookup accountLookup) {
    final List<BlockAccessList.AccountChanges> accounts = accountLookup.accountChanges();
    final List<Address> accountAddresses = new ArrayList<>(accounts.size());
    final List<byte[]> accountKeys = new ArrayList<>(accounts.size());
    final List<byte[]> storageKeys = new ArrayList<>();
    for (final BlockAccessList.AccountChanges accountChanges : accounts) {
      final Address address = accountChanges.address();
      // the lookup hashes each address once for the block, for the transactions too
      final byte[] addressHash =
          accountLookup.getAddressHash(address).orElseThrow().getBytes().toArrayUnsafe();
      accountAddresses.add(address);
      accountKeys.add(addressHash);
      final List<BlockAccessList.SlotChanges> storageChanges = accountChanges.storageChanges();
      final List<BlockAccessList.SlotRead> storageReads = accountChanges.storageReads();
      final int rawSlotCount = storageChanges.size() + storageReads.size();
      if (rawSlotCount == 0) {
        continue;
      }
      // Deduplicate storage slots by hash without streams/lambdas (plain iterator loops).
      final Set<StorageSlotKey> uniqueSlots = HashSet.newHashSet(rawSlotCount);
      for (final BlockAccessList.SlotChanges storageChange : storageChanges) {
        uniqueSlots.add(storageChange.slot());
      }
      for (final BlockAccessList.SlotRead storageRead : storageReads) {
        uniqueSlots.add(storageRead.slot());
      }
      final int rangeStart = storageKeys.size();
      for (final StorageSlotKey slot : uniqueSlots) {
        final byte[] slotHash = slot.getSlotHash().getBytes().toArrayUnsafe();
        final byte[] storageKey = new byte[addressHash.length + slotHash.length];
        System.arraycopy(addressHash, 0, storageKey, 0, addressHash.length);
        System.arraycopy(slotHash, 0, storageKey, addressHash.length, slotHash.length);
        storageKeys.add(storageKey);
      }
      if (isSortingEnabled) {
        storageKeys.subList(rangeStart, storageKeys.size()).sort(STORAGE_KEY_COMPARATOR);
      }
    }
    return new PrefetchKeys(accountAddresses, accountKeys, storageKeys);
  }

  /**
   * Unified method to fetch keys with optional batching.
   *
   * <p>If batchSize <= 0, fetches all keys in parallel (2 futures: accounts + storage).
   *
   * <p>If batchSize > 0, splits into multiple batches and fetches them in parallel.
   *
   * @return a future that completes when all fetch operations finish
   */
  private CompletableFuture<Void> fetchKeysAsync(
      final BonsaiWorldState worldState,
      final BlockAccessListAccountLookup accountLookup,
      final PrefetchKeys keys,
      final Executor fetchExecutor) {

    // Fetch accounts (with optional batching), kept for the block's transactions to read
    final List<CompletableFuture<Void>> futures =
        new ArrayList<>(
            fetchSegmentKeys(
                worldState,
                ACCOUNT_INFO_STATE,
                keys.accountKeys,
                "account",
                fetchExecutor,
                (offset, accounts) -> {
                  for (int i = 0; i < accounts.size(); i++) {
                    final Optional<Bytes> account = accounts.get(i);
                    if (account != null) {
                      accountLookup.putParentAccount(
                          keys.accountAddresses.get(offset + i), account);
                    }
                  }
                }));

    // Fetch storage (with optional batching)
    if (!keys.storageKeys.isEmpty()) {
      futures.addAll(
          fetchSegmentKeys(
              worldState,
              ACCOUNT_STORAGE_STORAGE,
              keys.storageKeys,
              "storage",
              fetchExecutor,
              (offset, slots) -> {}));
    }

    return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
  }

  /**
   * Fetch keys for a specific segment, with optional batching.
   *
   * @param worldState the world state
   * @param segment the segment identifier
   * @param keys the keys to fetch
   * @param segmentName human-readable segment name for logging
   * @param fetchExecutor the executor for fetch operations
   * @param onFetched receives the offset of each batch in {@code keys} and its values
   * @return list of futures for all batch operations
   */
  private List<CompletableFuture<Void>> fetchSegmentKeys(
      final BonsaiWorldState worldState,
      final SegmentIdentifier segment,
      final List<byte[]> keys,
      final String segmentName,
      final Executor fetchExecutor,
      final BiConsumer<Integer, List<Optional<Bytes>>> onFetched) {

    final List<CompletableFuture<Void>> futures = new ArrayList<>();

    if (!shouldBatch()) {
      // Single batch: fetch all keys at once
      futures.add(
          CompletableFuture.runAsync(
              () -> {
                onFetched.accept(0, prefetchKeys(worldState, segment, keys));
                LOG.debug("Prefetch: fetched {} {} keys in single batch", keys.size(), segmentName);
              },
              fetchExecutor));
    } else {
      // Multiple batches
      final int batchCount = calculateBatchCount(keys.size());
      for (int i = 0; i < batchCount; i++) {
        final List<byte[]> batch = getBatch(keys, i);
        final int batchNumber = i;

        futures.add(
            CompletableFuture.runAsync(
                () -> {
                  onFetched.accept(
                      batchNumber * batchSize, prefetchKeys(worldState, segment, batch));
                  LOG.trace(
                      "Prefetch: fetched {} batch {}/{} ({} keys)",
                      segmentName,
                      batchNumber + 1,
                      batchCount,
                      batch.size());
                },
                fetchExecutor));
      }

      LOG.debug("Prefetch: fetched {} {} keys in {} batches", keys.size(), segmentName, batchCount);
    }

    return futures;
  }

  private List<Optional<Bytes>> prefetchKeys(
      final BonsaiWorldState worldState, final SegmentIdentifier segment, final List<byte[]> keys) {
    return worldState.getWorldStateStorage().getMultipleFlat(segment, keys);
  }

  private boolean shouldBatch() {
    return batchSize > 0;
  }

  private int calculateBatchCount(final int totalKeys) {
    return (int) Math.ceil((double) totalKeys / batchSize);
  }

  private List<byte[]> getBatch(final List<byte[]> keys, final int batchIndex) {
    final int start = batchIndex * batchSize;
    final int end = Math.min(start + batchSize, keys.size());
    return keys.subList(start, end);
  }

  /** Container for collected prefetch keys. */
  private record PrefetchKeys(
      List<Address> accountAddresses, List<byte[]> accountKeys, List<byte[]> storageKeys) {}
}
