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
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Prefetches Bonsai flat-state entries described by a Block Access List so parallel transaction
 * execution can reuse the cross-block cache instead of independently loading the same keys.
 */
public class BalPrefetcher {

  private static final Logger LOG = LoggerFactory.getLogger(BalPrefetcher.class);

  private final boolean sortingEnabled;
  private final int batchSize;
  private final int fetchConcurrency;

  public BalPrefetcher(final boolean sortingEnabled, final int batchSize) {
    this(sortingEnabled, batchSize, 1);
  }

  public BalPrefetcher(
      final boolean sortingEnabled, final int batchSize, final int fetchConcurrency) {
    this.sortingEnabled = sortingEnabled;
    this.batchSize = batchSize;
    this.fetchConcurrency = Math.max(1, fetchConcurrency);
  }

  public CompletableFuture<Void> prefetch(
      final BonsaiWorldState worldState,
      final BlockAccessList blockAccessList,
      final Executor executor) {
    if (blockAccessList.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }

    return CompletableFuture.supplyAsync(
            () -> {
              worldState.disableCacheMerkleTrieLoader();

              final PrefetchKeys keys = collectKeys(blockAccessList.accountChanges());
              LOG.debug(
                  "BAL prefetch collected {} account keys and {} storage keys",
                  keys.accountKeys().size(),
                  keys.storageKeys().size());
              return keys;
            },
            executor)
        .thenCompose(
            keys ->
                fetchKeys(worldState, keys, executor)
                    .thenRun(
                        () ->
                            LOG.debug(
                                "BAL prefetch completed: {} accounts and {} storage slots",
                                keys.accountKeys().size(),
                                keys.storageKeys().size())));
  }

  private PrefetchKeys collectKeys(final List<BlockAccessList.AccountChanges> accounts) {
    final List<Bytes> accountKeys = new ArrayList<>(accounts.size());
    final List<Bytes> storageKeys = new ArrayList<>(expectedStorageKeyCount(accounts));

    for (final BlockAccessList.AccountChanges accountChanges : accounts) {
      final Address address = accountChanges.address();
      final Bytes addressHash = address.addressHash().getBytes();
      accountKeys.add(addressHash);

      final int slotCount =
          accountChanges.storageChanges().size() + accountChanges.storageReads().size();
      if (slotCount == 0) {
        continue;
      }

      accountChanges
          .storageChanges()
          .forEach(
              change ->
                  storageKeys.add(storageKey(addressHash, change.slot().getSlotHash().getBytes())));
      accountChanges
          .storageReads()
          .forEach(
              read ->
                  storageKeys.add(storageKey(addressHash, read.slot().getSlotHash().getBytes())));
    }

    if (sortingEnabled) {
      accountKeys.sort(BalPrefetcher::compareBytes);
      storageKeys.sort(BalPrefetcher::compareBytes);
    }

    return new PrefetchKeys(accountKeys, storageKeys);
  }

  private static int expectedStorageKeyCount(final List<BlockAccessList.AccountChanges> accounts) {
    long count = 0;
    for (final BlockAccessList.AccountChanges account : accounts) {
      count += account.storageChanges().size();
      count += account.storageReads().size();
    }
    return (int) Math.min(count, Integer.MAX_VALUE);
  }

  private static Bytes storageKey(final Bytes addressHash, final Bytes slotHash) {
    final byte[] addressBytes = addressHash.toArrayUnsafe();
    final byte[] slotBytes = slotHash.toArrayUnsafe();
    final byte[] key = new byte[addressBytes.length + slotBytes.length];
    System.arraycopy(addressBytes, 0, key, 0, addressBytes.length);
    System.arraycopy(slotBytes, 0, key, addressBytes.length, slotBytes.length);
    return Bytes.wrap(key);
  }

  private static int compareBytes(final Bytes left, final Bytes right) {
    return Arrays.compareUnsigned(left.toArrayUnsafe(), right.toArrayUnsafe());
  }

  private CompletableFuture<Void> fetchKeys(
      final BonsaiWorldState worldState, final PrefetchKeys keys, final Executor executor) {
    final int accountBatchCount = batchCount(keys.accountKeys().size());
    final int storageBatchCount = batchCount(keys.storageKeys().size());
    final int totalBatchCount = accountBatchCount + storageBatchCount;
    if (totalBatchCount == 0) {
      return CompletableFuture.completedFuture(null);
    }

    final AtomicInteger nextBatchIndex = new AtomicInteger();
    final int workerCount = Math.min(fetchConcurrency, totalBatchCount);
    final List<CompletableFuture<Void>> workers = new ArrayList<>(workerCount);

    for (int workerIndex = 0; workerIndex < workerCount; workerIndex++) {
      workers.add(
          CompletableFuture.runAsync(
              () ->
                  fetchBatches(
                      worldState, keys, nextBatchIndex, accountBatchCount, totalBatchCount),
              executor));
    }

    return CompletableFuture.allOf(workers.toArray(new CompletableFuture<?>[0]));
  }

  private void fetchBatches(
      final BonsaiWorldState worldState,
      final PrefetchKeys keys,
      final AtomicInteger nextBatchIndex,
      final int accountBatchCount,
      final int totalBatchCount) {
    int batchIndex;
    while ((batchIndex = nextBatchIndex.getAndIncrement()) < totalBatchCount) {
      if (batchIndex < accountBatchCount) {
        fetchBatch(worldState, ACCOUNT_INFO_STATE, keys.accountKeys(), batchIndex);
      } else {
        fetchBatch(
            worldState,
            ACCOUNT_STORAGE_STORAGE,
            keys.storageKeys(),
            batchIndex - accountBatchCount);
      }
    }
  }

  private void fetchBatch(
      final BonsaiWorldState worldState,
      final SegmentIdentifier segment,
      final List<Bytes> keys,
      final int batchIndex) {
    final boolean batching = shouldBatch(keys.size());
    final int start = batching ? batchIndex * batchSize : 0;
    final int end = batching ? Math.min(start + batchSize, keys.size()) : keys.size();
    worldState.getWorldStateStorage().getMultipleKeys(segment, keys.subList(start, end));
  }

  private int batchCount(final int keyCount) {
    return keyCount == 0 ? 0 : shouldBatch(keyCount) ? calculateBatchCount(keyCount) : 1;
  }

  private boolean shouldBatch(final int keyCount) {
    return batchSize > 0 && keyCount > batchSize;
  }

  private int calculateBatchCount(final int totalKeys) {
    return (totalKeys + batchSize - 1) / batchSize;
  }

  private record PrefetchKeys(List<Bytes> accountKeys, List<Bytes> storageKeys) {}
}
