/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.v2;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.snap.RetryingGetAccountRangeFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.snap.RetryingGetBytecodeFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.snap.RetryingGetStorageRangeFromPeerTask;
import org.hyperledger.besu.ethereum.eth.messages.snap.AccountRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.StorageRangeMessage;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldStateDownloaderException;
import org.hyperledger.besu.ethereum.proof.WorldStateProofProvider;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.FormatMethod;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fetches canonical state at the new pivot from peers during snap/2 reorg recovery. Every response
 * is verified against the pivot header (account range proofs against the pivot state root, storage
 * range proofs against the account's canonical storage root, code by hash). Any verification or
 * retrieval failure raises {@link WorldStateDownloaderException}, which the pivot catch-up treats
 * like any other catch-up failure (sync restart).
 *
 * <p>The three fetch operations are expressed as small functional interfaces so tests can serve
 * responses from a real world state instead of a peer.
 */
public class SnapV2ReorgStateFetcher {

  private static final Logger LOG = LoggerFactory.getLogger(SnapV2ReorgStateFetcher.class);

  private static final long FETCH_TIMEOUT_SECONDS = 10;

  @FunctionalInterface
  public interface AccountRangeFetcher {
    CompletableFuture<AccountRangeMessage.AccountRangeData> fetch(
        Bytes32 startKeyHash, Bytes32 endKeyHash, BlockHeader pivotBlockHeader);
  }

  @FunctionalInterface
  public interface StorageRangeFetcher {
    CompletableFuture<StorageRangeMessage.SlotRangeData> fetch(
        List<Bytes32> accountHashes,
        Bytes32 startKeyHash,
        Bytes32 endKeyHash,
        BlockHeader pivotBlockHeader);
  }

  @FunctionalInterface
  public interface BytecodeFetcher {
    CompletableFuture<Map<Bytes32, Bytes>> fetch(
        List<Bytes32> codeHashes, BlockHeader pivotBlockHeader);
  }

  private final AccountRangeFetcher accountRangeFetcher;
  private final StorageRangeFetcher storageRangeFetcher;
  private final BytecodeFetcher bytecodeFetcher;
  private final WorldStateProofProvider proofProvider;

  @VisibleForTesting
  public SnapV2ReorgStateFetcher(
      final AccountRangeFetcher accountRangeFetcher,
      final StorageRangeFetcher storageRangeFetcher,
      final BytecodeFetcher bytecodeFetcher,
      final WorldStateStorageCoordinator worldStateStorageCoordinator) {
    this.accountRangeFetcher = accountRangeFetcher;
    this.storageRangeFetcher = storageRangeFetcher;
    this.bytecodeFetcher = bytecodeFetcher;
    this.proofProvider = new WorldStateProofProvider(worldStateStorageCoordinator);
  }

  public static SnapV2ReorgStateFetcher fromEthContext(
      final EthContext ethContext,
      final MetricsSystem metricsSystem,
      final WorldStateStorageCoordinator worldStateStorageCoordinator) {
    return new SnapV2ReorgStateFetcher(
        (startKeyHash, endKeyHash, pivotBlockHeader) ->
            RetryingGetAccountRangeFromPeerTask.forAccountRange(
                    ethContext, startKeyHash, endKeyHash, pivotBlockHeader, metricsSystem)
                .run(),
        (accountHashes, startKeyHash, endKeyHash, pivotBlockHeader) ->
            RetryingGetStorageRangeFromPeerTask.forStorageRange(
                    ethContext,
                    accountHashes,
                    startKeyHash,
                    endKeyHash,
                    pivotBlockHeader,
                    metricsSystem)
                .run(),
        (codeHashes, pivotBlockHeader) ->
            RetryingGetBytecodeFromPeerTask.forByteCode(
                    ethContext, codeHashes, pivotBlockHeader, metricsSystem)
                .run(),
        worldStateStorageCoordinator);
  }

  /**
   * Fetches the flat accounts for the given account hashes at the pivot, one range request per
   * account. An account absent at the pivot maps to {@link Optional#empty()} (proven by the range
   * proof).
   */
  public CompletableFuture<Map<Hash, Optional<PmtStateTrieAccountValue>>> fetchAccounts(
      final Set<Hash> accountHashes, final BlockHeader pivotBlockHeader) {
    if (accountHashes.isEmpty()) {
      return CompletableFuture.completedFuture(Map.of());
    }

    final ConcurrentHashMap<Hash, Optional<PmtStateTrieAccountValue>> results =
        new ConcurrentHashMap<>();
    final List<CompletableFuture<Void>> futures = new ArrayList<>();

    for (final Hash accountHash : accountHashes) {
      final Bytes32 startKey = Bytes32.wrap(accountHash.getBytes());
      final Bytes32 endKey = RangeManager.nextKey(startKey);
      futures.add(
          accountRangeFetcher
              .fetch(startKey, endKey, pivotBlockHeader)
              .orTimeout(FETCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)
              .thenAccept(
                  response ->
                      results.put(
                          accountHash,
                          decodeVerifiedAccount(
                              response, startKey, endKey, accountHash, pivotBlockHeader))));
    }

    return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
        .thenApply(v -> Map.copyOf(results));
  }

  /**
   * Fetches individual storage slots of one account at the pivot, one range request per slot,
   * verifying proofs against the account's canonical storage root. A slot absent at the pivot maps
   * to {@link Optional#empty()} and must be removed locally.
   */
  public CompletableFuture<Map<Hash, Optional<UInt256>>> fetchSlots(
      final Hash accountHash,
      final Hash storageRoot,
      final Set<Hash> slotHashes,
      final BlockHeader pivotBlockHeader) {
    if (slotHashes.isEmpty()) {
      return CompletableFuture.completedFuture(Map.of());
    }

    final ConcurrentHashMap<Hash, Optional<UInt256>> results = new ConcurrentHashMap<>();
    final List<CompletableFuture<Void>> futures = new ArrayList<>();
    final Bytes32 accountHashBytes = Bytes32.wrap(accountHash.getBytes());

    for (final Hash slotHash : slotHashes) {
      final Bytes32 startKey = Bytes32.wrap(slotHash.getBytes());
      final Bytes32 endKey = RangeManager.nextKey(startKey);
      futures.add(
          storageRangeFetcher
              .fetch(List.of(accountHashBytes), startKey, endKey, pivotBlockHeader)
              .orTimeout(FETCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)
              .thenAccept(
                  response ->
                      results.put(
                          slotHash,
                          decodeVerifiedSlot(
                              response,
                              accountHash,
                              storageRoot,
                              startKey,
                              endKey,
                              slotHash,
                              pivotBlockHeader))));
    }

    return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
        .thenApply(v -> Map.copyOf(results));
  }

  /**
   * Fetches code by hash. Code is content-addressed, so verification is simply hashing the returned
   * bytes; a missing entry is an error (a restored account's canonical code must exist on the
   * canonical chain).
   */
  public CompletableFuture<Map<Hash, Bytes>> fetchCodes(
      final Set<Hash> codeHashes, final BlockHeader pivotBlockHeader) {
    if (codeHashes.isEmpty()) {
      return CompletableFuture.completedFuture(Map.of());
    }

    final List<Bytes32> requested =
        codeHashes.stream().map(h -> Bytes32.wrap(h.getBytes())).toList();
    return bytecodeFetcher
        .fetch(requested, pivotBlockHeader)
        .orTimeout(FETCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .thenApply(
            codes -> {
              final Map<Hash, Bytes> verified = new ConcurrentHashMap<>();
              for (final Hash codeHash : codeHashes) {
                final Bytes code = codes.get(Bytes32.wrap(codeHash.getBytes()));
                if (code == null || !Hash.hash(code).equals(codeHash)) {
                  throw fetchError(
                      "Code %s missing or hash-mismatched at pivot %s",
                      codeHash, pivotBlockHeader.getNumber());
                }
                verified.put(codeHash, code);
              }
              return Map.copyOf(verified);
            });
  }

  private Optional<PmtStateTrieAccountValue> decodeVerifiedAccount(
      final AccountRangeMessage.AccountRangeData response,
      final Bytes32 startKey,
      final Bytes32 endKey,
      final Hash accountHash,
      final BlockHeader pivotBlockHeader) {
    if (!proofProvider.isValidRangeProof(
        startKey,
        endKey,
        Bytes32.wrap(pivotBlockHeader.getStateRoot().getBytes()),
        response.proofs(),
        response.accounts())) {
      throw fetchError(
          "Invalid account range proof for account %s at pivot %s",
          accountHash, pivotBlockHeader.getNumber());
    }
    final Bytes accountData = response.accounts().get(startKey);
    if (accountData == null) {
      return Optional.empty();
    }
    return Optional.of(PmtStateTrieAccountValue.readFrom(RLP.input(accountData)));
  }

  private Optional<UInt256> decodeVerifiedSlot(
      final StorageRangeMessage.SlotRangeData response,
      final Hash accountHash,
      final Hash storageRoot,
      final Bytes32 startKey,
      final Bytes32 endKey,
      final Hash slotHash,
      final BlockHeader pivotBlockHeader) {
    final NavigableMap<Bytes32, Bytes> slots =
        response.slots().isEmpty() ? new TreeMap<>() : response.slots().get(0);
    if (!proofProvider.isValidRangeProof(
        startKey, endKey, Bytes32.wrap(storageRoot.getBytes()), response.proofs(), slots)) {
      throw fetchError(
          "Invalid storage range proof for slot %s of account %s at pivot %s",
          slotHash, accountHash, pivotBlockHeader.getNumber());
    }
    final Bytes slotValue = slots.get(startKey);
    if (slotValue == null) {
      return Optional.empty();
    }
    return Optional.of(
        UInt256.fromBytes(Bytes32.leftPad(org.apache.tuweni.rlp.RLP.decodeValue(slotValue))));
  }

  @FormatMethod
  private WorldStateDownloaderException fetchError(final String fmt, final Object... args) {
    final String msg = String.format(fmt, args);
    LOG.error("snap/2 reorg state fetch failed: {}", msg);
    return new WorldStateDownloaderException(msg);
  }
}
