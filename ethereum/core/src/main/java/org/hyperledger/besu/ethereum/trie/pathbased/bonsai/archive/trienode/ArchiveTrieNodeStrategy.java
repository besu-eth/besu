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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.TrieNodeStrategy;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * A {@link TrieNodeStrategy} that archives every trie-node write into {@code
 * TRIE_BRANCH_STORAGE_ARCHIVE} so that historical {@code eth_getProof} requests can be served
 * without replaying trie-log diffs.
 *
 * <p>Each put is delegated to the wrapped {@code base} strategy first (live flat DB), then, if the
 * archive gate is open, the full bare-RLP node is written into the archive column family in the
 * same transaction under an {@link ArchiveNodeKey} that encodes the block number. Progress is
 * recorded atomically in the same transaction on the first archive write per transaction.
 *
 * <p>The gate returns {@code true} while the node is behind the network head ({@code
 * !syncState.isInSync()}) and {@code false} once at the head, preventing live blocks within the
 * reorg window from entering the archive. Block 0 (genesis) is always archived.
 */
public class ArchiveTrieNodeStrategy implements TrieNodeStrategy {

  private final TrieNodeStrategy base;
  private final ArchiveNodeHistoryStore historyStore;
  private final ArchiveCoverageTracker coverageTracker;
  private final BooleanSupplier archiveGate;

  // Tracks the last transaction that recorded progress so that record() is called at most once per
  // transaction rather than once per trie-node write.
  private SegmentedKeyValueStorageTransaction lastRecordedTx;

  public ArchiveTrieNodeStrategy(
      final TrieNodeStrategy base,
      final ArchiveNodeHistoryStore historyStore,
      final ArchiveCoverageTracker coverageTracker,
      final BooleanSupplier gate) {
    this.base = Objects.requireNonNull(base);
    this.historyStore = Objects.requireNonNull(historyStore);
    this.coverageTracker = Objects.requireNonNull(coverageTracker);
    // Latch: once the gate returns false it stays false and never reopens.
    final AtomicBoolean latched = new AtomicBoolean(false);
    this.archiveGate =
        () -> {
          final boolean open = !latched.get() && gate.getAsBoolean();
          if (!open) latched.set(true);
          return open;
        };
  }

  private boolean shouldArchive(final long block) {
    return block == 0L || archiveGate.getAsBoolean();
  }

  private long currentBlockNumber(final SegmentedKeyValueStorage storage) {
    return storage
        .get(TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY)
        .map(b -> Bytes.wrap(b).toLong() + 1L)
        .orElse(0L);
  }

  private void maybeRecordProgress(
      final SegmentedKeyValueStorageTransaction transaction, final long block) {
    if (lastRecordedTx != transaction) {
      coverageTracker.record(transaction, block);
      lastRecordedTx = transaction;
    }
  }

  @Override
  public Optional<Bytes> getTrieNode(
      final Bytes location, final Bytes32 nodeHash, final SegmentedKeyValueStorage storage) {
    return base.getTrieNode(location, nodeHash, storage);
  }

  @Override
  public void putTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Optional<Hash> accountHash,
      final Bytes location,
      final Bytes32 nodeHash,
      final Bytes node) {
    base.putTrieNode(storage, transaction, accountHash, location, nodeHash, node);
    final long block = currentBlockNumber(storage);
    if (shouldArchive(block)) {
      final Bytes naturalKey =
          accountHash
              .map(hash -> ArchiveNodeKey.storage(hash.getBytes(), location))
              .orElseGet(() -> ArchiveNodeKey.account(location));
      historyStore.put(transaction, naturalKey, block, node);
      maybeRecordProgress(transaction, block);
    }
  }
}
