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
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.TrieNodeStrategy;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Objects;
import java.util.Optional;
import java.util.function.BooleanSupplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * A {@link TrieNodeStrategy} that archives every trie-node write into {@code
 * TRIE_BRANCH_STORAGE_ARCHIVE} so historical {@code eth_getProof} requests don't need trie-log
 * replay.
 *
 * <p>Each put delegates to {@code base} (live flat DB) first, then — if the archive gate is open —
 * reads the prior node value from storage (where it is typically hot in the top few in-memory
 * layers) and enqueues a capture request via {@link ArchiveTrieNodeCapture}. Workers compute the
 * history entry ({@link ArchiveNodeHistoryStore#getLatestBefore} + encode FULL/DIFF) off the import
 * thread. Results are joined and applied to the transaction in {@link #onBeforeCommit}, which runs
 * immediately before commit.
 *
 * <p>Reading the prior node on the calling thread — rather than inside a worker — avoids disk I/O
 * in the common case (the prior value is hot in the in-memory layered chain) and eliminates any
 * need for worker threads to access the layered storage. An archiving gap — gate closed then
 * reopened, or a restart — forces the next block to write FULL, since the newest archive entry no
 * longer matches the flat DB.
 */
public class ArchiveTrieNodeStrategy implements TrieNodeStrategy {

  private final TrieNodeStrategy base;
  private final BooleanSupplier archiveGate;
  private final ArchiveTrieNodeCapture capture;

  public ArchiveTrieNodeStrategy(
      final TrieNodeStrategy base,
      final ArchiveTrieNodeCapture capture,
      final BooleanSupplier archiveGate) {
    this.base = Objects.requireNonNull(base);
    this.capture = Objects.requireNonNull(capture);
    this.archiveGate = Objects.requireNonNull(archiveGate);
  }

  private static long readBlockNumber(final SegmentedKeyValueStorage storage) {
    return storage
        .get(TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY)
        .map(b -> Bytes.wrap(b).toLong() + 1L)
        .orElse(0L);
  }

  private boolean shouldCaptureBlock(final long block) {
    return block == 0L || archiveGate.getAsBoolean();
  }

  @Override
  public Optional<Bytes> getFlatAccountTrieNode(
      final Bytes location, final Bytes32 nodeHash, final SegmentedKeyValueStorage storage) {
    return base.getFlatAccountTrieNode(location, nodeHash, storage);
  }

  @Override
  public Optional<Bytes> getFlatStorageTrieNode(
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash,
      final SegmentedKeyValueStorage storage) {
    return base.getFlatStorageTrieNode(accountHash, location, nodeHash, storage);
  }

  @Override
  public void putFlatAccountTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location,
      final Bytes32 nodeHash,
      final Bytes node) {
    final long block = readBlockNumber(storage);
    base.putFlatAccountTrieNode(storage, transaction, location, nodeHash, node);
    if (shouldCaptureBlock(block)) {
      final Bytes prior =
          block == 0L
              ? null
              : base.getFlatAccountTrieNode(location, nodeHash, storage).orElse(null);
      capture.enqueue(
          ArchiveNodeKey.account(location),
          location,
          block,
          null,
          nodeHash,
          node,
          prior,
          transaction);
    }
  }

  @Override
  public void putFlatStorageTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash,
      final Bytes node) {
    final long block = readBlockNumber(storage);
    base.putFlatStorageTrieNode(storage, transaction, accountHash, location, nodeHash, node);
    if (shouldCaptureBlock(block)) {
      final Bytes prior =
          block == 0L
              ? null
              : base.getFlatStorageTrieNode(accountHash, location, nodeHash, storage).orElse(null);
      capture.enqueue(
          ArchiveNodeKey.storage(accountHash.getBytes(), location),
          location,
          block,
          accountHash,
          nodeHash,
          node,
          prior,
          transaction);
    }
  }

  @Override
  public void removeFlatAccountStateTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location) {
    final long block = readBlockNumber(storage);
    base.removeFlatAccountStateTrieNode(storage, transaction, location);
    if (shouldCaptureBlock(block)) {
      final Bytes prior =
          block == 0L
              ? null
              : base.getFlatAccountTrieNode(location, Bytes32.ZERO, storage).orElse(null);
      if (prior != null) {
        // Removing a node that doesn't exist is a no-op for the archive.
        capture.enqueue(
            ArchiveNodeKey.account(location),
            location,
            block,
            null,
            Bytes32.ZERO,
            null,
            prior,
            transaction);
      }
    }
  }

  @Override
  public void onBeforeCommit(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction) {
    capture.onBeforeCommit(transaction);
  }

  @Override
  public void onRollback(final SegmentedKeyValueStorageTransaction transaction) {
    capture.onRollback(transaction);
  }
}
