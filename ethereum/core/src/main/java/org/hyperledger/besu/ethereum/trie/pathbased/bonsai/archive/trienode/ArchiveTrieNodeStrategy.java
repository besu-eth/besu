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
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.TrieNodeStrategy;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.io.Closeable;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * A {@link TrieNodeStrategy} that archives every trie-node write into {@code
 * TRIE_BRANCH_STORAGE_ARCHIVE} so historical {@code eth_getProof} requests don't need trie-log
 * replay.
 *
 * <p>Each put delegates to {@code base} (live flat DB) first, then — if the archive gate is open —
 * reads the prior node value from storage (where it is typically hot in the top few in-memory
 * layers) and calls {@link ArchiveTrieNodeWriter#capture} to queue an async history-entry write.
 * Workers compute the history entry ({@link ArchiveNodeHistoryStore#getLatestBefore} + encode
 * FULL/DIFF) off the import thread. Results are joined and applied to the transaction in {@link
 * #onBeforeCommit}, which runs immediately before commit.
 *
 * <p>Reading the prior node on the calling thread — rather than inside a worker — avoids disk I/O
 * in the common case (the prior value is hot in the in-memory layered chain) and eliminates any
 * need for worker threads to access the layered storage. An archiving gap — gate closed then
 * reopened, or a restart — forces the next block to write FULL, since the newest archive entry no
 * longer matches the flat DB.
 */
public class ArchiveTrieNodeStrategy
    implements TrieNodeStrategy, Synchronizer.InSyncListener, Closeable {

  private final TrieNodeStrategy base;
  private final ArchiveTrieNodeWriter trieNodeWriter;

  // Starts true (archive everything) until the first in-sync event. Safe default for initial sync
  // and no-peers startup
  private final AtomicBoolean archiving = new AtomicBoolean(true);

  // True when a remote chain estimate exists (we have a peer that knows the chain head)
  private volatile BooleanSupplier hasRemoteChainEstimate;

  // Block number is constant within a transaction; cache it to avoid a storage read per node write.
  private final Cache<SegmentedKeyValueStorageTransaction, Long> txBlockNumberCache =
      Caffeine.newBuilder().weakKeys().build();

  /**
   * Builds an archiving strategy over {@code liveStorage}, owning a worker pool for async
   * history-entry computation.
   *
   * @param liveStorage the live world-state storage to archive writes from
   * @param trieCapturePool the worker pool, owned and shut down by the returned strategy
   * @param shallowCheckpointInterval FULL-checkpoint interval for shallow nodes
   * @param deepCheckpointInterval FULL-checkpoint interval for deep nodes
   * @return an archiving {@link TrieNodeStrategy} ready to install via {@code setTrieNodeStrategy}
   */
  public static ArchiveTrieNodeStrategy createArchiveStrategy(
      final SegmentedKeyValueStorage liveStorage,
      final ExecutorService trieCapturePool,
      final int shallowCheckpointInterval,
      final int deepCheckpointInterval) {
    return new ArchiveTrieNodeStrategy(
        new BonsaiTrieNodeStrategy(),
        new ArchiveTrieNodeWriter(
            new ArchiveNodeHistoryStore(liveStorage),
            new ArchiveCoverageTracker(liveStorage),
            trieCapturePool,
            shallowCheckpointInterval,
            deepCheckpointInterval),
        () -> false);
  }

  /**
   * Supplies whether a remote chain estimate exists. Must be set before the in-sync subscription is
   * registered, since it is consulted from {@link #onInSyncStatusChange(boolean)}.
   *
   * @param hasRemoteChainEstimate true when a peer with a known chain head is available
   */
  public void setHasRemoteChainEstimate(final BooleanSupplier hasRemoteChainEstimate) {
    this.hasRemoteChainEstimate =
        Objects.requireNonNull(hasRemoteChainEstimate, "hasRemoteChainEstimate must not be null");
  }

  /**
   * @param base the delegate strategy for the live flat DB
   * @param trieNodeWriter the writer that persists archived history entries
   * @param hasRemoteChainEstimate supplies whether a remote chain estimate exists (see field)
   */
  @VisibleForTesting
  public ArchiveTrieNodeStrategy(
      final TrieNodeStrategy base,
      final ArchiveTrieNodeWriter trieNodeWriter,
      final BooleanSupplier hasRemoteChainEstimate) {
    this.base = Objects.requireNonNull(base);
    this.trieNodeWriter = Objects.requireNonNull(trieNodeWriter);
    this.hasRemoteChainEstimate =
        Objects.requireNonNull(hasRemoteChainEstimate, "hasRemoteChainEstimate must not be null");
  }

  @Override
  public void onInSyncStatusChange(final boolean inSync) {
    // A peer-less node reports "in sync" forever, so only trust a corroborated in-sync signal.
    archiving.set(!inSync || !hasRemoteChainEstimate.getAsBoolean());
  }

  @VisibleForTesting
  void setArchiving(final boolean archive) {
    this.archiving.set(archive);
  }

  private static long readBlockNumber(final SegmentedKeyValueStorage storage) {
    return storage
        .get(TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY)
        .map(b -> Bytes.wrap(b).toLong() + 1L)
        .orElse(0L);
  }

  private boolean shouldCaptureBlock(final long block) {
    return block == 0L || archiving.get();
  }

  /**
   * A no-op re-write: the node is being written with the exact bytes already committed at this
   * location (e.g. a storage slot set to its existing value, which Bonsai still re-commits along
   * the touched path). Its history is already captured by the last real change — {@code
   * getLatestBefore} resolves to that entry — so archiving an empty-diff entry here is redundant
   * and would count toward the diff-chain length that forces FULL checkpoints. Creations ({@code
   * prior == null}) are never no-ops.
   */
  private boolean isNoOpRewrite(final Bytes prior, final Bytes node) {
    return prior != null && prior.equals(node);
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
    final long block = txBlockNumberCache.get(transaction, ignored -> readBlockNumber(storage));
    base.putFlatAccountTrieNode(storage, transaction, location, nodeHash, node);
    if (shouldCaptureBlock(block)) {
      final Bytes prior =
          block == 0L
              ? null
              : base.getFlatAccountTrieNode(location, Bytes32.ZERO, storage).orElse(null);
      if (isNoOpRewrite(prior, node)) {
        return;
      }
      trieNodeWriter.capture(
          ArchiveNodeKey.account(location), location, block, node, prior, transaction);
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
    final long block = txBlockNumberCache.get(transaction, ignored -> readBlockNumber(storage));
    base.putFlatStorageTrieNode(storage, transaction, accountHash, location, nodeHash, node);
    if (shouldCaptureBlock(block)) {
      final Bytes prior =
          block == 0L
              ? null
              : base.getFlatStorageTrieNode(accountHash, location, Bytes32.ZERO, storage)
                  .orElse(null);
      if (isNoOpRewrite(prior, node)) {
        return;
      }
      trieNodeWriter.capture(
          ArchiveNodeKey.storage(accountHash.getBytes(), location),
          location,
          block,
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
    final long block = txBlockNumberCache.get(transaction, ignored -> readBlockNumber(storage));
    base.removeFlatAccountStateTrieNode(storage, transaction, location);
    if (shouldCaptureBlock(block)) {
      final Bytes prior =
          block == 0L
              ? null
              : base.getFlatAccountTrieNode(location, Bytes32.ZERO, storage).orElse(null);
      if (prior != null) {
        // Removing a node that doesn't exist is a no-op for the archive.
        trieNodeWriter.capture(
            ArchiveNodeKey.account(location), location, block, null, prior, transaction);
      }
    }
  }

  @Override
  public void onBeforeCommit(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction) {
    txBlockNumberCache.invalidate(transaction);
    trieNodeWriter.onBeforeCommit(transaction);
  }

  @Override
  public void onRollback(final SegmentedKeyValueStorageTransaction transaction) {
    txBlockNumberCache.invalidate(transaction);
    trieNodeWriter.onRollback(transaction);
  }

  @Override
  public void close() {
    trieNodeWriter.close();
  }
}
