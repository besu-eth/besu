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
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_BLOCK_HASH_KEY;

import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeHistoryStore.ArchivedBlock;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.TrieNodeKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.TrieNodeStrategy;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.io.Closeable;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link TrieNodeStrategy} that archives trie-node writes into {@code
 * TRIE_BRANCH_STORAGE_ARCHIVE} for historical {@code eth_getProof}.
 *
 * <p>Each put reads the prior node on the calling thread, where it is usually hot in memory, and
 * queues the history entry for async encoding; {@link #onBeforeCommit} writes the entries into the
 * block's transaction.
 */
public class ArchiveTrieNodeStrategy
    implements TrieNodeStrategy, Synchronizer.InSyncListener, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ArchiveTrieNodeStrategy.class);

  private final TrieNodeStrategy base;
  private final ArchiveTrieNodeWriter trieNodeWriter;
  private final ArchiveNodeHistoryStore historyStore;
  private final Predicate<SegmentedKeyValueStorage> isCaptureStorage;
  private final AtomicBoolean reorgLogged = new AtomicBoolean(false);

  // Starts true (archive everything) until the first in-sync event. Safe default for initial sync
  // and no-peers startup
  private final AtomicBoolean archiving = new AtomicBoolean(true);

  // True when an external chain estimate exists (e.g. a peer or CL that knows the chain head)
  private volatile BooleanSupplier hasChainEstimate;

  // Transactions persisting a captured block, mapped to that block's number.
  private final Cache<SegmentedKeyValueStorageTransaction, Long> capturedBlocks =
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
    final ArchiveNodeHistoryStore historyStore = new ArchiveNodeHistoryStore(liveStorage);
    return new ArchiveTrieNodeStrategy(
        new BonsaiTrieNodeStrategy(),
        new ArchiveTrieNodeWriter(
            historyStore, trieCapturePool, shallowCheckpointInterval, deepCheckpointInterval),
        historyStore,
        storage -> storage == liveStorage,
        () -> false);
  }

  /**
   * Supplies whether an external chain estimate exists. Must be set before the in-sync subscription
   * is registered, since it is consulted from {@link #onInSyncStatusChange(boolean)}.
   */
  public void setHasChainEstimate(final BooleanSupplier hasChainEstimate) {
    this.hasChainEstimate =
        Objects.requireNonNull(hasChainEstimate, "hasChainEstimate must not be null");
  }

  /**
   * @param base the delegate strategy for the live trie
   * @param trieNodeWriter the writer that persists archived history entries
   * @param historyStore the archive store whose recorded progress captured blocks must extend
   * @param isCaptureStorage whether writes to a storage are archived
   * @param hasChainEstimate supplies whether an external chain estimate exists (see field)
   */
  @VisibleForTesting
  public ArchiveTrieNodeStrategy(
      final TrieNodeStrategy base,
      final ArchiveTrieNodeWriter trieNodeWriter,
      final ArchiveNodeHistoryStore historyStore,
      final Predicate<SegmentedKeyValueStorage> isCaptureStorage,
      final BooleanSupplier hasChainEstimate) {
    this.base = Objects.requireNonNull(base);
    this.trieNodeWriter = Objects.requireNonNull(trieNodeWriter);
    this.historyStore = Objects.requireNonNull(historyStore);
    this.isCaptureStorage = Objects.requireNonNull(isCaptureStorage);
    this.hasChainEstimate =
        Objects.requireNonNull(hasChainEstimate, "hasChainEstimate must not be null");
  }

  @Override
  public void onInSyncStatusChange(final boolean inSync) {
    // A peer-less node reports "in sync" forever, so only trust a corroborated in-sync signal.
    archiving.set(!inSync || !hasChainEstimate.getAsBoolean());
  }

  @VisibleForTesting
  void setArchiving(final boolean archive) {
    this.archiving.set(archive);
  }

  @Override
  public void onBlockPersist(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final BlockHeader blockHeader) {
    final long block = blockHeader.getNumber();
    if (isCaptureStorage.test(storage)
        && isSingleBlockPersist(storage, blockHeader)
        && (block == 0L || archiving.get())
        && extendsCoverage(blockHeader)) {
      trieNodeWriter.beginBlock(transaction, blockHeader);
      capturedBlocks.put(transaction, block);
    }
  }

  private static boolean isSingleBlockPersist(
      final SegmentedKeyValueStorage storage, final BlockHeader blockHeader) {
    final Optional<Bytes> persisted =
        storage.get(TRIE_BRANCH_STORAGE, WORLD_BLOCK_HASH_KEY).map(Bytes::wrap);
    return blockHeader.getNumber() == 0L
        ? persisted.isEmpty()
        : persisted.isPresent() && persisted.get().equals(blockHeader.getParentHash().getBytes());
  }

  private boolean extendsCoverage(final BlockHeader blockHeader) {
    final Optional<ArchivedBlock> current = historyStore.getArchivedBlock();
    if (current.isEmpty()) {
      return blockHeader.getNumber() == 0L;
    }
    final ArchivedBlock lastArchived = current.get();
    if (blockHeader.getNumber() != lastArchived.number() + 1) {
      return false;
    }
    final boolean buildsOnLastArchived = lastArchived.hash().equals(blockHeader.getParentHash());
    if (!buildsOnLastArchived && reorgLogged.compareAndSet(false, true)) {
      LOG.error(
          "Trie-node capture stopped at block {}: the chain reorged past archived history. "
              + "Proof history needs a resync.",
          lastArchived.number());
    }
    return buildsOnLastArchived;
  }

  // Bonsai re-commits unchanged nodes along a touched path; skip archiving them to avoid
  // inflating the diff-chain counter. Creations (prior == null) are never no-ops.
  private boolean isNoOpRewrite(final Bytes prior, final Bytes node) {
    return prior != null && prior.equals(node);
  }

  @Override
  public Optional<Bytes> getTrieNode(
      final SegmentedKeyValueStorage storage, final Bytes key, final Bytes32 nodeHash) {
    return base.getTrieNode(storage, key, nodeHash);
  }

  @Override
  public void putTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes key,
      final Bytes32 nodeHash,
      final Bytes node) {
    base.putTrieNode(storage, transaction, key, nodeHash, node);
    final Long block = capturedBlocks.getIfPresent(transaction);
    if (block != null) {
      final Bytes prior = readPriorNode(storage, key, block);
      if (isNoOpRewrite(prior, node)) {
        return;
      }
      capture(key, block, node, prior, transaction);
    }
  }

  @Override
  public void removeTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes key) {
    base.removeTrieNode(storage, transaction, key);
    final Long block = capturedBlocks.getIfPresent(transaction);
    if (block != null) {
      final Bytes prior = readPriorNode(storage, key, block);
      // Removing a node that doesn't exist is a no-op for the archive.
      if (prior != null) {
        capture(key, block, null, prior, transaction);
      }
    }
  }

  // Genesis (block 0) has no prior node by definition.
  private Bytes readPriorNode(
      final SegmentedKeyValueStorage storage, final Bytes key, final long block) {
    return block == 0L ? null : base.getTrieNode(storage, key, Bytes32.ZERO).orElse(null);
  }

  private void capture(
      final Bytes key,
      final long block,
      final Bytes node,
      final Bytes prior,
      final SegmentedKeyValueStorageTransaction transaction) {
    trieNodeWriter.capture(
        ArchiveNodeKey.of(key), TrieNodeKey.location(key), block, node, prior, transaction);
  }

  @Override
  public void onBeforeCommit(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction) {
    if (capturedBlocks.asMap().remove(transaction) != null) {
      trieNodeWriter.onBeforeCommit(transaction);
    }
  }

  @Override
  public void onRollback(final SegmentedKeyValueStorageTransaction transaction) {
    if (capturedBlocks.asMap().remove(transaction) != null) {
      trieNodeWriter.onRollback(transaction);
    }
  }

  @Override
  public void close() {
    trieNodeWriter.close();
  }
}
