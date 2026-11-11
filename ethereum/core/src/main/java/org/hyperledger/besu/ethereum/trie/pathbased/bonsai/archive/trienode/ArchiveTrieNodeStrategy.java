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
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeHistoryStore.ArchivedBlock;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.TrieNodeKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.TrieNodeStrategy;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.io.Closeable;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
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
 * <p>Each put takes the prior node from the nodes the block's state-root computation loaded and
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
  private final AtomicBoolean stopLogged = new AtomicBoolean(false);

  // Starts true (archive everything) until the first in-sync event. Safe default for initial sync
  // and no-peers startup
  private final AtomicBoolean archiving = new AtomicBoolean(true);

  // True when an external chain estimate exists (e.g. a peer or CL that knows the chain head)
  private volatile BooleanSupplier hasChainEstimate;

  private record CapturedBlock(long number, Map<Bytes, Bytes> priorNodes) {}

  // Nodes loaded from the capture storage since its last commit, by trie-node key. They are what
  // storage holds until that commit, so a captured block uses them as the nodes it replaces.
  private final AtomicReference<Map<Bytes, Bytes>> loadedNodes =
      new AtomicReference<>(new ConcurrentHashMap<>());

  // Transactions persisting a captured block, mapped to that block.
  private final Cache<SegmentedKeyValueStorageTransaction, CapturedBlock> capturedBlocks =
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
  public NodeLoader overrideNodeLoader(
      final SegmentedKeyValueStorage storage,
      final NodeLoader loader,
      final Function<Bytes, Bytes> keyOf) {
    if (!isCaptureStorage.test(storage)) {
      return loader;
    }
    return (location, hash) -> {
      // taken before the load, so a load racing a commit can't reach the next commit's map
      final Map<Bytes, Bytes> loaded = loadedNodes.get();
      final Optional<Bytes> node = loader.getNode(location, hash);
      // the empty trie node is never stored, so it is never a prior
      node.filter(n -> !hash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH))
          .ifPresent(n -> loaded.put(keyOf.apply(location), n));
      return node;
    };
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
      capturedBlocks.put(transaction, new CapturedBlock(block, loadedNodes.get()));
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
    final long block = blockHeader.getNumber();
    final Optional<ArchivedBlock> current = historyStore.getArchivedBlock();
    if (current.isEmpty()) {
      return block == 0L;
    }
    final ArchivedBlock last = current.get();
    if (block == last.number() + 1 && last.hash().equals(blockHeader.getParentHash())) {
      return true;
    }
    // Lower blocks are re-imports or siblings; capture never resumes, so log the first stop only.
    if (block > last.number() && stopLogged.compareAndSet(false, true)) {
      LOG.error(
          "Trie-node capture stopped: block {} ({}) does not extend archived block {} ({}). "
              + "Proof history needs a resync.",
          block,
          blockHeader.getBlockHash(),
          last.number(),
          last.hash());
    }
    return false;
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
    final CapturedBlock block = capturedBlocks.getIfPresent(transaction);
    if (block != null) {
      final Bytes prior = loadedPriorNode(key, block);
      if (isNoOpRewrite(prior, node)) {
        return;
      }
      capture(key, block.number(), node, prior, transaction);
    }
  }

  @Override
  public void removeTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes key) {
    base.removeTrieNode(storage, transaction, key);
    final CapturedBlock block = capturedBlocks.getIfPresent(transaction);
    if (block != null) {
      final Bytes prior = priorNode(storage, key, block);
      // Removing a node that doesn't exist is a no-op for the archive.
      if (prior != null) {
        capture(key, block.number(), null, prior, transaction);
      }
    }
  }

  // Live nodes are always loaded before being replaced; anything else is written FULL.
  private Bytes loadedPriorNode(final Bytes key, final CapturedBlock block) {
    return block.number() == 0L ? null : block.priorNodes().get(key);
  }

  // A removal reads storage when the node wasn't loaded, to know whether it existed.
  private Bytes priorNode(
      final SegmentedKeyValueStorage storage, final Bytes key, final CapturedBlock block) {
    final Bytes loaded = loadedPriorNode(key, block);
    if (loaded != null || block.number() == 0L) {
      return loaded;
    }
    return base.getTrieNode(storage, key, Bytes32.ZERO).orElse(null);
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
    if (isCaptureStorage.test(storage)) {
      // the commit may change any loaded node, so nothing loaded before it is a prior after it
      loadedNodes.set(new ConcurrentHashMap<>());
    }
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
