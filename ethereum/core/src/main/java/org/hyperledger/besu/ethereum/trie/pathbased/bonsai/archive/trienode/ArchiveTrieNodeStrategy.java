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
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_ROOT_HASH_KEY;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeHistoryStore.ArchivedBlock;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.TrieNodeKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.TrieNodeStrategy;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.util.log.LogUtil;

import java.io.Closeable;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.LongPredicate;

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
public class ArchiveTrieNodeStrategy implements TrieNodeStrategy, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ArchiveTrieNodeStrategy.class);
  private static final int LOG_MISSING_COVERAGE_INTERVAL_SECONDS = 60;
  // Well above a large block's loads; a load past it is only written FULL.
  static final int MAX_LOADED_NODES = 1_000_000;

  private final TrieNodeStrategy base;
  private final ArchiveTrieNodeWriter trieNodeWriter;
  private final ArchiveNodeHistoryStore historyStore;
  private final SegmentedKeyValueStorage captureStorage;
  private final LongPredicate isFinal;
  private final int maxLoadedNodes;
  private final AtomicBoolean logMissingCoverage = new AtomicBoolean(true);

  // Nodes read from one state, by key. They're the same whichever storage they were read from.
  // Empty when the next block can't be captured, so there's nothing to collect.
  private record LoadedNodes(Hash root, Optional<Map<Bytes, Bytes>> nodes) {}

  private static final LoadedNodes NO_LOADED_NODES = new LoadedNodes(Hash.ZERO, Optional.empty());

  private final AtomicReference<LoadedNodes> loadedNodes = new AtomicReference<>(NO_LOADED_NODES);

  // Transactions persisting a captured block, mapped to the nodes that block replaces.
  private final Cache<SegmentedKeyValueStorageTransaction, Map<Bytes, Bytes>> capturedBlocks =
      Caffeine.newBuilder().weakKeys().build();

  /**
   * @param base the delegate strategy for the live trie
   * @param trieNodeWriter the writer that persists archived history entries
   * @param historyStore the archive store whose recorded progress captured blocks must extend
   * @param captureStorage the storage whose block persists are archived
   * @param isFinal whether a block is final, and so safe to capture
   */
  public ArchiveTrieNodeStrategy(
      final TrieNodeStrategy base,
      final ArchiveTrieNodeWriter trieNodeWriter,
      final ArchiveNodeHistoryStore historyStore,
      final SegmentedKeyValueStorage captureStorage,
      final LongPredicate isFinal) {
    this(base, trieNodeWriter, historyStore, captureStorage, isFinal, MAX_LOADED_NODES);
  }

  @VisibleForTesting
  ArchiveTrieNodeStrategy(
      final TrieNodeStrategy base,
      final ArchiveTrieNodeWriter trieNodeWriter,
      final ArchiveNodeHistoryStore historyStore,
      final SegmentedKeyValueStorage captureStorage,
      final LongPredicate isFinal,
      final int maxLoadedNodes) {
    this.base = Objects.requireNonNull(base);
    this.trieNodeWriter = Objects.requireNonNull(trieNodeWriter);
    this.historyStore = Objects.requireNonNull(historyStore);
    this.captureStorage = Objects.requireNonNull(captureStorage);
    this.isFinal = Objects.requireNonNull(isFinal);
    this.maxLoadedNodes = maxLoadedNodes;
  }

  @Override
  public NodeLoader wrapNodeLoader(
      final Hash stateRoot, final NodeLoader loader, final Function<Bytes, Bytes> keyOf) {
    final Optional<Map<Bytes, Bytes>> maybeLoaded = loadedNodesFor(stateRoot);
    if (maybeLoaded.isEmpty()) {
      return loader;
    }
    final Map<Bytes, Bytes> loaded = maybeLoaded.get();
    return (location, hash) -> {
      final Optional<Bytes> node = loader.getNode(location, hash);
      // the empty trie node is never stored, so it is never a prior
      if (!hash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH) && loaded.size() < maxLoadedNodes) {
        node.ifPresent(n -> loaded.put(keyOf.apply(location), n));
      }
      return node;
    };
  }

  // Only nodes from the current head are useful, and only if the next block can be captured.
  // Decided once per state; check memory before disk.
  private Optional<Map<Bytes, Bytes>> loadedNodesFor(final Hash stateRoot) {
    final LoadedNodes current = loadedNodes.get();
    if (current.root().equals(stateRoot)) {
      return current.nodes();
    }
    if (!isPersistedRoot(stateRoot)) {
      return Optional.empty();
    }
    final Optional<Map<Bytes, Bytes>> nodes =
        coverageEndsAtPersistedBlock() ? Optional.of(new ConcurrentHashMap<>()) : Optional.empty();
    return loadedNodes
        .updateAndGet(c -> c.root().equals(stateRoot) ? c : new LoadedNodes(stateRoot, nodes))
        .nodes();
  }

  // The next block can only extend coverage if coverage ends at the persisted state.
  private boolean coverageEndsAtPersistedBlock() {
    final Optional<Bytes> archived =
        historyStore.getArchivedBlock().map(last -> last.hash().getBytes());
    return archived.isPresent() && archived.equals(persistedBlockHash());
  }

  private Optional<Bytes> persistedBlockHash() {
    return captureStorage.get(TRIE_BRANCH_STORAGE, WORLD_BLOCK_HASH_KEY).map(Bytes::wrap);
  }

  private boolean isPersistedRoot(final Hash stateRoot) {
    return captureStorage
        .get(TRIE_BRANCH_STORAGE, WORLD_ROOT_HASH_KEY)
        .filter(root -> stateRoot.equals(Hash.wrap(Bytes32.wrap(root))))
        .isPresent();
  }

  // Loads are priors only if their root is the state this block replaces; genesis has none.
  private Map<Bytes, Bytes> priorNodes() {
    final LoadedNodes loaded = loadedNodes.get();
    return isPersistedRoot(loaded.root()) ? loaded.nodes().orElse(Map.of()) : Map.of();
  }

  @Override
  public void onBlockPersist(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final BlockHeader blockHeader) {
    final long block = blockHeader.getNumber();
    if (storage == captureStorage
        && isFinal.test(block)
        && isSingleBlockPersist(blockHeader)
        && extendsCoverage(blockHeader)) {
      trieNodeWriter.beginBlock(transaction, blockHeader);
      capturedBlocks.put(transaction, priorNodes());
    }
  }

  private boolean isSingleBlockPersist(final BlockHeader blockHeader) {
    final Optional<Bytes> persisted = persistedBlockHash();
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
    if (block > last.number()) {
      logSkippedBlock(blockHeader, last);
    }
    return false;
  }

  private void logSkippedBlock(final BlockHeader blockHeader, final ArchivedBlock last) {
    LogUtil.throttledLog(
        () ->
            LOG.warn(
                "Not capturing trie nodes for block {} ({}): "
                    + "it does not extend archived block {} ({})",
                blockHeader.getNumber(),
                blockHeader.getBlockHash(),
                last.number(),
                last.hash()),
        logMissingCoverage,
        LOG_MISSING_COVERAGE_INTERVAL_SECONDS);
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
    final Map<Bytes, Bytes> priorNodes = capturedBlocks.getIfPresent(transaction);
    if (priorNodes == null) {
      return;
    }
    // A node whose prior wasn't loaded is written FULL.
    final Bytes prior = priorNodes.get(key);
    // Skip Bonsai's unchanged re-commits.
    if (node.equals(prior)) {
      return;
    }
    capture(key, node, prior, transaction);
  }

  @Override
  public void removeTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes key) {
    base.removeTrieNode(storage, transaction, key);
    final Map<Bytes, Bytes> priorNodes = capturedBlocks.getIfPresent(transaction);
    if (priorNodes == null) {
      return;
    }
    final Bytes prior = priorNode(storage, key, priorNodes);
    // Nothing to archive if the node didn't exist.
    if (prior != null) {
      capture(key, null, prior, transaction);
    }
  }

  // Falls back to storage to tell whether the node existed.
  private Bytes priorNode(
      final SegmentedKeyValueStorage storage, final Bytes key, final Map<Bytes, Bytes> priorNodes) {
    final Bytes loaded = priorNodes.get(key);
    return loaded != null ? loaded : base.getTrieNode(storage, key, Bytes32.ZERO).orElse(null);
  }

  private void capture(
      final Bytes key,
      final Bytes node,
      final Bytes prior,
      final SegmentedKeyValueStorageTransaction transaction) {
    trieNodeWriter.capture(
        ArchiveNodeKey.of(key), TrieNodeKey.location(key), node, prior, transaction);
  }

  @Override
  public void onBeforeCommit(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction) {
    if (storage == captureStorage) {
      // the next block's tries load again, so don't hold this block's nodes past its commit
      loadedNodes.set(NO_LOADED_NODES);
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
