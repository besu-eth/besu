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
  private static final int SKIP_LOG_INTERVAL_SECONDS = 60;
  // Well above a large block's loads; a load past it is only written FULL.
  static final int MAX_LOADED_NODES = 1_000_000;

  private final TrieNodeStrategy base;
  private final ArchiveTrieNodeWriter trieNodeWriter;
  private final ArchiveNodeHistoryStore historyStore;
  private final SegmentedKeyValueStorage captureStorage;
  private final LongPredicate isFinal;
  private final int maxLoadedNodes;

  private enum CaptureSkip {
    WORLD_STATE_NOT_AT_PARENT("the world state is not at the block's parent"),
    NOT_FINAL("the block is not final"),
    DOES_NOT_EXTEND_COVERAGE("the block does not extend the archived range");

    private final String reason;

    CaptureSkip(final String reason) {
      this.reason = reason;
    }
  }

  private final Map<CaptureSkip, AtomicBoolean> skipLogThrottles = new ConcurrentHashMap<>();

  // Nodes read from one state, by key. They're the same whichever storage they were read from.
  private record LoadedNodes(Hash root, Map<Bytes, Bytes> nodes) {
    static LoadedNodes none() {
      return new LoadedNodes(Hash.ZERO, new ConcurrentHashMap<>());
    }

    LoadedNodes at(final Hash stateRoot) {
      return root.equals(stateRoot) ? this : new LoadedNodes(stateRoot, new ConcurrentHashMap<>());
    }
  }

  private final AtomicReference<LoadedNodes> loadedNodes =
      new AtomicReference<>(LoadedNodes.none());

  // Transactions persisting a captured block, mapped to the nodes that block replaces.
  private final Cache<SegmentedKeyValueStorageTransaction, Map<Bytes, Bytes>> capturedBlocks =
      Caffeine.newBuilder().weakKeys().build();

  /**
   * @param base the delegate strategy for the live trie
   * @param trieNodeWriter the writer that persists archived history entries
   * @param historyStore the archive store whose recorded progress captured blocks must extend
   * @param captureStorage the storage whose block persists are archived
   * @param isFinal whether a block number is final, and so safe to capture inline
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
    // Only nodes from the current head are useful, so skip others. Check memory before disk.
    if (!(loadedNodes.get().root().equals(stateRoot) || isPersistedRoot(stateRoot))) {
      return loader;
    }
    final Map<Bytes, Bytes> loaded = loadedNodes.updateAndGet(c -> c.at(stateRoot)).nodes();
    return (location, hash) -> {
      final Optional<Bytes> node = loader.getNode(location, hash);
      // the empty trie node is never stored, so it is never a prior
      if (!hash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH) && loaded.size() < maxLoadedNodes) {
        node.ifPresent(n -> loaded.put(keyOf.apply(location), n));
      }
      return node;
    };
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
    return isPersistedRoot(loaded.root()) ? loaded.nodes() : Map.of();
  }

  @Override
  public void onBlockPersist(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final BlockHeader blockHeader) {
    if (storage != captureStorage) {
      return;
    }
    final Optional<CaptureSkip> skip = captureSkip(storage, blockHeader);
    if (skip.isPresent()) {
      logSkipped(skip.get());
      return;
    }
    trieNodeWriter.beginBlock(transaction, blockHeader);
    capturedBlocks.put(transaction, priorNodes());
  }

  private Optional<CaptureSkip> captureSkip(
      final SegmentedKeyValueStorage storage, final BlockHeader blockHeader) {
    if (!isSingleBlockPersist(storage, blockHeader)) {
      return Optional.of(CaptureSkip.WORLD_STATE_NOT_AT_PARENT);
    }
    if (!isFinal.test(blockHeader.getNumber())) {
      return Optional.of(CaptureSkip.NOT_FINAL);
    }
    if (!extendsCoverage(blockHeader)) {
      return Optional.of(CaptureSkip.DOES_NOT_EXTEND_COVERAGE);
    }
    return Optional.empty();
  }

  // Capture is skipped without failing the block, and the trailing archiver then covers it, so say
  // why. A block that does not extend the archived range also stops inline capture for every block
  // after it.
  private void logSkipped(final CaptureSkip skip) {
    LogUtil.throttledLog(
        () -> {
          final String archivedTo =
              historyStore.getArchivedBlock().map(b -> String.valueOf(b.number())).orElse("none");
          if (skip == CaptureSkip.NOT_FINAL) {
            LOG.debug(
                "Not capturing trie nodes: {} (archived to block {})", skip.reason, archivedTo);
          } else {
            LOG.info(
                "Not capturing trie nodes: {} (archived to block {})", skip.reason, archivedTo);
          }
        },
        skipLogThrottles.computeIfAbsent(skip, s -> new AtomicBoolean(true)),
        SKIP_LOG_INTERVAL_SECONDS);
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
    return block == last.number() + 1 && last.hash().equals(blockHeader.getParentHash());
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
      loadedNodes.set(LoadedNodes.none());
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
