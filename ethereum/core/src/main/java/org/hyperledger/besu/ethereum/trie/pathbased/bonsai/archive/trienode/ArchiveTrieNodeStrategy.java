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

import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveCoverageTracker.Coverage;
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
import java.util.function.Predicate;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
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

  private final TrieNodeStrategy base;
  private final ArchiveTrieNodeWriter trieNodeWriter;
  private final ArchiveCoverageTracker coverage;
  private final Predicate<SegmentedKeyValueStorage> isCaptureStorage;
  private final LongPredicate isFinal;
  private final AtomicBoolean reorgLogged = new AtomicBoolean(false);
  private static final int SKIP_LOG_INTERVAL_SECONDS = 60;

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

  private record CapturedBlock(long number, Map<Bytes, Bytes> priorNodes) {}

  // Nodes loaded from the capture storage since its last commit, by trie-node key. They are what
  // storage holds until that commit, so a captured block uses them as the nodes it replaces.
  private final AtomicReference<Map<Bytes, Bytes>> loadedNodes =
      new AtomicReference<>(new ConcurrentHashMap<>());

  // Transactions persisting a captured block, mapped to that block.
  private final Cache<SegmentedKeyValueStorageTransaction, CapturedBlock> capturedBlocks =
      Caffeine.newBuilder().weakKeys().build();

  public ArchiveTrieNodeStrategy(
      final TrieNodeStrategy base,
      final ArchiveTrieNodeWriter trieNodeWriter,
      final ArchiveCoverageTracker coverage,
      final Predicate<SegmentedKeyValueStorage> isCaptureStorage,
      final LongPredicate isFinal) {
    this.base = Objects.requireNonNull(base);
    this.trieNodeWriter = Objects.requireNonNull(trieNodeWriter);
    this.coverage = Objects.requireNonNull(coverage);
    this.isCaptureStorage = Objects.requireNonNull(isCaptureStorage);
    this.isFinal = Objects.requireNonNull(isFinal);
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
    if (!isCaptureStorage.test(storage)) {
      return;
    }
    final long block = blockHeader.getNumber();
    final Optional<CaptureSkip> skip = captureSkip(storage, blockHeader);
    if (skip.isPresent()) {
      logSkipped(skip.get());
      return;
    }
    trieNodeWriter.beginBlock(transaction, blockHeader);
    capturedBlocks.put(transaction, new CapturedBlock(block, loadedNodes.get()));
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
              coverage.getCoverage().map(c -> String.valueOf(c.lastBlock())).orElse("none");
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
    final Optional<Coverage> current = coverage.getCoverage();
    if (current.isEmpty()) {
      return blockHeader.getNumber() == 0L;
    }
    final Coverage lastArchived = current.get();
    if (blockHeader.getNumber() != lastArchived.lastBlock() + 1) {
      return false;
    }
    final boolean buildsOnLastArchived =
        lastArchived.lastBlockHash().equals(blockHeader.getParentHash());
    if (!buildsOnLastArchived && reorgLogged.compareAndSet(false, true)) {
      LOG.error(
          "Trie-node capture stopped at block {}: the chain reorged past archived history. "
              + "Proof history needs a resync.",
          lastArchived.lastBlock());
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
