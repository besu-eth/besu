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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateLayerStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background component that keeps the trie-node archive advancing after initial sync.
 *
 * <p>Trails a finalized frontier ({@code finalized} block, or {@code head − maxLayersToLoad} for
 * chains without finality). Per catch-up: takes a mutable layer over canonical head, rolls it back
 * to the cursor, then rolls forward one block at a time. Each block's {@code persist()} recomputes
 * the trie and emits {@code putFlat*TrieNode} writes that the archive capture strategy records via
 * a canonical archive transaction. Base (layer) writes stay in the discarded layer, never reaching
 * canonical. Because the frontier is always ≤ the finality/retention horizon, the capture path
 * never observes a reorg.
 */
public class ArchiveTrieNodeFrontierRoller implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ArchiveTrieNodeFrontierRoller.class);

  static final byte[] ARCHIVE_TRIE_NODE_SYNC_PROGRESS =
      "ARCHIVE_TRIE_NODE_SYNC_PROGRESS".getBytes(StandardCharsets.UTF_8);

  private final BonsaiWorldStateKeyValueStorage worldStateStorage;
  private final BonsaiWorldStateProvider worldStateProvider;
  private final TrieLogManager trieLogManager;
  private final Blockchain blockchain;
  private final ScheduledExecutorService executorService;
  private final ExecutorService trieCapturePool;
  private final int shallowCheckpointInterval;
  private final int deepCheckpointInterval;

  final AtomicLong cursor = new AtomicLong(0L);
  final AtomicLong ongoingTarget = new AtomicLong(0L);
  final AtomicBoolean catchUpRunning = new AtomicBoolean(false);
  volatile OptionalLong blockObserverId = OptionalLong.empty();
  private volatile boolean closed = false;

  // Stateful rolling context — kept alive between catch-up runs to avoid re-positioning from HEAD.
  // Null means not yet initialised or invalidated after an error. Always accessed from the single
  // catch-up executor thread.
  private BonsaiWorldStateLayerStorage rollerLayer;
  private BonsaiWorldState rollerState;
  private ArchiveTrieNodeStrategy rollerStrategy;
  // Package-private so unit tests can assert on positioning without going through the full stack.
  long rollerStatePosition = -1L;

  public ArchiveTrieNodeFrontierRoller(
      final BonsaiWorldStateKeyValueStorage worldStateStorage,
      final BonsaiWorldStateProvider worldStateProvider,
      final TrieLogManager trieLogManager,
      final Blockchain blockchain,
      final ScheduledExecutorService executorService,
      final ExecutorService trieCapturePool,
      final int shallowCheckpointInterval,
      final int deepCheckpointInterval) {
    this.worldStateStorage = worldStateStorage;
    this.worldStateProvider = worldStateProvider;
    this.trieLogManager = trieLogManager;
    this.blockchain = blockchain;
    this.executorService = executorService;
    this.trieCapturePool = trieCapturePool;
    this.shallowCheckpointInterval = shallowCheckpointInterval;
    this.deepCheckpointInterval = deepCheckpointInterval;
  }

  /**
   * Seeds the cursor from the Phase-A handoff, registers a block observer, and schedules an initial
   * catch-up.
   */
  public void startOngoing(final long initialCursor) {
    if (closed) {
      return;
    }
    final long persisted = loadPersistedCursor().orElse(0L);
    cursor.set(Math.max(initialCursor, persisted));
    ongoingTarget.set(
        FrontierTargetCalculator.computeFrontierTarget(
            blockchain, trieLogManager.getMaxLayersToLoad()));
    LOG.info(
        "Archive frontier roller starting: cursor={} target={}", cursor.get(), ongoingTarget.get());

    blockObserverId =
        OptionalLong.of(
            blockchain.observeBlockAdded(
                event -> {
                  if (event.isNewCanonicalHead()) {
                    ongoingTarget.accumulateAndGet(
                        FrontierTargetCalculator.computeFrontierTarget(
                            blockchain, trieLogManager.getMaxLayersToLoad()),
                        Math::max);
                    scheduleCatchUpIfNeeded();
                  }
                }));
    scheduleCatchUpIfNeeded();
  }

  private void scheduleCatchUpIfNeeded() {
    if (closed || !catchUpRunning.compareAndSet(false, true)) {
      return;
    }
    try {
      executorService.submit(this::catchUp);
    } catch (final RejectedExecutionException e) {
      catchUpRunning.set(false);
      LOG.debug("Frontier roller executor shut down; skipping catch-up to {}", ongoingTarget.get());
    }
  }

  private void catchUp() {
    final long from = cursor.get();
    final long target = ongoingTarget.get();
    try {
      captureRange(from, target);
    } catch (final Exception e) {
      LOG.error(
          "Archive frontier roller: error capturing range ({}, {}]; cursor stays at {}",
          from,
          target,
          cursor.get(),
          e);
    } finally {
      catchUpRunning.set(false);
      final long reached = cursor.get();
      if (reached >= target) {
        LOG.info("Archive frontier roller: fully caught up at block {}", reached);
      } else if (reached > from) {
        LOG.debug("Archive frontier roller: advanced to {} (target {})", reached, target);
      }
      if (!closed && reached < ongoingTarget.get()) {
        scheduleCatchUpIfNeeded();
      }
    }
  }

  /**
   * Captures archive-history entries for all blocks in {@code (fromExclusive, toInclusive]}.
   *
   * <p>On the first call (or after an error), creates a fresh layer over canonical head, rolls it
   * back to {@code fromExclusive} via {@link #positionAtCursor}, then rolls forward. On subsequent
   * calls the layer and state are kept alive between runs — no re-positioning needed — so the cost
   * is proportional to the number of blocks captured rather than the size of the trie-log window.
   *
   * <p>On exception the stateful context is torn down so the next call starts clean.
   */
  @VisibleForTesting
  void captureRange(final long fromExclusive, final long toInclusive) {
    if (toInclusive <= fromExclusive) {
      return;
    }
    boolean ok = false;
    try {
      ensureRollerState(fromExclusive);

      // Re-read cursor: ensureRollerState may have advanced it past fromExclusive when the
      // retention window was exceeded. The loop start reflects the actual position of rollerState.
      final long effectiveFrom = cursor.get();
      if (toInclusive <= effectiveFrom) {
        ok = true;
        return;
      }

      final ArchiveTrieNodeWriter writer = rollerStrategy.getTrieNodeWriter();

      for (long b = effectiveFrom + 1; b <= toInclusive; b++) {
        final BlockHeader header = blockchain.getBlockHeader(b).orElseThrow();
        final TrieLog log = trieLogManager.getTrieLogLayer(header.getHash()).orElseThrow();

        final SegmentedKeyValueStorageTransaction canonicalTx =
            worldStateStorage.getComposedWorldStateStorage().startLowPriorityTransaction();
        writer.setArchiveWriteTransaction(canonicalTx);

        final PathBasedWorldStateUpdateAccumulator<?> acc =
            (PathBasedWorldStateUpdateAccumulator<?>) rollerState.getAccumulator();
        acc.rollForward(log);
        acc.commit();
        rollerState.persist(header); // recompute → emits node writes → onBeforeCommit → canonicalTx

        canonicalTx.put(
            TRIE_BRANCH_STORAGE_ARCHIVE,
            ARCHIVE_TRIE_NODE_SYNC_PROGRESS,
            Bytes.ofUnsignedLong(b).toArrayUnsafe());
        canonicalTx.commit(); // archive history + coverage + cursor, atomically
        writer.setArchiveWriteTransaction(null);
        cursor.set(b);
        rollerStatePosition = b;
        if (b % 1000 == 0) {
          LOG.debug(
              "Archive frontier roller: block {} / {} ({} remaining)",
              b,
              toInclusive,
              toInclusive - b);
        }
      }
      ok = true;
    } finally {
      if (!ok) {
        tearDownRollerState();
      }
    }
  }

  /**
   * Ensures {@link #rollerLayer}, {@link #rollerState}, and {@link #rollerStrategy} are initialised
   * and positioned at {@code fromExclusive}. Reuses the existing context when the state is already
   * there; otherwise tears down any stale context and re-positions from HEAD.
   */
  private void ensureRollerState(final long fromExclusive) {
    if (rollerState != null && rollerStatePosition == fromExclusive) {
      // State is already positioned at fromExclusive from the previous run — nothing to do.
      return;
    }
    if (rollerState != null) {
      LOG.warn(
          "Archive frontier roller state is at {} but fromExclusive={}; re-positioning",
          rollerStatePosition,
          fromExclusive);
      tearDownRollerState();
    }

    // Guard: if the cursor is older than the trie-log retention window, we cannot roll back
    // far enough to reposition. Advance cursor to the oldest reachable block and position the
    // state there. The current captureRange call will archive nothing (toInclusive == cursor),
    // but the state is ready for the next call to roll forward from.
    final long head = blockchain.getChainHeadBlockNumber();
    final long maxLayers = trieLogManager.getMaxLayersToLoad();
    final long oldestReachable = Math.max(0L, head - maxLayers);
    final long positionTarget;
    if (fromExclusive < oldestReachable) {
      LOG.warn(
          "Archive frontier roller cursor {} is beyond trie-log retention (head={}, maxLayers={})."
              + " Advancing cursor to {} — blocks {}-{} will not be re-archived.",
          fromExclusive,
          head,
          maxLayers,
          oldestReachable,
          fromExclusive + 1,
          oldestReachable);
      cursor.set(oldestReachable);
      positionTarget = oldestReachable;
      // Fall through: position rollerState at oldestReachable so the next catch-up can roll
      // forward from there without re-positioning.
    } else {
      positionTarget = fromExclusive;
    }

    rollerStrategy =
        ArchiveTrieNodeStrategy.createRollerStrategy(
            worldStateStorage.getComposedWorldStateStorage(),
            trieCapturePool,
            shallowCheckpointInterval,
            deepCheckpointInterval);
    rollerLayer = new BonsaiWorldStateLayerStorage(worldStateStorage);
    rollerLayer.setTrieNodeStrategy(rollerStrategy);
    rollerState = worldStateProvider.newTrieEnabledWorldState(rollerLayer);
    positionAtCursor(rollerState, positionTarget);
    rollerStatePosition = positionTarget;
  }

  private void tearDownRollerState() {
    rollerState = null;
    rollerStrategy = null;
    rollerStatePosition = -1L;
    if (rollerLayer != null) {
      try {
        rollerLayer.close();
      } catch (final Exception e) {
        LOG.warn("Failed to close roller layer storage", e);
      }
      rollerLayer = null;
    }
  }

  private void positionAtCursor(final BonsaiWorldState state, final long targetBlock) {
    final long head = blockchain.getChainHeadBlockNumber();
    final long rollbackDepth = head - targetBlock;
    final long maxLayers = trieLogManager.getMaxLayersToLoad();
    if (rollbackDepth > maxLayers) {
      throw new IllegalStateException(
          String.format(
              "Cannot position roller at block %d: rollback depth %d exceeds trie-log retention %d (head=%d). "
                  + "Cursor is too far behind head to reposition via trie-log rollback.",
              targetBlock, rollbackDepth, maxLayers, head));
    }
    final PathBasedWorldStateUpdateAccumulator<?> acc =
        (PathBasedWorldStateUpdateAccumulator<?>) state.getAccumulator();
    for (long b = head; b > targetBlock; b--) {
      final BlockHeader header = blockchain.getBlockHeader(b).orElseThrow();
      final long blockNum = b;
      final TrieLog log =
          trieLogManager
              .getTrieLogLayer(header.getHash())
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "Trie-log missing for block "
                              + blockNum
                              + " while positioning roller at "
                              + targetBlock));
      acc.rollBack(log);
    }
    acc.commit();
    // No archive redirect during positioning (override is null): any node writes go to the
    // discarded layer transaction, not to canonical.
    state.persist(blockchain.getBlockHeader(targetBlock).orElseThrow());
  }

  @VisibleForTesting
  OptionalLong loadPersistedCursor() {
    return worldStateStorage
        .getComposedWorldStateStorage()
        .get(TRIE_BRANCH_STORAGE_ARCHIVE, ARCHIVE_TRIE_NODE_SYNC_PROGRESS)
        .map(b -> OptionalLong.of(Bytes.wrap(b).toLong()))
        .orElse(OptionalLong.empty());
  }

  public long getCursor() {
    return cursor.get();
  }

  @Override
  public void close() {
    closed = true;
    blockObserverId.ifPresent(blockchain::removeObserver);
    blockObserverId = OptionalLong.empty();
    executorService.shutdownNow();
    tearDownRollerState();
  }
}
