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
package org.hyperledger.besu.ethereum.eth.sync.common;

import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.consensus.merge.ForkchoiceEvent;
import org.hyperledger.besu.consensus.merge.NewPayloadListener;
import org.hyperledger.besu.consensus.merge.UnverifiedForkchoiceListener;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.sync.PivotBlockSelector;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Selects the pivot block for snap sync using FCU head and safe block headers.
 *
 * <p>On every {@code engine_forkchoiceUpdated} the latest head and safe block hashes are recorded.
 * {@link #selectNewPivotBlock()} downloads missing headers on demand and picks a pivot:
 *
 * <ol>
 *   <li>the safe block, if it is within the effective freshness threshold of the head;
 *   <li>otherwise the head block.
 * </ol>
 *
 * <p>The effective freshness threshold shrinks by one per estimated missed slot since the last FCU,
 * because the real chain tip has advanced by that many blocks. When the threshold reaches zero the
 * method fails with an exception so the caller knows the consensus client appears offline.
 */
public class PivotSelectorFromSafeBlock
    implements PivotBlockSelector, NewPayloadListener, UnverifiedForkchoiceListener {

  private static final Logger LOG = LoggerFactory.getLogger(PivotSelectorFromSafeBlock.class);
  private static final long DIAGNOSTIC_LOG_RATE_LIMIT = Duration.ofMinutes(1).toMillis();
  private static final Duration DEFAULT_SLOT_DURATION = Duration.ofSeconds(12);

  /**
   * Maximum distance (in blocks) the pivot can lag behind the chain head before we select a new
   * one. Set to 120 = 128 (snap-serving window) − 8 (≈ 1.5 min buffer at 12 s/slot), so the pivot
   * is replaced while it still has ~1.5 minutes left in the snap-serving window. Requires the
   * pivot-check interval to be ≤ 1.5 minutes.
   */
  public static final int SAFE_BLOCK_FRESHNESS_THRESHOLD = 120;

  /** Number of blocks behind the FCU head to use as pivot anchor, for reorg safety. */
  private static final int REORG_SAFETY_DISTANCE = 32;

  private final ProtocolContext protocolContext;
  private final GenesisConfigOptions genesisConfig;
  private final SingleBlockHeaderDownloader headerDownloader;
  private final ProtocolSchedule protocolSchedule;
  private final Clock clock;
  private final Runnable cleanupAction;

  private volatile Hash latestHeadHash = Hash.ZERO;
  private volatile Hash latestSafeHash = Hash.ZERO;
  private volatile Hash latestFinalizedHash = Hash.ZERO;
  private volatile long lastFcuTimeMillis = 0;
  private final Map<Hash, BlockHeader> headHeaders = new ConcurrentHashMap<>();
  private volatile long lastNoFcuInfoLog;
  private volatile BlockHeader lastReturnedPivot = null;

  /**
   * Construct a pivot selector. The caller is responsible for registering this selector as both a
   * {@code NewPayloadListener} and an {@code UnverifiedForkchoiceListener} on the merge context,
   * and for unsubscribing both via {@code cleanupAction}.
   */
  public PivotSelectorFromSafeBlock(
      final ProtocolContext protocolContext,
      final GenesisConfigOptions genesisConfig,
      final SingleBlockHeaderDownloader headerDownloader,
      final ProtocolSchedule protocolSchedule,
      final Clock clock,
      final Runnable cleanupAction) {
    this.protocolContext = protocolContext;
    this.genesisConfig = genesisConfig;
    this.headerDownloader = headerDownloader;
    this.protocolSchedule = protocolSchedule;
    this.clock = clock;
    this.cleanupAction = cleanupAction;
    this.lastNoFcuInfoLog = clock.millis();
  }

  @Override
  public void onNewPayload(final BlockHeader header) {
    headHeaders.put(header.getHash(), header);
  }

  @Override
  public void onNewUnverifiedForkchoice(final ForkchoiceEvent event) {
    lastFcuTimeMillis = clock.millis();
    latestHeadHash = event.getHeadBlockHash();
    latestSafeHash = event.hasValidSafeBlockHash() ? event.getSafeBlockHash() : Hash.ZERO;

    if (event.hasValidFinalizedBlockHash()) {
      final Hash newFinalizedHash = event.getFinalizedBlockHash();
      if (!newFinalizedHash.equals(latestFinalizedHash)) {
        latestFinalizedHash = newFinalizedHash;
        pruneHeadersBelowFinalized(newFinalizedHash);
      }
    }
  }

  private void pruneHeadersBelowFinalized(final Hash finalizedHash) {
    final BlockHeader finalizedHeader = headHeaders.get(finalizedHash);
    if (finalizedHeader == null) {
      return;
    }
    final long finalizedNumber = finalizedHeader.getNumber();
    headHeaders.values().removeIf(h -> h.getNumber() < finalizedNumber);
  }

  private CompletableFuture<BlockHeader> walkBackParents(
      final BlockHeader header, final int steps) {
    if (steps == 0) {
      return CompletableFuture.completedFuture(header);
    }
    return getOrDownload(header.getParentHash())
        .thenCompose(parent -> walkBackParents(parent, steps - 1));
  }

  private CompletableFuture<BlockHeader> getOrDownload(final Hash hash) {
    final BlockHeader cached = headHeaders.get(hash);
    if (cached != null) {
      return CompletableFuture.completedFuture(cached);
    }
    return headerDownloader
        .downloadBlockHeader(hash)
        .thenApply(
            h -> {
              headHeaders.put(hash, h);
              return h;
            });
  }

  @Override
  public CompletableFuture<PivotSyncState> selectNewPivotBlock() {
    final Hash headHash = latestHeadHash;
    if (Hash.ZERO.equals(headHash)) {
      return logAndFailNoFcu();
    }

    final BlockHeader cachedHead = headHeaders.get(headHash);
    final BlockHeader headerForSlotDuration =
        Optional.ofNullable(headHeaders.get(latestSafeHash))
            .or(() -> Optional.ofNullable(cachedHead))
            .orElse(null);
    final Duration slotDuration =
        headerForSlotDuration != null
            ? protocolSchedule.getByBlockHeader(headerForSlotDuration).getSlotDuration()
            : DEFAULT_SLOT_DURATION;
    final long nowMillis = clock.millis();
    final long millisSinceLastFcu = lastFcuTimeMillis > 0 ? nowMillis - lastFcuTimeMillis : 0;
    final long estimatedMissedBlocks = millisSinceLastFcu / slotDuration.toMillis();
    final long effectiveThreshold = SAFE_BLOCK_FRESHNESS_THRESHOLD - estimatedMissedBlocks;

    if (effectiveThreshold <= 0) {
      return CompletableFuture.failedFuture(
          new RuntimeException(
              "Consensus client appears offline: last FCU was "
                  + (millisSinceLastFcu / 1000)
                  + "s ago; pivot block would be outside the snap-serving window"));
    }

    final BlockHeader currentPivot = lastReturnedPivot;
    if (currentPivot != null && cachedHead != null) {
      final long distanceFromHead = cachedHead.getNumber() - currentPivot.getNumber();
      if (distanceFromHead < effectiveThreshold) {
        LOG.debug(
            "Reusing existing pivot block {} — head has only advanced {} blocks (threshold {})",
            currentPivot.getNumber(),
            distanceFromHead,
            effectiveThreshold);
        return CompletableFuture.completedFuture(new PivotSyncState(currentPivot, true));
      }
    }

    final CompletableFuture<BlockHeader> headFuture = getOrDownload(headHash);

    final Hash safeHash = latestSafeHash;
    final CompletableFuture<Optional<BlockHeader>> safeFuture =
        Hash.ZERO.equals(safeHash)
            ? CompletableFuture.completedFuture(Optional.empty())
            : getOrDownload(safeHash)
                .thenApply(Optional::of)
                .exceptionally(ignored -> Optional.empty());

    return headFuture
        .thenCombine(
            safeFuture,
            (head, maybeSafe) -> {
              if (maybeSafe.isPresent()) {
                final BlockHeader safe = maybeSafe.get();
                if (head.getNumber() - safe.getNumber() < effectiveThreshold) {
                  LOG.debug("Returning safe block {} as pivot", safe.getNumber());
                  return CompletableFuture.completedFuture(new PivotSyncState(safe, true));
                }
              }
              int blocksToWalk = (int) Math.min(REORG_SAFETY_DISTANCE, head.getNumber());
              LOG.debug(
                  "Safe block not available or too far from head, walking back {} blocks from {} for reorg safety",
                  blocksToWalk,
                  head.getNumber());
              return walkBackParents(head, blocksToWalk)
                  .thenApply(anchored -> new PivotSyncState(anchored, true));
            })
        .thenCompose(cf -> cf)
        .thenApply(
            state -> {
              state.getPivotBlockHeader().ifPresent(h -> lastReturnedPivot = h);
              return state;
            });
  }

  private CompletableFuture<PivotSyncState> logAndFailNoFcu() {
    final long now = clock.millis();
    if (lastNoFcuInfoLog + DIAGNOSTIC_LOG_RATE_LIMIT < now) {
      lastNoFcuInfoLog = now;
      LOG.info(
          "Waiting for consensus client, this may be because your consensus client is still syncing");
    }
    LOG.debug("No forkchoice update received yet");
    return CompletableFuture.failedFuture(
        new RuntimeException("No forkchoice update received yet"));
  }

  @Override
  public CompletableFuture<Void> prepareRetry() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public void close() {
    cleanupAction.run();
  }

  @Override
  public long getMinRequiredBlockNumber() {
    return genesisConfig.getTerminalBlockNumber().orElse(0L);
  }

  @Override
  public long getBestChainHeight() {
    final long localChainHeight = protocolContext.getBlockchain().getChainHeadBlockNumber();
    final BlockHeader headHeader = headHeaders.get(latestHeadHash);
    final long cachedHeadNumber = headHeader != null ? headHeader.getNumber() : 0L;
    return Math.max(cachedHeadNumber, localChainHeight);
  }
}
