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
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Selects the pivot block for snap sync using the FCU safe and head header.
 *
 * <p>The pivot is reused across calls until the head has advanced at least {@code
 * pivotBlockWindowValidity} blocks past it, ensuring the pivot stays within the 128-block
 * snap-serving window. The effective threshold shrinks by one per estimated missed slot since the
 * last FCU; when it reaches zero the method fails so the caller knows the consensus client appears
 * offline.
 */
public class PivotSelectorFromSafeBlock
    implements PivotBlockSelector, NewPayloadListener, UnverifiedForkchoiceListener {

  private static final Logger LOG = LoggerFactory.getLogger(PivotSelectorFromSafeBlock.class);
  private static final long DIAGNOSTIC_LOG_RATE_LIMIT = Duration.ofMinutes(1).toMillis();

  /**
   * Number of blocks behind the FCU head to anchor the pivot if no safe block is available. Chosen
   * to match the typical distance of the safe block (≈ 2 epochs = 64 slots) to provide reorg
   * protection.
   */
  private static final int PIVOT_DISTANCE = 64;

  private final ProtocolContext protocolContext;
  private final GenesisConfigOptions genesisConfig;
  private final SingleBlockHeaderDownloader headerDownloader;
  private final ProtocolSchedule protocolSchedule;
  private final Clock clock;
  private final int pivotBlockWindowValidity;
  private final Runnable cleanupAction;

  private volatile Hash latestHeadHash = Hash.ZERO;
  private volatile Hash latestSafeHash = Hash.ZERO;
  private volatile Hash latestFinalizedHash = Hash.ZERO;
  private volatile long lastFcuTimeMillis = 0;
  private final Cache<Hash, BlockHeader> headHeaders =
      Caffeine.newBuilder().maximumSize(1000).build();
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
      final int pivotBlockWindowValidity,
      final Runnable cleanupAction) {
    this.protocolContext = protocolContext;
    this.genesisConfig = genesisConfig;
    this.headerDownloader = headerDownloader;
    this.protocolSchedule = protocolSchedule;
    this.clock = clock;
    this.pivotBlockWindowValidity = pivotBlockWindowValidity;
    this.cleanupAction = cleanupAction;
    this.lastNoFcuInfoLog = clock.millis();
  }

  @Override
  public void onNewPayload(final BlockHeader header) {
    LOG.debug("Received new payload header {}, hash {}", header.getNumber(), header.getHash());
    headHeaders.put(header.getHash(), header);
  }

  @Override
  public void onNewUnverifiedForkchoice(final ForkchoiceEvent event) {
    LOG.debug("Received new FCU {}", event);
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
    final BlockHeader finalizedHeader = headHeaders.getIfPresent(finalizedHash);
    if (finalizedHeader == null) {
      return;
    }
    final long finalizedNumber = finalizedHeader.getNumber();
    headHeaders.asMap().values().removeIf(h -> h.getNumber() < finalizedNumber);
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
    final BlockHeader cached = headHeaders.getIfPresent(hash);
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
  public CompletableFuture<SnapSyncProcessState> selectNewPivotBlock() {
    final Hash headHash = latestHeadHash;
    if (Hash.ZERO.equals(headHash)) {
      return logAndFailNoFcu();
    }

    final long nowMillis = clock.millis();
    final long millisSinceLastFcu = lastFcuTimeMillis > 0 ? nowMillis - lastFcuTimeMillis : 0;

    return getOrDownload(headHash)
        .thenCompose(
            head -> {
              LOG.debug("Head block {} is at {}", head.getNumber(), head.getHash());
              final Duration slotDuration =
                  protocolSchedule.getByBlockHeader(head).getSlotDuration();
              final long estimatedMissedBlocks = millisSinceLastFcu / slotDuration.toMillis();
              final long effectiveThreshold = pivotBlockWindowValidity - estimatedMissedBlocks;

              if (effectiveThreshold <= 0) {
                return CompletableFuture.failedFuture(
                    new RuntimeException(
                        "Consensus client appears offline: last FCU was "
                            + (millisSinceLastFcu / 1000)
                            + "s ago; pivot block would be outside the snap-serving window"));
              }

              final BlockHeader currentPivot = lastReturnedPivot;
              if (currentPivot != null) {
                final long distanceFromHead = head.getNumber() - currentPivot.getNumber();
                if (distanceFromHead < effectiveThreshold) {
                  LOG.debug(
                      "Reusing existing pivot block {} — head has only advanced {} blocks (threshold {})",
                      currentPivot.getNumber(),
                      distanceFromHead,
                      effectiveThreshold);
                  return CompletableFuture.completedFuture(new SnapSyncProcessState(currentPivot));
                }
              }

              final BlockHeader cachedSafe = headHeaders.getIfPresent(latestSafeHash);
              if (cachedSafe != null
                  && head.getNumber() - cachedSafe.getNumber() < effectiveThreshold) {
                LOG.debug("Using safe block {} as pivot", cachedSafe.getNumber());
                return CompletableFuture.completedFuture(new SnapSyncProcessState(cachedSafe));
              }

              final int blocksToWalk = (int) Math.min(PIVOT_DISTANCE, head.getNumber());
              LOG.debug(
                  "Walking back {} blocks from head {} for pivot", blocksToWalk, head.getNumber());
              return walkBackParents(head, blocksToWalk)
                  .thenApply(newPivot -> new SnapSyncProcessState(newPivot));
            })
        .thenApply(
            state -> {
              state.getPivotBlockHeader().ifPresent(h -> lastReturnedPivot = h);
              return state;
            });
  }

  private CompletableFuture<SnapSyncProcessState> logAndFailNoFcu() {
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
    final BlockHeader headHeader = headHeaders.getIfPresent(latestHeadHash);
    final long cachedHeadNumber = headHeader != null ? headHeader.getNumber() : 0L;
    return Math.max(cachedHeadNumber, localChainHeight);
  }
}
