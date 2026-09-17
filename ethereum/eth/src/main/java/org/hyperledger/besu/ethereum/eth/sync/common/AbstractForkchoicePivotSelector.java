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
 * Shared plumbing for pivot selectors that anchor on forkchoice (FCU) reported heads: merge-context
 * listener handling, head header cache, parent walk-back, CL-offline window math, and last-pivot
 * bookkeeping. Subclasses implement the selection policy in {@link #selectNewPivotBlock()}.
 *
 * <p>The caller must register the selector as a {@code NewPayloadListener} and {@code
 * UnverifiedForkchoiceListener} on the merge context, and unsubscribe both via {@code
 * cleanupAction}.
 */
public abstract class AbstractForkchoicePivotSelector
    implements PivotBlockSelector, NewPayloadListener, UnverifiedForkchoiceListener {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractForkchoicePivotSelector.class);
  private static final long DIAGNOSTIC_LOG_RATE_LIMIT = Duration.ofMinutes(1).toMillis();

  protected final ProtocolContext protocolContext;
  protected final GenesisConfigOptions genesisConfig;
  protected final SingleBlockHeaderDownloader headerDownloader;
  protected final ProtocolSchedule protocolSchedule;
  protected final Clock clock;
  protected final int pivotBlockWindowValidity;
  protected final Runnable cleanupAction;

  private volatile Hash latestHeadHash = Hash.ZERO;
  private volatile Hash latestSafeHash = Hash.ZERO;
  private volatile Hash latestFinalizedHash = Hash.ZERO;
  private volatile long lastFcuTimeMillis = 0;
  private volatile long lastNoFcuInfoLog;
  private final Cache<Hash, BlockHeader> headHeaders =
      Caffeine.newBuilder().maximumSize(1000).build();
  private volatile BlockHeader lastReturnedPivotHeader = null;
  private volatile long lastReturnedPivotNumber = -1;

  protected AbstractForkchoicePivotSelector(
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
  public final void onNewPayload(final BlockHeader header) {
    LOG.debug("Received new payload header {}, hash {}", header.getNumber(), header.getHash());
    headHeaders.put(header.getHash(), header);
  }

  @Override
  public final void onNewUnverifiedForkchoice(final ForkchoiceEvent event) {
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

  protected final Hash getLatestHeadHash() {
    return latestHeadHash;
  }

  protected final Hash getLatestSafeHash() {
    return latestSafeHash;
  }

  protected final Hash getLatestFinalizedHash() {
    return latestFinalizedHash;
  }

  protected final long millisSinceLastFcu() {
    return lastFcuTimeMillis > 0 ? clock.millis() - lastFcuTimeMillis : 0;
  }

  protected final BlockHeader getCachedHeader(final Hash hash) {
    return headHeaders.getIfPresent(hash);
  }

  protected final CompletableFuture<BlockHeader> getOrDownload(final Hash hash) {
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

  protected final CompletableFuture<BlockHeader> walkBackParents(
      final BlockHeader header, final int steps) {
    if (steps == 0) {
      return CompletableFuture.completedFuture(header);
    }
    return getOrDownload(header.getParentHash())
        .thenCompose(parent -> walkBackParents(parent, steps - 1));
  }

  /** Pivot-window blocks remaining before the CL is considered offline; {@code <= 0} if offline. */
  protected final long remainingOfflineWindowBlocks(
      final BlockHeader head, final long millisSinceLastFcu) {
    final Duration slotDuration = protocolSchedule.getByBlockHeader(head).getSlotDuration();
    final long estimatedMissedBlocks = millisSinceLastFcu / slotDuration.toMillis();
    return pivotBlockWindowValidity - estimatedMissedBlocks;
  }

  protected final boolean hasLastPivot() {
    return lastReturnedPivotNumber > 0;
  }

  protected final long getLastReturnedPivotNumber() {
    return lastReturnedPivotNumber;
  }

  protected final SnapSyncProcessState recordLastPivot(final SnapSyncProcessState state) {
    state.getPivotBlockNumber().ifPresent(n -> lastReturnedPivotNumber = n);
    lastReturnedPivotHeader = state.getPivotBlockHeader().orElse(null);
    return state;
  }

  protected final SnapSyncProcessState lastPivotState() {
    return lastReturnedPivotHeader != null
        ? new SnapSyncProcessState(lastReturnedPivotHeader)
        : new SnapSyncProcessState(lastReturnedPivotNumber);
  }

  protected final CompletableFuture<SnapSyncProcessState> logAndFailNoFcu() {
    final long now = clock.millis();
    if (lastNoFcuInfoLog + DIAGNOSTIC_LOG_RATE_LIMIT < now) {
      lastNoFcuInfoLog = now;
      LOG.info(
          "Waiting for consensus client, this may be because your consensus client is still"
              + " syncing");
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
