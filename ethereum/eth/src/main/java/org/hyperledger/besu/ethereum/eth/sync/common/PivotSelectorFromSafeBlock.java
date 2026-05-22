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
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.sync.PivotBlockSelector;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PivotSelectorFromSafeBlock implements PivotBlockSelector {

  private static final Logger LOG = LoggerFactory.getLogger(PivotSelectorFromSafeBlock.class);
  private static final long DIAGNOSTIC_LOG_RATE_LIMIT = Duration.ofMinutes(1).toMillis();

  private static final long ONE_EPOCH_MILLIS = Duration.ofSeconds(32 * 12).toMillis();

  /** 4 epochs (128 slots × 12 s) minus 1 min safety margin = 1476 s ≈ 123 blocks. */
  private static final long SAFE_PIVOT_FRESHNESS_LIMIT_SECONDS = 4 * 32 * 12 - 60;

  private final ProtocolContext protocolContext;
  private final EthContext ethContext;
  private final GenesisConfigOptions genesisConfig;
  private final Supplier<Optional<ForkchoiceEvent>> forkchoiceStateSupplier;
  private final Runnable cleanupAction;
  private final SingleBlockHeaderDownloader headerDownloader;
  private final Clock clock;

  private volatile long lastNoFcuInfoLog;
  private volatile long lastClStuckWarnLog;
  private volatile Optional<BlockHeader> maybeCachedHeadBlockHeader = Optional.empty();

  private volatile Hash lastSafeBlockHash = Hash.ZERO;
  private volatile BlockHeader lastSafeBlockHeader = null;
  private volatile Hash lastHeadBlockHash = Hash.ZERO;
  private volatile long lastHeadBlockChange;
  private volatile long lastFallbackWarnLog = 0L;

  public PivotSelectorFromSafeBlock(
      final ProtocolContext protocolContext,
      final ProtocolSchedule protocolSchedule,
      final EthContext ethContext,
      final GenesisConfigOptions genesisConfig,
      final Supplier<Optional<ForkchoiceEvent>> forkchoiceStateSupplier,
      final Runnable cleanupAction,
      final SingleBlockHeaderDownloader headerDownloader,
      final Clock clock) {
    this.protocolContext = protocolContext;
    this.ethContext = ethContext;
    this.genesisConfig = genesisConfig;
    this.forkchoiceStateSupplier = forkchoiceStateSupplier;
    this.cleanupAction = cleanupAction;
    this.headerDownloader = headerDownloader;
    this.clock = clock;
    final long now = clock.millis();
    this.lastNoFcuInfoLog = now;
    this.lastClStuckWarnLog = now;
    this.lastHeadBlockChange = now;
  }

  @Override
  public CompletableFuture<PivotSyncState> selectNewPivotBlock() {
    final Optional<ForkchoiceEvent> maybeForkchoice = forkchoiceStateSupplier.get();
    final long now = clock.millis();

    if (maybeForkchoice.isEmpty()) {
      return logAndFailNoFcu(now);
    }

    final ForkchoiceEvent fcu = maybeForkchoice.get();

    final Hash headHash = fcu.getHeadBlockHash();
    if (!headHash.equals(lastHeadBlockHash)) {
      lastHeadBlockHash = headHash;
      lastHeadBlockChange = now;
    }

    if (fcu.hasValidSafeBlockHash()) {
      final Hash safeHash = fcu.getSafeBlockHash();
      if (!safeHash.equals(lastSafeBlockHash)) {
        lastSafeBlockHash = safeHash;
        lastSafeBlockHeader = null;
        lastFallbackWarnLog = 0L;
      }
      if (lastSafeBlockHeader == null || isSafeBlockFresh(now)) {
        return selectSafeBlockAsPivot(safeHash);
      }
    }

    // Safe is stale (or absent). Decide between non-finality and CL stuck.
    if (now - lastHeadBlockChange < ONE_EPOCH_MILLIS) {
      return selectHeadAsFallbackPivot(headHash, now);
    }

    return logAndFailClStuck(now);
  }

  private boolean isSafeBlockFresh(final long nowMillis) {
    final long blockAgeSeconds = nowMillis / 1000 - lastSafeBlockHeader.getTimestamp();
    return blockAgeSeconds < SAFE_PIVOT_FRESHNESS_LIMIT_SECONDS;
  }

  private CompletableFuture<PivotSyncState> selectSafeBlockAsPivot(final Hash safeHash) {
    if (lastSafeBlockHeader != null) {
      return CompletableFuture.completedFuture(new PivotSyncState(lastSafeBlockHeader));
    }
    LOG.debug("Downloading safe block header {} as pivot", safeHash);
    return headerDownloader
        .downloadBlockHeader(safeHash)
        .thenApply(
            header -> {
              lastSafeBlockHeader = header;
              return new PivotSyncState(header);
            });
  }

  private CompletableFuture<PivotSyncState> selectHeadAsFallbackPivot(
      final Hash headHash, final long now) {
    if (now - lastFallbackWarnLog >= ONE_EPOCH_MILLIS) {
      lastFallbackWarnLog = now;
      LOG.warn(
          "Safe block has not changed in over {} min but head is still advancing — using head {} as untrusted pivot.",
          SAFE_PIVOT_FRESHNESS_LIMIT_SECONDS / 60,
          headHash);
    } else {
      LOG.debug("Using head {} as fallback pivot", headHash);
    }
    return headerDownloader.downloadBlockHeader(headHash).thenApply(PivotSyncState::new);
  }

  private CompletableFuture<PivotSyncState> logAndFailNoFcu(final long now) {
    if (lastNoFcuInfoLog + DIAGNOSTIC_LOG_RATE_LIMIT < now) {
      lastNoFcuInfoLog = now;
      LOG.info(
          "Waiting for consensus client, this may be because your consensus client is still syncing");
    }
    LOG.debug("No forkchoice update received yet");
    return CompletableFuture.failedFuture(
        new RuntimeException("No forkchoice update received yet"));
  }

  private CompletableFuture<PivotSyncState> logAndFailClStuck(final long now) {
    if (lastClStuckWarnLog + DIAGNOSTIC_LOG_RATE_LIMIT < now) {
      lastClStuckWarnLog = now;
      LOG.warn(
          "Consensus client appears stuck — head block has not advanced in over {} min and safe block has not advanced in over {} min. Sync will retry.",
          ONE_EPOCH_MILLIS / 60_000,
          SAFE_PIVOT_FRESHNESS_LIMIT_SECONDS / 60);
    }
    return CompletableFuture.failedFuture(
        new RuntimeException("Consensus client appears stuck (no head/safe progress)"));
  }

  @Override
  public CompletableFuture<Void> prepareRetry() {
    // nothing to do
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

    return Math.max(
        forkchoiceStateSupplier
            .get()
            .map(ForkchoiceEvent::getHeadBlockHash)
            .map(
                headBlockHash ->
                    maybeCachedHeadBlockHeader
                        .filter(
                            cachedBlockHeader -> cachedBlockHeader.getHash().equals(headBlockHash))
                        .map(BlockHeader::getNumber)
                        .orElseGet(
                            () -> {
                              LOG.debug(
                                  "Downloading chain head block header by hash {}", headBlockHash);
                              try {
                                return ethContext
                                    .getEthPeers()
                                    .waitForPeer((peer) -> true)
                                    .thenCompose(
                                        unused ->
                                            headerDownloader.downloadBlockHeader(headBlockHash))
                                    .thenApply(
                                        blockHeader -> {
                                          maybeCachedHeadBlockHeader = Optional.of(blockHeader);
                                          return blockHeader.getNumber();
                                        })
                                    .get(20, TimeUnit.SECONDS);
                              } catch (Throwable t) {
                                LOG.debug(
                                    "Error trying to download chain head block header by hash {}",
                                    headBlockHash,
                                    t);
                              }
                              return null;
                            }))
            .orElse(0L),
        localChainHeight);
  }
}
