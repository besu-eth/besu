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

  /** Four epochs = 128 blocks × 12 s = Ethereum snap-serving window. */
  private static final long FOUR_EPOCHS_MILLIS = ONE_EPOCH_MILLIS * 4;

  /**
   * Freshness limit for the safe block as pivot: 4 epochs minus 1 min safety margin (≈ 123 blocks).
   */
  private static final long SAFE_PIVOT_FRESHNESS_LIMIT =
      FOUR_EPOCHS_MILLIS - Duration.ofMinutes(1).toMillis();

  private final ProtocolContext protocolContext;
  private final EthContext ethContext;
  private final GenesisConfigOptions genesisConfig;
  private final Supplier<Optional<ForkchoiceEvent>> forkchoiceStateSupplier;
  private final Runnable cleanupAction;
  private final SingleBlockHeaderDownloader headerDownloader;
  private final Clock clock;

  private volatile long lastNoFcuInfoLog = System.currentTimeMillis();
  private volatile long lastClStuckWarnLog = System.currentTimeMillis();
  private volatile Optional<BlockHeader> maybeCachedHeadBlockHeader = Optional.empty();

  private volatile Hash lastSafeBlockHash = Hash.ZERO;
  private volatile long lastSafeBlockChange = System.currentTimeMillis();
  private volatile Hash lastHeadBlockHash = Hash.ZERO;
  private volatile long lastHeadBlockChange = System.currentTimeMillis();
  private volatile Hash lastFallbackPivotHash = Hash.ZERO;
  private volatile long lastFallbackPivotChange = 0L;

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
        lastSafeBlockChange = now;
        lastFallbackPivotHash = Hash.ZERO;
        lastFallbackPivotChange = 0L;
      }
      if (now - lastSafeBlockChange < SAFE_PIVOT_FRESHNESS_LIMIT) {
        return selectSafeBlockAsPivot(safeHash);
      }
    }

    // Safe is stale (or absent). Decide between non-finality and CL stuck.
    if (now - lastHeadBlockChange < ONE_EPOCH_MILLIS) {
      return selectHeadAsFallbackPivot(headHash, now);
    }

    return logAndFailClStuck(now);
  }

  private CompletableFuture<PivotSyncState> selectSafeBlockAsPivot(final Hash safeHash) {
    LOG.debug("Returning safe block hash {} as pivot", safeHash);
    return headerDownloader.downloadBlockHeader(safeHash).thenApply(PivotSyncState::new);
  }

  private CompletableFuture<PivotSyncState> selectHeadAsFallbackPivot(
      final Hash headHash, final long now) {
    if (lastFallbackPivotHash.equals(Hash.ZERO)
        || !headHash.equals(lastFallbackPivotHash)
        || now - lastFallbackPivotChange >= ONE_EPOCH_MILLIS) {
      lastFallbackPivotHash = headHash;
      lastFallbackPivotChange = now;
      LOG.warn(
          "Safe block has not changed in over {} min but head is still advancing — falling back to head {} as untrusted pivot.",
          SAFE_PIVOT_FRESHNESS_LIMIT / 60_000,
          headHash);
    } else {
      LOG.debug("Returning previous fallback head {} as pivot", lastFallbackPivotHash);
    }
    return headerDownloader
        .downloadBlockHeader(lastFallbackPivotHash)
        .thenApply(PivotSyncState::new);
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
          SAFE_PIVOT_FRESHNESS_LIMIT / 60_000);
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
