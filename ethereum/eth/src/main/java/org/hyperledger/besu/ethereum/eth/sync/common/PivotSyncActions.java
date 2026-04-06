/*
 * Copyright ConsenSys AG.
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

import static java.util.concurrent.CompletableFuture.completedFuture;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.exceptions.NoAvailablePeersException;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask;
import org.hyperledger.besu.ethereum.eth.sync.ChainDownloader;
import org.hyperledger.besu.ethereum.eth.sync.PivotBlockSelector;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncChainDownloader;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PivotSyncActions {

  private static final Logger LOG = LoggerFactory.getLogger(PivotSyncActions.class);
  protected final SynchronizerConfiguration syncConfig;
  protected final WorldStateStorageCoordinator worldStateStorageCoordinator;
  protected final ProtocolSchedule protocolSchedule;
  protected final ProtocolContext protocolContext;
  protected final EthContext ethContext;
  protected final SyncState syncState;
  protected final PivotBlockSelector pivotBlockSelector;
  protected final MetricsSystem metricsSystem;
  protected final Counter pivotBlockSelectionCounter;
  protected final AtomicLong pivotBlockGauge = new AtomicLong(0);
  protected final java.nio.file.Path fastSyncDataDirectory;

  private volatile PivotUpdateListener chainDownloaderListener;

  public PivotSyncActions(
      final SynchronizerConfiguration syncConfig,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final EthContext ethContext,
      final SyncState syncState,
      final PivotBlockSelector pivotBlockSelector,
      final MetricsSystem metricsSystem,
      final Path fastSyncDataDirectory) {
    this.syncConfig = syncConfig;
    this.worldStateStorageCoordinator = worldStateStorageCoordinator;
    this.protocolSchedule = protocolSchedule;
    this.protocolContext = protocolContext;
    this.ethContext = ethContext;
    this.syncState = syncState;
    this.pivotBlockSelector = pivotBlockSelector;
    this.metricsSystem = metricsSystem;
    this.fastSyncDataDirectory = fastSyncDataDirectory;

    pivotBlockSelectionCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.SYNCHRONIZER,
            "fast_sync_pivot_block_selected_count",
            "Number of times a fast sync pivot block has been selected");
    metricsSystem.createLongGauge(
        BesuMetricCategory.SYNCHRONIZER,
        "fast_sync_pivot_block_current",
        "The current fast sync pivot block",
        pivotBlockGauge::get);
  }

  public SyncState getSyncState() {
    return syncState;
  }

  public long getBestChainHeight() {
    return pivotBlockSelector.getBestChainHeight();
  }

  public CompletableFuture<PivotSyncState> selectPivotBlock(final PivotSyncState fastSyncState) {
    return fastSyncState.hasPivotBlockHeader()
        ? completedFuture(fastSyncState)
        : selectNewPivotBlock();
  }

  private CompletableFuture<PivotSyncState> selectNewPivotBlock() {
    return pivotBlockSelector
        .selectNewPivotBlock()
        .exceptionallyCompose(throwable -> retrySelectPivotBlockAfterDelay());
  }

  <T> CompletableFuture<T> scheduleFutureTask(
      final Supplier<CompletableFuture<T>> future, final Duration duration) {
    return ethContext.getScheduler().scheduleFutureTask(future, duration);
  }

  private CompletableFuture<PivotSyncState> retrySelectPivotBlockAfterDelay() {
    return ethContext
        .getScheduler()
        .scheduleFutureTask(pivotBlockSelector::prepareRetry, Duration.ofSeconds(5))
        .thenCompose(ignore -> selectNewPivotBlock());
  }

  public CompletableFuture<PivotSyncState> downloadPivotBlockHeader(
      final PivotSyncState currentState) {
    return internalDownloadPivotBlockHeader(currentState).thenApply(this::updateStats);
  }

  private CompletableFuture<PivotSyncState> internalDownloadPivotBlockHeader(
      final PivotSyncState currentState) {
    if (currentState.hasPivotBlockHeader()) {
      LOG.debug("Initial sync state {} already contains the block header", currentState);
      return completedFuture(currentState);
    }

    return ethContext
        .getEthPeers()
        .waitForPeer((peer) -> true)
        .thenCompose(
            unused ->
                currentState
                    .getPivotBlockHash()
                    .map(hash -> downloadPivotBlockHeader(hash, currentState.isSourceTrusted()))
                    .orElseGet(
                        () ->
                            new PivotBlockRetriever(
                                    protocolSchedule,
                                    ethContext,
                                    currentState.getPivotBlockNumber().getAsLong(),
                                    syncConfig.getSyncMinimumPeerCount(),
                                    syncConfig.getSyncPivotDistance())
                                .downloadPivotBlockHeader()));
  }

  private PivotSyncState updateStats(final PivotSyncState fastSyncState) {
    pivotBlockSelectionCounter.inc();
    fastSyncState
        .getPivotBlockHeader()
        .ifPresent(blockHeader -> pivotBlockGauge.set(blockHeader.getNumber()));
    return fastSyncState;
  }

  public ChainDownloader createChainDownloader(
      final PivotSyncState currentState, final SyncDurationMetrics syncDurationMetrics) {
    return SnapSyncChainDownloader.create(
        syncConfig,
        worldStateStorageCoordinator,
        protocolSchedule,
        protocolContext,
        ethContext,
        syncState,
        metricsSystem,
        currentState,
        syncDurationMetrics,
        fastSyncDataDirectory);
  }

  private CompletableFuture<PivotSyncState> downloadPivotBlockHeader(
      final Hash hash, final boolean sourceIsTrusted) {
    LOG.debug("Downloading pivot block header by hash {}", hash);
    return ethContext
        .getScheduler()
        .scheduleServiceTask(
            () -> {
              GetHeadersFromPeerTask task =
                  new GetHeadersFromPeerTask(
                      hash,
                      pivotBlockSelector.getMinRequiredBlockNumber(),
                      1,
                      0,
                      GetHeadersFromPeerTask.Direction.FORWARD,
                      ethContext.getEthPeers().peerCount(),
                      protocolSchedule);
              PeerTaskExecutorResult<List<BlockHeader>> taskResult =
                  ethContext.getPeerTaskExecutor().execute(task);
              if (taskResult.responseCode() == PeerTaskExecutorResponseCode.NO_PEER_AVAILABLE
                  || taskResult.responseCode() == PeerTaskExecutorResponseCode.PEER_DISCONNECTED) {
                LOG.error(
                    "Failed to download pivot block header. Response Code was {}",
                    taskResult.responseCode());
                return CompletableFuture.failedFuture(new NoAvailablePeersException());
              } else if (taskResult.responseCode() != PeerTaskExecutorResponseCode.SUCCESS
                  || taskResult.result().isEmpty()) {
                LOG.error(
                    "Failed to download pivot block header. Response Code was {}",
                    taskResult.responseCode());
                return CompletableFuture.failedFuture(
                    new RuntimeException(
                        "Failed to download pivot block header. Response Code was "
                            + taskResult.responseCode()));
              } else {
                return CompletableFuture.completedFuture(taskResult.result().get().getFirst());
              }
            })
        .whenComplete(
            (blockHeader, throwable) -> {
              if (throwable != null) {
                LOG.debug("Error downloading block header by hash {}", hash);
              } else {
                LOG.atDebug()
                    .setMessage("Successfully downloaded pivot block header by hash {}")
                    .addArgument(blockHeader::toLogString)
                    .log();
              }
            })
        .thenApply(blockHeader -> new PivotSyncState(blockHeader, sourceIsTrusted));
  }

  public boolean isBlockchainBehind(final long blockNumber) {
    return protocolContext.getBlockchain().getChainHeadHeader().getNumber() < blockNumber;
  }

  /**
   * Sets the chain downloader listener to be notified of pivot updates from world state download.
   *
   * @param listener the pivot update listener
   */
  public void setChainDownloaderListener(final PivotUpdateListener listener) {
    this.chainDownloaderListener = listener;
    LOG.debug("Chain downloader listener registered for pivot updates");
  }

  /**
   * Gets the chain downloader listener for pivot update notifications.
   *
   * @return the pivot update listener, or null if not set
   */
  public PivotUpdateListener getChainDownloaderListener() {
    return chainDownloaderListener;
  }
}
