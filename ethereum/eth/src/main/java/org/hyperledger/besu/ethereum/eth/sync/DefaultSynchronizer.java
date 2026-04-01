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
package org.hyperledger.besu.ethereum.eth.sync;

import static com.google.common.base.Preconditions.checkNotNull;

import org.hyperledger.besu.consensus.merge.ForkchoiceEvent;
import org.hyperledger.besu.consensus.merge.UnverifiedForkchoiceListener;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.eth.manager.ChainHeadEstimate;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.sync.common.NoSyncRequiredState;
import org.hyperledger.besu.ethereum.eth.sync.common.PivotSyncDownloader;
import org.hyperledger.besu.ethereum.eth.sync.common.PivotSyncState;
import org.hyperledger.besu.ethereum.eth.sync.fullsync.FullSyncDownloader;
import org.hyperledger.besu.ethereum.eth.sync.fullsync.SyncTerminationCondition;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapDownloaderFactory;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.context.SnapSyncStatePersistenceManager;
import org.hyperledger.besu.ethereum.eth.sync.state.PendingBlocksManager;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.plugin.data.SyncStatus;
import org.hyperledger.besu.plugin.services.BesuEvents;
import org.hyperledger.besu.plugin.services.BesuEvents.SyncStatusListener;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.util.log.FramedLogMessage;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultSynchronizer implements Synchronizer, UnverifiedForkchoiceListener {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultSynchronizer.class);

  private final SyncState syncState;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final Optional<BlockPropagationManager> blockPropagationManager;
  private final Supplier<Optional<PivotSyncDownloader>> fastSyncFactory;
  private final SyncDurationMetrics syncDurationMetrics;
  private Optional<PivotSyncDownloader> fastSyncDownloader;
  private final Optional<FullSyncDownloader> fullSyncDownloader;
  private final ProtocolContext protocolContext;
  private final PivotBlockSelector pivotBlockSelector;
  private final SyncTerminationCondition terminationCondition;

  public DefaultSynchronizer(
      final SynchronizerConfiguration syncConfig,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final BlockBroadcaster blockBroadcaster,
      final EthContext ethContext,
      final PeerTaskExecutor peerTaskExecutor,
      final SyncState syncState,
      final Path dataDirectory,
      final StorageProvider storageProvider,
      final Clock clock,
      final MetricsSystem metricsSystem,
      final SyncTerminationCondition terminationCondition,
      final PivotBlockSelector pivotBlockSelector) {
    this.syncState = syncState;
    this.pivotBlockSelector = pivotBlockSelector;
    this.protocolContext = protocolContext;
    this.terminationCondition = terminationCondition;

    ChainHeadTracker.trackChainHeadForPeers(
        ethContext,
        protocolSchedule,
        protocolContext.getBlockchain(),
        this::calculateTrailingPeerRequirements);

    if (syncConfig.getSyncMode() == SyncMode.SNAP) {
      SnapServerChecker.createAndSetSnapServerChecker(ethContext, metricsSystem);
    }

    this.blockPropagationManager =
        terminationCondition.shouldStopDownload()
            ? Optional.empty()
            : Optional.of(
                new BlockPropagationManager(
                    syncConfig,
                    protocolSchedule,
                    protocolContext,
                    ethContext,
                    syncState,
                    new PendingBlocksManager(syncConfig),
                    metricsSystem,
                    blockBroadcaster));

    syncDurationMetrics = new SyncDurationMetrics(metricsSystem);

    this.fullSyncDownloader =
        terminationCondition.shouldStopDownload()
            ? Optional.empty()
            : Optional.of(
                new FullSyncDownloader(
                    syncConfig,
                    protocolSchedule,
                    protocolContext,
                    ethContext,
                    syncState,
                    metricsSystem,
                    terminationCondition,
                    peerTaskExecutor,
                    syncDurationMetrics));

    this.fastSyncFactory =
        switch (syncConfig.getSyncMode()) {
          case SNAP ->
              () ->
                  SnapDownloaderFactory.createSnapDownloader(
                      new SnapSyncStatePersistenceManager(storageProvider),
                      pivotBlockSelector,
                      syncConfig,
                      dataDirectory,
                      protocolSchedule,
                      protocolContext,
                      metricsSystem,
                      ethContext,
                      worldStateStorageCoordinator,
                      syncState,
                      clock,
                      syncDurationMetrics);
          default -> () -> Optional.empty();
        };

    // create a non-resync fast sync downloader:
    this.fastSyncDownloader = this.fastSyncFactory.get();

    metricsSystem.createLongGauge(
        BesuMetricCategory.ETHEREUM,
        "best_known_block_number",
        "The estimated highest block available",
        syncState::bestChainHeight);
    metricsSystem.createIntegerGauge(
        BesuMetricCategory.SYNCHRONIZER,
        "in_sync",
        "Whether or not the local node has caught up to the best known peer",
        () -> getSyncStatus().isPresent() ? 0 : 1);
  }

  public TrailingPeerRequirements calculateTrailingPeerRequirements() {
    return fastSyncDownloader
        .flatMap(PivotSyncDownloader::calculateTrailingPeerRequirements)
        .orElse(
            fullSyncDownloader
                .map(FullSyncDownloader::calculateTrailingPeerRequirements)
                .orElse(TrailingPeerRequirements.UNRESTRICTED));
  }

  @Override
  public CompletableFuture<Void> start() {
    if (running.compareAndSet(false, true)) {
      LOG.info("Starting synchronizer.");

      syncDurationMetrics.startTimer(SyncDurationMetrics.Labels.TOTAL_SYNC_DURATION);

      blockPropagationManager.ifPresent(
          manager -> {
            if (!manager.isRunning()) {
              manager.start();
            }
          });

      CompletableFuture<Void> future;
      if (fastSyncDownloader.isPresent()) {
        future = fastSyncDownloader.get().start().thenCompose(this::handleSyncResult);
      } else {
        syncState.markInitialSyncPhaseAsDone();
        future = startFullSync();
      }
      return future.thenApply(this::finalizeSync);
    } else {
      throw new IllegalStateException("Attempt to start an already started synchronizer.");
    }
  }

  @Override
  public void stop() {
    if (running.compareAndSet(true, false)) {
      LOG.info("Stopping synchronizer");
      fastSyncDownloader.ifPresent(PivotSyncDownloader::stop);
      fullSyncDownloader.ifPresent(FullSyncDownloader::stop);
      blockPropagationManager.ifPresent(
          manager -> {
            if (manager.isRunning()) {
              manager.stop();
            }
          });
    }
  }

  @Override
  public void awaitStop() {}

  private CompletableFuture<Void> handleSyncResult(final PivotSyncState result) {
    if (!running.get()) {
      // We've been shutdown which will have triggered the fast sync future to complete
      return CompletableFuture.completedFuture(null);
    }

    if (result instanceof NoSyncRequiredState) {
      LOG.info("Sync ended (no sync required)");
      syncState.markInitialSyncPhaseAsDone();
    } else {
      fastSyncDownloader.ifPresent(PivotSyncDownloader::deletePivotSyncState);
      final Optional<BlockHeader> maybePivotHeader = result.getPivotBlockHeader();
      maybePivotHeader.ifPresent(
          blockHeader -> protocolContext.getWorldStateArchive().resetArchiveStateTo(blockHeader));

      if (maybePivotHeader
          .map(
              bh ->
                  !protocolContext
                      .getWorldStateArchive()
                      .isWorldStateAvailable(bh.getStateRoot(), bh.getHash()))
          .orElse(false)) {
        LOG.warn(
            "World state not available for pivot block {} after snap sync completion, resyncing",
            maybePivotHeader.map(BlockHeader::toLogString).orElse("unknown"));
        resyncWorldState();
        return CompletableFuture.completedFuture(null);
      }

      if (result.hasPivotBlockHash())
        LOG.info(
            "Sync completed successfully with pivot block {}",
            result.getPivotBlockNumber().getAsLong());
      pivotBlockSelector.close();
      syncState.markInitialSyncPhaseAsDone();
    }

    if (terminationCondition.shouldContinueDownload()) {
      return startFullSync();
    } else {
      syncState.setReachedTerminalDifficulty(true);
      return CompletableFuture.completedFuture(null);
    }
  }

  private CompletableFuture<Void> startFullSync() {
    return fullSyncDownloader
        .map(FullSyncDownloader::start)
        .orElse(CompletableFuture.completedFuture(null))
        .thenRun(
            () -> {
              if (terminationCondition.shouldStopDownload()) {
                syncState.setReachedTerminalDifficulty(true);
              }
            });
  }

  @Override
  public Optional<SyncStatus> getSyncStatus() {
    if (!running.get()) {
      return Optional.empty();
    }
    return syncState.syncStatus();
  }

  /**
   * Returns true if the node is in sync.
   *
   * @return true if the node is in sync.
   */
  @Override
  public boolean isInSync() {
    return syncState.isInSync();
  }

  /**
   * Returns the best known block height of the network.
   *
   * @return the best known block height of the network, or empty if not known
   */
  @Override
  public Optional<Long> getBestPeerChainHead() {
    return syncState.getBestPeerChainHead().map(ChainHeadEstimate::getEstimatedHeight);
  }

  @Override
  public boolean resyncWorldState() {
    // if sync is running currently, stop it and delete the fast sync state
    if (fastSyncDownloader.isPresent() && running.get()) {
      stop();
      fastSyncDownloader.get().deletePivotSyncState();
    }
    // recreate fast sync with resync and start
    this.syncState.markInitialSyncRestart();
    this.syncState.markResyncNeeded();
    this.fastSyncDownloader = this.fastSyncFactory.get();
    start();
    return true;
  }

  @Override
  public boolean healWorldState(
      final Optional<Address> maybeAccountToRepair, final Bytes location) {
    // recreate fast sync with resync and start
    if (fastSyncDownloader.isPresent() && running.get()) {
      stop();
      fastSyncDownloader.get().deletePivotSyncState();
    }

    LOG.atDebug()
        .setMessage("heal stacktrace: \n{}")
        .addArgument(
            () -> {
              var sw = new StringWriter();
              new Exception().printStackTrace(new PrintWriter(sw, true));
              return sw.toString();
            })
        .log();

    final List<String> lines = new ArrayList<>();
    lines.add("Besu has identified a problem with its worldstate database.");
    lines.add("Your node will fetch the correct data from peers to repair the problem.");
    lines.add("Starting the sync pipeline...");
    LOG.atInfo().setMessage(FramedLogMessage.generate(lines)).log();

    this.syncState.markInitialSyncRestart();
    this.syncState.markResyncNeeded();
    maybeAccountToRepair.ifPresent(
        address -> {
          if (this.protocolContext.getWorldStateArchive() instanceof BonsaiWorldStateProvider) {
            ((BonsaiWorldStateProvider) this.protocolContext.getWorldStateArchive())
                .prepareStateHealing(
                    org.hyperledger.besu.datatypes.Address.wrap(address.getBytes()), location);
          }
          this.syncState.markAccountToRepair(maybeAccountToRepair);
        });
    this.fastSyncDownloader = this.fastSyncFactory.get();
    start();
    return true;
  }

  @Override
  public long subscribeSyncStatus(final SyncStatusListener listener) {
    checkNotNull(listener);
    return syncState.subscribeSyncStatus(listener);
  }

  @Override
  public boolean unsubscribeSyncStatus(final long subscriberId) {
    return syncState.unsubscribeSyncStatus(subscriberId);
  }

  @Override
  public long subscribeInSync(final InSyncListener listener) {
    return syncState.subscribeInSync(listener);
  }

  @Override
  public long subscribeInSync(final InSyncListener listener, final long syncTolerance) {
    return syncState.subscribeInSync(listener, syncTolerance);
  }

  @Override
  public boolean unsubscribeInSync(final long listenerId) {
    return syncState.unsubscribeSyncStatus(listenerId);
  }

  @Override
  public long subscribeInitialSync(final BesuEvents.InitialSyncCompletionListener listener) {
    return syncState.subscribeCompletionReached(listener);
  }

  @Override
  public boolean unsubscribeInitialSync(final long listenerId) {
    return syncState.unsubscribeInitialConditionReached(listenerId);
  }

  private Void finalizeSync(final Void unused) {
    LOG.info("Stopping block propagation.");
    blockPropagationManager.ifPresent(BlockPropagationManager::stop);
    LOG.info("Stopping the pruner.");
    running.set(false);

    syncDurationMetrics.stopTimer(SyncDurationMetrics.Labels.FLAT_DB_HEAL);
    syncDurationMetrics.stopTimer(SyncDurationMetrics.Labels.TOTAL_SYNC_DURATION);

    return null;
  }

  @Override
  public void onNewUnverifiedForkchoice(final ForkchoiceEvent event) {
    if (this.blockPropagationManager.isPresent()) {
      this.blockPropagationManager.get().onNewUnverifiedForkchoice(event);
    }
  }
}
