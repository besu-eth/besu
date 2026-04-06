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
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

import static org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncMetricsManager.Step.DOWNLOAD;
import static org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest.createAccountRangeDataRequest;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.sync.common.PivotSyncActions;
import org.hyperledger.besu.ethereum.eth.sync.common.PivotSyncState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.context.SnapSyncStatePersistenceManager;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.AccountRangeDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldStateDownloader;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.services.tasks.InMemoryTasksPriorityQueues;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.IntSupplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapWorldStateDownloader implements WorldStateDownloader {

  private static final Logger LOG = LoggerFactory.getLogger(SnapWorldStateDownloader.class);
  private final long minMillisBeforeStalling;
  private final Clock clock;
  private final MetricsSystem metricsSystem;

  private final EthContext ethContext;
  private final SnapSyncStatePersistenceManager snapContext;
  private final InMemoryTasksPriorityQueues<SnapDataRequest> snapTaskCollection;
  private final SnapSyncConfiguration snapSyncConfiguration;
  private final int maxOutstandingRequests;
  private final int maxNodeRequestsWithoutProgress;
  private final ProtocolContext protocolContext;
  private final WorldStateStorageCoordinator worldStateStorageCoordinator;

  private final AtomicReference<SnapWorldDownloadState> downloadState = new AtomicReference<>();
  private final SyncDurationMetrics syncDurationMetrics;

  private volatile SnapSyncChainDownloader chainDownloader;

  public SnapWorldStateDownloader(
      final EthContext ethContext,
      final SnapSyncStatePersistenceManager snapContext,
      final ProtocolContext protocolContext,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final InMemoryTasksPriorityQueues<SnapDataRequest> snapTaskCollection,
      final SnapSyncConfiguration snapSyncConfiguration,
      final int maxOutstandingRequests,
      final int maxNodeRequestsWithoutProgress,
      final long minMillisBeforeStalling,
      final Clock clock,
      final MetricsSystem metricsSystem,
      final SyncDurationMetrics syncDurationMetrics) {
    this.ethContext = ethContext;
    this.protocolContext = protocolContext;
    this.worldStateStorageCoordinator = worldStateStorageCoordinator;
    this.snapContext = snapContext;
    this.snapTaskCollection = snapTaskCollection;
    this.snapSyncConfiguration = snapSyncConfiguration;
    this.maxOutstandingRequests = maxOutstandingRequests;
    this.maxNodeRequestsWithoutProgress = maxNodeRequestsWithoutProgress;
    this.minMillisBeforeStalling = minMillisBeforeStalling;
    this.clock = clock;
    this.metricsSystem = metricsSystem;
    this.syncDurationMetrics = syncDurationMetrics;

    metricsSystem.createIntegerGauge(
        BesuMetricCategory.SYNCHRONIZER,
        "snap_world_state_node_requests_since_last_progress_current",
        "Number of world state requests made since the last time new data was returned",
        downloadStateValue(SnapWorldDownloadState::getRequestsSinceLastProgress));

    metricsSystem.createIntegerGauge(
        BesuMetricCategory.SYNCHRONIZER,
        "snap_world_state_inflight_requests_current",
        "Number of in progress requests for world state data",
        downloadStateValue(SnapWorldDownloadState::getOutstandingTaskCount));
  }

  private IntSupplier downloadStateValue(final Function<SnapWorldDownloadState, Integer> getter) {
    return () -> {
      final SnapWorldDownloadState state = this.downloadState.get();
      return state != null ? getter.apply(state) : 0;
    };
  }

  /**
   * Sets the chain downloader that should be wired to the world download state. This method should
   * be called before run() to establish the bidirectional reference between the chain downloader
   * and world state downloader.
   *
   * @param chainDownloader the chain downloader to wire up
   */
  public void setChainDownloader(final SnapSyncChainDownloader chainDownloader) {
    this.chainDownloader = chainDownloader;
  }

  @Override
  public CompletableFuture<Void> run(
      final PivotSyncActions fastSyncActions, final PivotSyncState fastSyncState) {
    synchronized (this) {
      final SnapWorldDownloadState oldDownloadState = this.downloadState.get();
      if (oldDownloadState != null && oldDownloadState.isDownloading()) {
        final CompletableFuture<Void> failed = new CompletableFuture<>();
        failed.completeExceptionally(
            new IllegalStateException(
                "Cannot run an already running " + this.getClass().getSimpleName()));
        return failed;
      }

      final SnapSyncProcessState snapSyncState = (SnapSyncProcessState) fastSyncState;
      final BlockHeader header = fastSyncState.getPivotBlockHeader().get();
      final Hash stateRoot = header.getStateRoot();
      LOG.info(
          "Downloading world state from peers for pivot block {}. State root {} pending requests {}",
          header.toLogString(),
          stateRoot,
          snapTaskCollection.size());

      final SnapSyncMetricsManager snapsyncMetricsManager =
          new SnapSyncMetricsManager(metricsSystem, ethContext);

      final SnapWorldDownloadState newDownloadState =
          new SnapWorldDownloadState(
              worldStateStorageCoordinator,
              snapContext,
              protocolContext.getBlockchain(),
              snapSyncState,
              snapTaskCollection,
              maxNodeRequestsWithoutProgress,
              minMillisBeforeStalling,
              snapsyncMetricsManager,
              clock,
              ethContext,
              syncDurationMetrics);

      final Map<Bytes32, Bytes32> ranges = RangeManager.generateAllRanges(16);
      snapsyncMetricsManager.initRange(ranges);

      final List<AccountRangeDataRequest> currentAccountRange =
          snapContext.getCurrentAccountRange();
      final Set<Bytes> inconsistentAccounts = snapContext.getAccountsHealingList();

      if (!currentAccountRange.isEmpty()) { // continue to download worldstate ranges
        newDownloadState.setAccountsHealingList(inconsistentAccounts);
        snapContext
            .getCurrentAccountRange()
            .forEach(
                snapDataRequest -> {
                  snapsyncMetricsManager.notifyRangeProgress(
                      DOWNLOAD, snapDataRequest.getStartKeyHash(), snapDataRequest.getEndKeyHash());
                  newDownloadState.enqueueRequest(snapDataRequest);
                });
      } else if (!snapContext.getAccountsHealingList().isEmpty()) { // restart only the heal step
        snapSyncState.setHealTrieStatus(true);
        worldStateStorageCoordinator.applyOnMatchingStrategies(
            List.of(DataStorageFormat.BONSAI, DataStorageFormat.X_BONSAI_ARCHIVE),
            strategy -> {
              BonsaiWorldStateKeyValueStorage onBonsai = (BonsaiWorldStateKeyValueStorage) strategy;
              onBonsai.clearFlatDatabase();
              onBonsai.clearTrieLog();
            });

        newDownloadState.setAccountsHealingList(inconsistentAccounts);
        newDownloadState.enqueueRequest(
            SnapDataRequest.createAccountTrieNodeDataRequest(
                stateRoot, Bytes.EMPTY, snapContext.getAccountsHealingList()));
      } else {
        // start from scratch
        worldStateStorageCoordinator.clear();
        ranges.forEach(
            (key, value) ->
                newDownloadState.enqueueRequest(
                    createAccountRangeDataRequest(stateRoot, key, value)));
      }

      Optional<CompleteTaskStep> maybeCompleteTask =
          Optional.of(new CompleteTaskStep(snapSyncState, metricsSystem));

      final DynamicPivotBlockSelector dynamicPivotBlockManager =
          new DynamicPivotBlockSelector(
              ethContext,
              fastSyncActions,
              snapSyncState,
              fastSyncActions.getChainDownloaderListener(),
              snapSyncConfiguration.getPivotBlockWindowValidity(),
              snapSyncConfiguration.getPivotBlockDistanceBeforeCaching());

      SnapWorldStateDownloadProcess downloadProcess =
          SnapWorldStateDownloadProcess.builder()
              .configuration(snapSyncConfiguration)
              .maxOutstandingRequests(maxOutstandingRequests)
              .dynamicPivotBlockSelector(dynamicPivotBlockManager)
              .loadLocalDataStep(
                  new LoadLocalDataStep(
                      worldStateStorageCoordinator,
                      newDownloadState,
                      snapSyncConfiguration,
                      metricsSystem,
                      snapSyncState))
              .requestDataStep(
                  new RequestDataStep(
                      ethContext,
                      worldStateStorageCoordinator,
                      snapSyncState,
                      newDownloadState,
                      snapSyncConfiguration,
                      metricsSystem))
              .persistDataStep(
                  new PersistDataStep(
                      snapSyncState,
                      worldStateStorageCoordinator,
                      newDownloadState,
                      snapSyncConfiguration))
              .completeTaskStep(maybeCompleteTask.get())
              .downloadState(newDownloadState)
              .fastSyncState(snapSyncState)
              .metricsSystem(metricsSystem)
              .build();

      newDownloadState.setPivotBlockSelector(dynamicPivotBlockManager);

      // Wire up bidirectional reference if chain downloader was provided
      if (chainDownloader != null) {
        chainDownloader.setWorldDownloadState(newDownloadState);
        newDownloadState.setWorldStateHealFinishedListener(chainDownloader);
        LOG.debug("Wired bidirectional references between chain and world state downloaders");
      }

      return newDownloadState.startDownload(downloadProcess, ethContext.getScheduler());
    }
  }

  @Override
  public void cancel() {
    synchronized (this) {
      final SnapWorldDownloadState downloadState = this.downloadState.get();
      if (downloadState != null) {
        downloadState.getDownloadFuture().cancel(true);
      }
    }
  }

  @Override
  public Optional<Long> getPulledStates() {
    return Optional.empty();
  }

  @Override
  public Optional<Long> getKnownStates() {
    return Optional.empty();
  }
}
