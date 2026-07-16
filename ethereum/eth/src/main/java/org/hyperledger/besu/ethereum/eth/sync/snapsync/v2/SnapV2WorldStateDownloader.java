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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.v2;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.sync.ChainDownloader;
import org.hyperledger.besu.ethereum.eth.sync.common.PivotSyncActions;
import org.hyperledger.besu.ethereum.eth.sync.common.WorldStateHealFinishedListener;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DynamicPivotBlockSelector;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncMetricsManager;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.context.SnapSyncStatePersistenceManager;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.v2.SnapV2AccountRangeRequest;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldStateDownloader;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.services.tasks.InMemoryTasksPriorityQueues;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.IntSupplier;

import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Static-pivot snap/2 world state downloader. */
public class SnapV2WorldStateDownloader implements WorldStateDownloader {

  private static final Logger LOG = LoggerFactory.getLogger(SnapV2WorldStateDownloader.class);

  static final int NO_PEER_RETRY_DELAY_MILLISECONDS = 5_000;

  private final long minMillisBeforeStalling;
  private final Clock clock;
  private final MetricsSystem metricsSystem;
  private final EthContext ethContext;
  private final SnapSyncStatePersistenceManager snapContext;
  private final InMemoryTasksPriorityQueues<SnapDataRequest> snapTaskCollection;
  private final SnapSyncConfiguration snapSyncConfiguration;
  private final int maxOutstandingRequests;
  private final int maxNodeRequestsWithoutProgress;
  private final WorldStateStorageCoordinator worldStateStorageCoordinator;
  private final MutableBlockchain blockchain;
  private final AtomicReference<SnapV2WorldDownloadState> downloadState = new AtomicReference<>();
  private final SyncDurationMetrics syncDurationMetrics;
  private volatile WorldStateHealFinishedListener worldStateHealFinishedListener;
  private volatile SnapV2PivotCatchupListener pivotCatchupListener;
  private final SnapV2BlockAccessListApplier blockAccessListApplier;

  public SnapV2WorldStateDownloader(
      final EthContext ethContext,
      final SnapSyncStatePersistenceManager snapContext,
      final MutableBlockchain blockchain,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final ProtocolSchedule protocolSchedule,
      final InMemoryTasksPriorityQueues<SnapDataRequest> snapTaskCollection,
      final SnapSyncConfiguration snapSyncConfiguration,
      final int maxOutstandingRequests,
      final int maxNodeRequestsWithoutProgress,
      final long minMillisBeforeStalling,
      final Clock clock,
      final MetricsSystem metricsSystem,
      final SyncDurationMetrics syncDurationMetrics) {
    this.ethContext = ethContext;
    this.worldStateStorageCoordinator = worldStateStorageCoordinator;
    this.blockchain = blockchain;
    this.snapContext = snapContext;
    this.snapTaskCollection = snapTaskCollection;
    this.snapSyncConfiguration = snapSyncConfiguration;
    this.maxOutstandingRequests = maxOutstandingRequests;
    this.maxNodeRequestsWithoutProgress = maxNodeRequestsWithoutProgress;
    this.minMillisBeforeStalling = minMillisBeforeStalling;
    this.clock = clock;
    this.metricsSystem = metricsSystem;
    this.syncDurationMetrics = syncDurationMetrics;
    this.blockAccessListApplier =
        new SnapV2BlockAccessListApplier(
            worldStateStorageCoordinator, blockchain, protocolSchedule);

    metricsSystem.createIntegerGauge(
        BesuMetricCategory.SYNCHRONIZER,
        "snap_v2_world_state_node_requests_since_last_progress_current",
        "Number of snap/2 world state requests since the last time new data was returned",
        downloadStateValue(SnapV2WorldDownloadState::getRequestsSinceLastProgress));

    metricsSystem.createIntegerGauge(
        BesuMetricCategory.SYNCHRONIZER,
        "snap_v2_world_state_inflight_requests_current",
        "Number of in progress requests for snap/2 world state data",
        downloadStateValue(SnapV2WorldDownloadState::getOutstandingTaskCount));
  }

  private IntSupplier downloadStateValue(final Function<SnapV2WorldDownloadState, Integer> getter) {
    return () -> {
      final SnapV2WorldDownloadState state = this.downloadState.get();
      return state != null ? getter.apply(state) : 0;
    };
  }

  @Override
  public void setChainDownloader(final ChainDownloader chainDownloader) {
    if (chainDownloader instanceof WorldStateHealFinishedListener listener) {
      this.worldStateHealFinishedListener = listener;
    }
    if (chainDownloader instanceof SnapV2PivotCatchupListener listener) {
      this.pivotCatchupListener = listener;
    }
  }

  @Override
  public CompletableFuture<Void> run(
      final PivotSyncActions fastSyncActions, final SnapSyncProcessState snapSyncState) {
    synchronized (this) {
      final SnapV2WorldDownloadState oldDownloadState = this.downloadState.get();
      if (oldDownloadState != null && oldDownloadState.isDownloading()) {
        final CompletableFuture<Void> failed = new CompletableFuture<>();
        failed.completeExceptionally(
            new IllegalStateException(
                "Cannot run an already running " + this.getClass().getSimpleName()));
        return failed;
      }

      if (ethContext.getEthPeers().peerCount() == 0) {
        LOG.debug(
            "No peers available, deferring snap/2 world state pipeline start for {} ms",
            NO_PEER_RETRY_DELAY_MILLISECONDS);
        return ethContext
            .getScheduler()
            .scheduleFutureTask(
                () -> run(fastSyncActions, snapSyncState),
                Duration.ofMillis(NO_PEER_RETRY_DELAY_MILLISECONDS));
      }

      final BlockHeader header = snapSyncState.getPivotBlockHeader().orElseThrow();
      final Hash stateRoot = header.getStateRoot();
      LOG.info(
          "Downloading snap/2 world state from peers for static pivot block {}. State root {}",
          header.toLogString(),
          stateRoot);

      snapContext.clear();
      snapTaskCollection.clear();
      worldStateStorageCoordinator.clear();

      final SnapSyncMetricsManager snapsyncMetricsManager =
          new SnapSyncMetricsManager(metricsSystem, ethContext);
      final DynamicPivotBlockSelector pivotBlockSelector =
          new DynamicPivotBlockSelector(
              ethContext,
              fastSyncActions,
              snapSyncState,
              null,
              snapSyncConfiguration.getPivotBlockCheckIntervalMillis());
      final SnapV2WorldDownloadState newDownloadState =
          new SnapV2WorldDownloadState(
              worldStateStorageCoordinator,
              snapContext,
              snapSyncState,
              snapTaskCollection,
              maxNodeRequestsWithoutProgress,
              minMillisBeforeStalling,
              snapsyncMetricsManager,
              clock,
              syncDurationMetrics,
              worldStateHealFinishedListener,
              pivotCatchupListener,
              blockAccessListApplier,
              blockchain,
              ethContext);

      final Map<Bytes32, Bytes32> ranges = RangeManager.generateAllRanges(16);
      snapsyncMetricsManager.initRange(ranges);
      ranges.forEach(
          (key, value) ->
              newDownloadState.enqueueRequest(new SnapV2AccountRangeRequest(header, key, value)));

      final SnapV2WorldStateDownloadProcess downloadProcess =
          SnapV2WorldStateDownloadProcess.create(
              ethContext,
              worldStateStorageCoordinator,
              snapSyncState,
              newDownloadState,
              pivotBlockSelector,
              snapSyncConfiguration,
              maxOutstandingRequests,
              metricsSystem);

      downloadState.set(newDownloadState);
      return newDownloadState.startDownload(downloadProcess, ethContext.getScheduler());
    }
  }

  @Override
  public void cancel() {
    synchronized (this) {
      final SnapV2WorldDownloadState state = this.downloadState.get();
      if (state != null) {
        state.getDownloadFuture().cancel(true);
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
