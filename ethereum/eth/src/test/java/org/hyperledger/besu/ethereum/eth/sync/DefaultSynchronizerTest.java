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
package org.hyperledger.besu.ethereum.eth.sync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.sync.common.NoSyncRequiredState;
import org.hyperledger.besu.ethereum.eth.sync.fullsync.SyncTerminationCondition;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapDownloaderFactory;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncController;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.ethereum.worldstate.WorldStateRepairBudget;
import org.hyperledger.besu.ethereum.worldstate.WorldStateRepairRequest;
import org.hyperledger.besu.ethereum.worldstate.WorldStateRepairResult;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.metrics.StubMetricsSystem;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.data.SyncStatus;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.plugin.services.metrics.OperationTimer;

import java.nio.file.Path;
import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class DefaultSynchronizerTest {

  private static final long CHAIN_HEAD = 42L;

  private final MutableBlockchain blockchain = mock(MutableBlockchain.class);
  private final ProtocolContext protocolContext = mock(ProtocolContext.class);
  private final SyncTerminationCondition terminationCondition =
      mock(SyncTerminationCondition.class);
  private final SnapSyncController snapSyncController = mock(SnapSyncController.class);
  private final EthContext ethContext = mock(EthContext.class);
  private final EthScheduler scheduler = mock(EthScheduler.class);
  private final AtomicReference<Runnable> queuedRepair = new AtomicReference<>();
  private SyncState syncState;

  private MockedStatic<ChainHeadTracker> chainHeadTracker;
  private MockedStatic<SnapServerChecker> snapServerChecker;
  private MockedStatic<SnapDownloaderFactory> snapDownloaderFactory;

  @BeforeEach
  public void setUp() {
    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getChainHeadBlockNumber()).thenReturn(CHAIN_HEAD);
    // Keeps the block propagation manager and full sync downloader out of the picture.
    when(terminationCondition.shouldStopDownload()).thenReturn(true);

    final EthPeers ethPeers = mock(EthPeers.class);
    when(ethPeers.bestPeerWithHeightEstimate()).thenReturn(Optional.empty());
    when(ethContext.getScheduler()).thenReturn(scheduler);
    doAnswer(
            invocation -> {
              queuedRepair.set(invocation.getArgument(0));
              return null;
            })
        .when(scheduler)
        .scheduleSyncWorkerTask(any(Runnable.class));
    syncState = new SyncState(blockchain, ethPeers, true, Optional.empty());

    chainHeadTracker = mockStatic(ChainHeadTracker.class);
    snapServerChecker = mockStatic(SnapServerChecker.class);
    snapDownloaderFactory = mockStatic(SnapDownloaderFactory.class);
    snapDownloaderFactory
        .when(
            () ->
                SnapDownloaderFactory.createSnapDownloader(
                    any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(),
                    any(), any()))
        .thenReturn(Optional.of(snapSyncController));
  }

  @AfterEach
  public void tearDown() {
    chainHeadTracker.close();
    snapServerChecker.close();
    snapDownloaderFactory.close();
  }

  @Test
  public void snapSyncReportsSyncStatusBeforeTheSnapDownloaderStarts() {
    final AtomicReference<Optional<SyncStatus>> statusWhenDownloaderStarted =
        new AtomicReference<>();
    when(snapSyncController.start())
        .thenAnswer(
            invocation -> {
              statusWhenDownloaderStarted.set(syncState.syncStatus());
              return new CompletableFuture<SnapSyncProcessState>();
            });
    final DefaultSynchronizer synchronizer = createSynchronizer(SyncMode.SNAP);

    synchronizer.start();

    assertThat(statusWhenDownloaderStarted.get()).isPresent();
    final SyncStatus syncStatus = synchronizer.getSyncStatus().orElseThrow();
    assertThat(syncStatus.getStartingBlock()).isEqualTo(CHAIN_HEAD);
    assertThat(syncStatus.getCurrentBlock()).isEqualTo(CHAIN_HEAD);
  }

  @Test
  public void snapSyncClearsReportedSyncStatusWhenNoSyncIsRequired() {
    when(snapSyncController.start())
        .thenReturn(CompletableFuture.completedFuture(new NoSyncRequiredState()));
    final DefaultSynchronizer synchronizer = createSynchronizer(SyncMode.SNAP);

    synchronizer.start();

    assertThat(syncState.syncStatus()).isEmpty();
  }

  @Test
  public void fullSyncDoesNotReportSnapSyncProgress() {
    final DefaultSynchronizer synchronizer = createSynchronizer(SyncMode.FULL);

    synchronizer.start();

    assertThat(syncState.syncStatus()).isEmpty();
  }

  @Test
  public void targetedRepairRunsOffTheCallerWithoutRestartingSynchronization() {
    final WorldStateArchive archive = mock(WorldStateArchive.class);
    final WorldStateRepairBudget budget = WorldStateRepairBudget.defaults();
    final var request =
        new WorldStateRepairRequest(
            Hash.ZERO,
            Hash.EMPTY_TRIE_HASH,
            Optional.of(
                new WorldStateRepairRequest.MissingTrieNode(
                    Hash.ZERO, org.apache.tuweni.bytes.Bytes.of(1), Optional.empty())));
    when(protocolContext.getWorldStateArchive()).thenReturn(archive);
    when(archive.repairWorldState(request, budget)).thenReturn(WorldStateRepairResult.REPAIRED);
    final DefaultSynchronizer synchronizer = createSynchronizer(SyncMode.SNAP);

    final CompletableFuture<WorldStateRepairResult> repair =
        synchronizer.repairWorldState(request, budget);

    assertThat(repair).isNotDone();
    verify(archive, never()).repairWorldState(any(), any());
    queuedRepair.get().run();
    assertThat(repair.join()).isEqualTo(WorldStateRepairResult.REPAIRED);
    verify(archive).repairWorldState(request, budget);
    assertThat(syncState.syncStatus()).isEmpty();
    verify(snapSyncController, never()).stop();
    verify(snapSyncController, never()).start();
    verify(snapSyncController, never()).deletePivotSyncState();
  }

  @Test
  public void stopCancelsQueuedRepairBeforeItTouchesTheArchive() {
    final WorldStateArchive archive = mock(WorldStateArchive.class);
    when(protocolContext.getWorldStateArchive()).thenReturn(archive);
    final DefaultSynchronizer synchronizer = createSynchronizer(SyncMode.SNAP);
    final WorldStateRepairBudget budget = WorldStateRepairBudget.defaults();
    final CompletableFuture<WorldStateRepairResult> repair =
        synchronizer.repairWorldState(Hash.ZERO, Hash.EMPTY_TRIE_HASH, budget);

    synchronizer.stop();

    assertThat(budget.isCancelled()).isTrue();
    assertThat(repair).isNotDone();
    queuedRepair.get().run();
    assertThat(repair.join()).isEqualTo(WorldStateRepairResult.CANCELLED);
    verify(archive, never()).repairWorldState(any(), any());
  }

  @Test
  public void onlyOneRepairMayAccessTheArchiveAtATime() {
    final DefaultSynchronizer synchronizer = createSynchronizer(SyncMode.SNAP);
    final WorldStateRepairBudget firstBudget = WorldStateRepairBudget.defaults();
    final CompletableFuture<WorldStateRepairResult> first =
        synchronizer.repairWorldState(Hash.ZERO, Hash.EMPTY_TRIE_HASH, firstBudget);

    assertThat(
            synchronizer
                .repairWorldState(
                    Hash.EMPTY_TRIE_HASH, Hash.ZERO, WorldStateRepairBudget.defaults())
                .join())
        .isEqualTo(WorldStateRepairResult.SUPERSEDED);
    assertThat(first).isNotDone();
    firstBudget.cancel();
    queuedRepair.get().run();
    assertThat(first.join()).isEqualTo(WorldStateRepairResult.CANCELLED);
  }

  @Test
  public void repairPreservesExistingGaugeSuppliersAndSyncProgress() {
    final StubMetricsSystem metrics = new StubMetricsSystem();
    final DefaultSynchronizer synchronizer = createSynchronizer(SyncMode.SNAP, metrics);
    syncState.setSyncProgress(12L, CHAIN_HEAD);
    final Optional<SyncStatus> progress = syncState.syncStatus();
    final double bestKnown = metrics.getGaugeValue("best_known_block_number");
    final double inSync = metrics.getGaugeValue("in_sync");
    final WorldStateArchive archive = mock(WorldStateArchive.class);
    when(protocolContext.getWorldStateArchive()).thenReturn(archive);
    when(archive.repairWorldState(any(), any())).thenReturn(WorldStateRepairResult.REPAIRED);

    final var result =
        synchronizer.repairWorldState(
            Hash.ZERO, Hash.EMPTY_TRIE_HASH, WorldStateRepairBudget.defaults());
    queuedRepair.get().run();

    assertThat(result.join()).isEqualTo(WorldStateRepairResult.REPAIRED);
    assertThat(syncState.syncStatus()).isEqualTo(progress);
    assertThat(metrics.getGaugeValue("best_known_block_number")).isEqualTo(bestKnown);
    assertThat(metrics.getGaugeValue("in_sync")).isEqualTo(inSync);
    verify(snapSyncController, never()).start();
    verify(snapSyncController, never()).stop();
  }

  @Test
  public void repairKeepsOriginalTimerRunningUntilActualSyncCompletion() {
    final StubMetricsSystem metrics = spy(new StubMetricsSystem());
    final var operation = mock(OperationTimer.class);
    final var timing = mock(OperationTimer.TimingContext.class);
    when(operation.startTimer()).thenReturn(timing);
    doReturn((LabelledMetric<OperationTimer>) labels -> operation)
        .when(metrics)
        .createSimpleLabelledTimer(
            BesuMetricCategory.SYNCHRONIZER, "sync_duration", "Time taken to sync", "name");
    final CompletableFuture<SnapSyncProcessState> initialSync = new CompletableFuture<>();
    when(snapSyncController.start()).thenReturn(initialSync);
    final var synchronizer = createSynchronizer(SyncMode.SNAP, metrics);
    synchronizer.start();
    final WorldStateArchive archive = mock(WorldStateArchive.class);
    when(protocolContext.getWorldStateArchive()).thenReturn(archive);
    when(archive.repairWorldState(any(), any())).thenReturn(WorldStateRepairResult.REPAIRED);
    synchronizer.getWorldStateRepairRequester().requestRecovery(Hash.ZERO, Hash.EMPTY_TRIE_HASH);
    queuedRepair.get().run();

    verify(operation).startTimer();
    verify(timing, never()).stopTimer();
    verify(snapSyncController).start();
    initialSync.complete(new NoSyncRequiredState());
    verify(timing).stopTimer();
    verify(operation).startTimer();
    assertThat(synchronizer.getWorldStateRepairRequester())
        .isSameAs(synchronizer.getWorldStateRepairRequester());
  }

  private DefaultSynchronizer createSynchronizer(final SyncMode syncMode) {
    return createSynchronizer(syncMode, new NoOpMetricsSystem());
  }

  private DefaultSynchronizer createSynchronizer(
      final SyncMode syncMode, final MetricsSystem metrics) {
    final SynchronizerConfiguration syncConfig = mock(SynchronizerConfiguration.class);
    when(syncConfig.getSyncMode()).thenReturn(syncMode);
    return new DefaultSynchronizer(
        syncConfig,
        null,
        protocolContext,
        null,
        null,
        ethContext,
        null,
        syncState,
        Path.of("unused"),
        mock(StorageProvider.class, RETURNS_DEEP_STUBS),
        Clock.systemUTC(),
        metrics,
        terminationCondition,
        null,
        Optional.empty());
  }
}
