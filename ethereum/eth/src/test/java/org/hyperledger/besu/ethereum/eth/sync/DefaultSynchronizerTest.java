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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.sync.common.NoSyncRequiredState;
import org.hyperledger.besu.ethereum.eth.sync.fullsync.SyncTerminationCondition;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.BackgroundBackfill;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapDownloaderFactory;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncController;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.data.SyncStatus;

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
  private final BackgroundBackfill backgroundBackfill = mock(BackgroundBackfill.class);
  private SyncState syncState;

  private MockedStatic<ChainHeadTracker> chainHeadTracker;
  private MockedStatic<SnapServerChecker> snapServerChecker;
  private MockedStatic<SnapDownloaderFactory> snapDownloaderFactory;
  private MockedStatic<BackgroundBackfill> backgroundBackfills;

  @BeforeEach
  public void setUp() {
    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getChainHeadBlockNumber()).thenReturn(CHAIN_HEAD);
    // Keeps the block propagation manager and full sync downloader out of the picture.
    when(terminationCondition.shouldStopDownload()).thenReturn(true);

    final EthPeers ethPeers = mock(EthPeers.class);
    when(ethPeers.bestPeerWithHeightEstimate()).thenReturn(Optional.empty());
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
    backgroundBackfills = mockStatic(BackgroundBackfill.class);
    backgroundBackfills
        .when(() -> BackgroundBackfill.create(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(backgroundBackfill);
  }

  @AfterEach
  public void tearDown() {
    chainHeadTracker.close();
    snapServerChecker.close();
    snapDownloaderFactory.close();
    backgroundBackfills.close();
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
  public void startsTheBackfillOnceTheInitialSyncCompletes() {
    final CompletableFuture<SnapSyncProcessState> sync = new CompletableFuture<>();
    when(snapSyncController.start()).thenReturn(sync);
    final WorldStateArchive worldStateArchive = mock(WorldStateArchive.class);
    when(protocolContext.getWorldStateArchive()).thenReturn(worldStateArchive);
    when(worldStateArchive.isWorldStateAvailable(any(), any())).thenReturn(true);
    final DefaultSynchronizer synchronizer = createSynchronizer(SyncMode.SNAP);

    synchronizer.start();
    verify(backgroundBackfill, never()).onInitialSyncCompleted();
    sync.complete(
        new SnapSyncProcessState(new BlockHeaderTestFixture().number(CHAIN_HEAD).buildHeader()));

    verify(backgroundBackfill).onInitialSyncCompleted();
  }

  @Test
  public void startsTheBackfillWhenNoInitialSyncIsNeeded() {
    final DefaultSynchronizer synchronizer = createSynchronizer(SyncMode.FULL);

    synchronizer.start();

    verify(backgroundBackfill).onInitialSyncCompleted();
  }

  @Test
  public void stopsTheBackfillWhenTheWorldStateIsSyncedAgain() {
    final DefaultSynchronizer synchronizer = createSynchronizer(SyncMode.FULL);
    // without a download to run, the synchronizer stops running at once, the back-fill does not
    synchronizer.start();

    synchronizer.resyncWorldState();

    verify(backgroundBackfill).onInitialSyncRestart();
  }

  @Test
  public void stopsAndAwaitsTheBackfillOnceTheSynchronizerNoLongerRuns()
      throws InterruptedException {
    final DefaultSynchronizer synchronizer = createSynchronizer(SyncMode.FULL);
    synchronizer.start();

    synchronizer.stop();
    synchronizer.awaitStop();

    verify(backgroundBackfill).stop();
    verify(backgroundBackfill).awaitStop();
  }

  @Test
  public void awaitsTheSnapSyncBeforeTheStorageCloses() throws InterruptedException {
    final DefaultSynchronizer synchronizer = createSynchronizer(SyncMode.SNAP);

    synchronizer.awaitStop();

    verify(snapSyncController).awaitStop();
  }

  private DefaultSynchronizer createSynchronizer(final SyncMode syncMode) {
    final SynchronizerConfiguration syncConfig = mock(SynchronizerConfiguration.class);
    when(syncConfig.getSyncMode()).thenReturn(syncMode);
    return new DefaultSynchronizer(
        syncConfig,
        null,
        protocolContext,
        null,
        null,
        mock(EthContext.class),
        null,
        syncState,
        Path.of("unused"),
        mock(StorageProvider.class, RETURNS_DEEP_STUBS),
        Clock.systemUTC(),
        new NoOpMetricsSystem(),
        terminationCondition,
        mock(PivotBlockSelector.class),
        Optional.empty());
  }
}
