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
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.common.BackwardHeaderDriver;
import org.hyperledger.besu.ethereum.eth.sync.common.ChainSyncState;
import org.hyperledger.besu.ethereum.eth.sync.common.ChainSyncStateStorage;
import org.hyperledger.besu.ethereum.eth.sync.common.SingleBlockHeaderDownloader;
import org.hyperledger.besu.ethereum.eth.sync.common.WrongChainException;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.services.pipeline.Pipeline;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Covers the incremental-continuation loop: what happens after the first download cycle completes
 * and the world state downloader reports a new pivot.
 *
 * <p>The loop is driven by signalling both a pivot update and world-state-heal-finished before
 * {@link SnapSyncChainDownloader#start()}. The pivot update is consumed by the first pass through
 * the wait loop (so a second cycle runs where the state requires one); on the following pass the
 * freshly installed {@code pivotUpdateFuture} is still pending while the heal future is already
 * complete, which terminates the download. That yields a deterministic, bounded number of cycles.
 */
@ExtendWith(MockitoExtension.class)
class SnapSyncChainDownloaderPivotUpdateTest {

  @Mock private SnapSyncChainDownloadPipelineFactory pipelineFactory;
  @Mock private ProtocolSchedule protocolSchedule;
  @Mock private ProtocolSpec protocolSpec;
  @Mock private ProtocolContext protocolContext;
  @Mock private EthContext ethContext;
  @Mock private EthPeers ethPeers;
  @Mock private SyncState syncState;
  @Mock private SyncDurationMetrics syncDurationMetrics;
  @Mock private MutableBlockchain blockchain;
  @Mock private EthScheduler scheduler;
  @Mock private SingleBlockHeaderDownloader headerDownloader;

  @TempDir private Path tempDir;

  private ChainSyncStateStorage chainSyncStateStorage;
  private BlockHeader genesisHeader;
  private BlockHeader initialPivot;
  private SynchronizerConfiguration syncConfig;

  @BeforeEach
  void setUp() {
    genesisHeader = header(0);
    initialPivot = header(1000);
    chainSyncStateStorage = new ChainSyncStateStorage(tempDir);
    syncConfig = SynchronizerConfiguration.builder().build();

    lenient().when(protocolContext.getBlockchain()).thenReturn(blockchain);
    lenient().when(blockchain.getGenesisBlockHeader()).thenReturn(genesisHeader);
    lenient().when(blockchain.getChainHeadBlockNumber()).thenReturn(500L);
    lenient().when(blockchain.getChainHeadHeader()).thenReturn(header(500));
    lenient().when(ethContext.getScheduler()).thenReturn(scheduler);
    lenient().when(ethContext.getEthPeers()).thenReturn(ethPeers);
    lenient().when(syncState.getCheckpoint()).thenReturn(Optional.empty());
    lenient().when(protocolSchedule.getByBlockHeader(any())).thenReturn(protocolSpec);
    lenient()
        .when(protocolSpec.getBlockHeaderFunctions())
        .thenReturn(new MainnetBlockHeaderFunctions());
  }

  @Test
  void pivotAdvanceRestartsStage1WithThePreviousPivotAsAnchor() throws Exception {
    final BlockHeader advancedPivot = header(2000);
    setupSuccessfulPipelineMocks();

    final SnapSyncChainDownloader downloader = downloader(initialPivot);
    downloader.onPivotUpdated(advancedPivot);
    downloader.onWorldStateHealFinished();

    downloader.start().get(5, TimeUnit.SECONDS);

    // Two Stage 1 runs: the initial pivot, then the advanced pivot anchored at the initial pivot so
    // only the newly appended range is re-downloaded.
    final ArgumentCaptor<ChainSyncState> captor = ArgumentCaptor.forClass(ChainSyncState.class);
    verify(pipelineFactory, times(2)).createBackwardHeaderDownloadPipeline(captor.capture());
    final List<ChainSyncState> stage1States = captor.getAllValues();

    assertThat(stage1States.get(0).pivotBlockHeader().getNumber()).isEqualTo(1000L);
    assertThat(stage1States.get(0).headerDownloadAnchor().getNumber()).isEqualTo(0L);

    assertThat(stage1States.get(1).pivotBlockHeader().getNumber()).isEqualTo(2000L);
    assertThat(stage1States.get(1).headerDownloadAnchor().getNumber()).isEqualTo(1000L);
    assertThat(stage1States.get(1).headersDownloadComplete()).isFalse();
    // The body checkpoint is never moved by a pivot update.
    assertThat(stage1States.get(1).bodyCheckpoint().getNumber()).isEqualTo(0L);

    assertThat(loadPersistedState().pivotBlockHeader().getNumber()).isEqualTo(2000L);
  }

  @Test
  void pivotRollbackOntoCanonicalBlockSkipsStage1AndCompletes() throws Exception {
    final BlockHeader rolledBackPivot = header(900);
    setupSuccessfulPipelineMocks();
    // The rolled-back pivot is already part of the header chain we downloaded for the old pivot.
    // Lenient: Stage 2's anchor probe also asks about the chain head, which must answer "false".
    lenient().when(blockchain.blockIsOnCanonicalChain(rolledBackPivot.getHash())).thenReturn(true);

    final SnapSyncChainDownloader downloader = downloader(initialPivot);
    downloader.onPivotUpdated(rolledBackPivot);
    downloader.onWorldStateHealFinished();

    downloader.start().get(5, TimeUnit.SECONDS);

    // Canonical index entries above the new pivot belong to the old pivot and must be dropped.
    verify(blockchain).unsafeRemoveCanonicalIndexRange(900L, 1000L);
    // Stage 1 ran once, for the original pivot only: the headers for the rolled-back pivot are
    // already present, so no second backward download is started.
    verify(pipelineFactory, times(1)).createBackwardHeaderDownloadPipeline(any());

    final ChainSyncState persisted = loadPersistedState();
    assertThat(persisted.pivotBlockHeader().getNumber()).isEqualTo(900L);
    assertThat(persisted.headersDownloadComplete()).isTrue();
    // withCanonicalPivot preserves both anchors.
    assertThat(persisted.headerDownloadAnchor().getNumber()).isEqualTo(0L);
    assertThat(persisted.bodyCheckpoint().getNumber()).isEqualTo(0L);
  }

  @Test
  void pivotRollbackOntoNonCanonicalBlockRestartsStage1BelowTheNewPivot() throws Exception {
    final BlockHeader rolledBackPivot = header(900);
    final BlockHeader headerBelowNewPivot = header(899);
    setupSuccessfulPipelineMocks();
    lenient().when(blockchain.blockIsOnCanonicalChain(rolledBackPivot.getHash())).thenReturn(false);
    // Lenient: Stage 2's binary search probes other heights, which must answer "absent".
    lenient().when(blockchain.getBlockHeader(899L)).thenReturn(Optional.of(headerBelowNewPivot));

    final SnapSyncChainDownloader downloader = downloader(initialPivot);
    downloader.onPivotUpdated(rolledBackPivot);
    downloader.onWorldStateHealFinished();

    downloader.start().get(5, TimeUnit.SECONDS);

    verify(blockchain).unsafeRemoveCanonicalIndexRange(900L, 1000L);

    final ArgumentCaptor<ChainSyncState> captor = ArgumentCaptor.forClass(ChainSyncState.class);
    verify(pipelineFactory, times(2)).createBackwardHeaderDownloadPipeline(captor.capture());
    final ChainSyncState restarted = captor.getAllValues().get(1);

    // The new pivot sits inside the already-complete chain, so Stage 1 anchors just below it and
    // re-downloads exactly the changed header.
    assertThat(restarted.pivotBlockHeader().getNumber()).isEqualTo(900L);
    assertThat(restarted.headerDownloadAnchor().getNumber()).isEqualTo(899L);
    assertThat(restarted.headersDownloadComplete()).isFalse();
  }

  @Test
  void pivotRollbackFailsWhenNoHeaderIsStoredBelowTheNewPivot() {
    final BlockHeader rolledBackPivot = header(900);
    setupSuccessfulPipelineMocks();
    lenient().when(blockchain.blockIsOnCanonicalChain(rolledBackPivot.getHash())).thenReturn(false);
    // Nothing stored at 899 (the mock's default answer): the local chain cannot be reconnected to
    // the new pivot, so the download must surface a WrongChainException and let snap sync re-pivot.

    final SnapSyncChainDownloader downloader = downloader(initialPivot);
    downloader.onPivotUpdated(rolledBackPivot);
    downloader.onWorldStateHealFinished();

    assertThatThrownBy(() -> downloader.start().get(5, TimeUnit.SECONDS))
        .hasRootCauseInstanceOf(WrongChainException.class);

    verify(pipelineFactory, times(1)).createBackwardHeaderDownloadPipeline(any());
  }

  @Test
  void worldStateCompletionIsCheckedBeforeParkingInTheWaitLoop() throws Exception {
    setupSuccessfulPipelineMocks();
    final SnapWorldDownloadState worldDownloadState = mock(SnapWorldDownloadState.class);

    final SnapSyncChainDownloader downloader = downloader(initialPivot);
    downloader.setWorldDownloadState(worldDownloadState);
    downloader.onWorldStateHealFinished();

    downloader.start().get(5, TimeUnit.SECONDS);

    // Healing must be triggered proactively rather than only on the next pivot update, otherwise a
    // world state that became complete during Stage 2 waits a full cycle before healing starts.
    verify(worldDownloadState).checkCompletion(initialPivot);
  }

  @Test
  void pivotUpdatesArrivingBeforeTheLoopRunsAreCoalescedToTheLatest() throws Exception {
    setupSuccessfulPipelineMocks();

    final SnapSyncChainDownloader downloader = downloader(initialPivot);
    downloader.onPivotUpdated(header(2000));
    downloader.onPivotUpdated(header(3000));
    downloader.onWorldStateHealFinished();

    downloader.start().get(5, TimeUnit.SECONDS);

    final ArgumentCaptor<ChainSyncState> captor = ArgumentCaptor.forClass(ChainSyncState.class);
    verify(pipelineFactory, times(2)).createBackwardHeaderDownloadPipeline(captor.capture());
    // Only the newest pivot is downloaded; the superseded #2000 update never starts a cycle.
    assertThat(captor.getAllValues().get(1).pivotBlockHeader().getNumber()).isEqualTo(3000L);
    verify(pipelineFactory, never())
        .createForwardBodiesAndReceiptsDownloadPipeline(anyLong(), eq(header(2000)), any());
  }

  private SnapSyncChainDownloader downloader(final BlockHeader pivotHeader) {
    return new SnapSyncChainDownloader(
        pipelineFactory,
        syncConfig,
        protocolSchedule,
        protocolContext,
        ethContext,
        syncState,
        syncDurationMetrics,
        pivotHeader,
        chainSyncStateStorage,
        headerDownloader);
  }

  private ChainSyncState loadPersistedState() {
    return chainSyncStateStorage.loadState(
        rlp -> BlockHeader.readFrom(rlp, new MainnetBlockHeaderFunctions()));
  }

  private static BlockHeader header(final long number) {
    return new BlockHeaderTestFixture().number(number).buildHeader();
  }

  @SuppressWarnings("unchecked")
  private void setupSuccessfulPipelineMocks() {
    final Pipeline<Long> backwardPipeline = mock(Pipeline.class);
    final BackwardHeaderDriver driver = mock(BackwardHeaderDriver.class);
    lenient().when(driver.getMatchedAncestor()).thenReturn(Optional.empty());
    lenient()
        .when(pipelineFactory.createBackwardHeaderDownloadPipeline(any()))
        .thenReturn(
            new SnapSyncChainDownloadPipelineFactory.BackwardHeaderPipelineResult(
                backwardPipeline, driver));
    final Pipeline<List<BlockHeader>> forwardPipeline = mock(Pipeline.class);
    lenient()
        .when(
            pipelineFactory.createForwardBodiesAndReceiptsDownloadPipeline(anyLong(), any(), any()))
        .thenReturn(forwardPipeline);
    lenient()
        .when(scheduler.startPipeline(any()))
        .thenReturn(CompletableFuture.completedFuture(null));
    lenient().when(ethPeers.peerCount()).thenReturn(1);
  }
}
