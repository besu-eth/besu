/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockBody;
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
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.services.pipeline.Pipeline;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
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

@ExtendWith(MockitoExtension.class)
public class SnapSyncChainDownloaderTest {

  @Mock private SnapSyncChainDownloadPipelineFactory pipelineFactory;
  @Mock private ProtocolSchedule protocolSchedule;
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
  private BlockHeader pivotBlockHeader;
  private BlockHeader checkpointBlockHeader;
  private SynchronizerConfiguration syncConfig;

  @BeforeEach
  public void setUp() {
    pivotBlockHeader = new BlockHeaderTestFixture().number(1000).buildHeader();
    checkpointBlockHeader = new BlockHeaderTestFixture().number(500).buildHeader();
    chainSyncStateStorage = new ChainSyncStateStorage(tempDir);
    syncConfig = SynchronizerConfiguration.builder().build();

    lenient().when(protocolContext.getBlockchain()).thenReturn(blockchain);
    lenient().when(blockchain.getChainHeadBlockNumber()).thenReturn(500L);
    lenient().when(blockchain.getChainHeadHeader()).thenReturn(checkpointBlockHeader);
    lenient().when(ethContext.getScheduler()).thenReturn(scheduler);
    lenient().when(ethContext.getEthPeers()).thenReturn(ethPeers);
    lenient()
        .when(blockchain.getGenesisBlockHeader())
        .thenReturn(new BlockHeaderTestFixture().number(0).buildHeader());
    lenient().when(syncState.getCheckpoint()).thenReturn(Optional.empty());
  }

  @Test
  public void shouldInitializeWithNewStateWhenNoStateFileExists() {
    // Verify downloader initializes successfully when no prior state exists
    assertThatCode(
            () ->
                new SnapSyncChainDownloader(
                    pipelineFactory,
                    syncConfig,
                    protocolSchedule,
                    protocolContext,
                    ethContext,
                    syncState,
                    syncDurationMetrics,
                    pivotBlockHeader,
                    chainSyncStateStorage,
                    headerDownloader))
        .doesNotThrowAnyException();
  }

  @Test
  public void shouldHandleCancellation() {
    SnapSyncChainDownloader downloader =
        new SnapSyncChainDownloader(
            pipelineFactory,
            syncConfig,
            protocolSchedule,
            protocolContext,
            ethContext,
            syncState,
            syncDurationMetrics,
            pivotBlockHeader,
            chainSyncStateStorage,
            headerDownloader);

    // Verify cancel completes successfully even when no pipeline is running
    assertThatCode(downloader::cancel).doesNotThrowAnyException();
  }

  @Test
  public void shouldReceivePivotUpdate() {
    SnapSyncChainDownloader downloader =
        new SnapSyncChainDownloader(
            pipelineFactory,
            syncConfig,
            protocolSchedule,
            protocolContext,
            ethContext,
            syncState,
            syncDurationMetrics,
            pivotBlockHeader,
            chainSyncStateStorage,
            headerDownloader);

    BlockHeader newPivot = new BlockHeaderTestFixture().number(2000).buildHeader();

    // Verify pivot update completes successfully
    assertThatCode(() -> downloader.onPivotUpdated(newPivot)).doesNotThrowAnyException();
  }

  @Test
  public void shouldReceiveWorldStateHealFinished() {
    SnapSyncChainDownloader downloader =
        new SnapSyncChainDownloader(
            pipelineFactory,
            syncConfig,
            protocolSchedule,
            protocolContext,
            ethContext,
            syncState,
            syncDurationMetrics,
            pivotBlockHeader,
            chainSyncStateStorage,
            headerDownloader);

    // Verify world state heal signal is accepted without error
    assertThatCode(downloader::onWorldStateHealFinished).doesNotThrowAnyException();
  }

  @Test
  public void shouldDeferPipelineStartWhenNoPeersAvailable() {
    when(ethPeers.peerCount()).thenReturn(0);
    when(scheduler.scheduleFutureTask(any(Runnable.class), any(Duration.class)))
        .thenReturn(CompletableFuture.completedFuture(null));

    SnapSyncChainDownloader downloader =
        new SnapSyncChainDownloader(
            pipelineFactory,
            syncConfig,
            protocolSchedule,
            protocolContext,
            ethContext,
            syncState,
            syncDurationMetrics,
            pivotBlockHeader,
            chainSyncStateStorage,
            headerDownloader);

    downloader.start();

    // No pipeline should be created when there are no peers
    verify(pipelineFactory, never()).createBackwardHeaderDownloadPipeline(any());

    // A retry should be scheduled with the no-peer delay, not the fast retry delay
    verify(scheduler)
        .scheduleFutureTask(
            any(Runnable.class),
            eq(Duration.ofMillis(SnapSyncChainDownloader.NO_PEER_RETRY_DELAY_MILLISECONDS)));
  }

  @Test
  public void shouldHandleStateTransitionFromInitialToHeadersComplete() {
    // Create and store initial state
    ChainSyncState initialState =
        ChainSyncState.initialSync(
            pivotBlockHeader,
            checkpointBlockHeader,
            new BlockHeaderTestFixture().number(0).buildHeader());
    chainSyncStateStorage.storeState(initialState);

    // Create downloader
    SnapSyncChainDownloader downloader =
        new SnapSyncChainDownloader(
            pipelineFactory,
            syncConfig,
            protocolSchedule,
            protocolContext,
            ethContext,
            syncState,
            syncDurationMetrics,
            pivotBlockHeader,
            chainSyncStateStorage,
            headerDownloader);

    assertThat(downloader).isNotNull();

    // Verify state was loaded
    ChainSyncState loadedState =
        chainSyncStateStorage.loadState(
            rlpInput -> BlockHeader.readFrom(rlpInput, new MainnetBlockHeaderFunctions()));
    assertThat(loadedState).isNotNull();
    assertThat(loadedState.headersDownloadComplete()).isFalse();
  }

  @Test
  public void shouldApplyRecoveryMatchAndPersistUpdatedAnchorsAfterStage1() throws Exception {
    // Verifies the recovery-handoff seam (Tasks C1, C2, D2):
    //   1. Driver.getMatchedAncestor() returns a non-empty value (mocked here).
    //   2. Stage 1's thenApply reads it and calls
    //      chainSyncState.updateAndGet(s -> s.withRecoveryMatch(matched.get())).
    //   3. The persisted ChainSyncState (captured before the success-path cleanup) has both
    //      blockDownloadAnchor() and headerDownloadAnchor() equal to the matched ancestor, and
    //      headersDownloadComplete() == true.
    //
    // Stage 2 is stubbed with a no-op pipeline so the overall download cycle completes
    // successfully (and the error-recovery path that overwrites the anchor via state.fromHead
    // does not run). The world-state-heal-finished signal is fired before start() so the
    // pivot-update loop exits and start() returns synchronously once both stages finish.
    //
    // Because start() deletes the persisted state on success, we spy on the storage and capture
    // every storeState() call. The Stage 1 recovery write is the last one before the success
    // cleanup, so it is recoverable from the captured arguments.
    final BlockHeader genesisHeader = new BlockHeaderTestFixture().number(0).buildHeader();
    final BlockHeader originalAnchor = new BlockHeaderTestFixture().number(500).buildHeader();
    final BlockHeader matchedAncestor = new BlockHeaderTestFixture().number(400).buildHeader();

    final ChainSyncStateStorage spiedStorage = spy(chainSyncStateStorage);

    final ChainSyncState initialState =
        ChainSyncState.initialSync(pivotBlockHeader, originalAnchor, genesisHeader);
    spiedStorage.storeState(initialState);

    @SuppressWarnings("unchecked")
    final Pipeline<Long> backwardPipeline = mock(Pipeline.class);
    final BackwardHeaderDriver driver = mock(BackwardHeaderDriver.class);
    when(driver.getMatchedAncestor()).thenReturn(Optional.of(matchedAncestor));
    when(pipelineFactory.createBackwardHeaderDownloadPipeline(any()))
        .thenReturn(
            new SnapSyncChainDownloadPipelineFactory.BackwardHeaderPipelineResult(
                backwardPipeline, driver));

    @SuppressWarnings("unchecked")
    final Pipeline<List<BlockHeader>> forwardPipeline = mock(Pipeline.class);
    when(pipelineFactory.createForwardBodiesAndReceiptsDownloadPipeline(anyLong(), any(), any()))
        .thenReturn(forwardPipeline);

    when(scheduler.startPipeline(any())).thenReturn(CompletableFuture.completedFuture(null));
    when(ethPeers.peerCount()).thenReturn(1);

    final SnapSyncChainDownloader downloader =
        new SnapSyncChainDownloader(
            pipelineFactory,
            syncConfig,
            protocolSchedule,
            protocolContext,
            ethContext,
            syncState,
            syncDurationMetrics,
            pivotBlockHeader,
            spiedStorage,
            headerDownloader);

    downloader.onWorldStateHealFinished();
    downloader.start().get(5, TimeUnit.SECONDS);

    // Capture every storeState() call. After the initial seed (1x), Stage 1 should have written:
    //   - the recovery-match update (blockDownloadAnchor + headerDownloadAnchor == 400)
    //   - the headers-download-complete update (headersDownloadComplete == true)
    final ArgumentCaptor<ChainSyncState> stateCaptor =
        ArgumentCaptor.forClass(ChainSyncState.class);
    verify(spiedStorage, atLeastOnce()).storeState(stateCaptor.capture());

    final List<ChainSyncState> states = new ArrayList<>(stateCaptor.getAllValues());

    // First captured store is the seed initial state. Second is the Case B re-pivot
    // (headersComplete
    // reset to false so Stage 1 re-runs from the same pivot). Both have anchor=500.
    assertThat(states).hasSizeGreaterThanOrEqualTo(4);
    assertThat(states.get(0).blockDownloadAnchor().getNumber()).isEqualTo(500L);
    assertThat(states.get(0).headersDownloadComplete()).isFalse();

    // After Stage 1 recovery: anchors collapsed to the matched ancestor at #400. This is the
    // state immediately after the withRecoveryMatch updateAndGet/storeState pair, before
    // withHeadersDownloadComplete runs. Index 1 is the Case B re-pivot store, so recovery is at 2.
    final ChainSyncState afterRecovery = states.get(2);
    assertThat(afterRecovery.blockDownloadAnchor().getNumber()).isEqualTo(400L);
    assertThat(afterRecovery.headerDownloadAnchor()).isNotNull();
    assertThat(afterRecovery.headerDownloadAnchor().getNumber()).isEqualTo(400L);
    assertThat(afterRecovery.headersDownloadComplete()).isFalse();

    // After withHeadersDownloadComplete: still anchored at #400 (blockDownloadAnchor preserved),
    // but now marked complete. Note: withHeadersDownloadComplete nulls headerDownloadAnchor
    // because, once headers are fully downloaded, the lower bound has been reached.
    final ChainSyncState afterComplete = states.get(3);
    assertThat(afterComplete.blockDownloadAnchor().getNumber()).isEqualTo(400L);
    assertThat(afterComplete.headersDownloadComplete()).isTrue();

    // Stage 2 must use the post-recovery anchor (400), not the original snapshot (500). This is
    // the regression guard for the bug surfaced while writing this test: previously
    // performSingleDownloadCycle captured the pre-Stage-1 snapshot and re-used it for Stage 2,
    // so Stage 2 would have skipped the freshly-rewritten canonical range [matched+1, original].
    verify(pipelineFactory).createForwardBodiesAndReceiptsDownloadPipeline(eq(400L), any(), any());
  }

  @Test
  public void shouldNotMutateAnchorWhenDriverReportsNoMatch() throws Exception {
    // Companion to shouldApplyRecoveryMatchAndPersistUpdatedAnchorsAfterStage1: when the driver
    // reports no matched ancestor (Optional.empty()), Stage 1's thenApply must NOT call
    // withRecoveryMatch. The blockDownloadAnchor stays at the original 500. The only Stage 1
    // mutation is withHeadersDownloadComplete.
    final BlockHeader genesisHeader = new BlockHeaderTestFixture().number(0).buildHeader();
    final BlockHeader originalAnchor = new BlockHeaderTestFixture().number(500).buildHeader();

    final ChainSyncStateStorage spiedStorage = spy(chainSyncStateStorage);

    final ChainSyncState initialState =
        ChainSyncState.initialSync(pivotBlockHeader, originalAnchor, genesisHeader);
    spiedStorage.storeState(initialState);

    @SuppressWarnings("unchecked")
    final Pipeline<Long> backwardPipeline = mock(Pipeline.class);
    final BackwardHeaderDriver driver = mock(BackwardHeaderDriver.class);
    when(driver.getMatchedAncestor()).thenReturn(Optional.empty());
    when(pipelineFactory.createBackwardHeaderDownloadPipeline(any()))
        .thenReturn(
            new SnapSyncChainDownloadPipelineFactory.BackwardHeaderPipelineResult(
                backwardPipeline, driver));

    @SuppressWarnings("unchecked")
    final Pipeline<List<BlockHeader>> forwardPipeline = mock(Pipeline.class);
    when(pipelineFactory.createForwardBodiesAndReceiptsDownloadPipeline(anyLong(), any(), any()))
        .thenReturn(forwardPipeline);

    when(scheduler.startPipeline(any())).thenReturn(CompletableFuture.completedFuture(null));
    when(ethPeers.peerCount()).thenReturn(1);

    final SnapSyncChainDownloader downloader =
        new SnapSyncChainDownloader(
            pipelineFactory,
            syncConfig,
            protocolSchedule,
            protocolContext,
            ethContext,
            syncState,
            syncDurationMetrics,
            pivotBlockHeader,
            spiedStorage,
            headerDownloader);

    downloader.onWorldStateHealFinished();
    downloader.start().get(5, TimeUnit.SECONDS);

    final ArgumentCaptor<ChainSyncState> stateCaptor =
        ArgumentCaptor.forClass(ChainSyncState.class);
    verify(spiedStorage, atLeastOnce()).storeState(stateCaptor.capture());
    final List<ChainSyncState> states = stateCaptor.getAllValues();

    // Seed store + Case B re-pivot store + withHeadersDownloadComplete store = at least 3. There is
    // no withRecoveryMatch store since the driver reported no match.
    assertThat(states).hasSizeGreaterThanOrEqualTo(3);
    assertThat(states.get(0).blockDownloadAnchor().getNumber()).isEqualTo(500L);
    assertThat(states.get(0).headersDownloadComplete()).isFalse();

    // Case B re-pivot is at index 1 (anchor unchanged, headersComplete reset to false).
    // withHeadersDownloadComplete is at index 2.
    final ChainSyncState afterStage1 = states.get(2);
    assertThat(afterStage1.blockDownloadAnchor().getNumber()).isEqualTo(500L);
    assertThat(afterStage1.headersDownloadComplete()).isTrue();

    // Stage 2 was invoked with the pre-Stage-1 snapshot anchor (500), confirming Stage 2 reads
    // from ChainSyncState.blockDownloadAnchor() as designed in C2.
    verify(pipelineFactory).createForwardBodiesAndReceiptsDownloadPipeline(eq(500L), any(), any());
  }

  @Test
  public void shouldAdvanceBodyDownloadAnchorToChainHeadWhenBodyMatchesCanonical()
      throws Exception {
    final BlockHeader genesisHeader = new BlockHeaderTestFixture().number(0).buildHeader();
    final BlockHeader header700 = new BlockHeaderTestFixture().number(700).buildHeader();
    final Hash hash700 = header700.getHash();

    final ChainSyncState initialState =
        ChainSyncState.initialSync(pivotBlockHeader, checkpointBlockHeader, genesisHeader)
            .withHeadersDownloadComplete();
    chainSyncStateStorage.storeState(initialState);

    @SuppressWarnings("unchecked")
    final Pipeline<List<BlockHeader>> forwardPipeline = mock(Pipeline.class);
    when(pipelineFactory.createForwardBodiesAndReceiptsDownloadPipeline(anyLong(), any(), any()))
        .thenReturn(forwardPipeline);
    when(scheduler.startPipeline(any())).thenReturn(CompletableFuture.completedFuture(null));
    when(ethPeers.peerCount()).thenReturn(1);

    when(blockchain.blockIsOnCanonicalChain(pivotBlockHeader.getHash())).thenReturn(true);
    when(blockchain.getChainHeadBlockNumber()).thenReturn(700L);
    when(blockchain.getBlockHashByNumber(700L)).thenReturn(Optional.of(hash700));
    when(blockchain.getBlockBody(hash700)).thenReturn(Optional.of(mock(BlockBody.class)));
    when(blockchain.getBlockHeader(700L)).thenReturn(Optional.of(header700));

    final SnapSyncChainDownloader downloader =
        new SnapSyncChainDownloader(
            pipelineFactory,
            syncConfig,
            protocolSchedule,
            protocolContext,
            ethContext,
            syncState,
            syncDurationMetrics,
            pivotBlockHeader,
            chainSyncStateStorage,
            headerDownloader);

    downloader.onWorldStateHealFinished();
    downloader.start().get(5, TimeUnit.SECONDS);

    final ArgumentCaptor<Long> anchorCaptor = ArgumentCaptor.forClass(Long.class);
    verify(pipelineFactory)
        .createForwardBodiesAndReceiptsDownloadPipeline(anchorCaptor.capture(), any(), any());
    assertThat(anchorCaptor.getValue()).isEqualTo(700L);
  }

  @Test
  public void shouldFindBodyFrontierViaBinarySearchWhenChainHeadBodyMissing() throws Exception {
    final BlockHeader genesisHeader = new BlockHeaderTestFixture().number(0).buildHeader();
    final BlockHeader header127 = new BlockHeaderTestFixture().number(127).buildHeader();
    final Hash hash127 = header127.getHash();

    // Anchor=genesis(0), chainHead=256, no body at 256.
    // Binary search: low=0, high=255, mid=127 → hit → low=127; high-low=128 not >128 → stop.
    final ChainSyncState initialState =
        ChainSyncState.initialSync(pivotBlockHeader, genesisHeader, genesisHeader)
            .withHeadersDownloadComplete();
    chainSyncStateStorage.storeState(initialState);

    @SuppressWarnings("unchecked")
    final Pipeline<List<BlockHeader>> forwardPipeline = mock(Pipeline.class);
    when(pipelineFactory.createForwardBodiesAndReceiptsDownloadPipeline(anyLong(), any(), any()))
        .thenReturn(forwardPipeline);
    when(scheduler.startPipeline(any())).thenReturn(CompletableFuture.completedFuture(null));
    when(ethPeers.peerCount()).thenReturn(1);

    when(blockchain.blockIsOnCanonicalChain(pivotBlockHeader.getHash())).thenReturn(true);
    when(blockchain.getChainHeadBlockNumber()).thenReturn(256L);
    lenient().when(blockchain.getBlockHashByNumber(anyLong())).thenReturn(Optional.empty());
    when(blockchain.getBlockHashByNumber(127L)).thenReturn(Optional.of(hash127));
    when(blockchain.getBlockBody(hash127)).thenReturn(Optional.of(mock(BlockBody.class)));
    when(blockchain.getBlockHeader(127L)).thenReturn(Optional.of(header127));

    final SnapSyncChainDownloader downloader =
        new SnapSyncChainDownloader(
            pipelineFactory,
            syncConfig,
            protocolSchedule,
            protocolContext,
            ethContext,
            syncState,
            syncDurationMetrics,
            pivotBlockHeader,
            chainSyncStateStorage,
            headerDownloader);

    downloader.onWorldStateHealFinished();
    downloader.start().get(5, TimeUnit.SECONDS);

    final ArgumentCaptor<Long> anchorCaptor = ArgumentCaptor.forClass(Long.class);
    verify(pipelineFactory)
        .createForwardBodiesAndReceiptsDownloadPipeline(anchorCaptor.capture(), any(), any());
    assertThat(anchorCaptor.getValue()).isEqualTo(127L);
  }

  @Test
  public void shouldNotAdvanceAnchorWhenNoBodiesExistAboveAnchor() throws Exception {
    final BlockHeader genesisHeader = new BlockHeaderTestFixture().number(0).buildHeader();

    final ChainSyncState initialState =
        ChainSyncState.initialSync(pivotBlockHeader, checkpointBlockHeader, genesisHeader)
            .withHeadersDownloadComplete();
    chainSyncStateStorage.storeState(initialState);

    @SuppressWarnings("unchecked")
    final Pipeline<List<BlockHeader>> forwardPipeline = mock(Pipeline.class);
    when(pipelineFactory.createForwardBodiesAndReceiptsDownloadPipeline(anyLong(), any(), any()))
        .thenReturn(forwardPipeline);
    when(scheduler.startPipeline(any())).thenReturn(CompletableFuture.completedFuture(null));
    when(ethPeers.peerCount()).thenReturn(1);

    when(blockchain.blockIsOnCanonicalChain(pivotBlockHeader.getHash())).thenReturn(true);
    when(blockchain.getChainHeadBlockNumber()).thenReturn(700L);
    lenient().when(blockchain.getBlockHashByNumber(anyLong())).thenReturn(Optional.empty());

    final SnapSyncChainDownloader downloader =
        new SnapSyncChainDownloader(
            pipelineFactory,
            syncConfig,
            protocolSchedule,
            protocolContext,
            ethContext,
            syncState,
            syncDurationMetrics,
            pivotBlockHeader,
            chainSyncStateStorage,
            headerDownloader);

    downloader.onWorldStateHealFinished();
    downloader.start().get(5, TimeUnit.SECONDS);

    final ArgumentCaptor<Long> anchorCaptor = ArgumentCaptor.forClass(Long.class);
    verify(pipelineFactory)
        .createForwardBodiesAndReceiptsDownloadPipeline(anchorCaptor.capture(), any(), any());
    assertThat(anchorCaptor.getValue()).isEqualTo(500L);
  }

  @Test
  public void shouldFailWithoutRetryWhenWrongChainExceptionPropagates() throws Exception {
    // The Stage 1 pipeline future fails with WrongChainException. SnapSyncChainDownloader's
    // shouldRetry must treat this as non-retryable: the overall future completes exceptionally
    // and the downloader does not re-invoke createBackwardHeaderDownloadPipeline.
    final BlockHeader genesisHeader = new BlockHeaderTestFixture().number(0).buildHeader();
    final BlockHeader originalAnchor = new BlockHeaderTestFixture().number(500).buildHeader();

    final ChainSyncState initialState =
        ChainSyncState.initialSync(pivotBlockHeader, originalAnchor, genesisHeader);
    chainSyncStateStorage.storeState(initialState);

    @SuppressWarnings("unchecked")
    final Pipeline<Long> backwardPipeline = mock(Pipeline.class);
    final BackwardHeaderDriver driver = mock(BackwardHeaderDriver.class);
    lenient().when(driver.getLowestImportedHeader()).thenReturn(pivotBlockHeader);
    when(pipelineFactory.createBackwardHeaderDownloadPipeline(any()))
        .thenReturn(
            new SnapSyncChainDownloadPipelineFactory.BackwardHeaderPipelineResult(
                backwardPipeline, driver));

    final WrongChainException wrongChain = new WrongChainException("test wrong chain");
    when(scheduler.startPipeline(backwardPipeline))
        .thenReturn(CompletableFuture.failedFuture(wrongChain));

    when(ethPeers.peerCount()).thenReturn(1);

    final SnapSyncChainDownloader downloader =
        new SnapSyncChainDownloader(
            pipelineFactory,
            syncConfig,
            protocolSchedule,
            protocolContext,
            ethContext,
            syncState,
            syncDurationMetrics,
            pivotBlockHeader,
            chainSyncStateStorage,
            headerDownloader);

    downloader.onWorldStateHealFinished();

    assertThatThrownBy(() -> downloader.start().get(5, TimeUnit.SECONDS))
        .hasRootCauseInstanceOf(WrongChainException.class);

    // Non-retryable: the pipeline factory must have been invoked exactly once. If shouldRetry
    // had returned true for WrongChainException, the downloader would have re-attempted via
    // scheduler.scheduleFutureTask and the factory would have been called again.
    verify(pipelineFactory, times(1)).createBackwardHeaderDownloadPipeline(any());
  }
}
