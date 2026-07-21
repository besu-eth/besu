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

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.common.BackwardHeaderDriver;
import org.hyperledger.besu.ethereum.eth.sync.common.ChainSyncState;
import org.hyperledger.besu.ethereum.eth.sync.common.ChainSyncStateStorage;
import org.hyperledger.besu.ethereum.eth.sync.common.CheckpointReorgException;
import org.hyperledger.besu.ethereum.eth.sync.common.SingleBlockHeaderDownloader;
import org.hyperledger.besu.ethereum.eth.sync.common.WrongChainException;
import org.hyperledger.besu.ethereum.eth.sync.common.checkpoint.Checkpoint;
import org.hyperledger.besu.ethereum.eth.sync.common.checkpoint.ImmutableCheckpoint;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.services.pipeline.Pipeline;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
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
  private BlockHeader pivotBlockHeader;
  private BlockHeader checkpointBlockHeader;
  // headerDownloadAnchor of a persisted (crashed) state; required to be non-null when stored.
  private BlockHeader crashedStateAnchor;
  private SynchronizerConfiguration syncConfig;

  @BeforeEach
  public void setUp() {
    pivotBlockHeader = new BlockHeaderTestFixture().number(1000).buildHeader();
    checkpointBlockHeader = new BlockHeaderTestFixture().number(500).buildHeader();
    crashedStateAnchor = new BlockHeaderTestFixture().number(0).buildHeader();
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
    // Allow ScheduleBasedBlockHeaderFunctions (used when loading state from storage) to hash
    // headers by delegating to MainnetBlockHeaderFunctions via the mocked protocol schedule.
    lenient().when(protocolSchedule.getByBlockHeader(any())).thenReturn(protocolSpec);
    lenient()
        .when(protocolSpec.getBlockHeaderFunctions())
        .thenReturn(new MainnetBlockHeaderFunctions());
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
  public void cancelShouldCompleteDownloadFutureWhenParkedInPivotUpdateLoop() {
    // Regression test for the world-state-stall hang: once Stage 1 and Stage 2 have completed, the
    // downloader parks in handlePivotUpdateLoop awaiting either a pivot update or the
    // world-state-heal-finished signal. A world-state stall cancels the chain download via
    // cancel(); that call MUST complete the chain-download future. Otherwise
    // allOf(worldStateFuture, chainFuture) in SnapSyncDownloader never completes, handleFailure
    // never runs, and snap sync never re-pivots (the node hangs).
    setupSuccessfulPipelineMocks();

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

    // Deliberately do NOT fire onWorldStateHealFinished() and do NOT supply a pivot update, so the
    // first cycle completes and the downloader parks in the pivot-update wait loop.
    final CompletableFuture<Void> downloadFuture = downloader.start();

    downloader.cancel();

    // Before the fix this blocks until the timeout; after the fix cancel() completes the future.
    assertThatThrownBy(() -> downloadFuture.get(5, TimeUnit.SECONDS))
        .hasRootCauseInstanceOf(CancellationException.class);
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
    //   1. Driver.getMatchedAncestor() returns a non-empty value (mocked here).
    //   2. Stage 1's thenApply reads it and applies the recovery-match anchor and the
    //      headers-download-complete flag in a single atomic updateAndGet/storeState.
    //   3. The persisted ChainSyncState (captured before the success-path cleanup) has
    //      headerDownloadAnchor() set to the matched ancestor while bodyCheckpoint() is preserved,
    //      and headersDownloadComplete() == true.
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
    // The matched ancestor must sit at or above the body checkpoint (500); recovery is never
    // allowed to reconnect below the trusted checkpoint.
    final BlockHeader matchedAncestor = new BlockHeaderTestFixture().number(600).buildHeader();

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

    // Capture every storeState() call. After the initial seed (1x) and the re-pivot restart (1x),
    // Stage 1 with a recovery match now writes a single consolidated store that applies the
    // recovery-match anchor (#600) and the headers-download-complete flag atomically.
    final ArgumentCaptor<ChainSyncState> stateCaptor =
        ArgumentCaptor.forClass(ChainSyncState.class);
    verify(spiedStorage, atLeastOnce()).storeState(stateCaptor.capture());

    final List<ChainSyncState> states = new ArrayList<>(stateCaptor.getAllValues());

    // First captured store is the seed initial state. Second is the re-pivot (headersComplete
    // reset to false so Stage 1 re-runs from the same pivot). Both have the original anchor.
    assertThat(states).hasSizeGreaterThanOrEqualTo(3);
    assertThat(states.get(0).bodyCheckpoint().getNumber()).isEqualTo(500L);
    assertThat(states.get(0).headersDownloadComplete()).isFalse();

    // After Stage 1 recovery: the recovery-match anchor (#600) and the headers-download-complete
    // flag are persisted together in one atomic updateAndGet/storeState (the previously separate
    // recovery-match and completion writes were consolidated). The body checkpoint is preserved at
    // #500. This is the last store before the success-path cleanup.
    final ChainSyncState afterRecoveryComplete = states.get(states.size() - 1);
    assertThat(afterRecoveryComplete.bodyCheckpoint().getNumber()).isEqualTo(500L);
    assertThat(afterRecoveryComplete.headerDownloadAnchor()).isNotNull();
    assertThat(afterRecoveryComplete.headerDownloadAnchor().getNumber()).isEqualTo(600L);
    assertThat(afterRecoveryComplete.headersDownloadComplete()).isTrue();

    // Stage 2 derives its start block from the canonical chain head (the default chain head at
    // #500 here), independently of the recovery match, which only moves the Stage 1 header anchor.
    verify(pipelineFactory).createForwardBodiesAndReceiptsDownloadPipeline(eq(500L), any(), any());
  }

  @Test
  public void shouldNotMutateAnchorWhenDriverReportsNoMatch() throws Exception {
    // Companion to shouldApplyRecoveryMatchAndPersistUpdatedAnchorsAfterStage1: when the driver
    // reports no matched ancestor (Optional.empty()), Stage 1's thenApply must NOT call
    // withRecoveryMatch. The bodyCheckpoint stays at the original 500. The only Stage 1
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

    // Seed store + re-pivot store + withHeadersDownloadComplete store = at least 3. There is
    // no withRecoveryMatch store since the driver reported no match.
    assertThat(states).hasSizeGreaterThanOrEqualTo(3);
    assertThat(states.get(0).bodyCheckpoint().getNumber()).isEqualTo(500L);
    assertThat(states.get(0).headersDownloadComplete()).isFalse();

    // Re-pivot is at index 1 (anchor unchanged, headersComplete reset to false).
    // withHeadersDownloadComplete is at index 2.
    final ChainSyncState afterStage1 = states.get(2);
    assertThat(afterStage1.bodyCheckpoint().getNumber()).isEqualTo(500L);
    assertThat(afterStage1.headersDownloadComplete()).isTrue();

    // Stage 2 was invoked with the pre-Stage-1 snapshot anchor (500), confirming Stage 2 reads
    // from ChainSyncState.bodyCheckpoint() as designed in C2.
    verify(pipelineFactory).createForwardBodiesAndReceiptsDownloadPipeline(eq(500L), any(), any());
  }

  // ── headersDownloadComplete == true, new pivot NOT on our chain ──────────────────

  @Test
  public void forwardShouldUseChainHeadAsBodiesAnchor() throws Exception {
    final BlockHeader oldPivot = new BlockHeaderTestFixture().number(1000).buildHeader();
    final BlockHeader storedBodyAnchor = new BlockHeaderTestFixture().number(500).buildHeader();
    final BlockHeader chainHeadAtCrash = new BlockHeaderTestFixture().number(800).buildHeader();
    final BlockHeader newPivot = new BlockHeaderTestFixture().number(1100).buildHeader();

    final ChainSyncState loadedState =
        new ChainSyncState(oldPivot, storedBodyAnchor, crashedStateAnchor, true);
    chainSyncStateStorage.storeState(loadedState);

    when(blockchain.blockIsOnCanonicalChain(newPivot.getHash())).thenReturn(false);
    // Stage 2 resumes from the chain head when the chain head already has a body stored.
    when(blockchain.getBlockBody(chainHeadAtCrash.getHash()))
        .thenReturn(Optional.of(mock(BlockBody.class)));
    when(blockchain.getChainHeadHeader()).thenReturn(chainHeadAtCrash);

    setupSuccessfulPipelineMocks();

    final SnapSyncChainDownloader downloader =
        new SnapSyncChainDownloader(
            pipelineFactory,
            syncConfig,
            protocolSchedule,
            protocolContext,
            ethContext,
            syncState,
            syncDurationMetrics,
            newPivot,
            chainSyncStateStorage,
            headerDownloader);

    downloader.onWorldStateHealFinished();
    downloader.start().get(5, TimeUnit.SECONDS);

    // Stage 1 initializes with the stored body anchor (500); the chain-head advancement
    // to 800 happens later, in Stage 2's recovery check.
    final ArgumentCaptor<ChainSyncState> captor = ArgumentCaptor.forClass(ChainSyncState.class);
    verify(pipelineFactory).createBackwardHeaderDownloadPipeline(captor.capture());
    final ChainSyncState stage1State = captor.getValue();

    assertThat(stage1State.pivotBlockHeader().getNumber()).isEqualTo(newPivot.getNumber());
    assertThat(stage1State.headerDownloadAnchor().getNumber()).isEqualTo(oldPivot.getNumber());
    assertThat(stage1State.bodyCheckpoint().getNumber()).isEqualTo(storedBodyAnchor.getNumber());
    assertThat(stage1State.headersDownloadComplete()).isFalse();

    // Stage 2 recovery detects the chain head (800) already has a body and resumes from it.
    verify(pipelineFactory)
        .createForwardBodiesAndReceiptsDownloadPipeline(
            eq(chainHeadAtCrash.getNumber()), any(), any());
  }

  @Test
  public void forwardShouldFallBackToStoredAnchorWhenChainHeadNotOnChain() throws Exception {
    final BlockHeader oldPivot = new BlockHeaderTestFixture().number(1000).buildHeader();
    final BlockHeader storedBodyAnchor = new BlockHeaderTestFixture().number(500).buildHeader();
    final BlockHeader chainHeadNotOnChain = new BlockHeaderTestFixture().number(800).buildHeader();
    final BlockHeader newPivot = new BlockHeaderTestFixture().number(1100).buildHeader();

    final ChainSyncState loadedState =
        new ChainSyncState(oldPivot, storedBodyAnchor, crashedStateAnchor, true);
    chainSyncStateStorage.storeState(loadedState);

    when(blockchain.blockIsOnCanonicalChain(newPivot.getHash())).thenReturn(false);
    // chainHead has no stored body (default Optional.empty), and no canonical block above the
    // stored anchor has a body, so Stage 2 falls back to the stored anchor.
    when(blockchain.getChainHeadHeader()).thenReturn(chainHeadNotOnChain);

    setupSuccessfulPipelineMocks();

    final SnapSyncChainDownloader downloader =
        new SnapSyncChainDownloader(
            pipelineFactory,
            syncConfig,
            protocolSchedule,
            protocolContext,
            ethContext,
            syncState,
            syncDurationMetrics,
            newPivot,
            chainSyncStateStorage,
            headerDownloader);

    downloader.onWorldStateHealFinished();
    downloader.start().get(5, TimeUnit.SECONDS);

    final ArgumentCaptor<ChainSyncState> captor = ArgumentCaptor.forClass(ChainSyncState.class);
    verify(pipelineFactory).createBackwardHeaderDownloadPipeline(captor.capture());
    final ChainSyncState stage1State = captor.getValue();

    assertThat(stage1State.pivotBlockHeader().getNumber()).isEqualTo(newPivot.getNumber());
    assertThat(stage1State.headerDownloadAnchor().getNumber()).isEqualTo(oldPivot.getNumber());
    assertThat(stage1State.bodyCheckpoint().getNumber()).isEqualTo(storedBodyAnchor.getNumber());
    assertThat(stage1State.headersDownloadComplete()).isFalse();
  }

  @Test
  public void reorgShouldUseStoredBodiesAnchorDirectly() throws Exception {
    // New pivot is LOWER than old pivot (reorg case).
    // bodyCheckpoint in loadedState was set to canonical common ancestor by a previous
    // Stage 1 recovery run. Stage 2 recovery checks the chain head but finds it at the anchor
    // level (no progress since the anchor was set), so no advancement happens.
    final BlockHeader newPivot = new BlockHeaderTestFixture().number(900).buildHeader();
    final BlockHeader oldPivot = new BlockHeaderTestFixture().number(1000).buildHeader();
    final BlockHeader storedBodyAnchor = new BlockHeaderTestFixture().number(500).buildHeader();
    final BlockHeader headerAt899 = new BlockHeaderTestFixture().number(899).buildHeader();

    final ChainSyncState loadedState =
        new ChainSyncState(oldPivot, storedBodyAnchor, crashedStateAnchor, true);
    chainSyncStateStorage.storeState(loadedState);

    when(blockchain.blockIsOnCanonicalChain(newPivot.getHash())).thenReturn(false);
    // Reorg: initialPivotHeader.number (900) < oldPivot.number (1000) →
    // headerAnchor = blockchain.getBlockHeader(899)
    when(blockchain.getBlockHeader(899L)).thenReturn(Optional.of(headerAt899));
    // Stage 2 recovery check: chain head is at storedBodyAnchor level → no advancement.
    when(blockchain.getChainHeadHeader()).thenReturn(storedBodyAnchor);

    setupSuccessfulPipelineMocks();

    final SnapSyncChainDownloader downloader =
        new SnapSyncChainDownloader(
            pipelineFactory,
            syncConfig,
            protocolSchedule,
            protocolContext,
            ethContext,
            syncState,
            syncDurationMetrics,
            newPivot,
            chainSyncStateStorage,
            headerDownloader);

    downloader.onWorldStateHealFinished();
    downloader.start().get(5, TimeUnit.SECONDS);

    final ArgumentCaptor<ChainSyncState> captor = ArgumentCaptor.forClass(ChainSyncState.class);
    verify(pipelineFactory).createBackwardHeaderDownloadPipeline(captor.capture());
    final ChainSyncState stage1State = captor.getValue();

    assertThat(stage1State.pivotBlockHeader().getNumber()).isEqualTo(newPivot.getNumber());
    // In the reorg case headerAnchor = getBlockHeader(newPivot - 1) = block 899, not oldPivot.
    assertThat(stage1State.headerDownloadAnchor().getNumber()).isEqualTo(headerAt899.getNumber());
    assertThat(stage1State.bodyCheckpoint().getNumber()).isEqualTo(storedBodyAnchor.getNumber());
    assertThat(stage1State.headersDownloadComplete()).isFalse();
    // Stage 2 starts from the stored anchor; chain head was at anchor level so no advancement.
    verify(pipelineFactory)
        .createForwardBodiesAndReceiptsDownloadPipeline(
            eq(storedBodyAnchor.getNumber()), any(), any());
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

  // ── Stage 1 not started ──────────────────────────────────────────────────────────

  @Test
  public void noProgressShouldReplaceWithNewPivot() throws Exception {
    final BlockHeader oldPivot = new BlockHeaderTestFixture().number(1000).buildHeader();
    final BlockHeader bodyAnchor = new BlockHeaderTestFixture().number(500).buildHeader();
    final BlockHeader newPivot = new BlockHeaderTestFixture().number(1100).buildHeader();

    // headersDownloadComplete=false, headerDownloadProgress=null
    final ChainSyncState loadedState =
        new ChainSyncState(oldPivot, bodyAnchor, crashedStateAnchor, false);
    chainSyncStateStorage.storeState(loadedState);

    setupSuccessfulPipelineMocks();

    final SnapSyncChainDownloader downloader =
        new SnapSyncChainDownloader(
            pipelineFactory,
            syncConfig,
            protocolSchedule,
            protocolContext,
            ethContext,
            syncState,
            syncDurationMetrics,
            newPivot,
            chainSyncStateStorage,
            headerDownloader);

    downloader.onWorldStateHealFinished();
    downloader.start().get(5, TimeUnit.SECONDS);

    final ArgumentCaptor<ChainSyncState> captor = ArgumentCaptor.forClass(ChainSyncState.class);
    verify(pipelineFactory, times(1)).createBackwardHeaderDownloadPipeline(captor.capture());

    final ChainSyncState stage1State = captor.getValue();
    assertThat(stage1State.pivotBlockHeader().getNumber()).isEqualTo(newPivot.getNumber());
    assertThat(stage1State.bodyCheckpoint().getNumber()).isEqualTo(bodyAnchor.getNumber());
    assertThat(stage1State.headersDownloadComplete()).isFalse();
  }

  // ── Stage 2 anchor recovery after restart ────────────────────────────────────────────

  @Test
  public void stage2RecoveryAdvancesAnchorWhenChainHeadIsCanonical() throws Exception {
    // Headers complete, new pivot not on canonical chain → stage2RecoveryCheckNeeded=true.
    // The previous session downloaded bodies up to block 800 before the process was killed.
    // On restart the chain head (800) is still canonical, so Stage 2 must resume from 800.
    final BlockHeader oldPivot = new BlockHeaderTestFixture().number(1000).buildHeader();
    final BlockHeader storedBodyAnchor = new BlockHeaderTestFixture().number(500).buildHeader();
    final BlockHeader chainHeadAtCrash = new BlockHeaderTestFixture().number(800).buildHeader();
    final BlockHeader newPivot = new BlockHeaderTestFixture().number(1100).buildHeader();

    final ChainSyncState loadedState =
        new ChainSyncState(oldPivot, storedBodyAnchor, crashedStateAnchor, true);
    chainSyncStateStorage.storeState(loadedState);

    when(blockchain.blockIsOnCanonicalChain(newPivot.getHash())).thenReturn(false);
    // Stage 2 resumes from the chain head when the chain head already has a body stored.
    when(blockchain.getBlockBody(chainHeadAtCrash.getHash()))
        .thenReturn(Optional.of(mock(BlockBody.class)));
    when(blockchain.getChainHeadHeader()).thenReturn(chainHeadAtCrash);

    setupSuccessfulPipelineMocks();

    final SnapSyncChainDownloader downloader =
        new SnapSyncChainDownloader(
            pipelineFactory,
            syncConfig,
            protocolSchedule,
            protocolContext,
            ethContext,
            syncState,
            syncDurationMetrics,
            newPivot,
            chainSyncStateStorage,
            headerDownloader);

    downloader.onWorldStateHealFinished();
    downloader.start().get(5, TimeUnit.SECONDS);

    // Stage 2 must start from the chain head (800), not the stale persisted anchor (500).
    verify(pipelineFactory).createForwardBodiesAndReceiptsDownloadPipeline(eq(800L), any(), any());
  }

  @Test
  public void stage2RecoverySkipsAdvancementWhenChainHeadEqualsAnchor() throws Exception {
    // The chain head equals the persisted anchor — nothing was downloaded in
    // Stage 2 before the crash, so no advancement should happen.
    final BlockHeader oldPivot = new BlockHeaderTestFixture().number(1000).buildHeader();
    final BlockHeader storedBodyAnchor = new BlockHeaderTestFixture().number(500).buildHeader();
    final BlockHeader newPivot = new BlockHeaderTestFixture().number(1100).buildHeader();

    final ChainSyncState loadedState =
        new ChainSyncState(oldPivot, storedBodyAnchor, crashedStateAnchor, true);
    chainSyncStateStorage.storeState(loadedState);

    when(blockchain.blockIsOnCanonicalChain(newPivot.getHash())).thenReturn(false);
    when(blockchain.getChainHeadHeader()).thenReturn(storedBodyAnchor);

    setupSuccessfulPipelineMocks();

    final SnapSyncChainDownloader downloader =
        new SnapSyncChainDownloader(
            pipelineFactory,
            syncConfig,
            protocolSchedule,
            protocolContext,
            ethContext,
            syncState,
            syncDurationMetrics,
            newPivot,
            chainSyncStateStorage,
            headerDownloader);

    downloader.onWorldStateHealFinished();
    downloader.start().get(5, TimeUnit.SECONDS);

    verify(pipelineFactory).createForwardBodiesAndReceiptsDownloadPipeline(eq(500L), any(), any());
  }

  @Test
  public void stage2RecoveryBinarySearchFindsHighestBodyBlockWhenChainHeadNotCanonical()
      throws Exception {
    // After Stage 1 re-ran with a new pivot, canonical headers above block 502 changed.
    // Chain head (503) has no stored body. Binary search finds 502 as the highest block whose
    // canonical header still has a body stored.
    //
    // Search with bodyCheckpoint=500, head=503 (mid = low + (high - low) / 2):
    //   round 1: mid=501 — body present → best=501, low=502
    //   round 2: mid=502 — body present → best=502, low=503
    //   round 3: mid=503 — no body      → high=502; exit → highest=502
    final BlockHeader oldPivot = new BlockHeaderTestFixture().number(1000).buildHeader();
    final BlockHeader header500 = new BlockHeaderTestFixture().number(500).buildHeader();
    final BlockHeader header501 = new BlockHeaderTestFixture().number(501).buildHeader();
    final BlockHeader header502 = new BlockHeaderTestFixture().number(502).buildHeader();
    final BlockHeader header503 = new BlockHeaderTestFixture().number(503).buildHeader();
    final BlockHeader newPivot = new BlockHeaderTestFixture().number(1100).buildHeader();

    final ChainSyncState loadedState =
        new ChainSyncState(oldPivot, header500, crashedStateAnchor, true);
    chainSyncStateStorage.storeState(loadedState);

    when(blockchain.blockIsOnCanonicalChain(newPivot.getHash())).thenReturn(false);
    when(blockchain.getChainHeadHeader()).thenReturn(header503);

    // Chain head (503) has no body → fall into the binary search over (500, 503].
    lenient().when(blockchain.getBlockHeader(501L)).thenReturn(Optional.of(header501));
    lenient().when(blockchain.getBlockHeader(502L)).thenReturn(Optional.of(header502));
    lenient().when(blockchain.getBlockHeader(503L)).thenReturn(Optional.of(header503));
    // Lenient: the chain-head body probe calls getBlockBody(503) (unstubbed) before the search,
    // which would otherwise trip strict-stubbing argument matching against these stubs.
    lenient()
        .when(blockchain.getBlockBody(header501.getHash()))
        .thenReturn(Optional.of(mock(BlockBody.class)));
    lenient()
        .when(blockchain.getBlockBody(header502.getHash()))
        .thenReturn(Optional.of(mock(BlockBody.class)));
    // No stub for getBlockBody(header503): Mockito returns Optional.empty(), so 503 has no body.

    setupSuccessfulPipelineMocks();

    final SnapSyncChainDownloader downloader =
        new SnapSyncChainDownloader(
            pipelineFactory,
            syncConfig,
            protocolSchedule,
            protocolContext,
            ethContext,
            syncState,
            syncDurationMetrics,
            newPivot,
            chainSyncStateStorage,
            headerDownloader);

    downloader.onWorldStateHealFinished();
    downloader.start().get(5, TimeUnit.SECONDS);

    verify(pipelineFactory).createForwardBodiesAndReceiptsDownloadPipeline(eq(502L), any(), any());
  }

  @Test
  public void stage2RecoveryBinarySearchFallsBackToAnchorWhenNoBodiesPresent() throws Exception {
    // Chain head (502) has no stored body; no blocks between the anchor and the chain head
    // have bodies on the new canonical chain. Binary search returns the anchor itself.
    //
    // Search with bodyCheckpoint=500, head=502 (mid = low + (high - low) / 2):
    //   round 1: mid=501 — no body → high=500
    //   round 2: mid=500 — no body → high=499; exit → highest=500 (anchor)
    final BlockHeader oldPivot = new BlockHeaderTestFixture().number(1000).buildHeader();
    final BlockHeader header500 = new BlockHeaderTestFixture().number(500).buildHeader();
    final BlockHeader header501 = new BlockHeaderTestFixture().number(501).buildHeader();
    final BlockHeader header502 = new BlockHeaderTestFixture().number(502).buildHeader();
    final BlockHeader newPivot = new BlockHeaderTestFixture().number(1100).buildHeader();

    final ChainSyncState loadedState =
        new ChainSyncState(oldPivot, header500, crashedStateAnchor, true);
    chainSyncStateStorage.storeState(loadedState);

    when(blockchain.blockIsOnCanonicalChain(newPivot.getHash())).thenReturn(false);
    when(blockchain.getChainHeadHeader()).thenReturn(header502);

    lenient().when(blockchain.getBlockHeader(500L)).thenReturn(Optional.of(header500));
    lenient().when(blockchain.getBlockHeader(501L)).thenReturn(Optional.of(header501));
    // No stub for getBlockBody on any header: Mockito returns Optional.empty() by default.

    setupSuccessfulPipelineMocks();

    final SnapSyncChainDownloader downloader =
        new SnapSyncChainDownloader(
            pipelineFactory,
            syncConfig,
            protocolSchedule,
            protocolContext,
            ethContext,
            syncState,
            syncDurationMetrics,
            newPivot,
            chainSyncStateStorage,
            headerDownloader);

    downloader.onWorldStateHealFinished();
    downloader.start().get(5, TimeUnit.SECONDS);

    // No bodies found above anchor: Stage 2 falls back to the stored anchor (500).
    verify(pipelineFactory).createForwardBodiesAndReceiptsDownloadPipeline(eq(500L), any(), any());
  }

  // ── Checkpoint validation after the initial header download to genesis ───────────────────

  @Test
  @SuppressWarnings("unchecked")
  public void checkpointValidationFailsWhenHeaderAtCheckpointHeightDoesNotMatch() {
    // With a configured checkpoint (#500) the body checkpoint is that checkpoint (never the chain
    // head). After Stage 1 reaches genesis, the header stored at the checkpoint height must match
    // the trusted checkpoint. A mismatch means the pivot is not on the checkpoint's chain → fatal.
    final Checkpoint checkpoint =
        ImmutableCheckpoint.builder()
            .blockNumber(checkpointBlockHeader.getNumber())
            .blockHash(checkpointBlockHeader.getHash())
            .totalDifficulty(Difficulty.ONE)
            .build();
    when(syncState.getCheckpoint()).thenReturn(Optional.of(checkpoint));
    when(headerDownloader.downloadBlockHeader(checkpointBlockHeader.getHash()))
        .thenReturn(CompletableFuture.completedFuture(checkpointBlockHeader));

    final BlockHeader mismatchedAtCheckpoint =
        new BlockHeaderTestFixture().number(500).timestamp(1L).buildHeader();
    assertThat(mismatchedAtCheckpoint.getHash()).isNotEqualTo(checkpointBlockHeader.getHash());

    when(blockchain.getBlockHeader(500L)).thenReturn(Optional.of(mismatchedAtCheckpoint));

    // The check runs at the end of Stage 1, before Stage 2, so only the backward pipeline is used.
    final Pipeline<Long> backwardPipeline = mock(Pipeline.class);
    final BackwardHeaderDriver driver = mock(BackwardHeaderDriver.class);
    lenient().when(driver.getMatchedAncestor()).thenReturn(Optional.empty());
    when(pipelineFactory.createBackwardHeaderDownloadPipeline(any()))
        .thenReturn(
            new SnapSyncChainDownloadPipelineFactory.BackwardHeaderPipelineResult(
                backwardPipeline, driver));
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
            chainSyncStateStorage,
            headerDownloader);

    downloader.onWorldStateHealFinished();

    assertThatThrownBy(() -> downloader.start().get(5, TimeUnit.SECONDS))
        .hasRootCauseInstanceOf(CheckpointReorgException.class);
  }

  @SuppressWarnings("unchecked")
  private void setupSuccessfulPipelineMocks() {
    final Pipeline<Long> backwardPipeline = mock(Pipeline.class);
    final BackwardHeaderDriver driver = mock(BackwardHeaderDriver.class);
    lenient().when(driver.getMatchedAncestor()).thenReturn(Optional.empty());
    when(pipelineFactory.createBackwardHeaderDownloadPipeline(any()))
        .thenReturn(
            new SnapSyncChainDownloadPipelineFactory.BackwardHeaderPipelineResult(
                backwardPipeline, driver));
    final Pipeline<List<BlockHeader>> forwardPipeline = mock(Pipeline.class);
    when(pipelineFactory.createForwardBodiesAndReceiptsDownloadPipeline(anyLong(), any(), any()))
        .thenReturn(forwardPipeline);
    when(scheduler.startPipeline(any())).thenReturn(CompletableFuture.completedFuture(null));
    when(ethPeers.peerCount()).thenReturn(1);
  }
}
