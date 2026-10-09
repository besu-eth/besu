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
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Covers the snap/2 specific behaviour of {@link SnapSyncChainDownloader}: the block-access-list
 * download that runs between Stage 1 and Stage 2, and the pivot catch-up that downloads the headers
 * and BALs between two pivots for the snap/2 world state downloader, independently of the main
 * download cycle.
 */
@ExtendWith(MockitoExtension.class)
class SnapSyncChainDownloaderSnapV2Test {

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
  private BlockHeader initialPivot;
  private SynchronizerConfiguration syncConfig;
  private Pipeline<List<BlockHeader>> balPipeline;
  private Pipeline<List<BlockHeader>> forwardPipeline;
  private final Deque<Runnable> scheduledRetries = new ArrayDeque<>();

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    initialPivot = header(1000);
    chainSyncStateStorage = new ChainSyncStateStorage(tempDir);
    syncConfig = SynchronizerConfiguration.builder().build();

    lenient().when(protocolContext.getBlockchain()).thenReturn(blockchain);
    lenient().when(blockchain.getGenesisBlockHeader()).thenReturn(header(0));
    lenient().when(blockchain.getChainHeadBlockNumber()).thenReturn(500L);
    lenient().when(blockchain.getChainHeadHeader()).thenReturn(header(500));
    lenient().when(ethContext.getScheduler()).thenReturn(scheduler);
    lenient().when(ethContext.getEthPeers()).thenReturn(ethPeers);
    lenient().when(ethPeers.peerCount()).thenReturn(1);
    lenient().when(syncState.getCheckpoint()).thenReturn(Optional.empty());
    lenient().when(protocolSchedule.getByBlockHeader(any())).thenReturn(protocolSpec);
    lenient()
        .when(protocolSpec.getBlockHeaderFunctions())
        .thenReturn(new MainnetBlockHeaderFunctions());

    final Pipeline<Long> backwardPipeline = mock(Pipeline.class);
    final BackwardHeaderDriver driver = mock(BackwardHeaderDriver.class);
    lenient().when(driver.getMatchedAncestor()).thenReturn(Optional.empty());
    lenient()
        .when(pipelineFactory.createBackwardHeaderDownloadPipeline(any()))
        .thenReturn(
            new SnapSyncChainDownloadPipelineFactory.BackwardHeaderPipelineResult(
                backwardPipeline, driver));
    balPipeline = mock(Pipeline.class);
    forwardPipeline = mock(Pipeline.class);
    lenient()
        .when(pipelineFactory.createBlockAccessListDownloadPipeline(anyLong(), any()))
        .thenReturn(balPipeline);
    lenient()
        .when(
            pipelineFactory.createForwardBodiesAndReceiptsDownloadPipeline(anyLong(), any(), any()))
        .thenReturn(forwardPipeline);
    lenient()
        .when(scheduler.startPipeline(any()))
        .thenReturn(CompletableFuture.completedFuture(null));
    // Delayed retries are queued and run by runScheduledRetries().
    lenient()
        .when(scheduler.scheduleFutureTask(any(Supplier.class), any(Duration.class)))
        .thenAnswer(
            inv -> {
              final Supplier<CompletableFuture<Object>> task = inv.getArgument(0);
              final CompletableFuture<Object> result = new CompletableFuture<>();
              scheduledRetries.add(
                  () ->
                      task.get()
                          .whenComplete(
                              (value, error) -> {
                                if (error != null) {
                                  result.completeExceptionally(error);
                                } else {
                                  result.complete(value);
                                }
                              }));
              return result;
            });
  }

  private void runScheduledRetries() {
    Runnable retry;
    while ((retry = scheduledRetries.poll()) != null) {
      retry.run();
    }
  }

  @Test
  void blockAccessListsAreDownloadedBeforeBodiesWhenSnap2IsEnabled() throws Exception {
    lenient().when(pipelineFactory.isSnap2Enabled()).thenReturn(true);

    final SnapSyncChainDownloader downloader = downloader();
    downloader.onWorldStateHealFinished();
    downloader.start().get(5, TimeUnit.SECONDS);

    // BALs are downloaded from the same anchor as the bodies (the genesis body checkpoint here,
    // since nothing canonical above it has a body) and up to the pivot.
    verify(pipelineFactory).createBlockAccessListDownloadPipeline(0L, initialPivot);
    verify(pipelineFactory).createForwardBodiesAndReceiptsDownloadPipeline(eq(0L), any(), any());

    // Order matters: the block access lists must be present before bodies are imported.
    final InOrder inOrder = inOrder(scheduler);
    inOrder.verify(scheduler).startPipeline(balPipeline);
    inOrder.verify(scheduler).startPipeline(forwardPipeline);
  }

  @Test
  void blockAccessListDownloadIsSkippedWhenTheAnchorAlreadyReachedThePivot() throws Exception {
    lenient().when(pipelineFactory.isSnap2Enabled()).thenReturn(true);
    // The chain head is the pivot itself and already has a body: there is nothing left to fetch.
    lenient().when(blockchain.getChainHeadHeader()).thenReturn(initialPivot);
    lenient().when(blockchain.blockIsOnCanonicalChain(initialPivot.getHash())).thenReturn(true);
    lenient()
        .when(blockchain.getBlockBody(initialPivot.getHash()))
        .thenReturn(Optional.of(mock(BlockBody.class)));

    final SnapSyncChainDownloader downloader = downloader();
    downloader.onWorldStateHealFinished();
    downloader.start().get(5, TimeUnit.SECONDS);

    verify(pipelineFactory, never()).createBlockAccessListDownloadPipeline(anyLong(), any());
    verify(pipelineFactory, never())
        .createForwardBodiesAndReceiptsDownloadPipeline(anyLong(), any(), any());
  }

  @Test
  void pivotCatchupIsRejectedWhenTheNewPivotDoesNotAdvance() {
    final SnapSyncChainDownloader downloader = downloader();

    final CompletableFuture<Void> catchup =
        downloader.preparePivotCatchup(initialPivot, initialPivot);

    assertThat(catchup).isCompletedExceptionally();
    assertThatThrownBy(() -> catchup.get(1, TimeUnit.SECONDS))
        .hasCauseInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void pivotCatchupDoesNotNeedTheChainDownloadToHaveStarted() throws Exception {
    // The world state can request a catch-up while the chain downloader is still loading its
    // state, e.g. right after a restart.
    final BlockHeader catchupPivot = header(2000);
    final SnapSyncChainDownloader downloader = downloader();
    catchupHeaderDownload(catchupPivot, CompletableFuture.completedFuture(null));
    catchupBalDownload(catchupPivot);

    downloader.preparePivotCatchup(initialPivot, catchupPivot).get(5, TimeUnit.SECONDS);
  }

  @Test
  void pivotCatchupStartsNoDownloadAfterCancellation() {
    final SnapSyncChainDownloader downloader = downloader();
    downloader.cancel();

    final CompletableFuture<Void> catchup =
        downloader.preparePivotCatchup(initialPivot, header(2000));

    assertThatThrownBy(() -> catchup.get(1, TimeUnit.SECONDS))
        .hasRootCauseInstanceOf(CancellationException.class);
    verify(scheduler, never()).startPipeline(any());
  }

  @Test
  void pivotCatchupIsRejectedWhileAnotherCatchupIsStillInFlight() {
    final SnapSyncChainDownloader downloader = downloader();
    startWithCycleBlockedInStage2(downloader);
    catchupHeaderDownload(header(2000), new CompletableFuture<>());

    final CompletableFuture<Void> firstCatchup =
        downloader.preparePivotCatchup(initialPivot, header(2000));
    final CompletableFuture<Void> secondCatchup =
        downloader.preparePivotCatchup(initialPivot, header(3000));

    assertThat(firstCatchup).isNotCompleted();
    assertThatThrownBy(() -> secondCatchup.get(1, TimeUnit.SECONDS))
        .hasCauseInstanceOf(IllegalStateException.class);
  }

  @Test
  void pivotCatchupCompletesWhileTheMainDownloadCycleIsStillRunning() throws Exception {
    final BlockHeader catchupPivot = header(2000);
    final SnapSyncChainDownloader downloader = downloader();
    final CompletableFuture<Void> chainDownload = startWithCycleBlockedInStage2(downloader);
    catchupHeaderDownload(catchupPivot, CompletableFuture.completedFuture(null));
    final Pipeline<List<BlockHeader>> catchupBalPipeline = catchupBalDownload(catchupPivot);

    final CompletableFuture<Void> catchup =
        downloader.preparePivotCatchup(initialPivot, catchupPivot);

    // The catch-up must not wait for the main cycle: on the first cycle that is the whole chain.
    catchup.get(5, TimeUnit.SECONDS);
    assertThat(chainDownload).isNotDone();
    verify(pipelineFactory)
        .createBackwardHeaderDownloadPipeline(
            argThat(
                s ->
                    s.pivotBlockHeader().equals(catchupPivot)
                        && s.headerDownloadAnchor().equals(initialPivot)
                        // the current pivot as checkpoint disables recovery below it
                        && s.bodyCheckpoint().equals(initialPivot)));
    verify(pipelineFactory)
        .createBlockAccessListDownloadPipeline(initialPivot.getNumber(), catchupPivot);
    verify(scheduler).startPipeline(catchupBalPipeline);
  }

  @Test
  void pivotCatchupIsDeferredToTheDownloadCycleWhenTheCurrentPivotWasReorgedOut() throws Exception {
    lenient().when(pipelineFactory.isSnap2Enabled()).thenReturn(true);
    final BlockHeader catchupPivot = header(2000);
    final SnapSyncChainDownloader downloader = downloader();
    catchupHeaderDownload(
        catchupPivot, CompletableFuture.failedFuture(new WrongChainException("reorged out")));

    final CompletableFuture<Void> catchup =
        downloader.preparePivotCatchup(initialPivot, catchupPivot);

    // Not failed: the main cycle recovers from the reorg in order and completes the request once
    // its BAL download reaches the new pivot.
    assertThat(catchup).isNotDone();
    downloader.onWorldStateHealFinished();
    downloader.start().get(5, TimeUnit.SECONDS);
    catchup.get(5, TimeUnit.SECONDS);
    verify(pipelineFactory).createBlockAccessListDownloadPipeline(anyLong(), eq(catchupPivot));
  }

  @Test
  @SuppressWarnings("unchecked")
  void pivotCatchupKeepsRetryingUntilTheDownloadSucceeds() throws Exception {
    // A failed catch-up fails the snap/2 world state download, which restarts from scratch, so
    // the catch-up must never give up on its own.
    final BlockHeader catchupPivot = header(2000);
    final SnapSyncChainDownloader downloader = downloader();
    startWithCycleBlockedInStage2(downloader);
    final Pipeline<Long> catchupHeaderPipeline = mock(Pipeline.class);
    final CompletableFuture<Void> timeout =
        CompletableFuture.failedFuture(new TimeoutException("BALs timed out"));
    catchupHeaderDownload(catchupPivot, catchupHeaderPipeline, timeout);
    when(scheduler.startPipeline(catchupHeaderPipeline))
        .thenReturn(
            timeout,
            timeout,
            timeout,
            timeout,
            timeout,
            timeout,
            timeout,
            timeout,
            CompletableFuture.completedFuture(null));
    catchupBalDownload(catchupPivot);

    final CompletableFuture<Void> catchup =
        downloader.preparePivotCatchup(initialPivot, catchupPivot);
    assertThat(catchup).isNotDone();
    runScheduledRetries();

    catchup.get(5, TimeUnit.SECONDS);
    verify(scheduler, times(9)).startPipeline(catchupHeaderPipeline);
  }

  @Test
  @SuppressWarnings("unchecked")
  void pivotCatchupStopsRetryingWhenTheChainDownloadIsCancelled() {
    final BlockHeader catchupPivot = header(2000);
    final SnapSyncChainDownloader downloader = downloader();
    startWithCycleBlockedInStage2(downloader);
    final Pipeline<Long> catchupHeaderPipeline = mock(Pipeline.class);
    catchupHeaderDownload(
        catchupPivot,
        catchupHeaderPipeline,
        CompletableFuture.failedFuture(new TimeoutException("BALs timed out")));
    final CompletableFuture<Void> catchup =
        downloader.preparePivotCatchup(initialPivot, catchupPivot);

    downloader.cancel();
    runScheduledRetries();

    assertThatThrownBy(() -> catchup.get(1, TimeUnit.SECONDS))
        .isInstanceOf(CancellationException.class);
    verify(scheduler, times(1)).startPipeline(catchupHeaderPipeline);
  }

  @Test
  void pivotCatchupRetryDelayDoublesUpToTheCap() {
    assertThat(
            IntStream.rangeClosed(1, 8)
                .mapToObj(SnapSyncChainDownloader::catchupRetryDelay)
                .map(Duration::toSeconds)
                .toList())
        .containsExactly(1L, 2L, 4L, 8L, 16L, 30L, 30L, 30L);
  }

  @Test
  @SuppressWarnings("unchecked")
  void pivotCatchupRetriesATransientDownloadFailure() throws Exception {
    // e.g. a RocksDB write conflict with the main cycle downloading the same range on restart
    final BlockHeader catchupPivot = header(2000);
    final SnapSyncChainDownloader downloader = downloader();
    startWithCycleBlockedInStage2(downloader);
    final Pipeline<Long> catchupHeaderPipeline = mock(Pipeline.class);
    catchupHeaderDownload(
        catchupPivot,
        catchupHeaderPipeline,
        CompletableFuture.failedFuture(new IllegalStateException("Busy")));
    when(scheduler.startPipeline(catchupHeaderPipeline))
        .thenReturn(
            CompletableFuture.failedFuture(new IllegalStateException("Busy")),
            CompletableFuture.completedFuture(null));
    catchupBalDownload(catchupPivot);

    final CompletableFuture<Void> catchup =
        downloader.preparePivotCatchup(initialPivot, catchupPivot);
    runScheduledRetries();

    catchup.get(5, TimeUnit.SECONDS);
    verify(pipelineFactory).createBlockAccessListDownloadPipeline(anyLong(), eq(catchupPivot));
  }

  @Test
  @SuppressWarnings("unchecked")
  void pendingPivotCatchupFailsWhenTheChainDownloadIsCancelled() {
    final SnapSyncChainDownloader downloader = downloader();
    startWithCycleBlockedInStage2(downloader);
    final Pipeline<Long> catchupHeaderPipeline = mock(Pipeline.class);
    catchupHeaderDownload(header(2000), catchupHeaderPipeline, new CompletableFuture<>());
    final CompletableFuture<Void> catchup =
        downloader.preparePivotCatchup(initialPivot, header(2000));

    downloader.cancel();

    // Leaving the request pending would park the snap/2 world state downloader forever.
    assertThatThrownBy(() -> catchup.get(1, TimeUnit.SECONDS))
        .isInstanceOf(CancellationException.class);
    verify(catchupHeaderPipeline).abort();
  }

  @Test
  void pivotCatchupIsAcceptedAgainAfterAPreviousRequestCompleted() throws Exception {
    final SnapSyncChainDownloader downloader = downloader();
    startWithCycleBlockedInStage2(downloader);
    catchupHeaderDownload(header(2000), CompletableFuture.completedFuture(null));
    catchupBalDownload(header(2000));
    downloader.preparePivotCatchup(initialPivot, header(2000)).get(5, TimeUnit.SECONDS);
    catchupHeaderDownload(header(3000), new CompletableFuture<>());

    // The in-flight guard must have been cleared, otherwise the next snap/2 pivot could never be
    // caught up with.
    final CompletableFuture<Void> secondCatchup =
        downloader.preparePivotCatchup(header(2000), header(3000));

    assertThat(secondCatchup).isNotCompletedExceptionally();
  }

  /** Starts the chain download and leaves its first cycle running in Stage 2 (bodies/receipts). */
  private CompletableFuture<Void> startWithCycleBlockedInStage2(
      final SnapSyncChainDownloader downloader) {
    lenient().when(pipelineFactory.isSnap2Enabled()).thenReturn(true);
    lenient().when(scheduler.startPipeline(forwardPipeline)).thenReturn(new CompletableFuture<>());
    final CompletableFuture<Void> chainDownload = downloader.start();
    verify(scheduler).startPipeline(forwardPipeline);
    return chainDownload;
  }

  @SuppressWarnings("unchecked")
  private BackwardHeaderDriver catchupHeaderDownload(
      final BlockHeader catchupPivot, final CompletableFuture<Void> result) {
    return catchupHeaderDownload(catchupPivot, mock(Pipeline.class), result);
  }

  private BackwardHeaderDriver catchupHeaderDownload(
      final BlockHeader catchupPivot,
      final Pipeline<Long> pipeline,
      final CompletableFuture<Void> result) {
    final BackwardHeaderDriver driver = mock(BackwardHeaderDriver.class);
    lenient().when(driver.getMatchedAncestor()).thenReturn(Optional.empty());
    when(pipelineFactory.createBackwardHeaderDownloadPipeline(
            argThat(
                s ->
                    s != null
                        && s.pivotBlockHeader().equals(catchupPivot)
                        && s.bodyCheckpoint().equals(s.headerDownloadAnchor()))))
        .thenReturn(
            new SnapSyncChainDownloadPipelineFactory.BackwardHeaderPipelineResult(
                pipeline, driver));
    when(scheduler.startPipeline(pipeline)).thenReturn(result);
    return driver;
  }

  @SuppressWarnings("unchecked")
  private Pipeline<List<BlockHeader>> catchupBalDownload(final BlockHeader catchupPivot) {
    final Pipeline<List<BlockHeader>> pipeline = mock(Pipeline.class);
    when(pipelineFactory.createBlockAccessListDownloadPipeline(anyLong(), eq(catchupPivot)))
        .thenReturn(pipeline);
    when(scheduler.startPipeline(pipeline)).thenReturn(CompletableFuture.completedFuture(null));
    return pipeline;
  }

  private SnapSyncChainDownloader downloader() {
    return new SnapSyncChainDownloader(
        pipelineFactory,
        syncConfig,
        protocolSchedule,
        protocolContext,
        ethContext,
        syncState,
        syncDurationMetrics,
        initialPivot,
        chainSyncStateStorage,
        headerDownloader);
  }

  private static BlockHeader header(final long number) {
    return new BlockHeaderTestFixture().number(number).buildHeader();
  }
}
