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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
  private Pipeline<Long> backwardPipeline;
  private Pipeline<List<BlockHeader>> balPipeline;
  private Pipeline<List<BlockHeader>> forwardPipeline;
  private final Deque<Runnable> scheduledRetries = new ArrayDeque<>();
  private final List<CompletableFuture<?>> scheduledRetryResults = new ArrayList<>();

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
    // The catch-up's BAL step checks that the headers it downloaded are still indexed.
    lenient()
        .when(blockchain.getBlockHeader(any(Hash.class)))
        .thenAnswer(inv -> Optional.ofNullable(CHAIN.get(inv.<Hash>getArgument(0))));
    lenient().when(blockchain.blockIsOnCanonicalChain(any())).thenReturn(true);
    lenient().when(ethContext.getScheduler()).thenReturn(scheduler);
    lenient().when(ethContext.getEthPeers()).thenReturn(ethPeers);
    lenient().when(ethPeers.peerCount()).thenReturn(1);
    lenient().when(syncState.getCheckpoint()).thenReturn(Optional.empty());
    lenient().when(protocolSchedule.getByBlockHeader(any())).thenReturn(protocolSpec);
    lenient()
        .when(protocolSpec.getBlockHeaderFunctions())
        .thenReturn(new MainnetBlockHeaderFunctions());

    backwardPipeline = mock(Pipeline.class);
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
              scheduledRetryResults.add(result);
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
  void pivotCatchupDoesNotNeedTheChainDownloadToHaveStarted() {
    // The world state can request a catch-up while the chain downloader is still loading its
    // state, e.g. right after a restart.
    final BlockHeader catchupPivot = header(2000);
    final SnapSyncChainDownloader downloader = downloader();
    catchupHeaderDownload(catchupPivot, CompletableFuture.completedFuture(null));
    catchupBalDownload(catchupPivot);

    assertThat(downloader.preparePivotCatchup(initialPivot, catchupPivot)).isCompleted();
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
  void pivotCatchupCompletesWhileTheMainDownloadCycleIsStillRunning() {
    final BlockHeader catchupPivot = header(2000);
    final SnapSyncChainDownloader downloader = downloader();
    final CompletableFuture<Void> chainDownload = startWithCycleBlockedInStage2(downloader);
    catchupHeaderDownload(catchupPivot, CompletableFuture.completedFuture(null));
    final Pipeline<List<BlockHeader>> catchupBalPipeline = catchupBalDownload(catchupPivot);

    final CompletableFuture<Void> catchup =
        downloader.preparePivotCatchup(initialPivot, catchupPivot);

    // The catch-up must not wait for the main cycle: on the first cycle that is the whole chain.
    assertThat(catchup).isCompleted();
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
    assertThat(catchup).isCompleted();
    verify(pipelineFactory).createBlockAccessListDownloadPipeline(anyLong(), eq(catchupPivot));
  }

  @Test
  @SuppressWarnings("unchecked")
  void pivotCatchupRetriesUntilTheDownloadSucceedsOnTheLastAttempt() {
    // A failed catch-up fails the snap/2 world state download, which restarts from scratch, so
    // the catch-up retries for as long as the attempt limit allows.
    final BlockHeader catchupPivot = header(2000);
    final SnapSyncChainDownloader downloader = downloader();
    startWithCycleBlockedInStage2(downloader);
    final Pipeline<Long> catchupHeaderPipeline = mock(Pipeline.class);
    catchupHeaderDownload(catchupPivot, catchupHeaderPipeline, new CompletableFuture<>());
    final AtomicInteger attempts = new AtomicInteger();
    when(scheduler.startPipeline(catchupHeaderPipeline))
        .thenAnswer(
            inv ->
                attempts.incrementAndGet() < SnapSyncChainDownloader.MAX_CATCHUP_STEP_ATTEMPTS
                    ? CompletableFuture.failedFuture(new TimeoutException("headers timed out"))
                    : CompletableFuture.completedFuture(null));
    catchupBalDownload(catchupPivot);

    final CompletableFuture<Void> catchup =
        downloader.preparePivotCatchup(initialPivot, catchupPivot);
    assertThat(catchup).isNotDone();
    runScheduledRetries();

    assertThat(catchup).isCompleted();
    verify(scheduler, times(SnapSyncChainDownloader.MAX_CATCHUP_STEP_ATTEMPTS))
        .startPipeline(catchupHeaderPipeline);
  }

  @Test
  @SuppressWarnings("unchecked")
  void pivotCatchupWaitsForPeersWithoutUsingUpAttempts() {
    // A failed catch-up restarts the world state download, so a peer outage must not fail it.
    final BlockHeader catchupPivot = header(2000);
    final SnapSyncChainDownloader downloader = downloader();
    startWithCycleBlockedInStage2(downloader);
    final Pipeline<Long> catchupHeaderPipeline = mock(Pipeline.class);
    catchupHeaderDownload(catchupPivot, catchupHeaderPipeline, new CompletableFuture<>());
    final AtomicInteger attempts = new AtomicInteger();
    when(scheduler.startPipeline(catchupHeaderPipeline))
        .thenAnswer(
            inv ->
                attempts.incrementAndGet() < SnapSyncChainDownloader.MAX_CATCHUP_STEP_ATTEMPTS
                    ? CompletableFuture.failedFuture(new TimeoutException("headers timed out"))
                    : CompletableFuture.completedFuture(null));
    catchupBalDownload(catchupPivot);
    when(ethPeers.peerCount()).thenReturn(0);

    final CompletableFuture<Void> catchup =
        downloader.preparePivotCatchup(initialPivot, catchupPivot);
    for (int i = 0; i < 2 * SnapSyncChainDownloader.MAX_CATCHUP_STEP_ATTEMPTS; i++) {
      scheduledRetries.remove().run();
    }

    // No pipeline is started without peers, so no attempt is used up while waiting.
    assertThat(catchup).isNotDone();
    verify(scheduler, never()).startPipeline(catchupHeaderPipeline);

    // Once peers are back the step still has all its attempts.
    when(ethPeers.peerCount()).thenReturn(1);
    runScheduledRetries();

    assertThat(catchup).isCompleted();
    verify(scheduler, times(SnapSyncChainDownloader.MAX_CATCHUP_STEP_ATTEMPTS))
        .startPipeline(catchupHeaderPipeline);
  }

  @Test
  @SuppressWarnings("unchecked")
  void pivotCatchupFailsAfterTheMaximumNumberOfAttempts() {
    // A permanent error must not stall the sync silently: the world state download restarts.
    final BlockHeader catchupPivot = header(2000);
    final SnapSyncChainDownloader downloader = downloader();
    startWithCycleBlockedInStage2(downloader);
    final Pipeline<Long> catchupHeaderPipeline = mock(Pipeline.class);
    catchupHeaderDownload(catchupPivot, catchupHeaderPipeline, new CompletableFuture<>());
    when(scheduler.startPipeline(catchupHeaderPipeline))
        .thenAnswer(inv -> CompletableFuture.failedFuture(new IllegalStateException("permanent")));

    final CompletableFuture<Void> catchup =
        downloader.preparePivotCatchup(initialPivot, catchupPivot);
    runScheduledRetries();

    assertThatThrownBy(() -> catchup.get(1, TimeUnit.SECONDS))
        .hasRootCauseInstanceOf(IllegalStateException.class);
    verify(scheduler, times(SnapSyncChainDownloader.MAX_CATCHUP_STEP_ATTEMPTS))
        .startPipeline(catchupHeaderPipeline);
  }

  @Test
  @SuppressWarnings("unchecked")
  void pivotCatchupRetriesOnlyTheBalDownloadWhenItFails() {
    final BlockHeader catchupPivot = header(2000);
    final SnapSyncChainDownloader downloader = downloader();
    startWithCycleBlockedInStage2(downloader);
    catchupHeaderDownload(catchupPivot, CompletableFuture.completedFuture(null));
    final Pipeline<List<BlockHeader>> catchupBalPipeline = catchupBalDownload(catchupPivot);
    when(scheduler.startPipeline(catchupBalPipeline))
        .thenReturn(
            CompletableFuture.failedFuture(new TimeoutException("BALs timed out")),
            CompletableFuture.completedFuture(null));

    final CompletableFuture<Void> catchup =
        downloader.preparePivotCatchup(initialPivot, catchupPivot);
    runScheduledRetries();

    assertThat(catchup).isCompleted();
    // The headers were stored by the first attempt, so only the BAL download is repeated.
    verify(pipelineFactory, times(1))
        .createBackwardHeaderDownloadPipeline(
            argThat(s -> s != null && s.pivotBlockHeader().equals(catchupPivot)));
    verify(pipelineFactory, times(2))
        .createBlockAccessListDownloadPipeline(initialPivot.getNumber(), catchupPivot);
  }

  // The lowest, a middle and the highest entry of the catch-up range (1000, 2000].
  @ParameterizedTest
  @ValueSource(longs = {1001, 1500, 2000})
  @SuppressWarnings("unchecked")
  void pivotCatchupRedownloadsTrimmedHeaders(final long trimmedBlockNumber) {
    final BlockHeader catchupPivot = header(2000);
    final SnapSyncChainDownloader downloader = downloader();
    startWithCycleBlockedInStage2(downloader);
    final Pipeline<Long> catchupHeaderPipeline = mock(Pipeline.class);
    catchupHeaderDownload(
        catchupPivot, catchupHeaderPipeline, CompletableFuture.completedFuture(null));
    final Pipeline<List<BlockHeader>> catchupBalPipeline = catchupBalDownload(catchupPivot);
    // One of the range's index entries was removed by the main loop, or replaced by another fork's
    // header; all the others are still there.
    when(blockchain.blockIsOnCanonicalChain(header(trimmedBlockNumber).getHash()))
        .thenReturn(false);

    final CompletableFuture<Void> catchup =
        downloader.preparePivotCatchup(initialPivot, catchupPivot);

    assertThat(catchup).isCompleted();
    // Once by the header step and again by the BAL step, before it starts the BAL download.
    verify(pipelineFactory, times(2))
        .createBackwardHeaderDownloadPipeline(
            argThat(
                s ->
                    s != null
                        && s.pivotBlockHeader().equals(catchupPivot)
                        && s.headerDownloadAnchor().equals(initialPivot)
                        && s.bodyCheckpoint().equals(initialPivot)));
    final InOrder inOrder = inOrder(scheduler);
    inOrder.verify(scheduler, times(2)).startPipeline(catchupHeaderPipeline);
    inOrder.verify(scheduler).startPipeline(catchupBalPipeline);
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
    // Runs the retry even though it was cancelled, so the cancellation check is covered too.
    runScheduledRetries();

    assertThatThrownBy(() -> catchup.get(1, TimeUnit.SECONDS))
        .isInstanceOf(CancellationException.class);
    assertThat(scheduledRetryResults).singleElement().satisfies(r -> assertThat(r).isCancelled());
    // Creating the header pipeline stores the pivot header, so no retry may create one.
    verify(pipelineFactory, times(1))
        .createBackwardHeaderDownloadPipeline(
            argThat(s -> s != null && s.pivotBlockHeader().equals(catchupPivot)));
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
  void pivotCatchupRetriesATransientDownloadFailure() {
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

    assertThat(catchup).isCompleted();
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
  void pivotCatchupIsAcceptedAgainAfterAPreviousRequestCompleted() {
    final SnapSyncChainDownloader downloader = downloader();
    startWithCycleBlockedInStage2(downloader);
    catchupHeaderDownload(header(2000), CompletableFuture.completedFuture(null));
    catchupBalDownload(header(2000));
    assertThat(downloader.preparePivotCatchup(initialPivot, header(2000))).isCompleted();
    catchupHeaderDownload(header(3000), new CompletableFuture<>());

    // The in-flight guard must have been cleared, otherwise the next snap/2 pivot could never be
    // caught up with.
    final CompletableFuture<Void> secondCatchup =
        downloader.preparePivotCatchup(header(2000), header(3000));

    assertThat(secondCatchup).isNotCompletedExceptionally();
  }

  @Test
  void deferredCatchupCompletesWhenTheMainCycleSkipsTheBalDownload() throws Exception {
    // e.g. after a restart, the main cycle starts at the requested pivot and already holds its
    // bodies, so its BAL download returns early.
    lenient().when(pipelineFactory.isSnap2Enabled()).thenReturn(true);
    lenient().when(blockchain.getChainHeadHeader()).thenReturn(initialPivot);
    lenient().when(blockchain.blockIsOnCanonicalChain(initialPivot.getHash())).thenReturn(true);
    lenient()
        .when(blockchain.getBlockBody(initialPivot.getHash()))
        .thenReturn(Optional.of(mock(BlockBody.class)));
    final BlockHeader oldPivot = header(500);
    final SnapSyncChainDownloader downloader = downloader();
    deferredCatchupHeaderDownload(oldPivot);

    final CompletableFuture<Void> catchup = downloader.preparePivotCatchup(oldPivot, initialPivot);
    assertThat(catchup).isNotDone();
    downloader.onWorldStateHealFinished();
    downloader.start().get(5, TimeUnit.SECONDS);

    assertThat(catchup).isCompleted();
    verify(pipelineFactory, never()).createBlockAccessListDownloadPipeline(anyLong(), any());
  }

  @Test
  void deferredCatchupCompletesWhenItsPivotIsAlreadyCanonical() throws Exception {
    // The main cycle already downloaded past the requested pivot, so the deferred pivot update
    // finds it canonical and starts no new cycle.
    lenient().when(pipelineFactory.isSnap2Enabled()).thenReturn(true);
    final BlockHeader oldPivot = header(500);
    final BlockHeader catchupPivot = header(800);
    lenient().when(blockchain.blockIsOnCanonicalChain(catchupPivot.getHash())).thenReturn(true);
    final SnapSyncChainDownloader downloader = downloader();
    deferredCatchupHeaderDownload(oldPivot);

    final CompletableFuture<Void> catchup = downloader.preparePivotCatchup(oldPivot, catchupPivot);
    assertThat(catchup).isNotDone();
    downloader.onWorldStateHealFinished();
    downloader.start().get(5, TimeUnit.SECONDS);

    assertThat(catchup).isCompleted();
    verify(pipelineFactory).createBlockAccessListDownloadPipeline(anyLong(), eq(initialPivot));
    verify(pipelineFactory, never())
        .createBlockAccessListDownloadPipeline(anyLong(), eq(catchupPivot));
  }

  @Test
  @SuppressWarnings("unchecked")
  void deferredCatchupStopsTheMainCycleInStage2() throws Exception {
    // Without stopping it, the catch-up would wait for the whole first cycle: hours of bodies.
    lenient().when(pipelineFactory.isSnap2Enabled()).thenReturn(true);
    final BlockHeader catchupPivot = header(2000);
    final CompletableFuture<Void> stage2 = new CompletableFuture<>();
    // Like a real pipeline, aborting completes the download with a CancellationException.
    doAnswer(inv -> stage2.completeExceptionally(new CancellationException("Pipeline aborted")))
        .when(forwardPipeline)
        .abort();
    when(scheduler.startPipeline(forwardPipeline))
        .thenReturn(stage2, CompletableFuture.completedFuture(null));
    final SnapSyncChainDownloader downloader = downloader();
    final CompletableFuture<Void> chainDownload = downloader.start();
    catchupHeaderDownload(
        catchupPivot, CompletableFuture.failedFuture(new WrongChainException("reorged out")));

    final CompletableFuture<Void> catchup =
        downloader.preparePivotCatchup(initialPivot, catchupPivot);

    verify(forwardPipeline).abort();
    assertThat(catchup).isCompleted();
    verify(pipelineFactory).createBlockAccessListDownloadPipeline(anyLong(), eq(catchupPivot));
    // Stage 2 resumes, now up to the new pivot.
    verify(pipelineFactory)
        .createForwardBodiesAndReceiptsDownloadPipeline(anyLong(), eq(catchupPivot), any());
    downloader.onWorldStateHealFinished();
    chainDownload.get(5, TimeUnit.SECONDS);
  }

  @Test
  void cancelStopsStage2WhenItArrivesWhileStage2IsBeingCreated() {
    // cancel() lands after the cycle's last check but before Stage 2 is registered.
    lenient().when(pipelineFactory.isSnap2Enabled()).thenReturn(true);
    final SnapSyncChainDownloader downloader = downloader();
    when(pipelineFactory.createForwardBodiesAndReceiptsDownloadPipeline(anyLong(), any(), any()))
        .thenAnswer(
            inv -> {
              downloader.cancel();
              return forwardPipeline;
            });
    when(scheduler.startPipeline(forwardPipeline)).thenReturn(new CompletableFuture<>());

    downloader.start();

    verify(forwardPipeline).abort();
  }

  @Test
  @SuppressWarnings("unchecked")
  void deferredCatchupStopsStage2WhenItArrivesWhileStage2IsBeingCreated() throws Exception {
    // The pre-emption lands after the cycle's last check but before Stage 2 is registered.
    lenient().when(pipelineFactory.isSnap2Enabled()).thenReturn(true);
    final BlockHeader catchupPivot = header(2000);
    final SnapSyncChainDownloader downloader = downloader();
    catchupHeaderDownload(
        catchupPivot, CompletableFuture.failedFuture(new WrongChainException("reorged out")));
    final AtomicReference<CompletableFuture<Void>> catchup = new AtomicReference<>();
    when(pipelineFactory.createForwardBodiesAndReceiptsDownloadPipeline(anyLong(), any(), any()))
        .thenAnswer(
            inv -> {
              if (catchup.get() == null) {
                catchup.set(downloader.preparePivotCatchup(initialPivot, catchupPivot));
              }
              return forwardPipeline;
            });
    final CompletableFuture<Void> stage2 = new CompletableFuture<>();
    doAnswer(inv -> stage2.completeExceptionally(new CancellationException("Pipeline aborted")))
        .when(forwardPipeline)
        .abort();
    when(scheduler.startPipeline(forwardPipeline))
        .thenReturn(stage2, CompletableFuture.completedFuture(null));

    final CompletableFuture<Void> chainDownload = downloader.start();

    verify(forwardPipeline).abort();
    assertThat(catchup.get()).isCompleted();
    verify(pipelineFactory)
        .createForwardBodiesAndReceiptsDownloadPipeline(anyLong(), eq(catchupPivot), any());
    downloader.onWorldStateHealFinished();
    chainDownload.get(5, TimeUnit.SECONDS);
  }

  @Test
  @SuppressWarnings("unchecked")
  void deferredCatchupStopsTheMainCycleOnceStage1Finishes() throws Exception {
    // The pivot update needs a complete header chain, so Stage 1 is not interrupted.
    lenient().when(pipelineFactory.isSnap2Enabled()).thenReturn(true);
    final BlockHeader catchupPivot = header(2000);
    final CompletableFuture<Void> stage1 = new CompletableFuture<>();
    when(scheduler.startPipeline(backwardPipeline))
        .thenReturn(stage1, CompletableFuture.completedFuture(null));
    final SnapSyncChainDownloader downloader = downloader();
    final CompletableFuture<Void> chainDownload = downloader.start();
    catchupHeaderDownload(
        catchupPivot, CompletableFuture.failedFuture(new WrongChainException("reorged out")));
    final CompletableFuture<Void> catchup =
        downloader.preparePivotCatchup(initialPivot, catchupPivot);

    verify(backwardPipeline, never()).abort();
    stage1.complete(null);

    assertThat(catchup).isCompleted();
    // The stopped cycle went straight to the new pivot without its BALs or bodies.
    verify(pipelineFactory, never())
        .createBlockAccessListDownloadPipeline(anyLong(), eq(initialPivot));
    verify(pipelineFactory, never())
        .createForwardBodiesAndReceiptsDownloadPipeline(anyLong(), eq(initialPivot), any());
    verify(pipelineFactory).createBlockAccessListDownloadPipeline(anyLong(), eq(catchupPivot));
    downloader.onWorldStateHealFinished();
    chainDownload.get(5, TimeUnit.SECONDS);
  }

  @Test
  @SuppressWarnings("unchecked")
  void stage1FailureIsRetriedFromTheSavedStateWhileACatchupWaitsToStopTheCycle() throws Exception {
    // Stage 1 had not reached the old pivot when it failed. Restarting at the new pivot with the
    // old pivot as anchor would leave the headers between the two anchors missing for good.
    lenient().when(pipelineFactory.isSnap2Enabled()).thenReturn(true);
    lenient()
        .when(scheduler.scheduleFutureTask(any(Runnable.class), any(Duration.class)))
        .thenAnswer(
            inv -> {
              inv.getArgument(0, Runnable.class).run();
              return CompletableFuture.completedFuture(null);
            });
    final BlockHeader catchupPivot = header(2000);
    final CompletableFuture<Void> stage1 = new CompletableFuture<>();
    when(scheduler.startPipeline(backwardPipeline))
        .thenReturn(stage1, CompletableFuture.completedFuture(null));
    final SnapSyncChainDownloader downloader = downloader();
    final CompletableFuture<Void> chainDownload = downloader.start();
    catchupHeaderDownload(
        catchupPivot, CompletableFuture.failedFuture(new WrongChainException("reorged out")));
    final CompletableFuture<Void> catchup =
        downloader.preparePivotCatchup(initialPivot, catchupPivot);

    stage1.completeExceptionally(new RuntimeException("peer timeout"));

    // Stage 1 is retried down to the original anchor, then the cycle stops for the new pivot.
    verify(pipelineFactory, times(2))
        .createBackwardHeaderDownloadPipeline(
            argThat(
                s ->
                    s != null
                        && s.pivotBlockHeader().equals(initialPivot)
                        && s.headerDownloadAnchor().getNumber() == 0));
    verify(pipelineFactory, never())
        .createBlockAccessListDownloadPipeline(anyLong(), eq(initialPivot));
    assertThat(catchup).isCompleted();
    downloader.onWorldStateHealFinished();
    chainDownload.get(5, TimeUnit.SECONDS);
  }

  @Test
  @SuppressWarnings("unchecked")
  void staleCatchupDownloadDoesNotRollTheMainCycleBackAfterItCompletedTheRequest()
      throws Exception {
    // After a restart the main cycle starts at the new pivot and downloads the same range as the
    // catch-up, so it can complete the request while the catch-up download is still running.
    lenient().when(pipelineFactory.isSnap2Enabled()).thenReturn(true);
    final BlockHeader oldPivot = header(500);
    final SnapSyncChainDownloader downloader = downloader();
    final Pipeline<Long> staleHeaderPipeline = mock(Pipeline.class);
    final CompletableFuture<Void> staleHeaderDownload = new CompletableFuture<>();
    when(pipelineFactory.createBackwardHeaderDownloadPipeline(
            argThat(s -> s != null && s.headerDownloadAnchor().equals(oldPivot))))
        .thenReturn(
            new SnapSyncChainDownloadPipelineFactory.BackwardHeaderPipelineResult(
                staleHeaderPipeline, mock(BackwardHeaderDriver.class)));
    when(scheduler.startPipeline(staleHeaderPipeline)).thenReturn(staleHeaderDownload);
    final CompletableFuture<Void> staleCatchup =
        downloader.preparePivotCatchup(oldPivot, initialPivot);

    final CompletableFuture<Void> stage2 = new CompletableFuture<>();
    when(scheduler.startPipeline(forwardPipeline)).thenReturn(stage2);
    final CompletableFuture<Void> chainDownload = downloader.start();
    // The main cycle's BAL download reached the requested pivot.
    assertThat(staleCatchup).isCompleted();
    // Completing the request stops its own download.
    verify(staleHeaderPipeline).abort();

    // The world state moves on to the next pivot, then the stale download finally finishes.
    catchupHeaderDownload(header(2000), new CompletableFuture<>());
    final CompletableFuture<Void> nextCatchup =
        downloader.preparePivotCatchup(initialPivot, header(2000));
    staleHeaderDownload.complete(null);

    stage2.complete(null);
    downloader.onWorldStateHealFinished();
    chainDownload.get(5, TimeUnit.SECONDS);

    // Waking the main loop with the old pivot would have treated it as a rollback.
    verify(blockchain, never()).unsafeRemoveCanonicalIndexRange(anyLong(), anyLong());
    assertThat(nextCatchup).isNotDone();
    // The stopped download does not go on to its BAL step: only the main cycle's BAL download ran.
    verify(pipelineFactory, times(1))
        .createBlockAccessListDownloadPipeline(anyLong(), eq(initialPivot));
  }

  @Test
  @SuppressWarnings("unchecked")
  void staleCatchupRetryIsCancelledWhenTheMainCycleCompletesTheRequest() {
    lenient().when(pipelineFactory.isSnap2Enabled()).thenReturn(true);
    final BlockHeader oldPivot = header(500);
    final SnapSyncChainDownloader downloader = downloader();
    final Pipeline<Long> staleHeaderPipeline = mock(Pipeline.class);
    when(pipelineFactory.createBackwardHeaderDownloadPipeline(
            argThat(s -> s != null && s.headerDownloadAnchor().equals(oldPivot))))
        .thenReturn(
            new SnapSyncChainDownloadPipelineFactory.BackwardHeaderPipelineResult(
                staleHeaderPipeline, mock(BackwardHeaderDriver.class)));
    when(scheduler.startPipeline(staleHeaderPipeline))
        .thenReturn(CompletableFuture.failedFuture(new TimeoutException("headers timed out")));
    final CompletableFuture<Void> staleCatchup =
        downloader.preparePivotCatchup(oldPivot, initialPivot);
    assertThat(scheduledRetryResults).hasSize(1);

    final CompletableFuture<Void> stage2 = new CompletableFuture<>();
    when(scheduler.startPipeline(forwardPipeline)).thenReturn(stage2);
    downloader.start();
    assertThat(staleCatchup).isCompleted();

    // Runs the retry even though it was cancelled, so the abort check is covered too.
    runScheduledRetries();

    assertThat(scheduledRetryResults.getFirst()).isCancelled();
    verify(pipelineFactory, times(1))
        .createBackwardHeaderDownloadPipeline(
            argThat(s -> s != null && s.headerDownloadAnchor().equals(oldPivot)));
    verify(scheduler, times(1)).startPipeline(staleHeaderPipeline);
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
  private void deferredCatchupHeaderDownload(final BlockHeader oldPivot) {
    final Pipeline<Long> pipeline = mock(Pipeline.class);
    when(pipelineFactory.createBackwardHeaderDownloadPipeline(
            argThat(s -> s != null && s.headerDownloadAnchor().equals(oldPivot))))
        .thenReturn(
            new SnapSyncChainDownloadPipelineFactory.BackwardHeaderPipelineResult(
                pipeline, mock(BackwardHeaderDriver.class)));
    when(scheduler.startPipeline(pipeline))
        .thenReturn(CompletableFuture.failedFuture(new WrongChainException("reorged out")));
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

  /** Headers 0..number of a single chain, linked by parent hash, keyed by hash. */
  private static final Map<Hash, BlockHeader> CHAIN = new ConcurrentHashMap<>();

  private static final List<BlockHeader> CHAIN_BY_NUMBER = new ArrayList<>();

  private static synchronized BlockHeader header(final long number) {
    while (CHAIN_BY_NUMBER.size() <= number) {
      final BlockHeaderTestFixture fixture =
          new BlockHeaderTestFixture().number(CHAIN_BY_NUMBER.size());
      if (!CHAIN_BY_NUMBER.isEmpty()) {
        fixture.parentHash(CHAIN_BY_NUMBER.getLast().getHash());
      }
      final BlockHeader header = fixture.buildHeader();
      CHAIN_BY_NUMBER.add(header);
      CHAIN.put(header.getHash(), header);
    }
    return CHAIN_BY_NUMBER.get((int) number);
  }
}
