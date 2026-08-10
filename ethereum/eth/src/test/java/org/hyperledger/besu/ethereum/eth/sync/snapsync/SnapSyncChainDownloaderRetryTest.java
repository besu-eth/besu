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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
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
import org.hyperledger.besu.ethereum.eth.sync.common.ChainSyncStateStorage;
import org.hyperledger.besu.ethereum.eth.sync.common.SingleBlockHeaderDownloader;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.StalledDownloadException;
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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Covers the retry loop and the stall escalation in {@link SnapSyncChainDownloader}: a retryable
 * failure must restart the cycle from the persisted state with exponential backoff, while a
 * download that keeps failing without making progress must escalate to a {@link
 * StalledDownloadException} so that {@code SnapSyncDownloader} re-pivots instead of retrying
 * forever.
 *
 * <p>Retries are normally scheduled on the {@link EthScheduler}; here the scheduler runs the
 * scheduled task inline so a whole retry sequence completes synchronously within the test.
 */
@ExtendWith(MockitoExtension.class)
class SnapSyncChainDownloaderRetryTest {

  /** Number of the first Stage 1 attempt that is counted as "no progress since the last retry". */
  private static final int FIRST_COUNTED_RETRY = 2;

  /** Mirrors SnapSyncChainDownloader.MAX_SAME_STATE_RETRIES. */
  private static final int MAX_SAME_STATE_RETRIES = 20;

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

  /** Delays passed to the scheduler, in call order. */
  private final List<Duration> scheduledDelays = new ArrayList<>();

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

    backwardPipeline = mock(Pipeline.class);
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

    // Run the retry inline instead of on a real scheduler, recording the requested backoff.
    lenient()
        .when(scheduler.scheduleFutureTask(any(Runnable.class), any(Duration.class)))
        .thenAnswer(
            invocation -> {
              scheduledDelays.add(invocation.getArgument(1, Duration.class));
              invocation.getArgument(0, Runnable.class).run();
              return CompletableFuture.completedFuture(null);
            });
  }

  @Test
  void transientFailureIsRetriedFromTheSavedState() throws Exception {
    failStage1ForFirstAttempts(1);

    final SnapSyncChainDownloader downloader = downloader();
    downloader.onWorldStateHealFinished();

    downloader.start().get(5, TimeUnit.SECONDS);

    // Stage 1 was attempted twice: the failure retried the whole cycle from the persisted state.
    verify(pipelineFactory, times(2)).createBackwardHeaderDownloadPipeline(any());
    assertThat(scheduledDelays)
        .containsExactly(Duration.ofMillis(SnapSyncChainDownloader.SMALL_DELAY_MILLISECONDS));
  }

  @Test
  void retryBackoffDoublesBetweenConsecutiveFailures() throws Exception {
    failStage1ForFirstAttempts(3);

    final SnapSyncChainDownloader downloader = downloader();
    downloader.onWorldStateHealFinished();

    downloader.start().get(5, TimeUnit.SECONDS);

    verify(pipelineFactory, times(4)).createBackwardHeaderDownloadPipeline(any());
    // Backoff starts at SMALL_DELAY_MILLISECONDS and doubles per consecutive failure, so a tight
    // retry loop cannot hammer peers.
    assertThat(scheduledDelays)
        .containsExactly(
            Duration.ofMillis(SnapSyncChainDownloader.SMALL_DELAY_MILLISECONDS),
            Duration.ofMillis(2L * SnapSyncChainDownloader.SMALL_DELAY_MILLISECONDS),
            Duration.ofMillis(4L * SnapSyncChainDownloader.SMALL_DELAY_MILLISECONDS));
  }

  @Test
  void failuresWithoutAnyProgressEscalateToAStalledDownload() {
    // Every attempt fails, the chain sync state never changes and the chain head never moves.
    lenient()
        .when(scheduler.startPipeline(backwardPipeline))
        .thenAnswer(
            invocation -> CompletableFuture.failedFuture(new RuntimeException("no peer data")));

    final SnapSyncChainDownloader downloader = downloader();
    downloader.onWorldStateHealFinished();

    // The escalation is what makes SnapSyncDownloader re-pivot rather than retry forever.
    assertThatThrownBy(() -> downloader.start().get(5, TimeUnit.SECONDS))
        .hasRootCauseInstanceOf(StalledDownloadException.class);

    // The first failure only records the baseline state; from the second on, each failure without
    // progress increments the counter, so the escalation fires on attempt MAX + 1.
    verify(pipelineFactory, times(MAX_SAME_STATE_RETRIES + FIRST_COUNTED_RETRY - 1))
        .createBackwardHeaderDownloadPipeline(any());
  }

  @Test
  void chainHeadProgressResetsTheStallCounter() throws Exception {
    // Stage 2 keeps importing bodies between failures: the chain head rises even though the
    // persisted chain sync state is unchanged, so this is progress and must not escalate.
    final AtomicLong chainHead = new AtomicLong(500L);
    lenient()
        .when(blockchain.getChainHeadBlockNumber())
        .thenAnswer(invocation -> chainHead.incrementAndGet());

    final int failures = MAX_SAME_STATE_RETRIES + 5;
    failStage1ForFirstAttempts(failures);

    final SnapSyncChainDownloader downloader = downloader();
    downloader.onWorldStateHealFinished();

    downloader.start().get(10, TimeUnit.SECONDS);

    // Far more consecutive failures than the stall threshold, yet the download still finished.
    verify(pipelineFactory, times(failures + 1)).createBackwardHeaderDownloadPipeline(any());
    verify(scheduler, atLeast(failures))
        .scheduleFutureTask(any(Runnable.class), any(Duration.class));
  }

  @Test
  void retryBackoffIsResetAfterASuccessfulCycle() throws Exception {
    // Two failures (backoff 100 ms, 200 ms), then a successful cycle, then a pivot update whose
    // cycle fails once more. The backoff for that failure must start again at 100 ms.
    failStage1OnAttempts(Set.of(1, 2, 4));

    final SnapSyncChainDownloader downloader = downloader();
    downloader.onPivotUpdated(header(2000));
    downloader.onWorldStateHealFinished();

    downloader.start().get(5, TimeUnit.SECONDS);

    assertThat(scheduledDelays)
        .containsExactly(
            Duration.ofMillis(SnapSyncChainDownloader.SMALL_DELAY_MILLISECONDS),
            Duration.ofMillis(2L * SnapSyncChainDownloader.SMALL_DELAY_MILLISECONDS),
            Duration.ofMillis(SnapSyncChainDownloader.SMALL_DELAY_MILLISECONDS));
  }

  /** Fails the Stage 1 pipeline for the first {@code failures} attempts, then succeeds. */
  private void failStage1ForFirstAttempts(final int failures) {
    final AtomicInteger attempts = new AtomicInteger();
    stubStage1(attempt -> attempt <= failures, attempts);
  }

  /** Fails the Stage 1 pipeline on the given (1-based) attempt numbers and succeeds otherwise. */
  private void failStage1OnAttempts(final Set<Integer> failingAttempts) {
    stubStage1(failingAttempts::contains, new AtomicInteger());
  }

  private void stubStage1(final Predicate<Integer> failsOnAttempt, final AtomicInteger attempts) {
    lenient()
        .when(scheduler.startPipeline(any()))
        .thenAnswer(
            invocation ->
                invocation.getArgument(0) == backwardPipeline
                        && failsOnAttempt.test(attempts.incrementAndGet())
                    ? CompletableFuture.failedFuture(new RuntimeException("transient"))
                    : CompletableFuture.completedFuture(null));
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
