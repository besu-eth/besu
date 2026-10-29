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
package org.hyperledger.besu.ethereum.eth.sync.snap2.chain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.ChainDataPruner;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.eth.manager.ChainHeadEstimate;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.sync.common.checkpoint.ImmutableCheckpoint;
import org.hyperledger.besu.ethereum.eth.sync.snap2.QueuedExecutor;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.ImmutableSnapSyncConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.metrics.StubMetricsSystem;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

/**
 * Checks when the back-fill starts and stops, and the lowest block it is given, read from the
 * blocks it reports left to store. Its workers stay queued unless a test runs them.
 */
class BackgroundBackfillTest {

  private static final long HEAD = 60;
  private static final long BODIES_FROM = 41;
  private static final long CHECKPOINT = 10;
  private static final String BLOCKS_REMAINING = "snap2_backfill_blocks_remaining";
  // one worker, no wait, and the lowest block recorded once it ends
  private static final BodyBackfill.Settings SETTINGS =
      new BodyBackfill.Settings(
          "test", 1, Long.MAX_VALUE, Duration.ZERO, Duration.ZERO, 8, false, true);

  private final MutableBlockchain blockchain = mock(MutableBlockchain.class);
  private final SyncState syncState = mock(SyncState.class);
  private final StubMetricsSystem metricsSystem = new StubMetricsSystem();
  private final List<RequestSender> senders = new ArrayList<>();
  private final List<QueuedExecutor> workerPools = new ArrayList<>();

  @BeforeEach
  void setUp() {
    when(blockchain.getChainHeadBlockNumber()).thenReturn(HEAD);
    when(blockchain.getBodiesFrom()).thenReturn(Optional.of(BODIES_FROM));
    when(syncState.getCheckpoint())
        .thenReturn(
            Optional.of(
                ImmutableCheckpoint.builder()
                    .blockNumber(CHECKPOINT)
                    .blockHash(Hash.ZERO)
                    .totalDifficulty(Difficulty.ONE)
                    .build()));
    when(syncState.isInSync()).thenReturn(true);
    when(syncState.getBestPeerChainHead()).thenReturn(Optional.of(mock(ChainHeadEstimate.class)));
  }

  @Test
  void downloadsDownToTheBlockAboveTheCheckpoint() {
    backfill().onInitialSyncCompleted();

    assertThat(metricsSystem.getGaugeValue(BLOCKS_REMAINING)).isEqualTo(BODIES_FROM - 11);
  }

  @Test
  void downloadsDownToTheBlockAboveGenesisWithoutCheckpoint() {
    when(syncState.getCheckpoint()).thenReturn(Optional.empty());

    backfill().onInitialSyncCompleted();

    assertThat(metricsSystem.getGaugeValue(BLOCKS_REMAINING)).isEqualTo(BODIES_FROM - 1);
  }

  @Test
  void keepsAPruningRunAwayFromTheBlocksItStores() {
    final ChainDataPruner pruner = mock(ChainDataPruner.class);
    when(pruner.lowestToBackfill(HEAD)).thenReturn(30L);

    backfill(Optional.of(pruner), true, SETTINGS, this::queuedWorkers).onInitialSyncCompleted();

    assertThat(metricsSystem.getGaugeValue(BLOCKS_REMAINING)).isEqualTo(BODIES_FROM - 30);
  }

  @Test
  void startsNothingWithoutBodiesLeftBySync() {
    when(blockchain.getBodiesFrom()).thenReturn(Optional.empty());

    backfill().onInitialSyncCompleted();

    assertThat(senders).isEmpty();
    assertThat(workerPools).isEmpty();
  }

  @Test
  void startsNothingOnceEveryNeededBodyIsStored() {
    when(blockchain.getBodiesFrom()).thenReturn(Optional.of(CHECKPOINT + 1));

    backfill().onInitialSyncCompleted();

    assertThat(senders).isEmpty();
  }

  @Test
  void startsNothingWhenTurnedOff() {
    backfill(Optional.empty(), false, SETTINGS, this::queuedWorkers).onInitialSyncCompleted();

    assertThat(senders).isEmpty();
    assertThat(workerPools).isEmpty();
  }

  @Test
  void startsOnceWhileItRuns() {
    final BackgroundBackfill backfill = backfill();

    backfill.onInitialSyncCompleted();
    backfill.onInitialSyncCompleted();

    assertThat(senders).hasSize(1);
  }

  @Test
  void stopsItsSchedulerOnlyOnceTheBackfillEnds() {
    backfill().onInitialSyncCompleted();
    final RequestSender sender = senders.getFirst();
    verify(sender, never()).stop();

    // no header is stored, so the back-fill ends at its first range
    workerPools.getFirst().runAll();

    verify(sender).stop();
  }

  @Test
  void stopsWhenTheInitialSyncStartsAgainAndStartsOnceItCompletes() {
    final BackgroundBackfill backfill = backfill();
    backfill.onInitialSyncCompleted();

    backfill.onInitialSyncRestart();

    verify(senders.getFirst()).stop();
    backfill.onInitialSyncCompleted();
    // built and started, its workers on a pool of their own
    assertThat(workerPools).hasSize(2);
  }

  // a completion as the back-fill ends, before its done completes, starts the next one
  @Test
  void startsAgainWhenTheSyncCompletesAsTheBackfillEnds() {
    final AtomicReference<BackgroundBackfill> backfill = new AtomicReference<>();
    backfill.set(
        backfill(
            () -> {
              final QueuedExecutor pool = spy(new QueuedExecutor());
              // the first back-fill shuts its pool down once it ended, before done completes
              if (workerPools.isEmpty()) {
                doAnswer(
                        invocation -> {
                          backfill.get().onInitialSyncCompleted();
                          return invocation.callRealMethod();
                        })
                    .when(pool)
                    .shutdown();
              }
              workerPools.add(pool);
              return pool;
            }));
    backfill.get().onInitialSyncCompleted();

    // no header is stored, so the back-fill ends at its first range
    workerPools.getFirst().runAll();

    assertThat(workerPools).hasSize(2);
  }

  @Test
  void stopsTheSchedulerOfABackfillThatFailsToBuild() {
    // without a worker, the back-fill refuses its settings
    final BodyBackfill.Settings noWorker =
        new BodyBackfill.Settings(
            "test", 0, Long.MAX_VALUE, Duration.ZERO, Duration.ZERO, 8, false, true);

    backfill(Optional.empty(), true, noWorker, this::queuedWorkers).onInitialSyncCompleted();

    verify(senders.getFirst()).stop();
  }

  @Test
  void startsAgainAfterABackfillThatFailedToStart() {
    final ExecutorService rejecting = mock(ExecutorService.class);
    doThrow(new RejectedExecutionException("shut down")).when(rejecting).execute(any());
    // the first pool refuses the workers, the next ones queue them
    final Iterator<ExecutorService> firstPool = List.of(rejecting).iterator();
    final BackgroundBackfill backfill =
        backfill(() -> firstPool.hasNext() ? firstPool.next() : queuedWorkers());
    backfill.onInitialSyncCompleted();
    verify(senders.getFirst()).stop();

    backfill.onInitialSyncCompleted();

    assertThat(workerPools).hasSize(1);
  }

  // the node counts as in sync while it knows no peer
  @Test
  void waitsForAPeerHeadBeforeItDownloads() {
    when(syncState.getBestPeerChainHead()).thenReturn(Optional.empty());

    assertThat(BackgroundBackfill.followsTheHead(syncState).getAsBoolean()).isFalse();
  }

  @Test
  void awaitStopStopsItForGoodAndWaitsForTheWorkersOfEveryBackfill() throws InterruptedException {
    final List<ExecutorService> pools = new ArrayList<>();
    final BackgroundBackfill backfill =
        backfill(
            () -> {
              final ExecutorService pool = mock(ExecutorService.class);
              pools.add(pool);
              return pool;
            });
    backfill.onInitialSyncCompleted();
    backfill.onInitialSyncRestart();
    backfill.onInitialSyncCompleted();

    backfill.awaitStop();
    backfill.onInitialSyncCompleted();

    assertThat(pools).hasSize(2);
    for (final ExecutorService pool : pools) {
      // interrupted, then awaited before the storage they read closes
      final InOrder order = inOrder(pool);
      order.verify(pool).shutdownNow();
      order.verify(pool).awaitTermination(anyLong(), eq(TimeUnit.NANOSECONDS));
    }
  }

  @Test
  void aFailureToStartLeavesTheOtherListenersGoing() {
    final BackgroundBackfill backfill =
        new BackgroundBackfill(
            blockchain,
            ProtocolScheduleFixture.MAINNET,
            syncState,
            Optional.empty(),
            true,
            SETTINGS,
            metricsSystem,
            () -> {
              throw new IllegalStateException("cannot create the scheduler");
            },
            this::queuedWorkers);

    assertThatCode(backfill::onInitialSyncCompleted).doesNotThrowAnyException();
  }

  @Test
  void createLeavesTheBackfillOffWhenTheOptionTurnsItOff() {
    final BackgroundBackfill backfill =
        BackgroundBackfill.create(
            blockchain,
            ProtocolScheduleFixture.MAINNET,
            mock(EthPeers.class),
            syncState,
            Optional.empty(),
            ImmutableSnapSyncConfiguration.builder()
                .isSnap2BackfillEnabled(false)
                .isSnapSyncTransactionIndexingEnabled(true)
                .build(),
            metricsSystem);

    backfill.onInitialSyncCompleted();

    assertThatThrownBy(() -> metricsSystem.getGaugeValue(BLOCKS_REMAINING))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private BackgroundBackfill backfill() {
    return backfill(this::queuedWorkers);
  }

  private BackgroundBackfill backfill(final Supplier<ExecutorService> workers) {
    return backfill(Optional.empty(), true, SETTINGS, workers);
  }

  private BackgroundBackfill backfill(
      final Optional<ChainDataPruner> pruner,
      final boolean enabled,
      final BodyBackfill.Settings settings,
      final Supplier<ExecutorService> workers) {
    return new BackgroundBackfill(
        blockchain,
        ProtocolScheduleFixture.MAINNET,
        syncState,
        pruner,
        enabled,
        settings,
        metricsSystem,
        () -> {
          final RequestSender sender = mock(RequestSender.class);
          senders.add(sender);
          return sender;
        },
        workers);
  }

  // the workers of a back-fill, run on the test thread when a test says
  private ExecutorService queuedWorkers() {
    final QueuedExecutor pool = new QueuedExecutor();
    workerPools.add(pool);
    return pool;
  }
}
