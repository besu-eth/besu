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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.worldstate.WorldStateRepairBudget;
import org.hyperledger.besu.ethereum.worldstate.WorldStateRepairResult;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.metrics.StubMetricsSystem;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.MetricCategory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class WorldStateRecoveryCoordinatorTest {
  private static final Hash BLOCK_A = Hash.hash(Bytes32.fromHexStringLenient("0x01"));
  private static final Hash BLOCK_B = Hash.hash(Bytes32.fromHexStringLenient("0x02"));
  private static final Hash STATE_A = Hash.hash(Bytes32.fromHexStringLenient("0x11"));
  private static final Hash STATE_B = Hash.hash(Bytes32.fromHexStringLenient("0x12"));

  @Mock private Synchronizer synchronizer;
  private WorldStateRecoveryCoordinator coordinator;
  private final AtomicLong nanoTime = new AtomicLong();
  private final List<ScheduledTask> scheduledTasks = new ArrayList<>();

  @BeforeEach
  void setUp() {
    coordinator =
        new WorldStateRecoveryCoordinator(
            synchronizer,
            null,
            nanoTime::get,
            Runnable::run,
            (task, delay) ->
                scheduledTasks.add(new ScheduledTask(nanoTime.get() + delay.toNanos(), task)));
  }

  @Test
  void suppressesDuplicateAndConcurrentRecoveryWhileTargetIsActive() throws Exception {
    final CountDownLatch initiated = new CountDownLatch(1);
    final CountDownLatch releaseInitiation = new CountDownLatch(1);
    when(synchronizer.repairWorldState(eq(BLOCK_A), eq(STATE_A), any()))
        .thenAnswer(
            ignored -> {
              initiated.countDown();
              if (!releaseInitiation.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Test did not release repair initiation");
              }
              return new CompletableFuture<WorldStateRepairResult>();
            });
    final ExecutorService callers = Executors.newFixedThreadPool(2);
    try {
      final Future<?> first = callers.submit(() -> coordinator.requestRecovery(BLOCK_A, STATE_A));
      assertThat(initiated.await(5, TimeUnit.SECONDS)).isTrue();
      final Future<?> duplicate =
          callers.submit(() -> coordinator.requestRecovery(BLOCK_A, STATE_A));
      duplicate.get(5, TimeUnit.SECONDS);
      releaseInitiation.countDown();
      first.get(5, TimeUnit.SECONDS);
    } finally {
      releaseInitiation.countDown();
      callers.shutdownNow();
    }
    coordinator.requestRecovery(BLOCK_A, STATE_A);

    verify(synchronizer).repairWorldState(eq(BLOCK_A), eq(STATE_A), any());
  }

  @Test
  void newerHeadsJoinBoundedQueueAndStartAfterSuccessfulDrain() {
    final CompletableFuture<WorldStateRepairResult> worker = new CompletableFuture<>();
    when(synchronizer.repairWorldState(eq(BLOCK_A), eq(STATE_A), any())).thenReturn(worker);
    when(synchronizer.repairWorldState(eq(BLOCK_B), eq(STATE_B), any()))
        .thenReturn(new CompletableFuture<>());

    coordinator.requestRecovery(BLOCK_A, STATE_A);
    final ArgumentCaptor<WorldStateRepairBudget> budget =
        ArgumentCaptor.forClass(WorldStateRepairBudget.class);
    verify(synchronizer).repairWorldState(eq(BLOCK_A), eq(STATE_A), budget.capture());
    coordinator.requestRecovery(BLOCK_B, STATE_B);
    assertThat(budget.getValue().isCancelled()).isFalse();

    worker.complete(WorldStateRepairResult.REPAIRED);
    verify(synchronizer).repairWorldState(eq(BLOCK_B), eq(STATE_B), any());
    coordinator.requestRecovery(BLOCK_B, STATE_B);
    verify(synchronizer).repairWorldState(eq(BLOCK_B), eq(STATE_B), any());
  }

  @Test
  void failureCooldownStartsAtDrainAndRequiresALaterForkchoice() {
    final CompletableFuture<WorldStateRepairResult> worker = new CompletableFuture<>();
    when(synchronizer.repairWorldState(eq(BLOCK_A), eq(STATE_A), any())).thenReturn(worker);
    when(synchronizer.repairWorldState(eq(BLOCK_B), eq(STATE_B), any()))
        .thenReturn(new CompletableFuture<>());
    coordinator.requestRecovery(BLOCK_A, STATE_A);
    advance(Duration.ofSeconds(5));
    worker.complete(WorldStateRepairResult.UNAVAILABLE);

    coordinator.requestRecovery(BLOCK_B, STATE_B);
    advance(Duration.ofSeconds(59));
    coordinator.requestRecovery(BLOCK_B, STATE_B);
    verify(synchronizer, never()).repairWorldState(eq(BLOCK_B), eq(STATE_B), any());
    advance(Duration.ofSeconds(1));
    verify(synchronizer, never()).repairWorldState(eq(BLOCK_B), eq(STATE_B), any());
    coordinator.requestRecovery(BLOCK_B, STATE_B);
    verify(synchronizer).repairWorldState(eq(BLOCK_B), eq(STATE_B), any());
  }

  @Test
  void deadlineCancelsBudgetButRetainsWorkerUntilDrain() {
    final CompletableFuture<WorldStateRepairResult> worker = new CompletableFuture<>();
    when(synchronizer.repairWorldState(eq(BLOCK_A), eq(STATE_A), any())).thenReturn(worker);
    when(synchronizer.repairWorldState(eq(BLOCK_B), eq(STATE_B), any()))
        .thenReturn(new CompletableFuture<>());
    coordinator.requestRecovery(BLOCK_A, STATE_A);
    final ArgumentCaptor<WorldStateRepairBudget> budget =
        ArgumentCaptor.forClass(WorldStateRepairBudget.class);
    verify(synchronizer).repairWorldState(eq(BLOCK_A), eq(STATE_A), budget.capture());

    advance(Duration.ofSeconds(10));
    assertThat(budget.getValue().isCancelled()).isTrue();
    assertThat(worker.isDone()).isFalse();
    coordinator.markAvailable(BLOCK_A, STATE_A);
    coordinator.requestRecovery(BLOCK_B, STATE_B);
    advance(Duration.ofSeconds(60));
    verify(synchronizer, never()).repairWorldState(eq(BLOCK_B), eq(STATE_B), any());

    worker.complete(WorldStateRepairResult.CANCELLED);
    coordinator.requestRecovery(BLOCK_B, STATE_B);
    verify(synchronizer, never()).repairWorldState(eq(BLOCK_B), eq(STATE_B), any());
    advance(Duration.ofSeconds(60));
    verify(synchronizer, never()).repairWorldState(eq(BLOCK_B), eq(STATE_B), any());
    coordinator.requestRecovery(BLOCK_B, STATE_B);
    verify(synchronizer).repairWorldState(eq(BLOCK_B), eq(STATE_B), any());
  }

  @Test
  void staleDeadlineCannotCancelANewerGeneration() {
    final CompletableFuture<WorldStateRepairResult> first = new CompletableFuture<>();
    when(synchronizer.repairWorldState(eq(BLOCK_A), eq(STATE_A), any())).thenReturn(first);
    when(synchronizer.repairWorldState(eq(BLOCK_B), eq(STATE_B), any()))
        .thenReturn(new CompletableFuture<>());
    coordinator.requestRecovery(BLOCK_A, STATE_A);
    advance(Duration.ofSeconds(1));
    first.complete(WorldStateRepairResult.ALREADY_AVAILABLE);
    coordinator.requestRecovery(BLOCK_B, STATE_B);
    final ArgumentCaptor<WorldStateRepairBudget> budget =
        ArgumentCaptor.forClass(WorldStateRepairBudget.class);
    verify(synchronizer).repairWorldState(eq(BLOCK_B), eq(STATE_B), budget.capture());

    advance(Duration.ofSeconds(9));
    assertThat(budget.getValue().isCancelled()).isFalse();
  }

  @Test
  void asynchronousFailureReleasesOwnershipButDoesNotRetry() {
    final CompletableFuture<WorldStateRepairResult> first = new CompletableFuture<>();
    when(synchronizer.repairWorldState(eq(BLOCK_A), eq(STATE_A), any())).thenReturn(first);
    when(synchronizer.repairWorldState(eq(BLOCK_B), eq(STATE_B), any()))
        .thenReturn(new CompletableFuture<>());
    coordinator.requestRecovery(BLOCK_A, STATE_A);
    first.completeExceptionally(new IllegalStateException("repair failed"));
    coordinator.requestRecovery(BLOCK_B, STATE_B);
    advance(Duration.ofSeconds(60));
    verify(synchronizer, never()).repairWorldState(eq(BLOCK_B), eq(STATE_B), any());
    coordinator.requestRecovery(BLOCK_B, STATE_B);
    verify(synchronizer).repairWorldState(eq(BLOCK_B), eq(STATE_B), any());
  }

  @Test
  void initiationExceptionAlsoRequiresCooldownAndANewForkchoice() {
    when(synchronizer.repairWorldState(eq(BLOCK_A), eq(STATE_A), any()))
        .thenThrow(new IllegalStateException("cannot start repair"));
    when(synchronizer.repairWorldState(eq(BLOCK_B), eq(STATE_B), any()))
        .thenReturn(new CompletableFuture<>());
    coordinator.requestRecovery(BLOCK_A, STATE_A);
    coordinator.requestRecovery(BLOCK_B, STATE_B);
    advance(Duration.ofSeconds(60));
    verify(synchronizer, never()).repairWorldState(eq(BLOCK_B), eq(STATE_B), any());
    coordinator.requestRecovery(BLOCK_B, STATE_B);
    verify(synchronizer).repairWorldState(eq(BLOCK_B), eq(STATE_B), any());
  }

  @Test
  void disabledCoordinatorNeverStartsWork() {
    final WorldStateRecoveryCoordinator disabled = WorldStateRecoveryCoordinator.disabled();
    disabled.requestRecovery(BLOCK_A, STATE_A);
    disabled.markAvailable(BLOCK_A, STATE_A);
    verifyNoInteractions(synchronizer);
  }

  @Test
  void queueRejectionReleasesOwnershipAndRequiresCooldownAndALaterForkchoice() {
    final AtomicLong submissions = new AtomicLong();
    coordinator =
        new WorldStateRecoveryCoordinator(
            synchronizer,
            null,
            nanoTime::get,
            task -> {
              if (submissions.getAndIncrement() == 0) {
                throw new RejectedExecutionException("Repair queue full");
              }
              task.run();
            },
            (task, delay) ->
                scheduledTasks.add(new ScheduledTask(nanoTime.get() + delay.toNanos(), task)));
    coordinator.requestRecovery(BLOCK_A, STATE_A);
    verifyNoInteractions(synchronizer);
    coordinator.requestRecovery(BLOCK_B, STATE_B);
    advance(Duration.ofSeconds(60));
    verifyNoInteractions(synchronizer);
    when(synchronizer.repairWorldState(eq(BLOCK_B), eq(STATE_B), any()))
        .thenReturn(new CompletableFuture<>());
    coordinator.requestRecovery(BLOCK_B, STATE_B);
    verify(synchronizer).repairWorldState(eq(BLOCK_B), eq(STATE_B), any());
  }

  @Test
  void metricsCountOnlyDrainedOutcomesAndKeepActiveDuringCancellation() {
    final StubMetricsSystem metrics =
        new StubMetricsSystem() {
          @Override
          public Set<MetricCategory> getEnabledCategories() {
            return Set.of(BesuMetricCategory.SYNCHRONIZER);
          }
        };
    coordinator =
        new WorldStateRecoveryCoordinator(
            synchronizer,
            metrics,
            nanoTime::get,
            Runnable::run,
            (task, delay) ->
                scheduledTasks.add(new ScheduledTask(nanoTime.get() + delay.toNanos(), task)));
    final CompletableFuture<WorldStateRepairResult> worker = new CompletableFuture<>();
    when(synchronizer.repairWorldState(eq(BLOCK_A), eq(STATE_A), any())).thenReturn(worker);
    coordinator.requestRecovery(BLOCK_A, STATE_A);
    advance(Duration.ofSeconds(10));
    assertThat(metrics.getGaugeValue("world_state_repair_active")).isEqualTo(1);
    assertThat(metrics.getCounterValue("world_state_repair_outcomes_total", "budget_exhausted"))
        .isZero();
    worker.complete(WorldStateRepairResult.CANCELLED);
    assertThat(metrics.getGaugeValue("world_state_repair_active")).isZero();
    assertThat(metrics.getGaugeValue("world_state_repair_cooldown_seconds")).isEqualTo(60);
    assertThat(metrics.getCounterValue("world_state_repair_outcomes_total", "budget_exhausted"))
        .isEqualTo(1);
  }

  @Test
  void identicalRequestsShareDrainFutureAndPendingReplacementCompletesExplicitly() {
    final CompletableFuture<WorldStateRepairResult> worker = new CompletableFuture<>();
    when(synchronizer.repairWorldState(eq(BLOCK_A), eq(STATE_A), any())).thenReturn(worker);
    final var first = coordinator.requestRecovery(BLOCK_A, STATE_A);
    assertThat(coordinator.requestRecovery(BLOCK_A, STATE_A)).isSameAs(first);
    final var pending = coordinator.requestRecovery(BLOCK_B, STATE_B);
    assertThat(coordinator.requestRecovery(BLOCK_B, STATE_B)).isSameAs(pending);
    final var replacement = coordinator.requestRecovery(BLOCK_B, STATE_A);
    assertThat(pending.join()).isEqualTo(WorldStateRepairResult.SUPERSEDED);
    worker.complete(WorldStateRepairResult.UNAVAILABLE);
    assertThat(first.join()).isEqualTo(WorldStateRepairResult.UNAVAILABLE);
    assertThat(replacement.join()).isEqualTo(WorldStateRepairResult.UNAVAILABLE);
    assertThat(coordinator.requestRecovery(BLOCK_A, STATE_A).join())
        .isEqualTo(WorldStateRepairResult.UNAVAILABLE);
  }

  @Test
  void stopRetainsDrainOwnershipAndReopenCannotReviveOldRequests() {
    final CompletableFuture<WorldStateRepairResult> worker = new CompletableFuture<>();
    when(synchronizer.repairWorldState(eq(BLOCK_A), eq(STATE_A), any())).thenReturn(worker);
    when(synchronizer.repairWorldState(eq(BLOCK_B), eq(STATE_B), any()))
        .thenReturn(CompletableFuture.completedFuture(WorldStateRepairResult.REPAIRED));
    final long generation = coordinator.generation();
    final var active = coordinator.requestRecovery(BLOCK_A, STATE_A);
    final var pending = coordinator.requestRecovery(BLOCK_B, STATE_B);
    coordinator.stop();
    assertThat(coordinator.generation()).isGreaterThan(generation);
    assertThat(pending.join()).isEqualTo(WorldStateRepairResult.CANCELLED);
    assertThat(active).isNotDone();
    assertThat(coordinator.requestRecovery(BLOCK_B, STATE_B).join())
        .isEqualTo(WorldStateRepairResult.CANCELLED);
    coordinator.start();
    final var reopened = coordinator.requestRecovery(BLOCK_B, STATE_B);
    verify(synchronizer, never()).repairWorldState(eq(BLOCK_B), eq(STATE_B), any());
    worker.complete(WorldStateRepairResult.REPAIRED);
    assertThat(active.join()).isEqualTo(WorldStateRepairResult.CANCELLED);
    assertThat(reopened.join()).isEqualTo(WorldStateRepairResult.REPAIRED);
  }

  @Test
  void repeatedBlockageActivatesCooldownWithoutAnotherAttempt() {
    when(synchronizer.repairWorldState(eq(BLOCK_A), eq(STATE_A), any()))
        .thenReturn(CompletableFuture.completedFuture(WorldStateRepairResult.REPAIRED));
    assertThat(coordinator.requestRecovery(BLOCK_A, STATE_A).join())
        .isEqualTo(WorldStateRepairResult.REPAIRED);
    coordinator.onRepeatedMissingState();
    assertThat(coordinator.requestRecovery(BLOCK_B, STATE_B).join())
        .isEqualTo(WorldStateRepairResult.UNAVAILABLE);
    verify(synchronizer, never()).repairWorldState(eq(BLOCK_B), eq(STATE_B), any());
  }

  @Test
  void disabledCategoryDoesNotRegisterRepairMetrics() {
    final var metrics = mock(MetricsSystem.class);
    new WorldStateRecoveryCoordinator(synchronizer, metrics);
    verify(metrics).getEnabledCategories();
    verifyNoMoreInteractions(metrics);
  }

  private void advance(final Duration duration) {
    nanoTime.addAndGet(duration.toNanos());
    while (true) {
      final var ready =
          scheduledTasks.stream().filter(task -> task.deadline() <= nanoTime.get()).findFirst();
      if (ready.isEmpty()) {
        return;
      }
      scheduledTasks.remove(ready.get());
      ready.get().action().run();
    }
  }

  private record ScheduledTask(long deadline, Runnable action) {}
}
