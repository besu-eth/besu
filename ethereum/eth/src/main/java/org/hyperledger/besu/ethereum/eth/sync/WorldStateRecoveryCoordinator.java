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

import static java.util.Objects.requireNonNull;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.worldstate.WorldStateRepairBudget;
import org.hyperledger.besu.ethereum.worldstate.WorldStateRepairRequester;
import org.hyperledger.besu.ethereum.worldstate.WorldStateRepairResult;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.plugin.services.metrics.MetricCategory;

import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinates bounded asynchronous repair across FCU versions without restarting synchronization.
 */
public final class WorldStateRecoveryCoordinator implements WorldStateRepairRequester {
  private static final Logger LOG = LoggerFactory.getLogger(WorldStateRecoveryCoordinator.class);
  private static final long COOLDOWN_NANOS = Duration.ofSeconds(60).toNanos();

  @Nullable private final Synchronizer synchronizer;
  private final LongSupplier nanoTime;
  private final Executor repairExecutor;
  private final BiConsumer<Runnable, Duration> schedule;
  @Nullable private LabelledMetric<Counter> outcomes;
  @Nullable private Attempt active;
  @Nullable private Request pending;
  private boolean open = true;
  private long lifecycleGeneration;
  private long cooldownStartedAt;
  private boolean coolingDown;
  private boolean cooldownLogged;

  public WorldStateRecoveryCoordinator(@Nullable final Synchronizer synchronizer) {
    this(synchronizer, null);
  }

  public WorldStateRecoveryCoordinator(
      @Nullable final Synchronizer synchronizer, @Nullable final MetricsSystem metricsSystem) {
    this(
        synchronizer,
        metricsSystem,
        System::nanoTime,
        // Synchronizer.repairWorldState only enqueues work; its worker runs off the FCU caller.
        Runnable::run,
        (task, delay) ->
            CompletableFuture.delayedExecutor(delay.toNanos(), TimeUnit.NANOSECONDS).execute(task));
  }

  WorldStateRecoveryCoordinator(
      @Nullable final Synchronizer synchronizer,
      @Nullable final MetricsSystem metricsSystem,
      final LongSupplier nanoTime,
      final Executor repairExecutor,
      final BiConsumer<Runnable, Duration> schedule) {
    this.synchronizer = synchronizer;
    this.nanoTime = requireNonNull(nanoTime);
    this.repairExecutor = requireNonNull(repairExecutor);
    this.schedule = requireNonNull(schedule);
    if (metricsSystem != null) {
      // Use the registered category instance without introducing a metrics-core dependency.
      metricsSystem.getEnabledCategories().stream()
          .filter(category -> category.getName().equals("synchronizer"))
          .findFirst()
          .ifPresent(category -> registerMetrics(metricsSystem, category));
    }
  }

  /** Creates a coordinator that is inert for tests or methods without a synchronizer. */
  public static WorldStateRecoveryCoordinator disabled() {
    return new WorldStateRecoveryCoordinator(null);
  }

  @Override
  public void markAvailable(final Hash blockHash, final Hash stateRoot) {
    final Request available;
    synchronized (this) {
      available =
          pending != null && pending.target.equals(new Target(blockHash, stateRoot))
              ? pending
              : null;
      if (available != null) {
        pending = null;
      }
    }
    if (available != null) {
      available.completion.complete(WorldStateRepairResult.ALREADY_AVAILABLE);
    }
  }

  @Override
  public CompletableFuture<WorldStateRepairResult> requestRecovery(
      final Hash blockHash, final Hash stateRoot) {
    final Target target = new Target(requireNonNull(blockHash), requireNonNull(stateRoot));
    final Request request;
    final Request superseded;
    synchronized (this) {
      if (!open) {
        return CompletableFuture.completedFuture(WorldStateRepairResult.CANCELLED);
      }
      if (synchronizer == null) {
        return CompletableFuture.completedFuture(WorldStateRepairResult.UNSUPPORTED);
      }
      if (active != null
          && active.target.equals(target)
          && active.lifecycleGeneration == lifecycleGeneration) {
        return active.completion;
      }
      if (pending != null && pending.target.equals(target)) {
        return pending.completion;
      }
      if (cooldownRemainingNanos() > 0) {
        return CompletableFuture.completedFuture(WorldStateRepairResult.UNAVAILABLE);
      }
      superseded = pending;
      request = new Request(target);
      pending = request;
    }
    if (superseded != null) {
      superseded.completion.complete(WorldStateRepairResult.SUPERSEDED);
    }
    startPending();
    return request.completion;
  }

  /** Closes admission and invalidates continuations while retaining the active slot until drain. */
  public void stop() {
    final Request cancelled;
    synchronized (this) {
      open = false;
      lifecycleGeneration++;
      cancelled = pending;
      pending = null;
      if (active != null) {
        active.cancel(WorldStateRepairResult.CANCELLED);
      }
    }
    if (cancelled != null) {
      cancelled.completion.complete(WorldStateRepairResult.CANCELLED);
    }
  }

  /** Reopens admission after manual resync without reviving earlier continuations. */
  public synchronized void start() {
    open = true;
    coolingDown = false;
    cooldownLogged = false;
  }

  @Override
  public synchronized long generation() {
    return lifecycleGeneration;
  }

  @Override
  public synchronized boolean isOpen() {
    return open;
  }

  @Override
  public void onRepeatedMissingState() {
    final Request deferred;
    synchronized (this) {
      coolingDown = true;
      cooldownStartedAt = nanoTime.getAsLong();
      deferred = pending;
      pending = null;
    }
    if (deferred != null) {
      deferred.completion.complete(WorldStateRepairResult.UNAVAILABLE);
    }
  }

  private void startPending() {
    final Attempt attempt;
    synchronized (this) {
      final Request request = pending;
      if (active != null || request == null || synchronizer == null || !open) {
        return;
      }
      final long cooldown = cooldownRemainingNanos();
      if (cooldown > 0) {
        if (!cooldownLogged) {
          cooldownLogged = true;
          LOG.info(
              "Deferring world-state repair for {} seconds during global cooldown",
              TimeUnit.NANOSECONDS.toSeconds(cooldown - 1) + 1);
        }
        return;
      }
      attempt = new Attempt(request, new WorldStateRepairBudget(nanoTime), lifecycleGeneration);
      active = attempt;
      pending = null;
      coolingDown = false;
      cooldownLogged = false;
    }
    try {
      schedule.accept(() -> expire(attempt), attempt.budget.remainingTime());
      repairExecutor.execute(() -> repair(attempt));
    } catch (final RuntimeException error) {
      complete(attempt, WorldStateRepairResult.FAILED, error);
    }
  }

  private void repair(final Attempt attempt) {
    final Synchronizer repairSynchronizer = requireNonNull(synchronizer);
    synchronized (this) {
      if (active != attempt) {
        return;
      }
    }
    if (attempt.budget.isCancelled() || attempt.budget.isExhausted()) {
      complete(attempt, WorldStateRepairResult.CANCELLED, null);
      return;
    }
    try {
      final CompletableFuture<WorldStateRepairResult> worker =
          requireNonNull(
              repairSynchronizer.repairWorldState(
                  attempt.target.blockHash(), attempt.target.stateRoot(), attempt.budget));
      // This is the worker's drain future, not a timeout wrapper. Never cancel or complete it here.
      worker.whenComplete((result, error) -> complete(attempt, result, error));
    } catch (final RuntimeException error) {
      complete(attempt, WorldStateRepairResult.FAILED, error);
    }
  }

  private synchronized void expire(final Attempt attempt) {
    if (active == attempt) {
      attempt.cancel(WorldStateRepairResult.BUDGET_EXHAUSTED);
    }
  }

  private void complete(
      final Attempt attempt,
      @Nullable final WorldStateRepairResult result,
      @Nullable final Throwable error) {
    final WorldStateRepairResult outcome;
    final Request deferred;
    synchronized (this) {
      if (active != attempt) {
        return;
      }
      outcome =
          attempt.cancellationReason != null
              ? attempt.cancellationReason
              : error != null || result == null ? WorldStateRepairResult.FAILED : result;
      active = null;
      if (attempt.lifecycleGeneration == lifecycleGeneration) {
        coolingDown =
            outcome != WorldStateRepairResult.REPAIRED
                && outcome != WorldStateRepairResult.ALREADY_AVAILABLE;
        cooldownStartedAt = nanoTime.getAsLong();
      }
      cooldownLogged = false;
      deferred = coolingDown ? pending : null;
      if (deferred != null) {
        pending = null;
      }
      if (outcomes != null) {
        outcomes.labels(outcome.name().toLowerCase(Locale.ROOT)).inc();
      }
    }
    if (error != null) {
      LOG.warn("Bounded world-state repair ended with {}", outcome, error);
    } else {
      LOG.info("Bounded world-state repair ended with {}", outcome);
    }
    // Complete outside the lock: callbacks may continue backward sync or request another target.
    attempt.completion.complete(outcome);
    if (deferred != null) {
      deferred.completion.complete(WorldStateRepairResult.UNAVAILABLE);
    }
    startPending();
  }

  private synchronized long cooldownRemainingNanos() {
    return coolingDown
        ? Math.max(0L, COOLDOWN_NANOS - Math.max(0L, nanoTime.getAsLong() - cooldownStartedAt))
        : 0L;
  }

  private synchronized int activeWorkers() {
    return active == null ? 0 : 1;
  }

  private void registerMetrics(final MetricsSystem metricsSystem, final MetricCategory category) {
    outcomes =
        metricsSystem.createLabelledCounter(
            category,
            "world_state_repair_outcomes_total",
            "Drained bounded world-state repair attempts by terminal outcome",
            "outcome");
    metricsSystem.createIntegerGauge(
        category,
        "world_state_repair_active",
        "Whether a repair worker still owns the repair slot, including cancellation drain",
        this::activeWorkers);
    metricsSystem.createGauge(
        category,
        "world_state_repair_cooldown_seconds",
        "Remaining global cooldown before another repair attempt can start",
        () -> cooldownRemainingNanos() / 1_000_000_000.0);
  }

  private record Target(Hash blockHash, Hash stateRoot) {}

  private static final class Request {
    private final Target target;
    private final CompletableFuture<WorldStateRepairResult> completion = new CompletableFuture<>();

    private Request(final Target target) {
      this.target = target;
    }
  }

  private static final class Attempt {
    private final Target target;
    private final WorldStateRepairBudget budget;
    private final long lifecycleGeneration;
    private final CompletableFuture<WorldStateRepairResult> completion;
    @Nullable private WorldStateRepairResult cancellationReason;

    private Attempt(
        final Request request,
        final WorldStateRepairBudget budget,
        final long lifecycleGeneration) {
      this.target = request.target;
      this.completion = request.completion;
      this.budget = budget;
      this.lifecycleGeneration = lifecycleGeneration;
    }

    private void cancel(final WorldStateRepairResult reason) {
      if (cancellationReason == null) {
        cancellationReason = reason;
      }
      budget.cancel();
    }
  }
}
