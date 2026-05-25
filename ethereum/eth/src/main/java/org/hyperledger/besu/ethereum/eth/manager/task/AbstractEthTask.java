/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.eth.manager.task;

import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.plugin.services.metrics.OperationTimer;

import java.util.Collection;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;

public abstract class AbstractEthTask<T> implements EthTask<T> {

  /**
   * Stackless singleton used when a sub-task is rejected because this task has already been
   * cancelled. Capturing a full stack trace on every cancellation adds unnecessary allocation and
   * CPU overhead (native stack-walk) in the common case where the trace is never inspected. Safe to
   * share: CancellationException carries no mutable state.
   */
  @SuppressWarnings("StaticAssignmentOfThrowable")
  private static final CancellationException TASK_CANCELLED_EXCEPTION =
      new CancellationException("Task already cancelled") {
        @Override
        public synchronized Throwable fillInStackTrace() {
          return this;
        }
      };

  private double taskTimeInSec = -1.0D;
  private final OperationTimer taskTimer;
  protected final CompletableFuture<T> result = new CompletableFuture<>();
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final Collection<CompletableFuture<?>> subTaskFutures = new ConcurrentLinkedDeque<>();

  protected AbstractEthTask(final MetricsSystem metricsSystem) {
    this.taskTimer = buildOperationTimer(metricsSystem, getClass().getSimpleName());
  }

  protected AbstractEthTask(final OperationTimer taskTimer) {
    this.taskTimer = taskTimer;
  }

  private static OperationTimer buildOperationTimer(
      final MetricsSystem metricsSystem, final String taskName) {
    final LabelledMetric<OperationTimer> ethTasksTimer =
        metricsSystem.createLabelledTimer(
            BesuMetricCategory.SYNCHRONIZER, "task", "Internal processing tasks", "taskName");
    if (ethTasksTimer == NoOpMetricsSystem.NO_OP_LABELLED_1_OPERATION_TIMER) {
      return () ->
          new OperationTimer.TimingContext() {
            final Stopwatch stopwatch = Stopwatch.createStarted();

            @Override
            public double stopTimer() {
              return stopwatch.elapsed(TimeUnit.MILLISECONDS) / 1000.0;
            }
          };
    } else {
      return ethTasksTimer.labels(taskName);
    }
  }

  @Override
  public final CompletableFuture<T> run() {
    if (!result.isDone() && started.compareAndSet(false, true)) {
      executeTaskTimed();
      result.whenComplete((r, t) -> cleanup());
    }
    return result;
  }

  @Override
  public final CompletableFuture<T> runAsync(final ExecutorService executor) {
    if (!result.isDone() && started.compareAndSet(false, true)) {
      executor.execute(this::executeTaskTimed);
      result.whenComplete((r, t) -> cleanup());
    }
    return result;
  }

  @Override
  public final void cancel() {
    result.cancel(false);
  }

  @VisibleForTesting
  public final boolean isDone() {
    return result.isDone();
  }

  public final boolean isFailed() {
    return result.isCompletedExceptionally();
  }

  @VisibleForTesting
  public final boolean isCancelled() {
    return result.isCancelled();
  }

  /**
   * Utility for executing completable futures that handles cleanup if this EthTask is cancelled.
   *
   * @param subTask a subTask to execute
   * @param <S> the type of data returned from the CompletableFuture
   * @return The completableFuture that was executed
   */
  protected final <S> CompletableFuture<S> executeSubTask(
      final Supplier<CompletableFuture<S>> subTask) {
    synchronized (result) {
      if (!isCancelled()) {
        final CompletableFuture<S> subTaskFuture = subTask.get();
        subTaskFutures.add(subTaskFuture);
        subTaskFuture.whenComplete((r, t) -> subTaskFutures.remove(subTaskFuture));
        return subTaskFuture;
      } else {
        return CompletableFuture.failedFuture(TASK_CANCELLED_EXCEPTION);
      }
    }
  }

  /**
   * Helper method for sending subTask to worker that will clean up if this EthTask is cancelled.
   *
   * @param scheduler the scheduler that will run worker task
   * @param subTask a subTask to execute
   * @param <S> the type of data returned from the CompletableFuture
   * @return The completableFuture that was executed
   */
  protected final <S> CompletableFuture<S> executeWorkerSubTask(
      final EthScheduler scheduler, final Supplier<CompletableFuture<S>> subTask) {
    return executeSubTask(() -> scheduler.scheduleSyncWorkerTask(subTask));
  }

  /** Execute core task logic. */
  protected abstract void executeTask();

  /** Executes the task while timed by a timer. */
  public void executeTaskTimed() {
    final OperationTimer.TimingContext timingContext = taskTimer.startTimer();
    try {
      executeTask();
    } finally {
      taskTimeInSec = timingContext.stopTimer();
    }
  }

  public double getTaskTimeInSec() {
    return taskTimeInSec;
  }

  /** Cleanup any resources when task completes. */
  protected void cleanup() {
    for (final CompletableFuture<?> subTaskFuture : subTaskFutures) {
      subTaskFuture.cancel(false);
    }
  }
}
