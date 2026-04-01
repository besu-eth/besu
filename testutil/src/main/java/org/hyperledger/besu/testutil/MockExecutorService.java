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
package org.hyperledger.besu.testutil;

import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/** The Mock executor service. */
public class MockExecutorService implements ExecutorService {

  private boolean autoRun = true;

  private final List<ExecutorTask<?>> tasks = new ArrayList<>();

  /** Default constructor */
  public MockExecutorService() {}

  /**
   * Gets tasks.
   *
   * @return the tasks
   */
  /**
   * Gets futures.
   *
   * @return the futures
   */
  // Test utility for inspecting executor's futures
  public List<Future<?>> getFutures() {
    return tasks.stream().map(ExecutorTask::getFuture).collect(Collectors.toList());
  }

  /**
   * Sets auto run.
   *
   * @param shouldAutoRunTasks the should auto run tasks
   */
  public void setAutoRun(final boolean shouldAutoRunTasks) {
    this.autoRun = shouldAutoRunTasks;
  }

  /** Run pending futures. */
  public void runPendingFutures() {
    final List<ExecutorTask<?>> currentTasks = new ArrayList<>(tasks);
    currentTasks.forEach(ExecutorTask::run);
  }

  /**
   * Gets pending futures count.
   *
   * @return the pending futures count
   */
  public long getPendingFuturesCount() {
    return tasks.stream().filter(ExecutorTask::isPending).count();
  }

  /**
   * Run pending futures in separate threads.
   *
   * @param executorService the executor service
   */
  public void runPendingFuturesInSeparateThreads(final ExecutorService executorService) {
    final List<ExecutorTask<?>> currentTasks = new ArrayList<>(tasks);
    currentTasks.forEach(task -> executorService.execute(task::run));
  }

  @Override
  public void shutdown() {}

  @Override
  public List<Runnable> shutdownNow() {
    return Collections.emptyList();
  }

  @Override
  public boolean isShutdown() {
    return false;
  }

  @Override
  public boolean isTerminated() {
    return false;
  }

  @Override
  public boolean awaitTermination(final long timeout, final TimeUnit unit)
      throws InterruptedException {
    return false;
  }

  @Override
  public <T> Future<T> submit(final Callable<T> task) {
    ExecutorTask<T> execTask = new ExecutorTask<>(task);
    tasks.add(execTask);
    if (autoRun) {
      execTask.run();
    }

    return execTask.getFuture();
  }

  @Override
  public <T> Future<T> submit(final Runnable task, final T result) {
    return submit(
        () -> {
          task.run();
          return result;
        });
  }

  @Override
  public Future<?> submit(final Runnable task) {
    return submit(
        () -> {
          task.run();
          return null;
        });
  }

  @Override
  public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      final Collection<? extends Callable<T>> tasks, final long timeout, final TimeUnit unit)
      throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T invokeAny(final Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T invokeAny(
      final Collection<? extends Callable<T>> tasks, final long timeout, final TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void execute(final Runnable command) {
    submit(command);
  }

  private static class ExecutorTask<T> {
    private final CompletableFuture<T> future;
    private final Callable<T> taskRunner;
    private boolean isPending = true;

    private ExecutorTask(final Callable<T> taskRunner) {
      this.future = spy(new CompletableFuture<>());
      this.taskRunner = taskRunner;
    }

    /** Run. */
    public void run() {
      if (!isPending) {
        return;
      }

      isPending = false;
      try {
        T result = taskRunner.call();
        future.complete(result);
      } catch (final Exception e) {
        future.completeExceptionally(e);
      }
    }

    /**
     * Gets future.
     *
     * @return the future
     */
    public CompletableFuture<T> getFuture() {
      return future;
    }

    /**
     * Is pending.
     *
     * @return the boolean
     */
    public boolean isPending() {
      return isPending;
    }
  }
}
