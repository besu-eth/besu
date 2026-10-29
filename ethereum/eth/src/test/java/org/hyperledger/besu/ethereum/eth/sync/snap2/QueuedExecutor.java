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
package org.hyperledger.besu.ethereum.eth.sync.snap2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Keeps what is submitted, to run it in turn on the test thread when the test says. Its periodic
 * tasks only run on a tick.
 */
public final class QueuedExecutor extends AbstractExecutorService
    implements ScheduledExecutorService {
  // far more than a test queues before it waits for an answer
  private static final int MAX_TASKS = 1_000_000;

  private final List<Runnable> tasks = new ArrayList<>();
  private final List<Runnable> periodic = new ArrayList<>();
  private boolean shutdown;

  /** Runs the tasks, and those they queue, until none is left. */
  public void runAll() {
    for (int ran = 0; !tasks.isEmpty(); ran++) {
      assertThat(ran).as("tasks run in a row").isLessThan(MAX_TASKS);
      tasks.removeFirst().run();
      // a pipeline stage, or an interrupted back-fill, leaves the test thread interrupted
      Thread.interrupted();
    }
  }

  /** Runs the periodic tasks, as when their delay passes, then what they queued. */
  public void tick() {
    if (!shutdown) {
      periodic.forEach(Runnable::run);
    }
    runAll();
  }

  @Override
  public void execute(final Runnable command) {
    tasks.add(command);
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(
      final Runnable command, final long initialDelay, final long delay, final TimeUnit unit) {
    periodic.add(command);
    return mock(ScheduledFuture.class);
  }

  @Override
  public ScheduledFuture<?> schedule(
      final Runnable command, final long delay, final TimeUnit unit) {
    throw new UnsupportedOperationException("only periodic tasks run on a tick");
  }

  @Override
  public <V> ScheduledFuture<V> schedule(
      final Callable<V> callable, final long delay, final TimeUnit unit) {
    throw new UnsupportedOperationException("only periodic tasks run on a tick");
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(
      final Runnable command, final long initialDelay, final long period, final TimeUnit unit) {
    throw new UnsupportedOperationException("only tasks with a fixed delay run on a tick");
  }

  @Override
  public void shutdown() {
    shutdown = true;
  }

  @Override
  public List<Runnable> shutdownNow() {
    shutdown = true;
    return List.of();
  }

  @Override
  public boolean isShutdown() {
    return shutdown;
  }

  @Override
  public boolean isTerminated() {
    return shutdown && tasks.isEmpty();
  }

  @Override
  public boolean awaitTermination(final long timeout, final TimeUnit unit) {
    return isTerminated();
  }
}
