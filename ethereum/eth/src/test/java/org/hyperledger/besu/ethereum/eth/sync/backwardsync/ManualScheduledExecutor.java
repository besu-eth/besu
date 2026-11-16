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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A scheduled executor that runs nothing on its own: tasks wait until the test calls {@link
 * #runPending()} or {@link #advance(Duration)}, on the calling thread. Delays count against a fake
 * clock that only {@link #advance(Duration)} moves.
 */
public final class ManualScheduledExecutor extends AbstractExecutorService
    implements ScheduledExecutorService {

  private final PriorityQueue<Task<?>> queue = new PriorityQueue<>();
  private long nanoTime;
  private long sequence;
  private boolean shutdown;

  /** Returns the fake clock, in nanoseconds. */
  public synchronized long nanoTime() {
    return nanoTime;
  }

  /** Runs the tasks that are due, and those they queue that are due, until none is left. */
  public void runPending() {
    for (Task<?> task = pollDue(nanoTime()); task != null; task = pollDue(nanoTime())) {
      task.run();
    }
  }

  /** Moves the clock forward, running the tasks as their delay passes, in order. */
  public void advance(final Duration duration) {
    final long target = nanoTime() + duration.toNanos();
    runPending();
    for (Long next = nextDue(); next != null && next <= target; next = nextDue()) {
      synchronized (this) {
        nanoTime = Math.max(nanoTime, next);
      }
      runPending();
    }
    synchronized (this) {
      nanoTime = target;
    }
    runPending();
  }

  /** Returns the number of tasks that wait, those of a delay not passed included. */
  public synchronized int pendingTasks() {
    return queue.size();
  }

  @Override
  public void execute(final Runnable command) {
    schedule(command, 0, TimeUnit.NANOSECONDS);
  }

  @Override
  public ScheduledFuture<?> schedule(
      final Runnable command, final long delay, final TimeUnit unit) {
    return enqueue(new Task<Void>(command, null, unit.toNanos(delay), 0));
  }

  @Override
  public <V> ScheduledFuture<V> schedule(
      final Callable<V> callable, final long delay, final TimeUnit unit) {
    return enqueue(new Task<>(callable, unit.toNanos(delay), 0));
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(
      final Runnable command, final long initialDelay, final long period, final TimeUnit unit) {
    return scheduleWithFixedDelay(command, initialDelay, period, unit);
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(
      final Runnable command, final long initialDelay, final long delay, final TimeUnit unit) {
    final long period = Math.max(1, unit.toNanos(delay));
    return enqueue(new Task<Void>(command, null, unit.toNanos(initialDelay), period));
  }

  @Override
  public synchronized void shutdown() {
    shutdown = true;
    // as a real executor, periodic tasks stop, delayed tasks still run
    queue.removeIf(task -> task.period > 0);
  }

  @Override
  public synchronized List<Runnable> shutdownNow() {
    shutdown = true;
    final List<Runnable> pending = new ArrayList<>(queue);
    queue.clear();
    return pending;
  }

  @Override
  public synchronized boolean isShutdown() {
    return shutdown;
  }

  @Override
  public synchronized boolean isTerminated() {
    return shutdown && queue.isEmpty();
  }

  @Override
  public boolean awaitTermination(final long timeout, final TimeUnit unit) {
    return isTerminated();
  }

  private synchronized <V> Task<V> enqueue(final Task<V> task) {
    if (shutdown) {
      throw new RejectedExecutionException("executor is shut down");
    }
    queue.add(task.dueAt(nanoTime, sequence++));
    return task;
  }

  private synchronized Task<?> pollDue(final long now) {
    final Task<?> next = queue.peek();
    return next != null && next.due <= now ? queue.poll() : null;
  }

  private synchronized Long nextDue() {
    // a cancelled task does not hold the clock back
    queue.removeIf(Task::isCancelled);
    final Task<?> next = queue.peek();
    return next == null ? null : next.due;
  }

  private final class Task<V> extends FutureTask<V> implements ScheduledFuture<V> {
    // the initial delay, then the period
    private long delay;
    private final long period;
    private long due;
    private long order;

    Task(final Runnable command, final V result, final long delay, final long period) {
      super(command, result);
      this.delay = delay;
      this.period = period;
    }

    Task(final Callable<V> callable, final long delay, final long period) {
      super(callable);
      this.delay = delay;
      this.period = period;
    }

    Task<V> dueAt(final long now, final long sequenceNumber) {
      due = now + delay;
      order = sequenceNumber;
      return this;
    }

    @Override
    public void run() {
      if (period == 0) {
        super.run();
      } else if (runAndReset() && !isShutdown()) {
        delay = period;
        enqueue(this);
      }
    }

    @Override
    public long getDelay(final TimeUnit unit) {
      return unit.convert(due - nanoTime(), TimeUnit.NANOSECONDS);
    }

    @Override
    public int compareTo(final Delayed other) {
      final Task<?> that = (Task<?>) other;
      return due != that.due ? Long.compare(due, that.due) : Long.compare(order, that.order);
    }
  }
}
