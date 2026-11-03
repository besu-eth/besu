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
package org.hyperledger.besu.ethereum.eth.sync.snap2.net;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Delayed;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A scheduler loop whose delayed tasks, timeouts and retry pauses, wait while held until the test
 * releases them. The periodic dispatch keeps running.
 */
final class TestLoop extends ScheduledThreadPoolExecutor {

  private final List<HeldTask> held = new ArrayList<>();
  private volatile boolean holding;

  TestLoop() {
    super(1, Thread.ofPlatform().name("snap2-test-scheduler").daemon().factory());
  }

  /** Holds the delayed tasks scheduled from now on. */
  void hold() {
    holding = true;
  }

  /** Runs the delayed tasks held so far; those they schedule are held in turn. */
  void release() {
    final List<HeldTask> tasks;
    synchronized (held) {
      tasks = List.copyOf(held);
      held.clear();
    }
    tasks.forEach(this::execute);
  }

  /** Returns the number of delayed tasks held, cancelled ones included. */
  int heldCount() {
    synchronized (held) {
      return held.size();
    }
  }

  @Override
  public ScheduledFuture<?> schedule(
      final Runnable command, final long delay, final TimeUnit unit) {
    if (!holding || delay <= 0 || isShutdown()) {
      return super.schedule(command, delay, unit);
    }
    final HeldTask task = new HeldTask(command);
    synchronized (held) {
      held.add(task);
    }
    return task;
  }

  private static final class HeldTask extends FutureTask<Void> implements ScheduledFuture<Void> {

    private HeldTask(final Runnable command) {
      super(command, null);
    }

    @Override
    public long getDelay(final TimeUnit unit) {
      return 0;
    }

    @Override
    public int compareTo(final Delayed other) {
      return Long.compare(getDelay(TimeUnit.NANOSECONDS), other.getDelay(TimeUnit.NANOSECONDS));
    }
  }
}
