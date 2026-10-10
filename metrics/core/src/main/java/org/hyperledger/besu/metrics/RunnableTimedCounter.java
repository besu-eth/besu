/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.metrics;

import org.hyperledger.besu.plugin.services.metrics.Counter;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Counter that triggers a specific task if the specified interval has elapsed. */
public class RunnableTimedCounter implements Counter {

  private final Counter backedCounter;
  private final Runnable task;
  private final long intervalMillis;
  private final AtomicLong stepCounter;
  private final AtomicLong nextExecutionAtMillis;

  /**
   * Instantiates a new Runnable timed counter.
   *
   * @param backedCounter the backed counter
   * @param task the task
   * @param interval the interval
   * @param unit the unit
   */
  public RunnableTimedCounter(
      final Counter backedCounter, final Runnable task, final long interval, final TimeUnit unit) {
    this.backedCounter = backedCounter;
    this.task = task;
    this.stepCounter = new AtomicLong(0);
    this.intervalMillis = unit.toMillis(interval);
    this.nextExecutionAtMillis = new AtomicLong(System.currentTimeMillis() + intervalMillis);
  }

  /**
   * Increments the stepCounter by 1
   *
   * <p>{@link #inc(long) inc} method
   */
  @Override
  public void inc() {
    this.inc(1);
  }

  /**
   * Increments the stepCounter by amount. Triggers the runnable if interval has elapsed
   *
   * <p>The task is triggered by at most one caller per elapsed interval: the deadline update is a
   * compare-and-set, so when several threads cross the same deadline concurrently only the thread
   * that wins the race runs the task.
   *
   * @param amount the value to add to the stepCounter.
   */
  @Override
  public void inc(final long amount) {
    backedCounter.inc(amount);
    stepCounter.addAndGet(amount);
    final long now = System.currentTimeMillis();
    final long nextExecution = nextExecutionAtMillis.get();
    if (nextExecution < now
        && nextExecutionAtMillis.compareAndSet(nextExecution, now + intervalMillis)) {
      task.run();
    }
  }

  /**
   * Get Step Counter.
   *
   * @return the long
   */
  public long get() {
    return stepCounter.get();
  }
}
