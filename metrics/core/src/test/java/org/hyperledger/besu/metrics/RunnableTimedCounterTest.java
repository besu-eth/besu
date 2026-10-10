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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.plugin.services.metrics.Counter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class RunnableTimedCounterTest {

  @Mock Counter backedCounter;

  @Test
  public void shouldNotRunTaskIfIntervalNotElapsed() {

    RunnableTimedCounter rtc =
        new RunnableTimedCounter(
            backedCounter, () -> fail("Must not be called"), 1L, TimeUnit.MINUTES);

    rtc.inc();
    verify(backedCounter).inc(1L);
  }

  @Test
  public void shouldRunTaskIfIntervalElapsed() throws InterruptedException {

    Runnable task = mock(Runnable.class);

    RunnableTimedCounter rtc =
        new RunnableTimedCounter(backedCounter, task, 1L, TimeUnit.MICROSECONDS);

    Thread.sleep(1L);

    rtc.inc();

    verify(backedCounter).inc(1L);
    verify(task).run();
  }

  @Test
  public void shouldRunTaskOnceWhenThreadsCrossTheDeadlineConcurrently()
      throws InterruptedException {
    final int threadCount = 16;
    final AtomicLong taskRunCount = new AtomicLong();
    final RunnableTimedCounter rtc =
        new RunnableTimedCounter(
            backedCounter, taskRunCount::incrementAndGet, 1L, TimeUnit.MICROSECONDS);

    // let the first deadline elapse so that every thread sees a deadline that has passed
    Thread.sleep(2L);

    final CyclicBarrier startBarrier = new CyclicBarrier(threadCount);
    final CountDownLatch finished = new CountDownLatch(threadCount);
    final List<Thread> threads = new ArrayList<>();
    final long startMillis = System.currentTimeMillis();
    for (int i = 0; i < threadCount; i++) {
      final Thread thread =
          new Thread(
              () -> {
                try {
                  startBarrier.await();
                  rtc.inc();
                } catch (final Exception e) {
                  fail("Unexpected exception while incrementing the counter", e);
                } finally {
                  finished.countDown();
                }
              });
      threads.add(thread);
      thread.start();
    }
    final long endMillis = System.currentTimeMillis();
    for (final Thread thread : threads) {
      thread.join();
    }

    verify(backedCounter, times(threadCount)).inc(1L);
    assertThat(taskRunCount.get()).isPositive();
    // Each execution schedules the next deadline no earlier than the current millisecond, so two
    // executions cannot fall in the same millisecond: the number of executions is bounded by the
    // number of milliseconds the concurrent increments actually spanned.
    assertThat(taskRunCount.get()).isLessThanOrEqualTo(endMillis - startMillis + 1);
  }
}
