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
package org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ScheduledExecutorTimerUtilTest {

  private ScheduledExecutorService scheduler;
  private ScheduledExecutorTimerUtil timerUtil;

  @BeforeEach
  public void setUp() {
    scheduler = Executors.newSingleThreadScheduledExecutor();
    timerUtil = new ScheduledExecutorTimerUtil(scheduler);
  }

  @AfterEach
  public void tearDown() {
    scheduler.shutdownNow();
  }

  @Test
  public void setTimer_firesOnceAfterDelay() {
    final AtomicInteger count = new AtomicInteger(0);
    timerUtil.setTimer(10, count::incrementAndGet);

    Awaitility.await().atMost(1, TimeUnit.SECONDS).until(() -> count.get() == 1);

    // wait a bit longer to confirm it doesn't fire again
    try {
      Thread.sleep(50);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertThat(count.get()).isEqualTo(1);
  }

  @Test
  public void setTimer_logsButDoesNotPropagateWhenHandlerThrows() {
    final AtomicInteger count = new AtomicInteger(0);
    timerUtil.setTimer(
        10,
        () -> {
          count.incrementAndGet();
          throw new RuntimeException("boom");
        });

    Awaitility.await().atMost(1, TimeUnit.SECONDS).until(() -> count.get() == 1);

    // wait a bit longer to confirm the throwing handler didn't crash the scheduler
    try {
      Thread.sleep(50);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertThat(count.get()).isEqualTo(1);
  }

  @Test
  public void setPeriodic_firesRepeatedly() {
    final AtomicInteger count = new AtomicInteger(0);
    final long id = timerUtil.setPeriodic(10, "test-timer", count::incrementAndGet);

    Awaitility.await().atMost(1, TimeUnit.SECONDS).until(() -> count.get() >= 3);

    timerUtil.cancelTimer(id);
  }

  @Test
  public void setPeriodic_continuesAfterHandlerThrows() {
    final AtomicInteger count = new AtomicInteger(0);
    final long id =
        timerUtil.setPeriodic(
            10,
            "test-timer",
            () -> {
              count.incrementAndGet();
              throw new RuntimeException("boom");
            });

    Awaitility.await().atMost(1, TimeUnit.SECONDS).until(() -> count.get() >= 3);

    timerUtil.cancelTimer(id);
  }

  @Test
  public void setPeriodic_zeroMsInterval_doesNotThrowAndFiresRepeatedly() {
    final AtomicInteger count = new AtomicInteger(0);
    final long id = timerUtil.setPeriodic(0, "test-timer", count::incrementAndGet);

    Awaitility.await().atMost(1, TimeUnit.SECONDS).until(() -> count.get() >= 3);

    timerUtil.cancelTimer(id);
  }

  @Test
  public void cancelTimer_preventsFiring() {
    final AtomicInteger count = new AtomicInteger(0);
    final long id = timerUtil.setTimer(100, count::incrementAndGet);
    timerUtil.cancelTimer(id);

    try {
      Thread.sleep(200);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertThat(count.get()).isEqualTo(0);
  }

  @Test
  public void cancelTimer_unknownId_doesNotThrow() {
    assertThatCode(() -> timerUtil.cancelTimer(99999L)).doesNotThrowAnyException();
  }

  @Test
  public void cancelPeriodic_stopsFiring() {
    final AtomicInteger count = new AtomicInteger(0);
    final long id = timerUtil.setPeriodic(10, "test-timer", count::incrementAndGet);

    Awaitility.await().atMost(1, TimeUnit.SECONDS).until(() -> count.get() >= 2);
    timerUtil.cancelTimer(id);
    final int countAtCancel = count.get();

    try {
      Thread.sleep(50);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertThat(count.get()).isEqualTo(countAtCancel);
  }
}
