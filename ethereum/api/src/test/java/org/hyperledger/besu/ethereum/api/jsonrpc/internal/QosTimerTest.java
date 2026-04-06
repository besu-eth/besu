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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class QosTimerTest {
  static Vertx vertx = Vertx.vertx();
  private final VertxTestContext testContext = new VertxTestContext();

  @Disabled(
      "fails on CI with short timeouts and don't want to slow test suite down with longer ones")
  @Test
  public void shouldExecuteConsecutivelyAtTimeout() throws InterruptedException {
    final long TEST_QOS_TIMEOUT = 100L;
    final int INVOCATIONS_COUNT = 2;
    final CountDownLatch latch = new CountDownLatch(INVOCATIONS_COUNT);
    final long startTime = System.currentTimeMillis();
    new QosTimer(vertx, TEST_QOS_TIMEOUT, z -> latch.countDown());
    latch.await();
    final long executionTime = System.currentTimeMillis() - startTime;
    assertThat(executionTime)
        .as("Execution ended ahead of time")
        .isGreaterThanOrEqualTo(TEST_QOS_TIMEOUT * INVOCATIONS_COUNT);
  }

  @Test
  public void shouldExecuteOnceAtTimeout() throws InterruptedException {
    final long TEST_QOS_TIMEOUT = 75L;
    final CountDownLatch latch = new CountDownLatch(1);
    final long startTime = System.currentTimeMillis();
    new QosTimer(vertx, TEST_QOS_TIMEOUT, z -> latch.countDown());
    latch.await();
    final long executionTime = System.currentTimeMillis() - startTime;
    assertThat(executionTime)
        .as("Execution ended ahead of time")
        .isGreaterThanOrEqualTo(TEST_QOS_TIMEOUT);
  }

  @Test
  public void shouldNotExecuteBeforeTimeout() throws InterruptedException {
    final long TEST_QOS_TIMEOUT = 200L;
    final CountDownLatch latch = new CountDownLatch(1);
    new QosTimer(vertx, TEST_QOS_TIMEOUT, z -> latch.countDown());
    assertThat(latch.await(50L, TimeUnit.MILLISECONDS)).isFalse();
  }

  @Test
  public void shouldNotRescheduleAfterStop() throws InterruptedException {
    final long TEST_QOS_TIMEOUT = 50L;
    final CountDownLatch latch = new CountDownLatch(1);
    final var timer = new QosTimer(vertx, TEST_QOS_TIMEOUT, z -> latch.countDown());
    timer.stop();
    // wait long enough for the timer to have fired if it weren't stopped
    assertThat(latch.await(200L, TimeUnit.MILLISECONDS)).isFalse();
  }

  @Test
  public void shouldNotExecuteWhenReset() throws InterruptedException {
    final long TEST_QOS_TIMEOUT = 50L;
    final CountDownLatch latch = new CountDownLatch(1);
    final var timer = new QosTimer(vertx, TEST_QOS_TIMEOUT, z -> latch.countDown());
    // reset QoS timer every 25 millis
    vertx.setPeriodic(25L, z -> timer.resetTimer());
    assertThat(latch.await(200L, TimeUnit.MILLISECONDS)).isFalse();
    testContext.completeNow();
  }
}
