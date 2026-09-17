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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.vertx.core.Vertx;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EngineQosTimerTest {
  private static final long TEST_QOS_TIMEOUT = 200L;

  private EngineQosTimer engineQosTimer;
  private Vertx vertx;
  private CountDownLatch warningLatch;
  private AtomicInteger warningCount;

  @BeforeEach
  public void setUp() {
    vertx = Vertx.vertx();
    warningLatch = new CountDownLatch(1);
    warningCount = new AtomicInteger(0);
    engineQosTimer =
        new EngineQosTimer(
            vertx,
            TEST_QOS_TIMEOUT,
            ignored -> {
              warningCount.incrementAndGet();
              warningLatch.countDown();
            });
  }

  @AfterEach
  public void cleanUp() {
    engineQosTimer.stop();
    vertx.close().toCompletionStage().toCompletableFuture().join();
  }

  @Test
  public void shouldNotWarnWhenCalledWithinTimeout() throws InterruptedException {
    vertx.setPeriodic(TEST_QOS_TIMEOUT / 4, ignored -> engineQosTimer.executionEngineCalled());

    assertThat(warningLatch.await(TEST_QOS_TIMEOUT * 3, TimeUnit.MILLISECONDS)).isFalse();
    assertThat(warningCount).hasValue(0);
  }

  @Test
  public void shouldWarnWhenNotCalledWithinTimeout() throws InterruptedException {
    assertThat(warningLatch.await(TEST_QOS_TIMEOUT * 5, TimeUnit.MILLISECONDS)).isTrue();
    assertThat(warningCount.get()).isGreaterThanOrEqualTo(1);
  }
}
