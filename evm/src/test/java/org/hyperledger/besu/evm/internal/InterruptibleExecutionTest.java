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
package org.hyperledger.besu.evm.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class InterruptibleExecutionTest {

  @AfterEach
  void cleanUp() {
    EvmKillSwitch.disarmForTesting();
    Thread.interrupted(); // do not leak an interrupt flag into other tests
  }

  @Test
  void runsTheTaskAndReturnsItsResult() {
    assertThat(new InterruptibleExecution().run(() -> 42)).isEqualTo(42);
  }

  @Test
  void interruptStopsARunningTaskAndReleasesTheArmWhenItExits() throws Exception {
    final InterruptibleExecution execution = new InterruptibleExecution();
    final CountDownLatch polling = new CountDownLatch(1);
    final AtomicReference<Throwable> thrown = new AtomicReference<>();
    final AtomicBoolean flagAfterTask = new AtomicBoolean();
    final Thread worker =
        new Thread(
            () -> {
              try {
                execution.run(
                    () -> {
                      polling.countDown();
                      while (true) {
                        EvmKillSwitch.poll();
                        Thread.onSpinWait();
                      }
                    });
              } catch (final Throwable t) {
                thrown.set(t);
              }
              flagAfterTask.set(Thread.currentThread().isInterrupted());
            });
    worker.start();
    assertThat(polling.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();

    execution.interrupt();
    worker.join(10_000);

    assertThat(worker.isAlive()).isFalse();
    assertThat(thrown.get()).isInstanceOf(EvmInterruptedException.class);
    // the kill observed its own completion: the arm is released and the flag did not leak
    assertThat(EvmKillSwitch.isArmed()).isFalse();
    assertThat(flagAfterTask).isFalse();
  }

  @Test
  void interruptBeforeOrAfterATaskIsANoOp() {
    final InterruptibleExecution execution = new InterruptibleExecution();
    execution.interrupt();
    assertThat(EvmKillSwitch.isArmed()).isFalse();
    assertThat(Thread.currentThread().isInterrupted()).isFalse();

    execution.run(() -> 1);
    execution.interrupt();
    assertThat(EvmKillSwitch.isArmed()).isFalse();
    assertThat(Thread.currentThread().isInterrupted()).isFalse();
  }

  @Test
  void clearsTheFlagAndReleasesWhenAnInterruptedTaskFinishesOnItsOwn() {
    final InterruptibleExecution execution = new InterruptibleExecution();
    execution.run(
        () -> {
          // the interrupt lands while the task runs, but the task never polls
          execution.interrupt();
          assertThat(EvmKillSwitch.isArmed()).isTrue();
          assertThat(Thread.currentThread().isInterrupted()).isTrue();
        });

    assertThat(EvmKillSwitch.isArmed()).isFalse();
    assertThat(Thread.currentThread().isInterrupted()).isFalse();
  }
}
