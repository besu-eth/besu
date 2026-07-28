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
package org.hyperledger.besu.ethereum.eth.manager;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EthSchedulerShutdownTest {

  private EthScheduler ethScheduler;
  private ExecutorService syncWorkerExecutor;
  private ScheduledExecutorService scheduledExecutor;
  private ExecutorService txWorkerExecutor;
  private ExecutorService servicesExecutor;
  private ExecutorService computationExecutor;
  private ExecutorService blockCreationExecutor;

  @BeforeEach
  public void setup() {
    scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    syncWorkerExecutor = Executors.newSingleThreadExecutor();
    txWorkerExecutor = Executors.newSingleThreadExecutor();
    servicesExecutor = Executors.newSingleThreadExecutor();
    computationExecutor = Executors.newSingleThreadExecutor();
    blockCreationExecutor = Executors.newSingleThreadExecutor();
    ethScheduler =
        new EthScheduler(
            syncWorkerExecutor,
            scheduledExecutor,
            txWorkerExecutor,
            servicesExecutor,
            computationExecutor,
            blockCreationExecutor);
  }

  @Test
  public void shutdown_syncWorkerShutsDown() throws InterruptedException {
    final MockEthTask task1 = new MockEthTask(1);
    final MockEthTask task2 = new MockEthTask();

    ethScheduler.scheduleSyncWorkerTask(task1::executeTask);
    ethScheduler.scheduleSyncWorkerTask(task2::executeTask);
    ethScheduler.stop();

    assertThat(syncWorkerExecutor.isShutdown()).isTrue();

    ethScheduler.awaitStop();

    assertThat(syncWorkerExecutor.isShutdown()).isTrue();
    assertThat(syncWorkerExecutor.isTerminated()).isTrue();
    assertThat(task2.hasBeenStarted()).isFalse();
  }

  @Test
  public void shutdown_scheduledWorkerShutsDown() throws InterruptedException {
    final MockEthTask task = new MockEthTask(1);

    ethScheduler.scheduleFutureTask(task::executeTask, Duration.ofMillis(0));
    ethScheduler.stop();

    assertThat(scheduledExecutor.isShutdown()).isTrue();

    ethScheduler.awaitStop();

    assertThat(scheduledExecutor.isShutdown()).isTrue();
    assertThat(scheduledExecutor.isTerminated()).isTrue();
  }

  @Test
  public void shutdown_txWorkerShutsDown() throws InterruptedException {
    final MockEthTask task1 = new MockEthTask(1);
    final MockEthTask task2 = new MockEthTask();

    ethScheduler.scheduleTxWorkerTask(task1::executeTask);
    ethScheduler.scheduleTxWorkerTask(task2::executeTask);
    ethScheduler.stop();

    assertThat(txWorkerExecutor.isShutdown()).isTrue();

    ethScheduler.awaitStop();

    assertThat(txWorkerExecutor.isShutdown()).isTrue();
    assertThat(txWorkerExecutor.isTerminated()).isTrue();
    assertThat(task2.hasBeenStarted()).isFalse();
  }

  @Test
  public void shutdown_servicesShutsDown() throws InterruptedException {
    final MockEthTask task1 = new MockEthTask(1);
    final MockEthTask task2 = new MockEthTask();

    ethScheduler.scheduleServiceTask(task1);
    ethScheduler.scheduleServiceTask(task2);
    ethScheduler.stop();

    assertThat(servicesExecutor.isShutdown()).isTrue();

    ethScheduler.awaitStop();

    assertThat(servicesExecutor.isShutdown()).isTrue();
    assertThat(servicesExecutor.isTerminated()).isTrue();
    assertThat(task2.hasBeenStarted()).isFalse();
  }

  @Test
  public void shutdown_computationShutsDown() throws InterruptedException {
    final MockEthTask task1 = new MockEthTask(1);
    final MockEthTask task2 = new MockEthTask();

    ethScheduler.scheduleComputationTask(
        () -> {
          task1.executeTask();
          return Integer.MAX_VALUE;
        });
    ethScheduler.scheduleComputationTask(
        () -> {
          task2.executeTask();
          return Integer.MAX_VALUE;
        });
    ethScheduler.stop();

    assertThat(computationExecutor.isShutdown()).isTrue();

    ethScheduler.awaitStop();

    assertThat(computationExecutor.isShutdown()).isTrue();
    assertThat(computationExecutor.isTerminated()).isTrue();
    assertThat(task2.hasBeenStarted()).isFalse();
  }

  @Test
  public void shutdown_blockCreationShutsDown() throws InterruptedException {
    final MockEthTask task1 = new MockEthTask(1);
    final MockEthTask task2 = new MockEthTask();

    ethScheduler.scheduleBlockCreationTask(1L, task1::executeTask);
    ethScheduler.scheduleBlockCreationTask(2L, task2::executeTask);
    ethScheduler.stop();

    assertThat(blockCreationExecutor.isShutdown()).isTrue();

    ethScheduler.awaitStop();

    assertThat(blockCreationExecutor.isShutdown()).isTrue();
    assertThat(blockCreationExecutor.isTerminated()).isTrue();
    assertThat(task2.hasBeenStarted()).isFalse();
  }
}
