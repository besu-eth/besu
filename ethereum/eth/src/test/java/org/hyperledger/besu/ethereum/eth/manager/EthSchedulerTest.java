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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;
import org.hyperledger.besu.testutil.MockExecutorService;
import org.hyperledger.besu.testutil.MockScheduledExecutor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EthSchedulerTest {

  private DeterministicEthScheduler ethScheduler;
  private MockExecutorService syncWorkerExecutor;
  private MockScheduledExecutor scheduledExecutor;
  private AtomicBoolean shouldTimeout;

  @BeforeEach
  public void setup() {
    shouldTimeout = new AtomicBoolean(false);
    ethScheduler = new DeterministicEthScheduler(shouldTimeout::get);
    syncWorkerExecutor = ethScheduler.mockSyncWorkerExecutor();
    scheduledExecutor = ethScheduler.mockScheduledExecutor();
  }

  @Test
  public void scheduleWorkerTask_completesWhenScheduledTaskCompletes() {
    final CompletableFuture<Object> future = new CompletableFuture<>();
    final CompletableFuture<Object> result = ethScheduler.scheduleSyncWorkerTask(() -> future);

    assertThat(result.isDone()).isFalse();
    future.complete("bla");
    assertThat(result.isDone()).isTrue();
    assertThat(result.isCompletedExceptionally()).isFalse();
    assertThat(result.isCancelled()).isFalse();
  }

  @Test
  public void scheduleWorkerTask_completesWhenScheduledTaskFails() {
    final CompletableFuture<Object> future = new CompletableFuture<>();
    final CompletableFuture<Object> result = ethScheduler.scheduleSyncWorkerTask(() -> future);

    assertThat(result.isDone()).isFalse();
    future.completeExceptionally(new RuntimeException("whoops"));
    assertThat(result.isDone()).isTrue();
    assertThat(result.isCompletedExceptionally()).isTrue();
    assertThat(result.isCancelled()).isFalse();
  }

  @Test
  public void scheduleWorkerTask_completesWhenScheduledTaskIsCancelled() {
    final CompletableFuture<Object> future = new CompletableFuture<>();
    final CompletableFuture<Object> result = ethScheduler.scheduleSyncWorkerTask(() -> future);

    assertThat(result.isDone()).isFalse();
    future.cancel(false);
    assertThat(result.isDone()).isTrue();
    assertThat(result.isCompletedExceptionally()).isTrue();
    assertThat(result.isCancelled()).isTrue();
  }

  @Test
  public void scheduleWorkerTask_cancelsScheduledFutureWhenResultIsCancelled() {
    final CompletableFuture<Object> result =
        ethScheduler.scheduleSyncWorkerTask(() -> new CompletableFuture<>());

    assertThat(syncWorkerExecutor.getFutures().size()).isEqualTo(1);
    final Future<?> future = syncWorkerExecutor.getFutures().get(0);

    verify(future, times(0)).cancel(anyBoolean());
    result.cancel(true);
    verify(future, times(1)).cancel(eq(false));
  }

  @Test
  public void scheduleFutureTask_completesWhenScheduledTaskCompletes() {
    final CompletableFuture<Object> future = new CompletableFuture<>();
    final CompletableFuture<Object> result =
        ethScheduler.scheduleFutureTask(() -> future, Duration.ofMillis(100));

    assertThat(result.isDone()).isFalse();
    future.complete("bla");
    assertThat(result.isDone()).isTrue();
    assertThat(result.isCompletedExceptionally()).isFalse();
    assertThat(result.isCancelled()).isFalse();
  }

  @Test
  public void scheduleFutureTask_completesWhenScheduledTaskFails() {
    final CompletableFuture<Object> future = new CompletableFuture<>();
    final CompletableFuture<Object> result =
        ethScheduler.scheduleFutureTask(() -> future, Duration.ofMillis(100));

    assertThat(result.isDone()).isFalse();
    future.completeExceptionally(new RuntimeException("whoops"));
    assertThat(result.isDone()).isTrue();
    assertThat(result.isCompletedExceptionally()).isTrue();
    assertThat(result.isCancelled()).isFalse();
  }

  @Test
  public void scheduleFutureTask_completesWhenScheduledTaskIsCancelled() {
    final CompletableFuture<Object> future = new CompletableFuture<>();
    final CompletableFuture<Object> result =
        ethScheduler.scheduleFutureTask(() -> future, Duration.ofMillis(100));

    assertThat(result.isDone()).isFalse();
    future.cancel(false);
    assertThat(result.isDone()).isTrue();
    assertThat(result.isCompletedExceptionally()).isTrue();
    assertThat(result.isCancelled()).isTrue();
  }

  @Test
  public void scheduleFutureTask_cancelsScheduledFutureWhenResultIsCancelled() {
    final CompletableFuture<Object> result =
        ethScheduler.scheduleFutureTask(() -> new CompletableFuture<>(), Duration.ofMillis(100));

    assertThat(scheduledExecutor.getFutures().size()).isEqualTo(1);
    final Future<?> future = scheduledExecutor.getFutures().get(0);

    verify(future, times(0)).cancel(anyBoolean());
    result.cancel(true);
    verify(future, times(1)).cancel(eq(false));
  }

  @Test
  public void timeout_resultCompletesWhenScheduledTaskCompletes() {
    final MockEthTask task = new MockEthTask();
    final CompletableFuture<Object> result = ethScheduler.timeout(task, Duration.ofSeconds(2));

    assertThat(task.hasBeenStarted()).isTrue();
    assertThat(task.isDone()).isFalse();
    assertThat(result.isDone()).isFalse();

    task.complete();
    assertThat(result.isDone()).isTrue();
    assertThat(result.isCompletedExceptionally()).isFalse();
    assertThat(result.isCancelled()).isFalse();
  }

  @Test
  public void timeout_resultCompletesWhenScheduledTaskFails() {
    final MockEthTask task = new MockEthTask();
    final CompletableFuture<Object> result = ethScheduler.timeout(task, Duration.ofSeconds(2));

    assertThat(task.hasBeenStarted()).isTrue();
    assertThat(task.isDone()).isFalse();
    assertThat(result.isDone()).isFalse();

    task.fail();
    assertThat(result.isDone()).isTrue();
    assertThat(result.isCompletedExceptionally()).isTrue();
    assertThat(result.isCancelled()).isFalse();
  }

  @Test
  public void timeout_resultCompletesOnTimeout() {
    shouldTimeout.set(true);
    final MockEthTask task = new MockEthTask();
    final CompletableFuture<Object> result = ethScheduler.timeout(task, Duration.ofSeconds(2));

    // Timeout fires immediately, so everything should be done
    assertThat(task.hasBeenStarted()).isTrue();
    assertThat(task.isDone()).isTrue();
    assertThat(result.isDone()).isTrue();
    assertThat(result.isCompletedExceptionally()).isTrue();
    assertThatThrownBy(result::get).hasCauseInstanceOf(TimeoutException.class);
    assertThat(result.isCancelled()).isFalse();
  }

  @Test
  public void timeout_cancelsTaskWhenResultIsCancelled() {
    final MockEthTask task = new MockEthTask();
    final CompletableFuture<Object> result = ethScheduler.timeout(task, Duration.ofSeconds(2));

    assertThat(task.hasBeenStarted()).isTrue();
    assertThat(task.isDone()).isFalse();
    assertThat(result.isDone()).isFalse();

    result.cancel(false);
    assertThat(task.isDone()).isTrue();
    assertThat(task.isFailed()).isTrue();
    assertThat(task.isCancelled()).isTrue();
  }

  @Test
  public void itemsSubmittedToOrderedProcessorAreProcessedInOrder() throws InterruptedException {
    final int numOfItems = 100;
    final Random random = new Random();
    final EthScheduler realEthScheduler = new EthScheduler(1, 1, 1, new NoOpMetricsSystem());

    final List<String> processedStrings = new CopyOnWriteArrayList<>();

    final Consumer<String> stringProcessor =
        s -> {
          processedStrings.add(s);
          try {
            Thread.sleep(random.nextInt(20));
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        };

    final var orderProcessor = realEthScheduler.createOrderedProcessor(stringProcessor);
    IntStream.range(0, numOfItems)
        .mapToObj(String::valueOf)
        .forEach(
            s -> {
              orderProcessor.submit(s);
              try {
                Thread.sleep(random.nextInt(20));
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            });

    final List<String> expectedStrings = new ArrayList<>(numOfItems);
    IntStream.range(0, numOfItems).mapToObj(String::valueOf).forEach(expectedStrings::add);

    Awaitility.await().until(() -> processedStrings.size() == numOfItems);

    assertThat(processedStrings).containsExactlyElementsOf(expectedStrings);
  }

  @Test
  public void scheduleBlockCreationTask_restoresThreadNameOnSuccessAndFailure() throws Exception {
    final EthScheduler realEthScheduler = new EthScheduler(1, 1, 1, new NoOpMetricsSystem());
    try {
      // Test success case
      final java.util.concurrent.atomic.AtomicReference<Thread> threadRef =
          new java.util.concurrent.atomic.AtomicReference<>();
      final java.util.concurrent.atomic.AtomicReference<String> threadNameDuringTask =
          new java.util.concurrent.atomic.AtomicReference<>();

      realEthScheduler
          .scheduleBlockCreationTask(
              123L,
              () -> {
                threadRef.set(Thread.currentThread());
                threadNameDuringTask.set(Thread.currentThread().getName());
              })
          .get();

      assertThat(threadNameDuringTask.get()).endsWith("-123");
      assertThat(threadRef.get().getName()).doesNotEndWith("-123");

      // Test exception case
      final java.util.concurrent.atomic.AtomicReference<Thread> exceptionThreadRef =
          new java.util.concurrent.atomic.AtomicReference<>();
      final java.util.concurrent.atomic.AtomicReference<String> exceptionThreadNameDuringTask =
          new java.util.concurrent.atomic.AtomicReference<>();

      assertThatThrownBy(
              () ->
                  realEthScheduler
                      .scheduleBlockCreationTask(
                          789L,
                          () -> {
                            exceptionThreadRef.set(Thread.currentThread());
                            exceptionThreadNameDuringTask.set(Thread.currentThread().getName());
                            throw new RuntimeException("test exception");
                          })
                      .get())
          .hasCauseInstanceOf(RuntimeException.class)
          .hasMessageContaining("test exception");

      assertThat(exceptionThreadNameDuringTask.get()).endsWith("-789");
      assertThat(exceptionThreadRef.get().getName()).doesNotEndWith("-789");
    } finally {
      realEthScheduler.stop();
    }
  }
}
