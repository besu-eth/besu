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
package org.hyperledger.besu.consensus.common.bft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.consensus.common.bft.events.RoundExpiry;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class BftProcessorTest {
  private EventMultiplexer mockeEventMultiplexer;

  @BeforeEach
  public void initialise() {
    mockeEventMultiplexer = mock(EventMultiplexer.class);
  }

  @Test
  public void handlesStopGracefully() throws InterruptedException {
    final BftEventQueue mockQueue = mock(BftEventQueue.class);
    Mockito.when(mockQueue.poll(anyLong(), any())).thenReturn(null);
    final BftProcessor processor = new BftProcessor(mockQueue, mockeEventMultiplexer);

    // Start the BftProcessor
    final ExecutorService processorExecutor = Executors.newSingleThreadExecutor();
    final Future<?> processorFuture = processorExecutor.submit(processor);

    // Make sure we've hit the queue at least once
    verify(mockQueue, timeout(3000).atLeastOnce()).poll(anyLong(), any());

    // Instruct the processor to stop
    processor.stop();

    // Executor shutdown should wait for the processor to gracefully exit
    processorExecutor.shutdown();
    // If it hasn't within 200 ms then something will be wrong
    final boolean executorCompleted =
        processorExecutor.awaitTermination(2000, TimeUnit.MILLISECONDS);
    assertThat(executorCompleted).isTrue();

    // The processor task has exited
    assertThat(processorFuture.isDone()).isTrue();
  }

  @Test
  public void cleanupExecutorsAfterShutdownNow() throws InterruptedException {
    final BftProcessor processor = new BftProcessor(new BftEventQueue(1000), mockeEventMultiplexer);

    // Start the BftProcessor
    final ExecutorService processorExecutor = Executors.newSingleThreadExecutor();
    final Future<?> processorFuture = processorExecutor.submit(processor);

    // Instruct the processor to stop
    processor.stop();

    // Executor shutdown should interrupt the processor
    processorExecutor.shutdownNow();
    // If it hasn't within 200 ms then something will be wrong
    final boolean executorCompleted =
        processorExecutor.awaitTermination(2000, TimeUnit.MILLISECONDS);
    assertThat(executorCompleted).isTrue();

    // The processor task has exited
    assertThat(processorFuture.isDone()).isTrue();
  }

  @Test
  public void handlesQueueInterruptGracefully() throws InterruptedException {
    // Setup a queue that will always interrupt
    final BftEventQueue mockQueue = mock(BftEventQueue.class);
    Mockito.when(mockQueue.poll(anyLong(), any())).thenThrow(new InterruptedException());

    final BftProcessor processor = new BftProcessor(mockQueue, mockeEventMultiplexer);

    // Start the BftProcessor
    final ExecutorService processorExecutor = Executors.newSingleThreadExecutor();
    final Future<?> processorFuture = processorExecutor.submit(processor);

    // Make sure we've hit the queue at least once
    verify(mockQueue, timeout(3000).atLeastOnce()).poll(anyLong(), any());

    // Executor shutdown should wait for the processor to gracefully exit
    processorExecutor.shutdown();

    // The processor task hasn't exited off the back of the interrupts
    assertThat(processorFuture.isDone()).isFalse();

    processor.stop();

    // If it hasn't within 200 ms then something will be wrong and we're not waking up
    final boolean executorCompleted =
        processorExecutor.awaitTermination(200, TimeUnit.MILLISECONDS);
    assertThat(executorCompleted).isTrue();

    // The processor task has woken up and exited
    assertThat(processorFuture.isDone()).isTrue();
  }

  @Test
  public void awaitStopWaitsForProcessorAfterRestart() throws Exception {
    final BftEventQueue mockQueue = mock(BftEventQueue.class);
    final AtomicReference<CountDownLatch> pollEntered =
        new AtomicReference<>(new CountDownLatch(1));
    final AtomicReference<CountDownLatch> releasePoll =
        new AtomicReference<>(new CountDownLatch(1));
    Mockito.when(mockQueue.poll(anyLong(), any()))
        .thenAnswer(
            invocation -> {
              pollEntered.get().countDown();
              releasePoll.get().await();
              return null;
            });

    final BftProcessor processor = new BftProcessor(mockQueue, mockeEventMultiplexer);
    final ExecutorService processorExecutor = Executors.newSingleThreadExecutor();
    final ExecutorService awaitExecutor = Executors.newSingleThreadExecutor();

    try {
      processor.start();
      final Future<?> firstRun = processorExecutor.submit(processor);
      assertThat(pollEntered.get().await(1, TimeUnit.SECONDS)).isTrue();
      processor.stop();
      releasePoll.get().countDown();
      processor.awaitStop();
      firstRun.get(1, TimeUnit.SECONDS);

      pollEntered.set(new CountDownLatch(1));
      releasePoll.set(new CountDownLatch(1));
      processor.start();
      final Future<?> secondRun = processorExecutor.submit(processor);
      assertThat(pollEntered.get().await(1, TimeUnit.SECONDS)).isTrue();
      processor.stop();

      final CountDownLatch awaitStarted = new CountDownLatch(1);
      final Future<Void> secondAwait =
          awaitExecutor.submit(
              () -> {
                awaitStarted.countDown();
                processor.awaitStop();
                return null;
              });
      assertThat(awaitStarted.await(1, TimeUnit.SECONDS)).isTrue();

      assertThatThrownBy(() -> secondAwait.get(100, TimeUnit.MILLISECONDS))
          .isInstanceOf(TimeoutException.class);

      releasePoll.get().countDown();
      secondAwait.get(1, TimeUnit.SECONDS);
      secondRun.get(1, TimeUnit.SECONDS);
    } finally {
      releasePoll.get().countDown();
      processorExecutor.shutdownNow();
      awaitExecutor.shutdownNow();
    }
  }

  @Test
  public void drainEventsIntoStateMachine() throws InterruptedException {
    final BftEventQueue queue = new BftEventQueue(1000);
    queue.start();
    final BftProcessor processor = new BftProcessor(queue, mockeEventMultiplexer);

    // Start the BftProcessor
    final ExecutorService processorExecutor = Executors.newSingleThreadExecutor();
    processorExecutor.execute(processor);

    final RoundExpiry roundExpiryEvent = new RoundExpiry(new ConsensusRoundIdentifier(1, 1));

    queue.add(roundExpiryEvent);
    queue.add(roundExpiryEvent);

    Awaitility.await().atMost(3000, TimeUnit.MILLISECONDS).until(queue::isEmpty);

    processor.stop();
    processorExecutor.shutdown();

    verify(mockeEventMultiplexer, times(2)).handleBftEvent(eq(roundExpiryEvent));
  }
}
