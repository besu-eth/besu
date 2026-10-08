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
package org.hyperledger.besu.services.pipeline;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

public class AsyncOperationProcessorTest {

  @SuppressWarnings("unchecked")
  private final ReadPipe<CompletableFuture<String>> readPipe = mock(ReadPipe.class);

  @SuppressWarnings("unchecked")
  private final WritePipe<String> writePipe = mock(WritePipe.class);

  @Test
  public void shouldImmediatelyOutputTasksThatAreAlreadyCompleteEvenIfOutputPipeIsFull() {
    final AsyncOperationProcessor<CompletableFuture<String>, String> processor =
        createProcessor(false);
    when(writePipe.hasRemainingCapacity()).thenReturn(false);
    when(readPipe.get()).thenReturn(completedFuture("a"));

    processor.processNextInput(readPipe, writePipe);
    verify(writePipe).put("a");
  }

  @Test
  public void shouldNotExceedConcurrentJobLimit() {
    final AsyncOperationProcessor<CompletableFuture<String>, String> processor =
        createProcessor(false);
    final CompletableFuture<String> task1 = new CompletableFuture<>();
    final CompletableFuture<String> task2 = new CompletableFuture<>();
    final CompletableFuture<String> task3 = new CompletableFuture<>();
    final CompletableFuture<String> task4 = new CompletableFuture<>();
    when(readPipe.get()).thenReturn(task1).thenReturn(task2).thenReturn(task3).thenReturn(task4);

    // 3 tasks started
    processor.processNextInput(readPipe, writePipe);
    processor.processNextInput(readPipe, writePipe);
    processor.processNextInput(readPipe, writePipe);
    verify(readPipe, times(3)).get();

    // Reached limit of concurrent tasks so this round does nothing.
    processor.processNextInput(readPipe, writePipe);
    verify(readPipe, times(3)).get();

    task1.complete("a");

    // Next round will output the completed task
    processor.processNextInput(readPipe, writePipe);
    verify(writePipe).put("a");

    // And so now we are able to start another one.
    processor.processNextInput(readPipe, writePipe);
    verify(readPipe, times(4)).get();
  }

  @Test
  public void shouldOutputRemainingInProgressTasksWhenFinalizing() {
    final AsyncOperationProcessor<CompletableFuture<String>, String> processor =
        createProcessor(false);
    final CompletableFuture<String> task1 = new CompletableFuture<>();
    final CompletableFuture<String> task2 = new CompletableFuture<>();
    when(readPipe.get()).thenReturn(task1).thenReturn(task2);

    // Start the two tasks
    processor.processNextInput(readPipe, writePipe);
    processor.processNextInput(readPipe, writePipe);
    verify(readPipe, times(2)).get();
    verifyNoInteractions(writePipe);

    task1.complete("a");
    task2.complete("b");

    // Processing
    processor.attemptFinalization(writePipe);
    verify(writePipe).put("a");
    verify(writePipe).put("b");
  }

  @Test
  public void shouldCancelInProgressTasksWhenAborted() {
    final AsyncOperationProcessor<CompletableFuture<String>, String> processor =
        createProcessor(false);
    final CompletableFuture<String> task1 = new CompletableFuture<>();
    final CompletableFuture<String> task2 = new CompletableFuture<>();
    when(readPipe.get()).thenReturn(task1).thenReturn(task2);

    processor.processNextInput(readPipe, writePipe);
    processor.processNextInput(readPipe, writePipe);

    processor.abort();

    assertThat(task1).isCancelled();
    assertThat(task2).isCancelled();
  }

  @Test
  public void shouldInterruptThreadWhenFutureCompletes() {
    // Ensures that if we're waiting for the next input we wake up and output completed tasks
    final AsyncOperationProcessor<CompletableFuture<String>, String> processor =
        createProcessor(false);

    final CompletableFuture<String> task1 = new CompletableFuture<>();
    when(readPipe.get()).thenReturn(task1);

    // Start the two tasks
    processor.processNextInput(readPipe, writePipe);

    task1.complete("a");

    assertThat(Thread.currentThread().isInterrupted()).isTrue();
  }

  @Test
  public void shouldPreserveOrderWhenRequested() {
    final AsyncOperationProcessor<CompletableFuture<String>, String> processor =
        createProcessor(true);
    final CompletableFuture<String> task1 = new CompletableFuture<>();
    final CompletableFuture<String> task2 = new CompletableFuture<>();
    final CompletableFuture<String> task3 = new CompletableFuture<>();
    final CompletableFuture<String> task4 = new CompletableFuture<>();
    when(readPipe.get()).thenReturn(task1).thenReturn(task2).thenReturn(task3).thenReturn(task4);

    // 3 tasks started
    processor.processNextInput(readPipe, writePipe);
    processor.processNextInput(readPipe, writePipe);
    processor.processNextInput(readPipe, writePipe);
    verify(readPipe, times(3)).get();

    // Reached limit of concurrent tasks so this round does nothing.
    processor.processNextInput(readPipe, writePipe);
    verify(readPipe, times(3)).get();

    // Second task completes but shouldn't be output because task1 is not complete yet
    task2.complete("b");

    processor.processNextInput(readPipe, writePipe);
    verify(readPipe, times(3)).get();
    verifyNoInteractions(writePipe);

    task1.complete("a");

    // Next round will output the two completed tasks in order
    processor.processNextInput(readPipe, writePipe);
    verify(writePipe).put("a");
    verify(writePipe).put("b");

    // And so now we are able to start another one.
    processor.processNextInput(readPipe, writePipe);
    verify(readPipe, times(4)).get();

    // And should finalize in order
    task4.complete("d");
    task3.complete("c");
    assertThat(processor.attemptFinalization(writePipe)).isTrue();
    verify(writePipe).put("c");
    verify(writePipe).put("d");
  }

  @Test
  public void shouldThrowExceptionWhenFutureCompletesExceptionally() {
    final AsyncOperationProcessor<CompletableFuture<String>, String> processor =
        createProcessor(false);
    final CompletableFuture<String> task1 = new CompletableFuture<>();
    when(readPipe.get()).thenReturn(task1);

    processor.processNextInput(readPipe, writePipe);

    final Exception exception = new IndexOutOfBoundsException("Oh dear");
    task1.completeExceptionally(exception);

    assertThatThrownBy(() -> processor.processNextInput(readPipe, writePipe))
        .hasRootCause(exception);
  }

  @Test
  public void shouldRejectZeroMaxConcurrency() {
    assertThatThrownBy(() -> new AsyncOperationProcessor<>(Function.identity(), 0, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Maximum concurrency must be greater than 0");
  }

  @Test
  public void shouldRejectNegativeMaxConcurrency() {
    assertThatThrownBy(() -> new AsyncOperationProcessor<>(Function.identity(), -1, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Maximum concurrency must be greater than 0");
  }

  private AsyncOperationProcessor<CompletableFuture<String>, String> createProcessor(
      final boolean preserveOrder) {
    return new AsyncOperationProcessor<>(Function.identity(), 3, preserveOrder);
  }
}
