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

import static java.util.Arrays.asList;
import static java.util.Collections.synchronizedList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.waitAtMost;
import static org.hyperledger.besu.metrics.noop.NoOpMetricsSystem.NO_OP_LABELLED_2_COUNTER;

import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Stream;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class PipelineBuilderTest {

  private static final ThreadFactory THREAD_FACTORY =
      new ThreadFactoryBuilder()
          .setNameFormat(PipelineBuilderTest.class.getSimpleName() + "-%d")
          .setDaemon(true)
          .build();
  private final Iterator<Integer> tasks =
      asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15).iterator();

  private final ExecutorService executorService = Executors.newCachedThreadPool(THREAD_FACTORY);

  @AfterEach
  public void afterClass() throws Exception {
    executorService.shutdownNow();
    if (!executorService.awaitTermination(10, SECONDS)) {
      fail("Executor service did not shut down cleanly");
    }
  }

  @Test
  public void shouldPipeTasksFromSupplierToCompleter() throws Exception {
    final List<Integer> output = new ArrayList<>();
    final Pipeline<Integer> pipeline =
        PipelineBuilder.createPipelineFrom(
                "input", tasks, 10, NO_OP_LABELLED_2_COUNTER, false, "test")
            .andFinishWith("end", output::add);
    final CompletableFuture<?> result = pipeline.start(executorService);
    result.get(10, SECONDS);
    assertThat(output).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
  }

  @Test
  public void shouldPassInputThroughIntermediateStage() throws Exception {
    final List<String> output = new ArrayList<>();
    final Pipeline<Integer> pipeline =
        PipelineBuilder.createPipelineFrom(
                "input", tasks, 10, NO_OP_LABELLED_2_COUNTER, false, "test")
            .thenProcess("toString", Object::toString)
            .andFinishWith("end", output::add);

    final CompletableFuture<?> result = pipeline.start(executorService);
    result.get(10, SECONDS);
    assertThat(output)
        .containsExactly(
            "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15");
  }

  @Test
  public void shouldCombineIntoBatches() throws Exception {
    final BlockingQueue<List<Integer>> output = new ArrayBlockingQueue<>(10);
    final Pipeline<Integer> pipeline =
        PipelineBuilder.<Integer>createPipeline(
                "source", 20, NO_OP_LABELLED_2_COUNTER, false, "test")
            .inBatches(6)
            .andFinishWith("end", output::offer);

    final Pipe<Integer> input = pipeline.getInputPipe();
    tasks.forEachRemaining(input::put);

    final CompletableFuture<?> result = pipeline.start(executorService);

    assertThat(output.poll(10, SECONDS)).containsExactly(1, 2, 3, 4, 5, 6);
    assertThat(output.poll(10, SECONDS)).containsExactly(7, 8, 9, 10, 11, 12);
    assertThat(output.poll(10, SECONDS)).containsExactly(13, 14, 15);

    assertThat(output).isEmpty();
    assertThat(result).isNotDone();

    // Should not wait to fill the batch.
    input.put(16);
    assertThat(output.poll(10, SECONDS)).containsExactly(16);
    input.put(17);
    assertThat(output.poll(10, SECONDS)).containsExactly(17);

    input.close();
    result.get(10, SECONDS);
    assertThat(output).isEmpty();
  }

  @Test
  public void shouldAggregateAllBatchesIntoSingleBatch() throws Exception {
    final BlockingQueue<List<Integer>> output = new ArrayBlockingQueue<>(1);
    final Pipeline<Integer> pipeline =
        PipelineBuilder.<Integer>createPipeline(
                "source", 20, NO_OP_LABELLED_2_COUNTER, false, "test")
            .inBatches(4)
            .inSingleBatch()
            .andFinishWith("end", output::offer);

    final Pipe<Integer> input = pipeline.getInputPipe();
    tasks.forEachRemaining(input::put);
    input.close();

    pipeline.start(executorService).get(10, SECONDS);

    assertThat(output.poll(10, SECONDS))
        .containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
    assertThat(output).isEmpty();
  }

  @Test
  public void shouldProcessAsync() throws Exception {
    final List<String> output = new ArrayList<>();
    final Pipeline<Integer> pipeline =
        PipelineBuilder.createPipelineFrom(
                "input", tasks, 10, NO_OP_LABELLED_2_COUNTER, false, "test")
            .thenProcessAsync("toString", value -> completedFuture(Integer.toString(value)), 3)
            .andFinishWith("end", output::add);
    final CompletableFuture<?> result = pipeline.start(executorService);
    result.get(10, SECONDS);
    assertThat(output)
        .containsExactlyInAnyOrder(
            "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15");
  }

  @Test
  public void shouldProcessAsyncOrdered() throws Exception {
    final Map<Integer, CompletableFuture<String>> futures = new ConcurrentHashMap<>();
    final List<String> output = new CopyOnWriteArrayList<>();
    final Pipeline<Integer> pipeline =
        PipelineBuilder.createPipelineFrom(
                "input", tasks, 15, NO_OP_LABELLED_2_COUNTER, false, "test")
            .thenProcessAsyncOrdered(
                "toString",
                value -> {
                  final CompletableFuture<String> future = new CompletableFuture<>();
                  futures.put(value, future);
                  return future;
                },
                8)
            .andFinishWith("end", output::add);
    final CompletableFuture<?> result = pipeline.start(executorService);

    waitForSize(futures.values(), 8);
    // Complete current items out of order, except for 3
    for (final int value : asList(2, 7, 1, 5, 8, 4, 6)) {
      futures.get(value).complete(Integer.toString(value));
    }

    // 1 and 2 should be output and two new async processes started
    waitForSize(output, 2);
    assertThat(output).containsExactly("1", "2");
    waitForSize(futures.values(), 10);

    // Complete task 3 and all the remaining items should now be started
    futures.get(3).complete("3");
    waitForSize(futures.values(), 15);
    // And the first 8 items should have been output
    waitForSize(output, 8);
    assertThat(output).containsExactly("1", "2", "3", "4", "5", "6", "7", "8");

    // Complete the remaining items.
    for (final int value : asList(14, 11, 10, 15, 12, 13, 9)) {
      futures.get(value).complete(Integer.toString(value));
    }

    // And the final result should have everything in order
    result.get(10, SECONDS);
    assertThat(output)
        .containsExactlyInAnyOrder(
            "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15");
  }

  @Test
  public void shouldLimitInFlightProcessesWhenProcessingAsync() throws Exception {
    final List<String> output = new ArrayList<>();
    final List<CompletableFuture<String>> futures = new CopyOnWriteArrayList<>();
    final Pipeline<Integer> pipeline =
        PipelineBuilder.createPipelineFrom(
                "input",
                asList(1, 2, 3, 4, 5, 6, 7).iterator(),
                10,
                NO_OP_LABELLED_2_COUNTER,
                false,
                "test")
            .thenProcessAsync(
                "createFuture",
                value -> {
                  final CompletableFuture<String> future = new CompletableFuture<>();
                  futures.add(future);
                  return future;
                },
                3)
            .andFinishWith("end", output::add);
    final CompletableFuture<?> result = pipeline.start(executorService);

    waitForSize(futures, 3);

    assertThat(result).isNotDone();

    // Completing one task should cause another to be started.
    futures.get(1).complete("2");
    waitForSize(futures, 4);

    futures.get(0).complete("1");
    futures.get(2).complete("3");
    futures.get(3).complete("4");

    waitForSize(futures, 7);
    futures.get(4).complete("5");
    futures.get(5).complete("6");
    futures.get(6).complete("7");

    result.get(10, SECONDS);
    assertThat(output).containsExactly("2", "1", "3", "4", "5", "6", "7");
  }

  @Test
  public void shouldFlatMapItems() throws Exception {
    final List<Integer> output = new ArrayList<>();
    final Pipeline<Integer> pipeline =
        PipelineBuilder.createPipelineFrom(
                "input", tasks, 10, NO_OP_LABELLED_2_COUNTER, false, "test")
            .thenFlatMap("flatMap", input -> Stream.of(input, input * 2), 20)
            .andFinishWith("end", output::add);

    pipeline.start(executorService).get(10, SECONDS);

    assertThat(output)
        .containsExactly(
            1, 2, 2, 4, 3, 6, 4, 8, 5, 10, 6, 12, 7, 14, 8, 16, 9, 18, 10, 20, 11, 22, 12, 24, 13,
            26, 14, 28, 15, 30);
  }

  @Test
  public void shouldProcessInParallel() throws Exception {
    final List<String> output = synchronizedList(new ArrayList<>());
    final CountDownLatch latch = new CountDownLatch(1);
    final Pipeline<Integer> pipeline =
        PipelineBuilder.createPipelineFrom(
                "input", tasks, 10, NO_OP_LABELLED_2_COUNTER, false, "test")
            .thenProcessInParallel(
                "stageName",
                value -> {
                  if (value == 2) {
                    try {
                      latch.await();
                    } catch (InterruptedException e) {
                      throw new RuntimeException(e);
                    }
                  }
                  return value.toString();
                },
                2)
            .andFinishWith("end", output::add);
    final CompletableFuture<?> result = pipeline.start(executorService);

    // One thread will block but the other should process the remaining entries.
    waitForSize(output, 14);
    assertThat(result).isNotDone();

    latch.countDown();

    result.get(10, SECONDS);

    assertThat(output)
        .containsExactly(
            "1", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "2");
  }

  @Test
  public void shouldFlatMapInParallel() throws Exception {
    final List<String> output = synchronizedList(new ArrayList<>());
    final CountDownLatch latch = new CountDownLatch(1);
    final Pipeline<Integer> pipeline =
        PipelineBuilder.createPipelineFrom(
                "input", tasks, 10, NO_OP_LABELLED_2_COUNTER, false, "test")
            .thenFlatMapInParallel(
                "stageName",
                value -> {
                  if (value == 2) {
                    try {
                      latch.await();
                    } catch (InterruptedException e) {
                      throw new RuntimeException(e);
                    }
                  }
                  return Stream.of(value.toString(), "x" + value);
                },
                2,
                10)
            .andFinishWith("end", output::add);
    final CompletableFuture<?> result = pipeline.start(executorService);

    // One thread will block but the other should process the remaining entries.
    waitForSize(output, 28);
    assertThat(result).isNotDone();

    latch.countDown();

    result.get(10, SECONDS);

    assertThat(output)
        .containsExactly(
            "1", "x1", "3", "x3", "4", "x4", "5", "x5", "6", "x6", "7", "x7", "8", "x8", "9", "x9",
            "10", "x10", "11", "x11", "12", "x12", "13", "x13", "14", "x14", "15", "x15", "2",
            "x2");
  }

  @Test
  public void shouldAbortPipeline() throws Exception {
    final int allowProcessingUpTo = 5;
    final AtomicBoolean processorInterrupted = new AtomicBoolean(false);
    final List<Integer> output = synchronizedList(new ArrayList<>());
    final CountDownLatch startedProcessingValueSix = new CountDownLatch(1);
    final Pipeline<Integer> pipeline =
        PipelineBuilder.createPipelineFrom(
                "input", tasks, 10, NO_OP_LABELLED_2_COUNTER, false, "test")
            .thenProcess(
                "stageName",
                value -> {
                  if (value > allowProcessingUpTo) {
                    try {
                      startedProcessingValueSix.countDown();
                      Thread.sleep(TimeUnit.MINUTES.toNanos(2));
                    } catch (final InterruptedException e) {
                      processorInterrupted.set(true);
                    }
                  }
                  return value;
                })
            .andFinishWith("end", output::add);

    final CompletableFuture<?> result = pipeline.start(executorService);

    startedProcessingValueSix.await(10, SECONDS);
    waitForSize(output, allowProcessingUpTo);

    pipeline.abort();

    assertThatThrownBy(() -> result.get(10, SECONDS)).isInstanceOf(CancellationException.class);
    assertThat(output).containsExactly(1, 2, 3, 4, 5);

    waitAtMost(10, SECONDS).untilAsserted(() -> assertThat(processorInterrupted).isTrue());
  }

  @Test
  public void shouldAbortPipelineWhenFutureIsCancelled() throws Exception {
    final int allowProcessingUpTo = 5;
    final AtomicBoolean processorInterrupted = new AtomicBoolean(false);
    final List<Integer> output = synchronizedList(new ArrayList<>());
    final CountDownLatch startedProcessingValueSix = new CountDownLatch(1);
    final Pipeline<Integer> pipeline =
        PipelineBuilder.createPipelineFrom(
                "input", tasks, 10, NO_OP_LABELLED_2_COUNTER, false, "test")
            .thenProcess(
                "stageName",
                value -> {
                  if (value > allowProcessingUpTo) {
                    try {
                      startedProcessingValueSix.countDown();
                      Thread.sleep(TimeUnit.MINUTES.toNanos(2));
                    } catch (final InterruptedException e) {
                      processorInterrupted.set(true);
                    }
                  }
                  return value;
                })
            .andFinishWith("end", output::add);

    final CompletableFuture<?> result = pipeline.start(executorService);

    startedProcessingValueSix.await(10, SECONDS);
    waitForSize(output, allowProcessingUpTo);

    result.cancel(false);

    assertThatThrownBy(() -> result.get(10, SECONDS)).isInstanceOf(CancellationException.class);
    assertThat(output).containsExactly(1, 2, 3, 4, 5);

    waitAtMost(10, SECONDS).untilAsserted(() -> assertThat(processorInterrupted).isTrue());
  }

  @Test
  public void shouldAbortPipelineWhenProcessorThrowsException() {
    final RuntimeException expectedError = new RuntimeException("Oops");
    final Pipeline<Integer> pipeline =
        PipelineBuilder.createPipelineFrom(
                "input", tasks, 10, NO_OP_LABELLED_2_COUNTER, false, "test")
            .thenProcess(
                "stageName",
                (Function<Integer, Integer>)
                    value -> {
                      throw expectedError;
                    })
            .andFinishWith("end", new ArrayList<Integer>()::add);

    final CompletableFuture<?> result = pipeline.start(executorService);

    assertThatThrownBy(() -> result.get(10, SECONDS))
        .isInstanceOf(ExecutionException.class)
        .hasRootCauseExactlyInstanceOf(RuntimeException.class)
        .extracting(Throwable::getCause)
        .isSameAs(expectedError);
  }

  @Test
  public void shouldTrackTaskCountMetrics() throws Exception {
    final Map<String, SimpleCounter> counters = new ConcurrentHashMap<>();
    final LabelledMetric<Counter> labelledCounter =
        labels ->
            counters.computeIfAbsent(labels[0] + "-" + labels[1], label -> new SimpleCounter());
    final Pipeline<Integer> pipeline =
        PipelineBuilder.createPipelineFrom("input", tasks, 10, labelledCounter, false, "test")
            .thenProcess("map", Function.identity())
            .thenProcessInParallel("parallel", Function.identity(), 3)
            .thenProcessAsync("async", CompletableFuture::completedFuture, 3)
            .thenProcessAsyncOrdered("asyncOrdered", CompletableFuture::completedFuture, 3)
            .inBatches(4)
            .thenFlatMap("flatMap", List::stream, 10)
            .andFinishWith("finish", new ArrayList<>()::add);

    pipeline.start(executorService).get(10, SECONDS);

    final List<String> stepNames =
        asList("input", "map", "parallel", "async", "asyncOrdered", "flatMap");
    final List<String> expectedMetricNames =
        Stream.concat(
                Stream.of("asyncOrdered_outputPipe-batches"),
                stepNames.stream()
                    .map(stageName -> stageName + "_outputPipe")
                    .flatMap(
                        metricName ->
                            Stream.of(
                                metricName + "-added",
                                metricName + "-removed",
                                metricName + "-aborted")))
            .collect(toList());
    assertThat(counters).containsOnlyKeys(expectedMetricNames);

    expectedMetricNames.stream()
        .filter(name -> !name.endsWith("-batches") && !name.endsWith("-aborted"))
        .forEach(metric -> assertThat(counters.get(metric).count).hasValue(15));

    assertThat(counters.get("asyncOrdered_outputPipe-batches").count).hasValueBetween(4, 15);
  }

  private void waitForSize(final Collection<?> collection, final int targetSize) {
    waitAtMost(10, SECONDS).untilAsserted(() -> assertThat(collection).hasSize(targetSize));
  }

  private static class SimpleCounter implements Counter {
    private final AtomicLong count = new AtomicLong(0);

    @Override
    public void inc() {
      count.incrementAndGet();
    }

    @Override
    public void inc(final long amount) {
      count.addAndGet(amount);
    }
  }
}
