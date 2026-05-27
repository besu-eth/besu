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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;

import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Supports building a new pipeline. Pipelines are comprised of a source, various processing stages
 * and a consumer, each of which run in their own thread.
 *
 * <p>The pipeline completes when all items from the source have passed through each stage and are
 * received by the consumer. The pipeline will halt immediately if an exception is thrown from any
 * processing stage.
 *
 * @param <I> the type of item input to the very start of this pipeline.
 * @param <T> the output type of the last stage in the pipeline.
 */
public class PipelineBuilder<I, T> {

  private final Pipe<I> inputPipe;
  private final Collection<Stage> stages;
  private final Collection<Pipe<?>> pipes;
  private final String lastStageName;
  private final ReadPipe<T> pipeEnd;
  private final int bufferSize;
  private final LabelledMetric<Counter> outputCounter;
  private final boolean tracingEnabled;
  private final String pipelineName;

  /**
   * Instantiates a new Pipeline builder.
   *
   * @param inputPipe the input pipe
   * @param stages the stages
   * @param pipes the pipes
   * @param lastStageName the last stage name
   * @param pipeEnd the pipe end
   * @param bufferSize the buffer size
   * @param outputCounter the output counter
   * @param tracingEnabled the tracing enabled
   * @param pipelineName the pipeline name
   */
  public PipelineBuilder(
      final Pipe<I> inputPipe,
      final Collection<Stage> stages,
      final Collection<Pipe<?>> pipes,
      final String lastStageName,
      final ReadPipe<T> pipeEnd,
      final int bufferSize,
      final LabelledMetric<Counter> outputCounter,
      final boolean tracingEnabled,
      final String pipelineName) {
    checkArgument(!pipes.isEmpty(), "Must have at least one pipe in a pipeline");
    this.lastStageName = lastStageName;
    this.outputCounter = outputCounter;
    this.inputPipe = inputPipe;
    this.stages = stages;
    this.pipes = pipes;
    this.pipeEnd = pipeEnd;
    this.bufferSize = bufferSize;
    this.tracingEnabled = tracingEnabled;
    this.pipelineName = pipelineName;
  }

  /**
   * Create a new pipeline that processes inputs from <i>source</i>. The pipeline completes when
   * <i>source</i> returns <code>false</code> from {@link Iterator#hasNext()} and the last item has
   * been reached the end of the pipeline.
   *
   * @param <T> the type of items input into the pipeline.
   * @param sourceName the name of this stage. Used as the label for the output count metric.
   * @param source the source to pull items from for processing.
   * @param bufferSize the number of items to be buffered between each stage in the pipeline.
   * @param itemCounter the counter to increment for each output of a stage. Must accept two labels,
   *     the stage name and action (output or drained).
   * @param tracingEnabled whether this pipeline should be traced
   * @param pipelineName the name of the pipeline for tracing purposes
   * @return a {@link PipelineBuilder} ready to extend the pipeline with additional stages.
   */
  public static <T> PipelineBuilder<T, T> createPipelineFrom(
      final String sourceName,
      final Iterator<T> source,
      final int bufferSize,
      final LabelledMetric<Counter> itemCounter,
      final boolean tracingEnabled,
      final String pipelineName) {
    final Pipe<T> pipe = createPipe(bufferSize, sourceName, itemCounter);
    final IteratorSourceStage<T> sourceStage = new IteratorSourceStage<>(sourceName, source, pipe);
    return new PipelineBuilder<>(
        pipe,
        singleton(sourceStage),
        singleton(pipe),
        sourceName,
        pipe,
        bufferSize,
        itemCounter,
        tracingEnabled,
        pipelineName);
  }

  /**
   * Create a new pipeline that processes inputs added to <i>pipe</i>. The pipeline completes when
   * <i>pipe</i> is closed and the last item has been reached the end of the pipeline.
   *
   * @param <T> the type of items input into the pipeline.
   * @param sourceName the name of this stage. Used as the label for the output count metric.
   * @param bufferSize the number of items to be buffered between each stage in the pipeline.
   * @param outputCounter the counter to increment for each output of a stage. Must have a single
   *     label which will be filled with the stage name.
   * @param tracingEnabled whether this pipeline should be traced
   * @param pipelineName the name of the pipeline for tracing purposes
   * @return a {@link PipelineBuilder} ready to extend the pipeline with additional stages.
   */
  public static <T> PipelineBuilder<T, T> createPipeline(
      final String sourceName,
      final int bufferSize,
      final LabelledMetric<Counter> outputCounter,
      final boolean tracingEnabled,
      final String pipelineName) {
    final Pipe<T> pipe = createPipe(bufferSize, sourceName, outputCounter);
    return new PipelineBuilder<>(
        pipe,
        emptyList(),
        singleton(pipe),
        sourceName,
        pipe,
        bufferSize,
        outputCounter,
        tracingEnabled,
        pipelineName);
  }

  /**
   * Adds a 1-to-1 processing stage to the pipeline. A single thread processes each item in the
   * pipeline with <i>processor</i> outputting its return value to the next stage.
   *
   * @param <O> the output type for this processing step.
   * @param stageName the name of this stage. Used as the label for the output count metric.
   * @param processor the processing to apply to each item.
   * @return a {@link PipelineBuilder} ready to extend the pipeline with additional stages.
   */
  public <O> PipelineBuilder<I, O> thenProcess(
      final String stageName, final Function<T, O> processor) {
    final Processor<T, O> singleStepStage = new MapProcessor<>(processor);
    return addStage(singleStepStage, stageName);
  }

  /**
   * Adds a 1-to-1 processing stage to the pipeline. Multiple threads process items in the pipeline
   * concurrently with <i>processor</i> outputting its return value to the next stage.
   *
   * <p>Note: The order of items is not preserved.
   *
   * @param <O> the output type for this processing step.
   * @param stageName the name of this stage. Used as the label for the output count metric.
   * @param processor the processing to apply to each item.
   * @param numberOfThreads the number of threads to use for processing.
   * @return a {@link PipelineBuilder} ready to extend the pipeline with additional stages.
   */
  public <O> PipelineBuilder<I, O> thenProcessInParallel(
      final String stageName, final Function<T, O> processor, final int numberOfThreads) {
    return thenProcessInParallel(
        stageName, () -> new MapProcessor<>(processor), numberOfThreads, bufferSize);
  }

  /**
   * Adds a 1-to-1, asynchronous processing stage to the pipeline. A single thread reads items from
   * the input and calls <i>processor</i> to begin processing. While a single thread is used to
   * begin processing, up to <i>maxConcurrency</i> items may be in progress concurrently. When the
   * returned {@link CompletableFuture} completes successfully the result is passed to the next
   * stage.
   *
   * <p>If the returned {@link CompletableFuture} completes exceptionally the pipeline will abort.
   *
   * <p>Note: The order of items is not preserved.
   *
   * @param <O> the output type for this processing step.
   * @param stageName the name of this stage. Used as the label for the output count metric.
   * @param processor the processing to apply to each item.
   * @param maxConcurrency the maximum number of items being processed concurrently.
   * @return a {@link PipelineBuilder} ready to extend the pipeline with additional stages.
   */
  public <O> PipelineBuilder<I, O> thenProcessAsync(
      final String stageName,
      final Function<T, CompletableFuture<O>> processor,
      final int maxConcurrency) {
    return addStage(new AsyncOperationProcessor<>(processor, maxConcurrency, false), stageName);
  }

  /**
   * Adds a 1-to-1, asynchronous processing stage to the pipeline. A single thread reads items from
   * the input and calls <i>processor</i> to begin processing. While a single thread is used to
   * begin processing, up to <i>maxConcurrency</i> items may be in progress concurrently. As each
   * returned {@link CompletableFuture} completes successfully the result is passed to the next
   * stage in order.
   *
   * <p>If the returned {@link CompletableFuture} completes exceptionally the pipeline will abort.
   *
   * <p>Note: While processing may occur concurrently, order is preserved when results are output.
   *
   * @param <O> the output type for this processing step.
   * @param stageName the name of this stage. Used as the label for the output count metric.
   * @param processor the processing to apply to each item.
   * @param maxConcurrency the maximum number of items being processed concurrently.
   * @return a {@link PipelineBuilder} ready to extend the pipeline with additional stages.
   */
  public <O> PipelineBuilder<I, O> thenProcessAsyncOrdered(
      final String stageName,
      final Function<T, CompletableFuture<O>> processor,
      final int maxConcurrency) {
    return addStage(new AsyncOperationProcessor<>(processor, maxConcurrency, true), stageName);
  }

  /**
   * Batches items into groups of at most <i>maximumBatchSize</i>. Batches are created eagerly to
   * minimize delay so may not be full.
   *
   * <p>Order of items is preserved.
   *
   * <p>The output buffer size is reduced to <code>bufferSize / maximumBatchSize + 1</code>.
   *
   * @param maximumBatchSize the maximum number of items to include in a batch.
   * @return a {@link PipelineBuilder} ready to extend the pipeline with additional stages.
   */
  public PipelineBuilder<I, List<T>> inBatches(final int maximumBatchSize) {
    checkArgument(maximumBatchSize > 0, "Maximum batch size must be greater than 0");
    return new PipelineBuilder<>(
        inputPipe,
        stages,
        pipes,
        lastStageName,
        new BatchingReadPipe<>(
            pipeEnd,
            maximumBatchSize,
            outputCounter.labels(lastStageName + "_outputPipe", "batches")),
        (int) Math.ceil(((double) bufferSize) / maximumBatchSize),
        outputCounter,
        tracingEnabled,
        pipelineName);
  }

  /**
   * Batches items into groups of at most <i>maximumBatchSize</i>. Batches are created eagerly to
   * minimize delay so may not be full.
   *
   * <p>Order of items is preserved.
   *
   * <p>The output buffer size is reduced to <code>bufferSize / maximumBatchSize + 1</code>.
   *
   * @param maximumBatchSize the maximum number of items to include in a batch.
   * @param stopBatchCondition the condition before ending the batch
   * @return a {@link PipelineBuilder} ready to extend the pipeline with additional stages.
   */
  public PipelineBuilder<I, List<T>> inBatches(
      final int maximumBatchSize, final Function<List<T>, Integer> stopBatchCondition) {
    return new PipelineBuilder<>(
        inputPipe,
        stages,
        pipes,
        lastStageName,
        new BatchingReadPipe<>(
            pipeEnd,
            maximumBatchSize,
            outputCounter.labels(lastStageName + "_outputPipe", "batches"),
            stopBatchCondition),
        (int) Math.ceil(((double) bufferSize) / maximumBatchSize),
        outputCounter,
        tracingEnabled,
        pipelineName);
  }

  /**
   * Aggregates all incoming batches into a single batch.
   *
   * <p>This stage is intended to be used after {@link #inBatches(int)} (or any stage producing
   * {@link List} items), when downstream processing must operate on one final aggregate batch.
   *
   * @return a {@link PipelineBuilder} ready to extend the pipeline with additional stages.
   */
  @SuppressWarnings("unchecked")
  public PipelineBuilder<I, T> inSingleBatch() {
    return new PipelineBuilder<>(
        inputPipe,
        stages,
        pipes,
        lastStageName,
        (ReadPipe<T>)
            new AggregatingReadPipe<>(
                (ReadPipe<List<T>>) pipeEnd,
                outputCounter.labels(lastStageName + "_outputPipe", "aggregated_batches")),
        1,
        outputCounter,
        tracingEnabled,
        pipelineName);
  }

  /**
   * Adds a 1-to-many processing stage to the pipeline. For each item in the stream, <i>mapper</i>
   * is called and each item of the {@link Stream} it returns is output as an individual item. The
   * returned Stream may be empty to remove an item.
   *
   * <p>This can be used to reverse the effect of {@link #inBatches(int)} with:
   *
   * <pre>thenFlatMap(List::stream, newBufferSize)</pre>
   *
   * @param <O> the type of items to be output from this stage.
   * @param stageName the name of this stage. Used as the label for the output count metric.
   * @param mapper the function to process each item with.
   * @param newBufferSize the output buffer size to use from this stage onwards.
   * @return a {@link PipelineBuilder} ready to extend the pipeline with additional stages.
   */
  public <O> PipelineBuilder<I, O> thenFlatMap(
      final String stageName, final Function<T, Stream<O>> mapper, final int newBufferSize) {
    return addStage(new FlatMapProcessor<>(mapper), newBufferSize, stageName);
  }

  /**
   * Adds a 1-to-many processing stage to the pipeline. For each item in the stream, <i>mapper</i>
   * is called and each item of the {@link Stream} it returns is output as an individual item. The
   * returned Stream may be empty to remove an item. Multiple threads process items in the pipeline
   * concurrently.
   *
   * <p>This can be used to reverse the effect of {@link #inBatches(int)} with:
   *
   * <pre>thenFlatMap(List::stream, newBufferSize)</pre>
   *
   * @param <O> the type of items to be output from this stage.
   * @param stageName the name of this stage. Used as the label for the output count metric.
   * @param mapper the function to process each item with.
   * @param numberOfThreads the number of threads to use for processing.
   * @param newBufferSize the output buffer size to use from this stage onwards.
   * @return a {@link PipelineBuilder} ready to extend the pipeline with additional stages.
   */
  public <O> PipelineBuilder<I, O> thenFlatMapInParallel(
      final String stageName,
      final Function<T, Stream<O>> mapper,
      final int numberOfThreads,
      final int newBufferSize) {
    return thenProcessInParallel(
        stageName, () -> new FlatMapProcessor<>(mapper), numberOfThreads, newBufferSize);
  }

  /**
   * End the pipeline with a {@link Consumer} that is the last stage of the pipeline.
   *
   * @param stageName the name of this stage. Used as the label for the output count metric.
   * @param completer the {@link Consumer} that accepts the final output of the pipeline.
   * @return the constructed pipeline ready to execute.
   */
  public Pipeline<I> andFinishWith(final String stageName, final Consumer<T> completer) {
    return new Pipeline<>(
        inputPipe,
        pipelineName,
        tracingEnabled,
        stages,
        pipes,
        new CompleterStage<>(stageName, pipeEnd, completer));
  }

  private <O> PipelineBuilder<I, O> thenProcessInParallel(
      final String stageName,
      final Supplier<Processor<T, O>> createProcessor,
      final int numberOfThreads,
      final int newBufferSize) {
    final Pipe<O> newPipeEnd = createPipe(newBufferSize, stageName, outputCounter);
    final WritePipe<O> outputPipe = new SharedWritePipe<>(newPipeEnd, numberOfThreads);
    final ArrayList<Stage> newStages = new ArrayList<>(stages);
    for (int i = 0; i < numberOfThreads; i++) {
      final Stage processStage =
          new ProcessingStage<>(stageName, pipeEnd, outputPipe, createProcessor.get());
      newStages.add(processStage);
    }
    return new PipelineBuilder<>(
        inputPipe,
        newStages,
        concat(pipes, newPipeEnd),
        stageName,
        newPipeEnd,
        newBufferSize,
        outputCounter,
        tracingEnabled,
        pipelineName);
  }

  private <O> PipelineBuilder<I, O> addStage(
      final Processor<T, O> processor, final String stageName) {
    return addStage(processor, bufferSize, stageName);
  }

  private <O> PipelineBuilder<I, O> addStage(
      final Processor<T, O> processor, final int newBufferSize, final String stageName) {
    final Pipe<O> outputPipe = createPipe(newBufferSize, stageName, outputCounter);
    final Stage processStage = new ProcessingStage<>(stageName, pipeEnd, outputPipe, processor);
    final List<Stage> newStages = concat(stages, processStage);
    return new PipelineBuilder<>(
        inputPipe,
        newStages,
        concat(pipes, outputPipe),
        processStage.getName(),
        outputPipe,
        newBufferSize,
        outputCounter,
        tracingEnabled,
        pipelineName);
  }

  private <X> List<X> concat(final Collection<X> existing, final X newItem) {
    final List<X> newList = new ArrayList<>(existing);
    newList.add(newItem);
    return newList;
  }

  private static <O> Pipe<O> createPipe(
      final int newBufferSize,
      final String stageName,
      final LabelledMetric<Counter> outputCounter) {
    final String labelName = stageName + "_outputPipe";
    return new Pipe<>(
        newBufferSize,
        outputCounter.labels(labelName, "added"),
        outputCounter.labels(labelName, "removed"),
        outputCounter.labels(labelName, "aborted"),
        stageName);
  }
}
