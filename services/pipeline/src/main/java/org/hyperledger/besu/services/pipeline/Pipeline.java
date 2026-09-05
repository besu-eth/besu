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

import static java.util.stream.Collectors.toList;

import org.hyperledger.besu.services.pipeline.exception.AsyncOperationException;
import org.hyperledger.besu.util.ExceptionUtils;
import org.hyperledger.besu.util.log.LogUtil;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Pipeline.
 *
 * @param <I> the input pipe type parameter
 */
public class Pipeline<I> {
  private static final Logger LOG = LoggerFactory.getLogger(Pipeline.class);
  private final Pipe<I> inputPipe;
  private final Collection<Stage> stages;
  private final Collection<Pipe<?>> pipes;
  private final CompleterStage<?> completerStage;
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final Tracer tracer =
      GlobalOpenTelemetry.getTracer("org.hyperledger.besu.services.pipeline", "1.0.0");

  /**
   * Flags that the pipeline is being completed so that when we abort we can close the streams
   * without the completion stage then marking the future successful before we finish the abort
   * process and mark it as exceptionally completed. We can't just use synchronized because it winds
   * up being the same thread coming in via a callback so already has the lock.
   */
  private final AtomicBoolean completing = new AtomicBoolean(false);

  private final CompletableFuture<Void> overallFuture = new CompletableFuture<>();
  private final String name;
  private final boolean tracingEnabled;
  private volatile @Nullable List<Future<?>> futures;

  /**
   * Instantiates a new Pipeline.
   *
   * @param inputPipe the input pipe
   * @param name the name
   * @param tracingEnabled the tracing enabled
   * @param stages the stages
   * @param pipes the pipes
   * @param completerStage the completer stage
   */
  Pipeline(
      final Pipe<I> inputPipe,
      final String name,
      final boolean tracingEnabled,
      final Collection<Stage> stages,
      final Collection<Pipe<?>> pipes,
      final CompleterStage<?> completerStage) {
    this.inputPipe = inputPipe;
    this.tracingEnabled = tracingEnabled;
    this.name = name;
    this.stages = stages;
    this.pipes = pipes;
    this.completerStage = completerStage;

    if (LOG.isTraceEnabled()) {
      StringBuilder sb = new StringBuilder();
      sb.append("Building pipeline ");
      sb.append(name);
      sb.append(". Stages: ");
      for (Stage nextStage : stages) {
        sb.append(nextStage.getName());
        sb.append(" -> ");
      }
      sb.append("END");
      LOG.trace("{}", sb.toString());
    }
  }

  /**
   * Get the input pipe for this pipeline.
   *
   * @return the input pipe.
   */
  public Pipe<I> getInputPipe() {
    return inputPipe;
  }

  /**
   * Starts execution of the pipeline. Each stage in the pipeline requires a dedicated thread from
   * the supplied executor service.
   *
   * @param executorService the {@link ExecutorService} to execute each stage in.
   * @return a future that will be completed when the pipeline completes. If the pipeline fails or
   *     is aborted the returned future will be completed exceptionally.
   */
  public synchronized CompletableFuture<Void> start(final ExecutorService executorService) {
    if (!started.compareAndSet(false, true)) {
      return overallFuture;
    }
    futures =
        Stream.concat(stages.stream(), Stream.of(completerStage))
            .map(task -> runWithErrorHandling(executorService, task))
            .collect(toList());
    completerStage
        .getFuture()
        .whenComplete(
            (result, error) -> {
              if (completing.compareAndSet(false, true)) {
                if (error != null) {
                  overallFuture.completeExceptionally(error);
                } else {
                  overallFuture.complete(null);
                }
              }
            });
    overallFuture.exceptionally(
        error -> {
          if (ExceptionUtils.rootCause(error) instanceof CancellationException) {
            abort();
          }
          return null;
        });
    return overallFuture;
  }

  /**
   * Abort execution of this pipeline. The future returned by {@link #start(ExecutorService)} will
   * be completed with a {@link CancellationException}.
   *
   * <p>A best effort is made to halt all processing by the pipeline immediately by interrupting
   * each execution thread and pipes connecting each stage will no longer accept or provide further
   * items.
   *
   * @throws IllegalStateException if the pipeline has not been started
   */
  public void abort() {
    final CancellationException exception = new CancellationException("Pipeline aborted");
    abort(exception);
  }

  private Future<?> runWithErrorHandling(final ExecutorService executorService, final Stage task) {
    return executorService.submit(
        () -> {
          final @Nullable Span taskSpan =
              tracingEnabled
                  ? tracer
                      .spanBuilder(task.getName())
                      .setAttribute("pipeline", name)
                      .setSpanKind(SpanKind.INTERNAL)
                      .startSpan()
                  : null;
          final Thread thread = Thread.currentThread();
          final String originalName = thread.getName();
          try {
            thread.setName(originalName + " (" + task.getName() + ")");
            task.run();
          } catch (final Throwable t) {
            if (taskSpan != null) {
              taskSpan.setStatus(StatusCode.ERROR);
            }
            if (t instanceof CompletionException
                || t instanceof CancellationException
                || t instanceof AsyncOperationException) {
              LOG.trace("Unhandled exception in pipeline. Aborting.", t);
            } else {
              LOG.info(
                  LogUtil.summarizeBesuStackTrace(
                      "Unexpected exception in pipeline. Aborting.", t));
              LOG.debug("Unexpected exception in pipeline. Aborting.", t);
            }
            try {
              abort(t);
            } catch (final Throwable t2) {
              // Seems excessive but exceptions that propagate out of this method won't be logged
              // because the executor just completes the future exceptionally, and we never
              // need to call get on it which would normally expose the error.
              LOG.error("Failed to abort pipeline after error", t2);
            }
          } finally {
            if (taskSpan != null) {
              taskSpan.end();
            }
            thread.setName(originalName);
          }
        });
  }

  private synchronized void abort(final Throwable error) {
    final List<Future<?>> runningFutures = futures;
    if (runningFutures == null) {
      throw new IllegalStateException("Pipeline must be started before it can be aborted");
    }
    if (completing.compareAndSet(false, true)) {
      inputPipe.abort();
      pipes.forEach(Pipe::abort);
      runningFutures.forEach(future -> future.cancel(true));
      overallFuture.completeExceptionally(error);
    }
  }
}
