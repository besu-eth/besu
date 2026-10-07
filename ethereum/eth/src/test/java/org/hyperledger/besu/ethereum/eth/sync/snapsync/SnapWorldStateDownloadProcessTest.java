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
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.services.pipeline.Pipeline;
import org.hyperledger.besu.services.pipeline.WritePipe;
import org.hyperledger.besu.services.tasks.Task;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SnapWorldStateDownloadProcessTest {

  private static final int FETCH_PIPELINES = 7;

  private final EthScheduler scheduler = mock(EthScheduler.class);
  private final List<Pipeline<Task<SnapDataRequest>>> fetchPipelines = new ArrayList<>();
  private final List<CompletableFuture<Void>> fetchFutures = new ArrayList<>();
  private final CompletableFuture<Void> completionFuture = new CompletableFuture<>();

  @SuppressWarnings("unchecked")
  private final WritePipe<Task<SnapDataRequest>> requestsToComplete = mock(WritePipe.class);

  private Pipeline<Task<SnapDataRequest>> completionPipeline;
  private SnapWorldStateDownloadProcess process;

  @BeforeEach
  public void setUp() {
    for (int i = 0; i < FETCH_PIPELINES; i++) {
      final CompletableFuture<Void> future = new CompletableFuture<>();
      fetchPipelines.add(pipeline(future));
      fetchFutures.add(future);
    }
    completionPipeline = pipeline(completionFuture);
    process =
        new SnapWorldStateDownloadProcess(
            fetchPipelines.get(0),
            fetchPipelines.get(1),
            fetchPipelines.get(2),
            fetchPipelines.get(3),
            fetchPipelines.get(4),
            fetchPipelines.get(5),
            fetchPipelines.get(6),
            completionPipeline,
            requestsToComplete);
  }

  @Test
  public void shouldCompleteWhenAllPipelinesFinish() {
    final CompletableFuture<Void> result = process.start(scheduler);

    fetchFutures.forEach(future -> future.complete(null));
    verify(requestsToComplete).close();
    assertThat(result).isNotDone();

    completionFuture.complete(null);
    assertThat(result).isCompleted();
  }

  @Test
  public void shouldFailWithTheErrorOfAPipelineWhileTheOthersAreStillRunning() {
    final CompletableFuture<Void> result = process.start(scheduler);
    final RuntimeException error = new RuntimeException("flat storage heal failed");

    fetchFutures.get(6).completeExceptionally(error);

    assertThat(errorOf(result)).isSameAs(error);
    fetchPipelines.forEach(pipeline -> verify(pipeline).abort());
    verify(completionPipeline).abort();
    verify(requestsToComplete, never()).close();
  }

  @Test
  public void shouldFailWithTheErrorOfTheCompletionPipeline() {
    final CompletableFuture<Void> result = process.start(scheduler);
    final RuntimeException error = new RuntimeException("completion failed");

    completionFuture.completeExceptionally(error);

    assertThat(errorOf(result)).isSameAs(error);
    fetchPipelines.forEach(pipeline -> verify(pipeline).abort());
  }

  @Test
  public void shouldBeCancelledWhenAborted() {
    final CompletableFuture<Void> result = process.start(scheduler);

    process.abort();

    assertThat(errorOf(result)).isInstanceOf(CancellationException.class);
  }

  private static Throwable errorOf(final CompletableFuture<Void> result) {
    assertThat(result).isDone();
    return result.handle((unused, error) -> error).join();
  }

  @SuppressWarnings("unchecked")
  private Pipeline<Task<SnapDataRequest>> pipeline(final CompletableFuture<Void> future) {
    final Pipeline<Task<SnapDataRequest>> pipeline = mock(Pipeline.class);
    when(scheduler.startPipeline(pipeline)).thenReturn(future);
    doAnswer(
            invocation -> {
              future.completeExceptionally(new CancellationException("Pipeline aborted"));
              return null;
            })
        .when(pipeline)
        .abort();
    return pipeline;
  }
}
