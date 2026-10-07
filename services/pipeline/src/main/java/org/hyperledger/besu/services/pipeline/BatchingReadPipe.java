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

import org.hyperledger.besu.plugin.services.metrics.Counter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import org.jspecify.annotations.Nullable;

/**
 * The Batching read pipe.
 *
 * @param <T> the type parameter
 */
public class BatchingReadPipe<T> implements ReadPipe<List<T>> {

  private final ReadPipe<T> input;
  private final int maximumBatchSize;
  private final Counter batchCounter;
  private final Function<List<T>, Integer> stopBatchCondition;

  /**
   * Instantiates a new Batching read pipe.
   *
   * @param input the input
   * @param maximumBatchSize the maximum batch size
   * @param batchCounter the batch counter
   */
  public BatchingReadPipe(
      final ReadPipe<T> input, final int maximumBatchSize, final Counter batchCounter) {
    this(input, maximumBatchSize, batchCounter, ts -> maximumBatchSize - ts.size());
  }

  /**
   * Instantiates a new Batching read pipe.
   *
   * @param input the input
   * @param maximumBatchSize the maximum batch size
   * @param batchCounter the batch counter
   * @param batchEndCondition the batch end condition
   */
  public BatchingReadPipe(
      final ReadPipe<T> input,
      final int maximumBatchSize,
      final Counter batchCounter,
      final Function<List<T>, Integer> batchEndCondition) {
    this.input = input;
    this.maximumBatchSize = maximumBatchSize;
    this.batchCounter = batchCounter;
    this.stopBatchCondition = batchEndCondition;
  }

  @Override
  public boolean hasMore() {
    return input.hasMore();
  }

  @Override
  public boolean isAborted() {
    return input.isAborted();
  }

  @Override
  public @Nullable List<T> get() {
    final T firstItem = input.get();
    if (firstItem == null) {
      // Contract of get is to explicitly return null when no more items are available.
      // An empty list is not a suitable thing to return here.
      return null;
    }
    final List<T> batch = new ArrayList<>();
    batch.add(firstItem);
    Integer remainingData = stopBatchCondition.apply(batch);
    while (remainingData > 0
        && (batch.size() + remainingData) <= maximumBatchSize
        && input.hasMore()) {
      if (input.drainTo(batch, remainingData) == 0) {
        break;
      }
      remainingData = stopBatchCondition.apply(batch);
    }
    batchCounter.inc();
    return batch;
  }

  @Override
  public @Nullable List<T> poll() {
    final List<T> batch = new ArrayList<>();
    Integer remainingData = stopBatchCondition.apply(batch);
    while (remainingData > 0
        && (batch.size() + remainingData) <= maximumBatchSize
        && input.hasMore()) {
      if (input.drainTo(batch, remainingData) == 0) {
        break;
      }
      remainingData = stopBatchCondition.apply(batch);
    }
    if (batch.isEmpty()) {
      // Poll has to return null if the pipe is empty
      return null;
    }
    batchCounter.inc();
    return batch;
  }

  @Override
  public int drainTo(final Collection<List<T>> output, final int maxElements) {
    final List<T> nextBatch = poll();
    if (nextBatch != null) {
      output.add(nextBatch);
      return nextBatch.size();
    }
    return 0;
  }
}
