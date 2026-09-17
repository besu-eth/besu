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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;

/**
 * A read pipe that aggregates all incoming batches into a single batch.
 *
 * @param <T> the type of item included in batches
 */
public class AggregatingReadPipe<T> implements ReadPipe<List<T>> {

  private final ReadPipe<List<T>> input;
  private final Counter aggregateCounter;
  private final Queue<T> pendingItems = new ArrayDeque<>();

  /**
   * Instantiates a new Aggregating read pipe.
   *
   * @param input the input batches
   * @param aggregateCounter the aggregate counter
   */
  public AggregatingReadPipe(final ReadPipe<List<T>> input, final Counter aggregateCounter) {
    this.input = input;
    this.aggregateCounter = aggregateCounter;
  }

  @Override
  public boolean hasMore() {
    return input.hasMore() || !pendingItems.isEmpty();
  }

  @Override
  public boolean isAborted() {
    return input.isAborted();
  }

  @Override
  public List<T> get() {
    drainInputToPendingByGet();
    return emitPendingUpTo(Integer.MAX_VALUE);
  }

  @Override
  public List<T> poll() {
    drainInputToPendingByPoll();

    if (input.hasMore()) {
      return null;
    }

    return emitPendingUpTo(Integer.MAX_VALUE);
  }

  @Override
  public int drainTo(final Collection<List<T>> output, final int maxElements) {
    if (maxElements <= 0) {
      return 0;
    }

    drainInputToPendingByPoll();

    final List<T> aggregate = emitPendingUpTo(maxElements);
    if (aggregate != null) {
      output.add(aggregate);
      return aggregate.size();
    }
    return 0;
  }

  private List<T> emitPendingUpTo(final int maximumSize) {
    if (pendingItems.isEmpty()) {
      return null;
    }
    final int outputSize = Math.min(maximumSize, pendingItems.size());
    final List<T> output = new ArrayList<>(outputSize);
    for (int i = 0; i < outputSize; i++) {
      output.add(pendingItems.remove());
    }
    aggregateCounter.inc();
    return output;
  }

  private void drainInputToPendingByPoll() {
    List<T> batch;
    while ((batch = input.poll()) != null) {
      pendingItems.addAll(batch);
    }
  }

  private void drainInputToPendingByGet() {
    while (input.hasMore()) {
      final List<T> batch = input.get();
      if (batch != null) {
        pendingItems.addAll(batch);
      }
    }
  }
}
