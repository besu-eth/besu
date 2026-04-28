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
package org.hyperledger.besu.ethereum.mainnet.parallelization;

import org.hyperledger.besu.ethereum.mainnet.ExecutionStats;
import org.hyperledger.besu.ethereum.mainnet.SlowBlockTracer;
import org.hyperledger.besu.ethereum.mainnet.systemcall.BlockProcessingContext;
import org.hyperledger.besu.evm.tracing.EVMExecutionMetricsTracer;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.util.Optional;

/**
 * Factory for creating background tracer instances used during parallel transaction execution.
 *
 * <p>{@link SlowBlockTracer} has mutable per-block state ({@code executionStats}) and cannot be
 * shared between threads. This factory creates fresh {@link EVMExecutionMetricsTracer} instances
 * for background (parallel) execution so that EVM opcode metrics are captured and can be merged
 * back after conflict-free parallel execution via {@link #consolidateTracerResults}.
 *
 * <p>The {@link SlowBlockTracer} is the single tracer used during block processing; it counts EVM
 * ops inline into its {@link ExecutionStats}. For parallel workers, a lightweight {@link
 * EVMExecutionMetricsTracer} is used instead, and its counts are merged into the main {@link
 * ExecutionStats} on successful parallel execution.
 */
public class BackgroundTracerFactory {

  private BackgroundTracerFactory() {}

  /**
   * Creates a tracer for background (parallel) transaction execution.
   *
   * <p>If the block tracer is a {@link SlowBlockTracer}, returns a fresh {@link
   * EVMExecutionMetricsTracer} to capture EVM op metrics without sharing mutable state. Otherwise,
   * returns the original tracer as-is.
   *
   * @param blockProcessingContext the block processing context containing the block tracer
   * @return a background tracer instance, or the original tracer if no special handling needed
   */
  public static OperationTracer createBackgroundTracer(
      final BlockProcessingContext blockProcessingContext) {
    if (blockProcessingContext == null) {
      return OperationTracer.NO_TRACING;
    }

    final OperationTracer blockTracer = blockProcessingContext.getOperationTracer();
    if (blockTracer == null) {
      return OperationTracer.NO_TRACING;
    }

    if (blockTracer instanceof SlowBlockTracer) {
      // Create a fresh EVMExecutionMetricsTracer for background execution so that EVM op counts
      // can be merged back into the main ExecutionStats after conflict-free parallel execution
      return new EVMExecutionMetricsTracer();
    }

    // For other tracer types, return the original (caller is responsible for thread safety)
    return blockTracer;
  }

  /**
   * Consolidates tracer results from a successful parallel execution into the block's main tracer.
   * Merges background {@link EVMExecutionMetricsTracer} counters into the main {@link
   * ExecutionStats} and increments the transaction count for this confirmed parallel transaction.
   *
   * @param backgroundTracer the background tracer used during parallel execution
   * @param blockProcessingContext the block processing context containing the main tracer
   */
  public static void consolidateTracerResults(
      final OperationTracer backgroundTracer, final BlockProcessingContext blockProcessingContext) {
    if (blockProcessingContext == null) {
      return;
    }

    final OperationTracer blockTracer = blockProcessingContext.getOperationTracer();
    if (blockTracer == null) {
      return;
    }

    final Optional<SlowBlockTracer> slowBlockTracer = findSlowBlockTracer(blockTracer);
    if (slowBlockTracer.isEmpty()) {
      return;
    }

    final ExecutionStats executionStats = slowBlockTracer.get().getExecutionStats();
    if (executionStats == null) {
      return;
    }

    // Merge EVM op counts from the background worker into the block-level stats
    if (backgroundTracer instanceof EVMExecutionMetricsTracer metricsTracer) {
      executionStats.mergeEvmCountsFrom(metricsTracer);
    }

    // Increment tx_count for this confirmed parallel transaction
    executionStats.incrementTransactionCount();
  }

  /**
   * Checks whether the given tracer is a {@link SlowBlockTracer} that captures metrics.
   *
   * @param tracer the tracer to inspect
   * @return true if the tracer is a SlowBlockTracer
   */
  public static boolean hasMetricsTracer(final OperationTracer tracer) {
    return tracer instanceof SlowBlockTracer;
  }

  /**
   * Finds a {@link SlowBlockTracer} in the given tracer.
   *
   * @param tracer the tracer to search within
   * @return the SlowBlockTracer if found
   */
  public static Optional<SlowBlockTracer> findSlowBlockTracer(final OperationTracer tracer) {
    if (tracer instanceof SlowBlockTracer slowBlockTracer) {
      return Optional.of(slowBlockTracer);
    }
    return Optional.empty();
  }
}
