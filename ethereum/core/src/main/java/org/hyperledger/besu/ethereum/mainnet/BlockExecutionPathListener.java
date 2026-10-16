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
package org.hyperledger.besu.ethereum.mainnet;

import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.core.BlockHeader;

/**
 * Told which executor runs each block. A node uses {@link #NONE}; a test runner passes one in
 * through {@link BalConfiguration#getExecutionPathListener} to report the path a fixture took.
 */
public interface BlockExecutionPathListener {

  /** The default listener: it ignores everything. */
  BlockExecutionPathListener NONE = new BlockExecutionPathListener() {};

  /**
   * The block runs on the parallel executor.
   *
   * @param header the block's header
   * @param scheduler {@code bal} when transactions are scheduled from the block access list, {@code
   *     optimistic} when there is no list to schedule from
   */
  default void onParallel(final BlockHeader header, final String scheduler) {}

  /**
   * The block runs on the sequential executor.
   *
   * @param header the block's header
   * @param reason why parallel execution was ruled out: {@code disabled} when the schedule built
   *     the sequential block processor, {@code not-path-based} when the world state cannot run
   *     transactions in parallel
   */
  default void onSequential(final BlockHeader header, final String reason) {}

  /**
   * The parallel executor failed the block and it was run again sequentially. The block's result is
   * the sequential one.
   *
   * @param header the block's header
   * @param parallelResult the parallel executor's failed result
   * @param sequentialResult the sequential re-run's result
   */
  default void onSequentialFallback(
      final BlockHeader header,
      final BlockProcessingResult parallelResult,
      final BlockProcessingResult sequentialResult) {}
}
