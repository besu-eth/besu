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
package org.hyperledger.besu.ethereum.mainnet.slowblock;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.plugin.services.tracer.BlockAwareOperationTracer;

/**
 * The block-level half of slow-block tracing: a recorder for everything block processing runs on
 * the import thread (system calls, withdrawals, replayed transactions), plus the block lifecycle
 * that owns the per-block aggregate and emits the log.
 *
 * <p>Block-aware purely to opt into system-call tracing, which is otherwise skipped; the block
 * start and end hooks of that interface are unused because slow-block timing needs an end hook
 * after {@code persist} rather than at {@code traceEndBlock}, so that the state root wait and the
 * storage commit are inside the measured window.
 */
public class SlowBlockTracer extends SlowBlockTxRecorder implements BlockAwareOperationTracer {

  private final SlowBlockMetrics metrics;
  private final SlowBlockReadCounts blockReads = new SlowBlockReadCounts();
  private SlowBlockDiskReadCounters.Snapshot diskReadsAtStart;

  /**
   * Creates a tracer for one block.
   *
   * @param thresholdMs blocks are logged when total processing time in ms is at least this value
   */
  public SlowBlockTracer(final long thresholdMs) {
    this.metrics = new SlowBlockMetrics(thresholdMs);
  }

  @Override
  public boolean isSystemCallTracingEnabled() {
    return true;
  }

  @Override
  public boolean isExtendedTracing() {
    // BlockAwareOperationTracer defaults this to true; slow-block metrics need none of the extra
    // work it turns on.
    return false;
  }

  /**
   * The per-block aggregate, shared with the parallel path and the storage and timing hooks.
   *
   * @return the metrics for this block
   */
  public SlowBlockMetrics metrics() {
    return metrics;
  }

  /**
   * The read observer to install on the block-level world state accumulator.
   *
   * @return the block-level read counts
   */
  public SlowBlockReadCounts blockReadObserver() {
    return blockReads;
  }

  /**
   * Starts the block: records its identity and opens the timing and disk-read windows.
   *
   * @param blockHeader the header of the block being processed
   * @param txCount the number of transactions in the block
   */
  public void startBlock(final BlockHeader blockHeader, final int txCount) {
    diskReadsAtStart = SlowBlockDiskReadCounters.snapshot();
    metrics.startBlock(
        blockHeader.getNumber(),
        blockHeader.getBlockHash().getBytes().toHexString(),
        blockHeader.getGasUsed(),
        txCount);
  }

  /**
   * Closes the block after it has been persisted, folding in the block-level counters and emitting
   * the slow-block log when the block met the threshold.
   */
  public void endBlockPersist() {
    metrics.endBlock();
    final SlowBlockDiskReadCounters.Snapshot diskReads =
        SlowBlockDiskReadCounters.snapshot().since(diskReadsAtStart);
    metrics.setCacheMisses(diskReads.accounts(), diskReads.storageSlots(), diskReads.code());
    metrics.mergeExecution(this, blockReads);
    if (metrics.meetsThreshold()) {
      SlowBlockJsonLogger.log(metrics);
    }
  }
}
