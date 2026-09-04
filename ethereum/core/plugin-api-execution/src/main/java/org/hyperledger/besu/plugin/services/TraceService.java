/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.plugin.services;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.Unstable;
import org.hyperledger.besu.plugin.data.BlockTraceResult;
import org.hyperledger.besu.plugin.services.tracer.BlockAwareOperationTracer;

import java.util.function.Consumer;

/** The Trace service interface */
@Unstable
public interface TraceService extends BesuService {
  /**
   * Traces a block
   *
   * @param blockNumber the block number
   * @param tracer the tracer (OperationTracer)
   * @return BlockTraceResult the result of the trace
   */
  BlockTraceResult traceBlock(long blockNumber, BlockAwareOperationTracer tracer);

  /**
   * Traces a block by hash
   *
   * @param hash the block hash
   * @param tracer the tracer (OperationTracer)
   * @return BlockTraceResult the result of the trace
   */
  BlockTraceResult traceBlock(Hash hash, BlockAwareOperationTracer tracer);

  /**
   * Traces range of blocks
   *
   * @param fromBlockNumber the beginning of the range (inclusive)
   * @param toBlockNumber the end of the range (inclusive)
   * @param beforeTracing consumer invoked with the world updater of each block before it is traced
   * @param afterTracing consumer invoked with the world updater of each block after it is traced
   * @param tracer an instance of OperationTracer
   */
  void trace(
      final long fromBlockNumber,
      final long toBlockNumber,
      final Consumer<WorldUpdater> beforeTracing,
      final Consumer<WorldUpdater> afterTracing,
      final BlockAwareOperationTracer tracer);
}
