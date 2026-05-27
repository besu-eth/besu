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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.vm.DebugOperationTracer;
import org.hyperledger.besu.evm.tracing.TraceFrame;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;
import java.util.Optional;

/** Used to produce debug traces of blocks */
public class BlockTracer {

  private final BlockReplay blockReplay;
  // Either the initial block state or the state of the prior TX, including miner rewards.
  private WorldUpdater chainedUpdater;

  public BlockTracer(final BlockReplay blockReplay) {
    this.blockReplay = blockReplay;
  }

  public Optional<BlockTrace> trace(
      final Tracer.TraceableState mutableWorldState,
      final Hash blockHash,
      final DebugOperationTracer tracer) {
    return blockReplay.block(blockHash, prepareReplayAction(mutableWorldState, tracer));
  }

  public Optional<BlockTrace> trace(
      final Tracer.TraceableState mutableWorldState,
      final Block block,
      final DebugOperationTracer tracer) {
    return blockReplay.block(block, prepareReplayAction(mutableWorldState, tracer));
  }

  private BlockReplay.TransactionAction<TransactionTrace> prepareReplayAction(
      final MutableWorldState mutableWorldState, final DebugOperationTracer tracer) {
    return (transaction, header, blockchain, transactionProcessor, blobGasPrice) -> {
      // if we have no prior updater, it must be the first TX, so use the block's initial state
      if (chainedUpdater == null) {
        chainedUpdater = mutableWorldState.updater();
      } else {
        chainedUpdater.markTransactionBoundary();
      }
      // create an updater for just this tx
      chainedUpdater = chainedUpdater.updater();
      final TransactionProcessingResult result =
          transactionProcessor.processTransaction(
              chainedUpdater,
              header,
              transaction,
              header.getCoinbase(),
              tracer,
              blockReplay
                  .getProtocolSpec(header)
                  .getPreExecutionProcessor()
                  .createBlockHashLookup(blockchain, header),
              blobGasPrice);
      final List<TraceFrame> traceFrames = tracer.copyTraceFrames();
      tracer.reset();
      return new TransactionTrace(transaction, result, traceFrames);
    };
  }
}
