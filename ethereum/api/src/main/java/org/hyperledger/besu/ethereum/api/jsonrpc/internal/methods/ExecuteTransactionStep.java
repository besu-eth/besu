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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.hyperledger.besu.ethereum.mainnet.feemarket.ExcessBlobGasCalculator.calculateExcessBlobGasForParent;

import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.ImmutableTransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.vm.DebugOperationTracer;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.tracing.TraceFrame;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class ExecuteTransactionStep implements Function<TransactionTrace, TransactionTrace> {

  private final TraceBlock.ChainUpdater chainUpdater;
  private final DebugOperationTracer tracer;
  private final MainnetTransactionProcessor transactionProcessor;
  private final Blockchain blockchain;
  private final ProtocolSpec protocolSpec;
  private final Block block;

  public ExecuteTransactionStep(
      final TraceBlock.ChainUpdater chainUpdater,
      final MainnetTransactionProcessor transactionProcessor,
      final Blockchain blockchain,
      final DebugOperationTracer tracer,
      final ProtocolSpec protocolSpec,
      final Block block) {
    this.chainUpdater = chainUpdater;
    this.transactionProcessor = transactionProcessor;
    this.blockchain = blockchain;
    this.tracer = tracer;
    this.protocolSpec = protocolSpec;
    this.block = block;
  }

  public ExecuteTransactionStep(
      final TraceBlock.ChainUpdater chainUpdater,
      final MainnetTransactionProcessor transactionProcessor,
      final Blockchain blockchain,
      final DebugOperationTracer tracer,
      final ProtocolSpec protocolSpec) {
    this(chainUpdater, transactionProcessor, blockchain, tracer, protocolSpec, null);
  }

  @Override
  public TransactionTrace apply(final TransactionTrace transactionTrace) {
    Block block = this.block;
    // case where transactionTrace is created only to trace a block reward
    if (block == null) {
      block =
          transactionTrace
              .getBlock()
              .orElseThrow(
                  () ->
                      new RuntimeException(
                          "Expecting reward block to be in transactionTrace but was empty"));
    }
    List<TraceFrame> traceFrames = null;
    TransactionProcessingResult result = null;

    // If it is not a reward Block trace
    if (transactionTrace.getTransaction() != null) {
      BlockHeader header = block.getHeader();
      final Optional<BlockHeader> maybeParentHeader =
          blockchain.getBlockHeader(header.getParentHash());
      final Wei blobGasPrice =
          protocolSpec
              .getFeeMarket()
              .blobGasPricePerGas(
                  maybeParentHeader
                      .map(parent -> calculateExcessBlobGasForParent(protocolSpec, parent))
                      .orElse(BlobGas.ZERO));
      final BlockHashLookup blockHashLookup =
          protocolSpec.getPreExecutionProcessor().createBlockHashLookup(blockchain, header);

      result =
          transactionProcessor.processTransaction(
              chainUpdater.getNextUpdater(),
              header,
              transactionTrace.getTransaction(),
              header.getCoinbase(),
              tracer,
              blockHashLookup,
              ImmutableTransactionValidationParams.builder().build(),
              blobGasPrice,
              Optional.empty());

      traceFrames = tracer.copyTraceFrames();
      tracer.reset();
    }
    return new TransactionTrace(
        transactionTrace.getTransaction(),
        result,
        traceFrames,
        transactionTrace.getBlock(),
        transactionTrace.getTransactionIndex());
  }
}
