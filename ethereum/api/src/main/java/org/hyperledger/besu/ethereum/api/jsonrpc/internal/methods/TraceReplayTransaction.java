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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.hyperledger.besu.ethereum.mainnet.feemarket.ExcessBlobGasCalculator.calculateExcessBlobGasForParent;

import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.TraceBlock.ChainUpdater;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.TraceTypeParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.Tracer;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.Tracer.TraceableState;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TraceReplayResult;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.TransactionLocation;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.ImmutableTransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.vm.DebugOperationTracer;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder.OpCodeTracerConfig;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Replays one mined transaction on top of the state left by the transactions before it in its
 * block, producing the same per-transaction result as {@code trace_replayBlockTransactions}.
 */
public class TraceReplayTransaction implements JsonRpcMethod {
  private static final Logger LOG = LoggerFactory.getLogger(TraceReplayTransaction.class);
  private final ProtocolSchedule protocolSchedule;
  private final BlockchainQueries blockchainQueries;

  public TraceReplayTransaction(
      final ProtocolSchedule protocolSchedule, final BlockchainQueries blockchainQueries) {
    this.protocolSchedule = protocolSchedule;
    this.blockchainQueries = blockchainQueries;
  }

  @Override
  public String getName() {
    return RpcMethod.TRACE_REPLAY_TRANSACTION.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    final Hash transactionHash;
    try {
      transactionHash = requestContext.getRequiredParameter(0, Hash.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid transaction hash parameter (index 0)",
          RpcErrorType.INVALID_TRANSACTION_HASH_PARAMS,
          e);
    }
    final TraceTypeParameter traceTypeParameter;
    try {
      traceTypeParameter = requestContext.getRequiredParameter(1, TraceTypeParameter.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid trace type parameter (index 1)", RpcErrorType.INVALID_TRACE_TYPE_PARAMS, e);
    }
    LOG.trace(
        "Received RPC rpcName={} txHash={} traceType={}",
        getName(),
        transactionHash,
        traceTypeParameter);

    final Blockchain blockchain = blockchainQueries.getBlockchain();
    final Optional<TransactionLocation> location =
        blockchain.getTransactionLocation(transactionHash);
    if (location.isEmpty()) {
      return new JsonRpcSuccessResponse(requestContext.getRequest().getId(), null);
    }
    // An empty result means the block body or its parent state is unavailable.
    return blockchain
        .getBlockByHash(location.get().getBlockHash())
        .flatMap(
            block ->
                Tracer.processTracing(
                    blockchainQueries,
                    Optional.of(block.getHeader()),
                    state ->
                        Optional.of(
                            respond(
                                requestContext,
                                state,
                                block,
                                location.get().getTransactionIndex(),
                                traceTypeParameter.getTraceTypes()))))
        .orElseGet(() -> error(requestContext, RpcErrorType.PRUNED_HISTORY_UNAVAILABLE));
  }

  // Answer failures here; the world state query would report them as missing state.
  private JsonRpcResponse respond(
      final JsonRpcRequestContext requestContext,
      final TraceableState state,
      final Block block,
      final int transactionIndex,
      final Set<TraceTypeParameter.TraceType> traceTypes) {
    try {
      return new JsonRpcSuccessResponse(
          requestContext.getRequest().getId(), replay(state, block, transactionIndex, traceTypes));
    } catch (final MerkleTrieException e) {
      LOG.debug("Missing trie nodes while replaying block {}", block.getHash(), e);
      return error(requestContext, RpcErrorType.PRUNED_HISTORY_UNAVAILABLE);
    } catch (final RuntimeException e) {
      LOG.error(
          "Failed to replay transaction {} of block {}", transactionIndex, block.getHash(), e);
      return error(requestContext, RpcErrorType.INTERNAL_ERROR);
    }
  }

  private static JsonRpcErrorResponse error(
      final JsonRpcRequestContext requestContext, final RpcErrorType errorType) {
    return new JsonRpcErrorResponse(requestContext.getRequest().getId(), errorType);
  }

  private TraceReplayResult replay(
      final TraceableState state,
      final Block block,
      final int transactionIndex,
      final Set<TraceTypeParameter.TraceType> traceTypes) {
    final Blockchain blockchain = blockchainQueries.getBlockchain();
    final BlockHeader header = block.getHeader();
    final ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(header);
    final ChainUpdater chainUpdater = new ChainUpdater(state);
    final List<Transaction> transactions = block.getBody().getTransactions();

    // Apply the preceding transactions exactly as block replay executes them, without tracing.
    final Wei blobGasPrice =
        protocolSpec
            .getFeeMarket()
            .blobGasPricePerGas(
                blockchain
                    .getBlockHeader(header.getParentHash())
                    .map(parent -> calculateExcessBlobGasForParent(protocolSpec, parent))
                    .orElse(BlobGas.ZERO));
    final BlockHashLookup blockHashLookup =
        protocolSpec.getPreExecutionProcessor().createBlockHashLookup(blockchain, header);
    for (int i = 0; i < transactionIndex; i++) {
      requireValid(
          protocolSpec
              .getTransactionProcessor()
              .processTransaction(
                  chainUpdater.getNextUpdater(),
                  header,
                  transactions.get(i),
                  header.getCoinbase(),
                  OperationTracer.NO_TRACING,
                  blockHashLookup,
                  ImmutableTransactionValidationParams.builder().build(),
                  blobGasPrice));
    }

    final DebugOperationTracer tracer =
        new DebugOperationTracer(
            OpCodeTracerConfigBuilder.createFrom(OpCodeTracerConfig.DEFAULT)
                .traceStorage(false)
                .traceMemory(false)
                .traceStack(true)
                .build(),
            false);
    final TransactionTrace transactionTrace =
        new ExecuteTransactionStep(
                chainUpdater,
                protocolSpec.getTransactionProcessor(),
                blockchain,
                tracer,
                protocolSpec,
                block)
            .apply(new TransactionTrace(transactions.get(transactionIndex), Optional.of(block)));
    requireValid(transactionTrace.getResult());
    return new TraceReplayTransactionStep(protocolSchedule, block, traceTypes)
        .apply(transactionTrace)
        .join();
  }

  // A mined transaction is valid at its position, so an invalid result is a replay failure.
  private static void requireValid(final TransactionProcessingResult result) {
    if (result.isInvalid()) {
      throw new IllegalStateException(
          "Transaction replay failed: " + result.getValidationResult().getErrorMessage());
    }
  }
}
