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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.TransactionTraceParams;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.Tracer;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTracer;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.DebugTraceTransactionResult;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.TransactionWithMetadata;
import org.hyperledger.besu.ethereum.debug.TraceOptions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.vm.DebugOperationTracer;

import java.util.Optional;

public class DebugTraceTransaction implements JsonRpcMethod {

  private final TransactionTracer transactionTracer;
  private final BlockchainQueries blockchain;
  private final ProtocolSchedule protocolSchedule;

  public DebugTraceTransaction(
      final BlockchainQueries blockchain,
      final TransactionTracer transactionTracer,
      final ProtocolSchedule protocolSchedule) {
    this.blockchain = blockchain;
    this.transactionTracer = transactionTracer;
    this.protocolSchedule = protocolSchedule;
  }

  @Override
  public String getName() {
    return RpcMethod.DEBUG_TRACE_TRANSACTION.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    final Hash hash;
    try {
      hash = requestContext.getRequiredParameter(0, Hash.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid transaction hash parameter (index 0)",
          RpcErrorType.INVALID_TRANSACTION_HASH_PARAMS,
          e);
    }
    final Optional<TransactionWithMetadata> transactionWithMetadata =
        blockchain.transactionByHash(hash);
    if (transactionWithMetadata.isPresent()) {
      final TraceOptions traceOptions;
      try {
        traceOptions =
            requestContext
                .getOptionalParameter(1, TransactionTraceParams.class)
                .map(TransactionTraceParams::traceOptions)
                .orElse(TraceOptions.DEFAULT);
      } catch (JsonRpcParameterException e) {
        throw new InvalidJsonRpcParameters(
            "Invalid transaction trace parameter (index 1)",
            RpcErrorType.INVALID_TRANSACTION_TRACE_PARAMS,
            e);
      } catch (IllegalArgumentException e) {
        // Handle invalid tracer type from TracerType.fromString()
        throw new InvalidJsonRpcParameters(
            e.getMessage(), RpcErrorType.INVALID_TRANSACTION_TRACE_PARAMS, e);
      }
      final DebugTraceTransactionResult debugResult =
          debugTraceTransactionResult(hash, transactionWithMetadata.get(), traceOptions);

      return new JsonRpcSuccessResponse(
          requestContext.getRequest().getId(), debugResult.getResult());
    } else {
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(), RpcErrorType.TRANSACTION_NOT_FOUND);
    }
  }

  private DebugTraceTransactionResult debugTraceTransactionResult(
      final Hash txHash,
      final TransactionWithMetadata transactionWithMetadata,
      final TraceOptions traceOptions) {
    final Hash blockHash = transactionWithMetadata.getBlockHash().get();

    final DebugOperationTracer execTracer =
        new DebugOperationTracer(traceOptions.opCodeTracerConfig(), true);

    return blockchain
        .getBlockchain()
        .getBlockHeader(blockHash)
        .map(protocolSchedule::getByBlockHeader)
        .flatMap(
            protocolSpec ->
                Tracer.processTracing(
                    blockchain,
                    blockHash,
                    mutableWorldState ->
                        transactionTracer
                            .traceTransaction(mutableWorldState, blockHash, txHash, execTracer)
                            .map(
                                DebugTraceTransactionStepFactory.create(
                                    traceOptions, protocolSpec))))
        .orElse(null);
  }
}
