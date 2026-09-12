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

import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.INTERNAL_ERROR;

import org.hyperledger.besu.ethereum.api.ApiConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameterOrBlockHash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.TransactionTraceParams;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.debug.TraceOptions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.transaction.CallParameter;
import org.hyperledger.besu.ethereum.transaction.PreCloseStateHandler;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;

import java.util.Optional;
import java.util.OptionalLong;

public class DebugTraceCall extends AbstractTraceCall {

  public DebugTraceCall(
      final BlockchainQueries blockchainQueries,
      final ProtocolSchedule protocolSchedule,
      final TransactionSimulator transactionSimulator) {
    this(blockchainQueries, protocolSchedule, transactionSimulator, null);
  }

  public DebugTraceCall(
      final BlockchainQueries blockchainQueries,
      final ProtocolSchedule protocolSchedule,
      final TransactionSimulator transactionSimulator,
      final ApiConfiguration apiConfiguration) {
    super(blockchainQueries, protocolSchedule, transactionSimulator, apiConfiguration);
  }

  @Override
  public String getName() {
    return RpcMethod.DEBUG_TRACE_CALL.getMethodName();
  }

  @Override
  protected TraceOptions getTraceOptions(final JsonRpcRequestContext requestContext) {
    try {
      return requestContext
          .getOptionalParameter(2, TransactionTraceParams.class)
          .map(TransactionTraceParams::traceOptions)
          .orElse(TraceOptions.DEFAULT);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid transaction trace parameter (index 2)",
          RpcErrorType.INVALID_TRANSACTION_TRACE_PARAMS,
          e);
    } catch (IllegalArgumentException e) {
      // Handle invalid tracer type from TracerType.fromString()
      throw new InvalidJsonRpcParameters(
          e.getMessage(), RpcErrorType.INVALID_TRANSACTION_TRACE_PARAMS, e);
    }
  }

  @Override
  protected Object findResultByParamType(final JsonRpcRequestContext request) {
    final BlockParameterOrBlockHash blockParam;
    try {
      blockParam =
          request
              .getOptionalParameter(1, BlockParameterOrBlockHash.class)
              .orElse(BlockParameterOrBlockHash.LATEST);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block parameter (index 1)", RpcErrorType.INVALID_BLOCK_PARAMS, e);
    }

    if (blockParam.getBlockHash()) {
      final Optional<BlockHeader> maybeHeader =
          blockParam.getHash().flatMap(hash -> getBlockchainQueries().getBlockHeaderByHash(hash));
      if (maybeHeader.isEmpty()) {
        return new JsonRpcErrorResponse(request.getRequest().getId(), RpcErrorType.BLOCK_NOT_FOUND);
      }
      final BlockHeader header = maybeHeader.get();
      if (blockParam.getRequireCanonical()
          && !getBlockchainQueries().blockIsOnCanonicalChain(header.getBlockHash())) {
        return new JsonRpcErrorResponse(
            request.getRequest().getId(), RpcErrorType.JSON_RPC_NOT_CANONICAL_ERROR);
      }
      return resultByBlockNumber(request, header.getNumber());
    }

    if (blockParam.isLatest()) return latestResult(request);
    if (blockParam.isFinalized()) return finalizedResult(request);
    if (blockParam.isSafe()) return safeResult(request);
    if (blockParam.isPending()) return pendingResult(request);

    final OptionalLong blockNumber = blockParam.getNumber();
    if (blockNumber.isEmpty()) {
      return new JsonRpcErrorResponse(
          request.getRequest().getId(), RpcErrorType.INVALID_BLOCK_PARAMS);
    }
    return resultByBlockNumber(request, blockNumber.getAsLong());
  }

  @Override
  protected BlockParameter blockParameter(final JsonRpcRequestContext request) {
    return BlockParameter.LATEST;
  }

  @Override
  protected TraceExecution createTraceExecution(
      final JsonRpcRequestContext requestContext,
      final TraceOptions traceOptions,
      final ProtocolSpec protocolSpec) {
    final DebugTraceTransactionStep step = DebugTraceTransactionStep.of(traceOptions, protocolSpec);
    final PreCloseStateHandler<Object> handler =
        (mutableWorldState, maybeSimulatorResult) ->
            maybeSimulatorResult.map(
                result -> {
                  if (result.isInvalid()) {
                    final JsonRpcError error =
                        new JsonRpcError(
                            INTERNAL_ERROR, result.getValidationResult().getErrorMessage());
                    return new JsonRpcErrorResponse(requestContext.getRequest().getId(), error);
                  }

                  final TransactionTrace transactionTrace =
                      new TransactionTrace(
                          result.transaction(),
                          result.result(),
                          step.getOperationTracer().getTraceFrames());
                  return step.buildResult(transactionTrace).getResult();
                });
    return new TraceExecution(step.getOperationTracer(), handler);
  }

  @Override
  protected TransactionValidationParams buildTransactionValidationParams(
      final BlockHeader header, final CallParameter callParams) {
    return CallParameterUtil.isAllowExceedingBalance(header, callParams)
        ? TransactionValidationParams.transactionSimulatorAllowExceedingBalanceAndFutureNonce()
        : TransactionValidationParams.transactionSimulatorAllowFutureNonce();
  }
}
