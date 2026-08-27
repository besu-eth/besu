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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.TransactionTraceParams;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.OpCodeLoggerTracerResult;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.debug.TraceOptions;
import org.hyperledger.besu.ethereum.debug.TracerType;
import org.hyperledger.besu.ethereum.mainnet.ImmutableTransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.transaction.PreCloseStateHandler;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.ethereum.vm.DebugOperationTracer;

import java.util.Optional;

public class DebugTraceCall extends AbstractTraceCall {
  private static final TransactionValidationParams TRANSACTION_VALIDATION_PARAMS =
      ImmutableTransactionValidationParams.builder()
          .from(TransactionValidationParams.transactionSimulator())
          .isAllowFutureNonce(true)
          .isAllowExceedingBalance(true)
          .allowUnderpriced(true)
          .build();

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
    super(blockchainQueries, protocolSchedule, transactionSimulator, true, apiConfiguration);
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
  protected BlockParameter blockParameter(final JsonRpcRequestContext request) {
    final Optional<BlockParameter> maybeBlockParameter;
    try {
      maybeBlockParameter = request.getOptionalParameter(1, BlockParameter.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block parameter (index 1)", RpcErrorType.INVALID_BLOCK_PARAMS, e);
    }

    return maybeBlockParameter.orElse(BlockParameter.LATEST);
  }

  @Override
  protected PreCloseStateHandler<Object> getSimulatorResultHandler(
      final JsonRpcRequestContext requestContext,
      final DebugOperationTracer tracer,
      final ProtocolSpec protocolSpec) {
    return (mutableWorldState, maybeSimulatorResult) ->
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
                      result.transaction(), result.result(), tracer.getTraceFrames());
              final TraceOptions opts = getTraceOptions(requestContext);
              if (opts.tracerType() == TracerType.OPCODE_TRACER) {
                return new OpCodeLoggerTracerResult(transactionTrace, tracer.isLimitReached());
              }
              return DebugTraceTransactionStepFactory.create(opts, protocolSpec)
                  .apply(transactionTrace)
                  .getResult();
            });
  }

  @Override
  protected TransactionValidationParams buildTransactionValidationParams() {
    return TRANSACTION_VALIDATION_PARAMS;
  }
}
