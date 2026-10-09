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

import org.hyperledger.besu.ethereum.api.ApiConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.TraceTypeParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.debug.TraceOptions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.transaction.CallParameter;
import org.hyperledger.besu.ethereum.transaction.ImmutableCallParameter;
import org.hyperledger.besu.ethereum.transaction.PreCloseStateHandler;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.ethereum.vm.DebugOperationTracer;

import java.util.OptionalLong;
import java.util.Set;

public class TraceCall extends AbstractTraceCall {
  public TraceCall(
      final BlockchainQueries blockchainQueries,
      final ProtocolSchedule protocolSchedule,
      final TransactionSimulator transactionSimulator) {
    this(blockchainQueries, protocolSchedule, transactionSimulator, null);
  }

  public TraceCall(
      final BlockchainQueries blockchainQueries,
      final ProtocolSchedule protocolSchedule,
      final TransactionSimulator transactionSimulator,
      final ApiConfiguration apiConfiguration) {
    super(blockchainQueries, protocolSchedule, transactionSimulator, apiConfiguration);
  }

  @Override
  public String getName() {
    return transactionSimulator != null ? RpcMethod.TRACE_CALL.getMethodName() : null;
  }

  @Override
  protected CallParameter getCallParams(final JsonRpcRequestContext requestContext) {
    return withoutNonce(super.getCallParams(requestContext));
  }

  /**
   * Returns the call without its nonce. trace_call and trace_callMany accept a nonce but neither
   * validate nor use it: the call runs at the sender's nonce in the state it executes on, and a
   * CREATE address derives from that nonce.
   *
   * @param callParams the requested call
   * @return the call to simulate
   */
  protected static CallParameter withoutNonce(final CallParameter callParams) {
    return ImmutableCallParameter.copyOf(callParams).withNonce(OptionalLong.empty());
  }

  @Override
  protected TraceOptions getTraceOptions(final JsonRpcRequestContext requestContext) {
    return buildTraceOptions(getTraceTypes(requestContext));
  }

  private Set<TraceTypeParameter.TraceType> getTraceTypes(
      final JsonRpcRequestContext requestContext) {
    try {
      return requestContext.getRequiredParameter(1, TraceTypeParameter.class).getTraceTypes();
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid trace type parameter (index 1)", RpcErrorType.INVALID_TRACE_TYPE_PARAMS, e);
    }
  }

  @Override
  protected TraceExecution createTraceExecution(
      final JsonRpcRequestContext requestContext,
      final TraceOptions traceOptions,
      final ProtocolSpec protocolSpec) {
    final DebugOperationTracer tracer =
        new DebugOperationTracer(traceOptions.opCodeTracerConfig(), false);
    final PreCloseStateHandler<Object> handler =
        (mutableWorldState, maybeSimulatorResult) ->
            maybeSimulatorResult.map(
                result -> {
                  if (result.isInvalid()) {
                    return new JsonRpcErrorResponse(
                        requestContext.getRequest().getId(),
                        JsonRpcError.from(result.getValidationResult()));
                  }

                  final TransactionTrace transactionTrace =
                      new TransactionTrace(
                          result.transaction(), result.result(), tracer.getTraceFrames());

                  final Block block =
                      blockchainQueriesSupplier.get().getBlockchain().getChainHeadBlock();
                  return getTraceCallResult(
                      protocolSchedule,
                      getTraceTypes(requestContext),
                      result,
                      transactionTrace,
                      block);
                });
    return new TraceExecution(tracer, handler);
  }
}
