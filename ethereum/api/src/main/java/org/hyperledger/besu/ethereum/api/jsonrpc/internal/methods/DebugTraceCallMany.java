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

import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.BLOCK_NOT_FOUND;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.INTERNAL_ERROR;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.DebugTraceCallManyParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.DebugTraceCallParameterTuple;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.TransactionTraceParams;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.debug.TraceOptions;
import org.hyperledger.besu.ethereum.mainnet.ImmutableTransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.transaction.CallParameter;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulatorResult;
import org.hyperledger.besu.ethereum.vm.DebugOperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebugTraceCallMany extends AbstractBlockParameterMethod {

  private static final Logger LOG = LoggerFactory.getLogger(DebugTraceCallMany.class);

  private static final TransactionValidationParams TRANSACTION_VALIDATION_PARAMS =
      ImmutableTransactionValidationParams.builder()
          .from(TransactionValidationParams.transactionSimulator())
          .isAllowFutureNonce(true)
          .isAllowExceedingBalance(true)
          .allowUnderpriced(true)
          .build();

  private final ProtocolSchedule protocolSchedule;
  private final TransactionSimulator transactionSimulator;

  public DebugTraceCallMany(
      final BlockchainQueries blockchainQueries,
      final ProtocolSchedule protocolSchedule,
      final TransactionSimulator transactionSimulator) {
    super(blockchainQueries);
    this.protocolSchedule = protocolSchedule;
    this.transactionSimulator = transactionSimulator;
  }

  @Override
  public String getName() {
    return RpcMethod.DEBUG_TRACE_CALL_MANY.getMethodName();
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
  protected Object resultByBlockNumber(
      final JsonRpcRequestContext requestContext, final long blockNumber) {

    final DebugTraceCallManyParameter[] callTuples;
    try {
      callTuples = requestContext.getRequiredParameter(0, DebugTraceCallManyParameter[].class);
    } catch (final Exception e) {
      LOG.error("Error parsing debug_traceCallMany parameters: {}", e.getLocalizedMessage());
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(), RpcErrorType.INVALID_TRACE_CALL_MANY_PARAMS);
    }

    final Optional<BlockHeader> maybeBlockHeader =
        getBlockchainQueries().getBlockHeaderByNumber(blockNumber);
    if (maybeBlockHeader.isEmpty()) {
      return new JsonRpcErrorResponse(requestContext.getRequest().getId(), BLOCK_NOT_FOUND);
    }
    final BlockHeader blockHeader = maybeBlockHeader.get();
    final ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(blockHeader);
    final Address miningBeneficiary =
        protocolSpec.getMiningBeneficiaryCalculator().calculateBeneficiary(blockHeader);

    return getBlockchainQueries()
        .getAndMapWorldState(
            blockHeader.getBlockHash(),
            ws -> {
              final WorldUpdater updater = transactionSimulator.getEffectiveWorldStateUpdater(ws);
              final List<Object> results = new ArrayList<>(callTuples.length);
              for (final DebugTraceCallManyParameter param : callTuples) {
                final DebugTraceCallParameterTuple tuple = param.getTuple();
                final TraceOptions traceOptions = resolveTraceOptions(tuple.getTraceParams());
                final WorldUpdater localUpdater = updater.updater();
                final Object callResult =
                    traceSingleCall(
                        tuple.getCallParameter(),
                        traceOptions,
                        blockHeader,
                        protocolSpec,
                        miningBeneficiary,
                        localUpdater);
                results.add(callResult);
                if (!(callResult instanceof JsonRpcError)) {
                  localUpdater.commit();
                }
              }
              return Optional.<Object>of(results);
            })
        .orElseGet(
            () ->
                (Object)
                    new JsonRpcErrorResponse(requestContext.getRequest().getId(), INTERNAL_ERROR));
  }

  private TraceOptions resolveTraceOptions(final TransactionTraceParams traceParams) {
    try {
      return traceParams != null ? traceParams.traceOptions() : TraceOptions.DEFAULT;
    } catch (final IllegalArgumentException e) {
      throw new InvalidJsonRpcParameters(
          e.getMessage(), RpcErrorType.INVALID_TRANSACTION_TRACE_PARAMS, e);
    }
  }

  private Object traceSingleCall(
      final CallParameter callParameter,
      final TraceOptions traceOptions,
      final BlockHeader blockHeader,
      final ProtocolSpec protocolSpec,
      final Address miningBeneficiary,
      final WorldUpdater worldUpdater) {

    final DebugOperationTracer tracer =
        new DebugOperationTracer(traceOptions.opCodeTracerConfig(), true);

    final Optional<TransactionSimulatorResult> maybeResult =
        transactionSimulator.processWithWorldUpdater(
            callParameter,
            Optional.ofNullable(traceOptions.stateOverrides()),
            TRANSACTION_VALIDATION_PARAMS,
            tracer,
            blockHeader,
            worldUpdater,
            miningBeneficiary,
            Optional.empty());

    if (maybeResult.isEmpty()) {
      return new JsonRpcError(INTERNAL_ERROR, "Transaction simulator returned no result");
    }

    final TransactionSimulatorResult simulatorResult = maybeResult.get();
    if (simulatorResult.isInvalid()) {
      return new JsonRpcError(
          INTERNAL_ERROR, simulatorResult.getValidationResult().getErrorMessage());
    }

    final TransactionTrace transactionTrace =
        new TransactionTrace(
            simulatorResult.transaction(), simulatorResult.result(), tracer.getTraceFrames());
    return DebugTraceTransactionStepFactory.create(traceOptions, protocolSpec)
        .apply(transactionTrace)
        .getResult();
  }
}
