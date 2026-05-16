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

import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.BLOCK_NOT_FOUND;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.INTERNAL_ERROR;

import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.DebugTraceCallManyParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.TransactionTraceParams;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements {@code debug_traceCallMany}: executes a batch of calls sequentially against a shared
 * world state (each call sees the state changes from all prior calls) and returns an array of
 * structLog trace results — one per call — in the same format as {@code debug_traceCall}.
 *
 * <p>Parameters:
 *
 * <ol>
 *   <li>calls — array of {@code [callParams, traceConfig?]} pairs
 *   <li>blockParameter — block to execute against (default: {@code latest})
 * </ol>
 */
public class DebugTraceCallMany extends AbstractBlockParameterMethod {

  private static final Logger LOG = LoggerFactory.getLogger(DebugTraceCallMany.class);

  private static final TransactionValidationParams VALIDATION_PARAMS =
      ImmutableTransactionValidationParams.builder()
          .from(TransactionValidationParams.transactionSimulator())
          .isAllowFutureNonce(true)
          .isAllowExceedingBalance(true)
          .allowUnderpriced(true)
          .build();

  private static final ObjectMapper MAPPER = new ObjectMapper();

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
    try {
      return request.getOptionalParameter(1, BlockParameter.class).orElse(BlockParameter.LATEST);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block parameter (index 1)", RpcErrorType.INVALID_BLOCK_PARAMS, e);
    }
  }

  @Override
  protected Object resultByBlockNumber(
      final JsonRpcRequestContext requestContext, final long blockNumber) {
    if (requestContext.getRequest().getParamLength() < 1
        || requestContext.getRequest().getParamLength() > 2) {
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(), RpcErrorType.INVALID_PARAM_COUNT);
    }

    final DebugTraceCallManyParameter[] callParameters;
    try {
      callParameters = requestContext.getRequiredParameter(0, DebugTraceCallManyParameter[].class);
      LOG.atTrace()
          .setMessage("Received RPC rpcName={} params={} block={}")
          .addArgument(this::getName)
          .addArgument(callParameters)
          .addArgument(blockNumber)
          .log();
    } catch (final Exception e) {
      LOG.error("Error parsing {} parameters: {}", getName(), e.getLocalizedMessage());
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(), RpcErrorType.INVALID_PARAMS);
    }

    final Optional<BlockHeader> maybeBlockHeader =
        blockchainQueriesSupplier.get().getBlockHeaderByNumber(blockNumber);

    if (maybeBlockHeader.isEmpty()) {
      return new JsonRpcErrorResponse(requestContext.getRequest().getId(), BLOCK_NOT_FOUND);
    }
    final BlockHeader blockHeader = maybeBlockHeader.get();

    final List<JsonNode> results = new ArrayList<>();

    final Optional<Object> maybeResult =
        getBlockchainQueries()
            .getAndMapWorldState(
                blockHeader.getBlockHash(),
                ws -> {
                  final WorldUpdater updater =
                      transactionSimulator.getEffectiveWorldStateUpdater(ws);
                  try {
                    Arrays.stream(callParameters)
                        .forEachOrdered(
                            param -> {
                              final WorldUpdater localUpdater = updater.updater();
                              results.add(
                                  executeSingleCall(
                                      param.getTuple().getCallParameter(),
                                      param.getTuple().getTraceParams(),
                                      blockHeader,
                                      localUpdater));
                              localUpdater.commit();
                            });
                  } catch (final TransactionInvalidException e) {
                    LOG.error("Invalid transaction simulator result in {}", getName());
                    return Optional.of(
                        new JsonRpcErrorResponse(
                            requestContext.getRequest().getId(), INTERNAL_ERROR));
                  } catch (final EmptySimulatorResultException e) {
                    LOG.error("Empty simulator result in {}", getName());
                    return Optional.of(
                        new JsonRpcErrorResponse(
                            requestContext.getRequest().getId(), INTERNAL_ERROR));
                  } catch (final Exception e) {
                    LOG.error("Unexpected error in {}: {}", getName(), e.getLocalizedMessage(), e);
                    return Optional.of(
                        new JsonRpcErrorResponse(
                            requestContext.getRequest().getId(), INTERNAL_ERROR));
                  }
                  return Optional.of(results);
                });
    return maybeResult.orElseGet(
        () -> new JsonRpcErrorResponse(requestContext.getRequest().getId(), INTERNAL_ERROR));
  }

  private JsonNode executeSingleCall(
      final CallParameter callParameter,
      final TransactionTraceParams traceParams,
      final BlockHeader blockHeader,
      final WorldUpdater worldUpdater) {
    final TraceOptions traceOptions =
        traceParams != null ? traceParams.traceOptions() : TraceOptions.DEFAULT;

    final ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(blockHeader);
    final DebugOperationTracer tracer =
        new DebugOperationTracer(traceOptions.opCodeTracerConfig(), false);

    final var miningBeneficiary =
        protocolSpec.getMiningBeneficiaryCalculator().calculateBeneficiary(blockHeader);

    final Optional<TransactionSimulatorResult> maybeResult =
        transactionSimulator.processWithWorldUpdater(
            callParameter,
            Optional.ofNullable(traceOptions.stateOverrides()),
            VALIDATION_PARAMS,
            tracer,
            blockHeader,
            worldUpdater,
            miningBeneficiary,
            Optional.empty());

    if (maybeResult.isEmpty()) {
      throw new EmptySimulatorResultException();
    }
    final TransactionSimulatorResult simulatorResult = maybeResult.get();
    if (simulatorResult.isInvalid()) {
      throw new TransactionInvalidException();
    }

    final TransactionTrace transactionTrace =
        new TransactionTrace(
            simulatorResult.transaction(), simulatorResult.result(), tracer.getTraceFrames());

    return MAPPER.valueToTree(
        DebugTraceTransactionStepFactory.create(traceOptions, protocolSpec)
            .apply(transactionTrace)
            .getResult());
  }

  private static class TransactionInvalidException extends RuntimeException {

    TransactionInvalidException() {
      super();
    }
  }

  private static class EmptySimulatorResultException extends RuntimeException {

    EmptySimulatorResultException() {
      super();
    }
  }
}
