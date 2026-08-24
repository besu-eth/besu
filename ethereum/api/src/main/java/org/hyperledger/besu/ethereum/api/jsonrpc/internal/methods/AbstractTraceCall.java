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

import org.hyperledger.besu.ethereum.api.ApiConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.debug.TraceOptions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.transaction.CallParameter;
import org.hyperledger.besu.ethereum.transaction.PreCloseStateHandler;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.ethereum.vm.DebugOperationTracer;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTraceCall extends AbstractTraceByBlock {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractTraceCall.class);

  /**
   * A flag to indicate if call operations should trace just the operation cost (false, Geth style,
   * debug_ series RPCs) or the operation cost and all gas granted to the child call (true, Parity
   * style, trace_ series RPCs)
   */
  private final boolean recordChildCallGas;

  private final long serverStepLimit;

  protected AbstractTraceCall(
      final BlockchainQueries blockchainQueries,
      final ProtocolSchedule protocolSchedule,
      final TransactionSimulator transactionSimulator,
      final boolean recordChildCallGas) {
    this(blockchainQueries, protocolSchedule, transactionSimulator, recordChildCallGas, null);
  }

  protected AbstractTraceCall(
      final BlockchainQueries blockchainQueries,
      final ProtocolSchedule protocolSchedule,
      final TransactionSimulator transactionSimulator,
      final boolean recordChildCallGas,
      final ApiConfiguration apiConfiguration) {
    super(blockchainQueries, protocolSchedule, transactionSimulator);
    this.recordChildCallGas = recordChildCallGas;
    this.serverStepLimit =
        apiConfiguration != null ? apiConfiguration.getDebugTraceStepLimit() : 0L;
  }

  @Override
  protected Object resultByBlockNumber(
      final JsonRpcRequestContext requestContext, final long blockNumber) {
    final CallParameter callParams = CallParameterUtil.validateAndGetCallParams(requestContext);
    final TraceOptions traceOptions = getTraceOptions(requestContext);
    final String blockNumberString = String.valueOf(blockNumber);
    LOG.atTrace()
        .setMessage("Received RPC rpcName={} callParams={} block={} traceTypes={}")
        .addArgument(this::getName)
        .addArgument(callParams)
        .addArgument(blockNumberString)
        .addArgument(traceOptions)
        .log();

    final Optional<BlockHeader> maybeBlockHeader =
        blockchainQueriesSupplier.get().getBlockHeaderByNumber(blockNumber);

    if (maybeBlockHeader.isEmpty()) {
      return new JsonRpcErrorResponse(requestContext.getRequest().getId(), BLOCK_NOT_FOUND);
    }

    final ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(maybeBlockHeader.get());

    final TraceOptions effectiveTraceOptions = applyServerStepLimit(traceOptions);
    final DebugOperationTracer tracer =
        new DebugOperationTracer(effectiveTraceOptions.opCodeTracerConfig(), recordChildCallGas);
    return transactionSimulator
        .process(
            callParams,
            Optional.ofNullable(effectiveTraceOptions.stateOverrides()),
            buildTransactionValidationParams(),
            tracer,
            getSimulatorResultHandler(requestContext, tracer, protocolSpec),
            maybeBlockHeader.get())
        .orElseGet(
            () -> new JsonRpcErrorResponse(requestContext.getRequest().getId(), INTERNAL_ERROR));
  }

  /**
   * Clamps the caller-supplied step limit to the operator-configured server ceiling. If the server
   * limit is 0 (operator opt-out), the caller's value is used as-is. If the caller supplies 0
   * (unlimited), the server ceiling is applied. Otherwise the minimum of the two is used.
   */
  private TraceOptions applyServerStepLimit(final TraceOptions traceOptions) {
    if (serverStepLimit <= 0) {
      return traceOptions;
    }
    final int callerLimit = traceOptions.opCodeTracerConfig().limit();
    final int effectiveLimit =
        callerLimit > 0
            ? (int) Math.min(callerLimit, Math.min(serverStepLimit, Integer.MAX_VALUE))
            : (int) Math.min(serverStepLimit, Integer.MAX_VALUE);
    if (effectiveLimit == callerLimit) {
      return traceOptions;
    }
    final var newConfig =
        OpCodeTracerConfigBuilder.createFrom(traceOptions.opCodeTracerConfig())
            .limit(effectiveLimit)
            .build();
    return new TraceOptions(
        traceOptions.tracerType(),
        newConfig,
        traceOptions.tracerConfig(),
        traceOptions.stateOverrides());
  }

  protected abstract TraceOptions getTraceOptions(final JsonRpcRequestContext requestContext);

  protected abstract PreCloseStateHandler<Object> getSimulatorResultHandler(
      final JsonRpcRequestContext requestContext,
      final DebugOperationTracer tracer,
      final ProtocolSpec protocolSpec);
}
