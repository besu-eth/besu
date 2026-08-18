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

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.calltrace.CallTracer;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.debug.TraceOptions;
import org.hyperledger.besu.ethereum.debug.TracerType;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.transaction.CallParameter;
import org.hyperledger.besu.ethereum.transaction.PreCloseStateHandler;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.ethereum.vm.DebugOperationTracer;
import org.hyperledger.besu.evm.tracing.OperationTracer;

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

  protected AbstractTraceCall(
      final BlockchainQueries blockchainQueries,
      final ProtocolSchedule protocolSchedule,
      final TransactionSimulator transactionSimulator,
      final boolean recordChildCallGas) {
    super(blockchainQueries, protocolSchedule, transactionSimulator);
    this.recordChildCallGas = recordChildCallGas;
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

    final boolean isCallTracer = traceOptions.tracerType() == TracerType.CALL_TRACER;
    final OperationTracer tracer =
        isCallTracer
            ? new CallTracer(
                Boolean.TRUE.equals(traceOptions.tracerConfig().getOrDefault("onlyTopCall", false)))
            : new DebugOperationTracer(traceOptions.opCodeTracerConfig(), recordChildCallGas);
    return transactionSimulator
        .process(
            callParams,
            Optional.ofNullable(traceOptions.stateOverrides()),
            buildTransactionValidationParams(),
            tracer,
            getSimulatorResultHandler(requestContext, tracer, protocolSpec),
            maybeBlockHeader.get())
        .orElseGet(
            () -> new JsonRpcErrorResponse(requestContext.getRequest().getId(), INTERNAL_ERROR));
  }

  protected abstract TraceOptions getTraceOptions(final JsonRpcRequestContext requestContext);

  protected abstract PreCloseStateHandler<Object> getSimulatorResultHandler(
      final JsonRpcRequestContext requestContext,
      final OperationTracer tracer,
      final ProtocolSpec protocolSpec);
}
