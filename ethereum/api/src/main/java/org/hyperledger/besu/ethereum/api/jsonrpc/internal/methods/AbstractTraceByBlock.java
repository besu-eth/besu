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

import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.TraceTypeParameter.TraceType;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.TraceTypeParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TraceCallResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.diff.StateTraceGenerator;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.flat.FlatTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.flat.FlatTraceGenerator;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.vm.VmTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.vm.VmTraceGenerator;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.debug.TraceOptions;
import org.hyperledger.besu.ethereum.debug.TracerType;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulatorResult;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder.OpCodeTracerConfig;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class AbstractTraceByBlock extends AbstractBlockParameterMethod
    implements JsonRpcMethod {
  protected final ProtocolSchedule protocolSchedule;
  protected final TransactionSimulator transactionSimulator;

  protected static final ObjectMapper mapper = new ObjectMapper();

  protected AbstractTraceByBlock(
      final BlockchainQueries blockchainQueries,
      final ProtocolSchedule protocolSchedule,
      final TransactionSimulator transactionSimulator) {
    super(blockchainQueries);

    this.protocolSchedule = protocolSchedule;
    this.transactionSimulator = transactionSimulator;
  }

  @Override
  protected BlockParameter blockParameter(final JsonRpcRequestContext request) {
    final Optional<BlockParameter> maybeBlockParameter;
    try {
      maybeBlockParameter = request.getOptionalParameter(2, BlockParameter.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block parameter (index 2)", RpcErrorType.INVALID_BLOCK_PARAMS, e);
    }

    if (maybeBlockParameter.isPresent()) {
      return maybeBlockParameter.get();
    }

    return BlockParameter.LATEST;
  }

  protected JsonNode getTraceCallResult(
      final ProtocolSchedule protocolSchedule,
      final Set<TraceTypeParameter.TraceType> traceTypes,
      final TransactionSimulatorResult simulatorResult,
      final TransactionTrace transactionTrace,
      final Block block) {
    final TraceCallResult.Builder builder = TraceCallResult.builder();

    transactionTrace
        .getResult()
        .getRevertReason()
        .ifPresentOrElse(
            revertReason -> builder.output(revertReason.toHexString()),
            () -> builder.output(simulatorResult.getOutput().toString()));

    if (traceTypes.contains(TraceType.STATE_DIFF)) {
      new StateTraceGenerator()
          .generateStateDiff(transactionTrace)
          .forEachOrdered(builder::stateDiff);
    }

    if (traceTypes.contains(TraceType.TRACE)) {
      FlatTraceGenerator.generateFromTransactionTrace(
              protocolSchedule, transactionTrace, block, new AtomicInteger())
          .forEachOrdered(trace -> builder.addTrace((FlatTrace) trace));
    }

    if (traceTypes.contains(TraceType.VM_TRACE)) {
      new VmTraceGenerator(transactionTrace)
          .generateTraceStream()
          .forEachOrdered(vmTrace -> builder.vmTrace((VmTrace) vmTrace));
    }

    return mapper.valueToTree(builder.build());
  }

  protected TransactionValidationParams buildTransactionValidationParams() {
    return TransactionValidationParams.transactionSimulator();
  }

  protected TraceOptions buildTraceOptions(final Set<TraceTypeParameter.TraceType> traceTypes) {
    return new TraceOptions(
        TracerType.OPCODE_TRACER,
        OpCodeTracerConfigBuilder.createFrom(OpCodeTracerConfig.DEFAULT)
            .traceStorage(traceTypes.contains(TraceType.STATE_DIFF))
            .traceMemory(false)
            .traceStack(
                traceTypes.contains(TraceType.TRACE) || traceTypes.contains(TraceType.VM_TRACE))
            .build(),
        Map.of());
  }
}
