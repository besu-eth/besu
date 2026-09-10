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

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.DebugTraceTransactionResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.FourByteTracer;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.OpCodeLoggerTracerResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.calltrace.CallTracer;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.diff.StateDiffTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.diff.StateTraceGenerator;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.diff.StateTraceResult;
import org.hyperledger.besu.ethereum.debug.TraceOptions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.vm.DebugOperationTracer;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import com.fasterxml.jackson.annotation.JsonGetter;

/**
 * Encapsulates both the {@link OperationTracer} and the logic to build a {@link
 * DebugTraceTransactionResult} for a debug trace request.
 */
public interface DebugTraceTransactionStep {

  /**
   * The operation tracer to drive transaction execution with.
   *
   * @return the operation tracer
   */
  OperationTracer getOperationTracer();

  /**
   * Builds the debug trace result from the completed transaction execution trace.
   *
   * @param transactionTrace the trace produced by executing the transaction
   * @return the completed debug trace transaction result
   */
  DebugTraceTransactionResult buildResult(TransactionTrace transactionTrace);

  /**
   * Creates a {@link DebugTraceTransactionStep} for the given trace options and protocol spec.
   *
   * @param traceOptions the trace options
   * @param protocolSpec the protocol spec
   * @return the step
   */
  static DebugTraceTransactionStep of(
      final TraceOptions traceOptions, final ProtocolSpec protocolSpec) {
    final boolean recordChildCallGas = true;
    return switch (traceOptions.tracerType()) {
      case CALL_TRACER ->
          new DebugTraceTransactionStep() {
            private final CallTracer tracer = new CallTracer(traceOptions);

            @Override
            public OperationTracer getOperationTracer() {
              return tracer;
            }

            @Override
            public DebugTraceTransactionResult buildResult(final TransactionTrace trace) {
              return new DebugTraceTransactionResult(
                  trace, tracer.buildResult(trace.getTransaction(), trace.getResult()));
            }
          };
      case OPCODE_TRACER ->
          new DebugTraceTransactionStep() {
            private final DebugOperationTracer tracer =
                new DebugOperationTracer(traceOptions.opCodeTracerConfig(), recordChildCallGas);

            @Override
            public OperationTracer getOperationTracer() {
              return tracer;
            }

            @Override
            public DebugTraceTransactionResult buildResult(final TransactionTrace trace) {
              return new DebugTraceTransactionResult(
                  trace, new OpCodeLoggerTracerResult(trace, tracer.isLimitReached()));
            }
          };
      case PRESTATE_TRACER ->
          new DebugTraceTransactionStep() {
            private final DebugOperationTracer tracer =
                new DebugOperationTracer(traceOptions.opCodeTracerConfig(), recordChildCallGas);
            private final StateTraceGenerator generator = new StateTraceGenerator();
            private final boolean diffMode =
                Boolean.TRUE.equals(traceOptions.tracerConfig().getOrDefault("diffMode", false));

            @Override
            public OperationTracer getOperationTracer() {
              return tracer;
            }

            @Override
            public DebugTraceTransactionResult buildResult(final TransactionTrace trace) {
              final StateDiffTrace diffTrace =
                  (diffMode
                          ? generator.generateStateDiff(trace)
                          : generator.generatePreState(trace))
                      .findFirst()
                      .orElseGet(StateDiffTrace::new);
              return new DebugTraceTransactionResult(
                  trace, new StateTraceResult(diffTrace, diffMode));
            }
          };
      case FOUR_BYTE_TRACER ->
          new DebugTraceTransactionStep() {
            private final FourByteTracer tracer =
                new FourByteTracer(protocolSpec.getPrecompileContractRegistry());

            @Override
            public OperationTracer getOperationTracer() {
              return tracer;
            }

            @Override
            public DebugTraceTransactionResult buildResult(final TransactionTrace trace) {
              return new DebugTraceTransactionResult(trace, tracer.buildResult());
            }
          };
      case FLAT_CALL_TRACER ->
          new DebugTraceTransactionStep() {
            private final DebugOperationTracer tracer =
                new DebugOperationTracer(traceOptions.opCodeTracerConfig(), recordChildCallGas);

            @Override
            public OperationTracer getOperationTracer() {
              return tracer;
            }

            @Override
            public DebugTraceTransactionResult buildResult(final TransactionTrace trace) {
              return new DebugTraceTransactionResult(trace, new UnimplementedTracerResult());
            }
          };
    };
  }

  class UnimplementedTracerResult {
    @JsonGetter("error")
    public String getError() {
      return "Not Yet Implemented";
    }
  }
}
