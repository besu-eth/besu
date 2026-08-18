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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.FourByteTracerResultConverter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.OpCodeLoggerTracerResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.calltrace.CallTracer;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.diff.StateDiffTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.diff.StateTraceGenerator;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.diff.StateTraceResult;
import org.hyperledger.besu.ethereum.debug.TraceOptions;
import org.hyperledger.besu.ethereum.debug.TracerType;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.fasterxml.jackson.annotation.JsonGetter;

/**
 * Factory for creating transaction steps for various tracers.
 *
 * <p>This factory provides methods to create functions that process a {@link TransactionTrace} and
 * return a {@link DebugTraceTransactionResult} with the appropriate tracer result based on the
 * specified tracer type. Both synchronous and asynchronous processing options are available through
 * the {@code create} and {@code createAsync} methods respectively.
 */
public class DebugTraceTransactionStepFactory {

  /**
   * Creates a function that processes a {@link TransactionTrace} and returns a {@link
   * DebugTraceTransactionResult} with the appropriate tracer result based on the specified tracer
   * type.
   *
   * @param traceOptions the trace options containing the tracer type and configuration
   * @param protocolSpec the protocol spec for the block being traced
   * @param tracer the operation tracer used to produce the {@link TransactionTrace}; required for
   *     {@link TracerType#CALL_TRACER}, which reads its result directly from the tracer instead of
   *     an opcode-level trace
   * @return a function that processes a {@link TransactionTrace} and returns a {@link
   *     DebugTraceTransactionResult} with the appropriate tracer result
   */
  public static Function<TransactionTrace, DebugTraceTransactionResult> create(
      final TraceOptions traceOptions,
      final ProtocolSpec protocolSpec,
      final OperationTracer tracer) {
    TracerType tracerType = traceOptions.tracerType();
    return switch (tracerType) {
      case OPCODE_TRACER ->
          transactionTrace -> {
            // default - struct/opcode logger tracer
            var result = new OpCodeLoggerTracerResult(transactionTrace);
            return new DebugTraceTransactionResult(transactionTrace, result);
          };
      case CALL_TRACER ->
          transactionTrace -> {
            var result =
                ((CallTracer) tracer)
                    .buildResult(transactionTrace.getTransaction(), transactionTrace.getResult());
            return new DebugTraceTransactionResult(transactionTrace, result);
          };
      case FLAT_CALL_TRACER ->
          transactionTrace -> {
            // TODO: Implement flatCallTracer logic and wire it here
            var result = new UnimplementedTracerResult();
            return new DebugTraceTransactionResult(transactionTrace, result);
          };
      case PRESTATE_TRACER ->
          transactionTrace -> {
            final var generator = new StateTraceGenerator();
            final boolean diffMode =
                Boolean.TRUE.equals(traceOptions.tracerConfig().getOrDefault("diffMode", false));
            final StateDiffTrace trace =
                (diffMode
                        ? generator.generateStateDiff(transactionTrace)
                        : generator.generatePreState(transactionTrace))
                    .findFirst()
                    .orElseGet(StateDiffTrace::new);
            return new DebugTraceTransactionResult(
                transactionTrace, new StateTraceResult(trace, diffMode));
          };
      case FOUR_BYTE_TRACER ->
          transactionTrace -> {
            var result = FourByteTracerResultConverter.convert(transactionTrace, protocolSpec);
            return new DebugTraceTransactionResult(transactionTrace, result);
          };
    };
  }

  /**
   * Creates an asynchronous function that processes a {@link TransactionTrace} and returns a {@link
   * DebugTraceTransactionResult} with the appropriate tracer result based on the specified tracer
   * type.
   *
   * @param traceOptions the options of tracer to use for processing the transaction trace
   * @param protocolSpec the protocol spec for the block being traced
   * @param tracer the operation tracer used to produce the {@link TransactionTrace}
   * @return an asynchronous function that processes a {@link TransactionTrace} and returns a {@link
   *     DebugTraceTransactionResult} with the appropriate tracer result
   */
  public static Function<TransactionTrace, CompletableFuture<DebugTraceTransactionResult>>
      createAsync(
          final TraceOptions traceOptions,
          final ProtocolSpec protocolSpec,
          final OperationTracer tracer) {
    return transactionTrace ->
        CompletableFuture.supplyAsync(
            () -> create(traceOptions, protocolSpec, tracer).apply(transactionTrace));
  }

  public static class UnimplementedTracerResult {
    @JsonGetter("error")
    public String getError() {
      return "Not Yet Implemented";
    }
  }
}
