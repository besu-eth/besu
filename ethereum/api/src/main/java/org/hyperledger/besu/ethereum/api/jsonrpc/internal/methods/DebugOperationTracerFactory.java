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

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.calltrace.CallTracer;
import org.hyperledger.besu.ethereum.debug.TraceOptions;
import org.hyperledger.besu.ethereum.debug.TracerType;
import org.hyperledger.besu.ethereum.vm.DebugOperationTracer;
import org.hyperledger.besu.evm.tracing.OperationTracer;

/**
 * Selects and constructs the {@link OperationTracer} appropriate for a debug/trace RPC request's
 * {@link TraceOptions}: a native {@link CallTracer} for {@link TracerType#CALL_TRACER}, or the
 * opcode-level {@link DebugOperationTracer} for every other tracer type.
 */
final class DebugOperationTracerFactory {

  private DebugOperationTracerFactory() {
    // Utility class - prevent instantiation
  }

  /**
   * Creates the operation tracer for the given trace options.
   *
   * @param traceOptions the trace options containing the tracer type and configuration
   * @param recordChildCallGas whether the opcode-level tracer should record the gas granted to
   *     child calls in addition to the operation cost (Parity {@code trace_*} style); ignored for
   *     {@link TracerType#CALL_TRACER}
   * @return the operation tracer to drive execution with
   */
  static OperationTracer create(final TraceOptions traceOptions, final boolean recordChildCallGas) {
    if (traceOptions.tracerType() == TracerType.CALL_TRACER) {
      return new CallTracer(
          Boolean.TRUE.equals(traceOptions.tracerConfig().getOrDefault("onlyTopCall", false)));
    }
    return new DebugOperationTracer(traceOptions.opCodeTracerConfig(), recordChildCallGas);
  }
}
