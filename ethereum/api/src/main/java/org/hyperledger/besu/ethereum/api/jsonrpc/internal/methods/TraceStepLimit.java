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

import org.hyperledger.besu.ethereum.debug.TraceOptions;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder;

/**
 * Applies the operator-configured ceiling on opcode trace steps to caller-supplied trace options.
 */
final class TraceStepLimit {

  private TraceStepLimit() {}

  /**
   * Clamps the caller's opcode step limit to the server ceiling. A caller asking for no limit (0)
   * gets the server ceiling; a caller asking for more than the ceiling gets the ceiling.
   *
   * @param traceOptions the caller-supplied trace options
   * @param serverStepLimit the configured server ceiling, or 0 for no ceiling
   * @return the trace options to drive execution with
   */
  static TraceOptions clamp(final TraceOptions traceOptions, final long serverStepLimit) {
    if (serverStepLimit <= 0) {
      return traceOptions;
    }
    final int callerLimit = traceOptions.opCodeTracerConfig().limit();
    final int ceiling = (int) Math.min(serverStepLimit, Integer.MAX_VALUE);
    final int effectiveLimit = callerLimit > 0 ? Math.min(callerLimit, ceiling) : ceiling;
    if (effectiveLimit == callerLimit) {
      return traceOptions;
    }
    return new TraceOptions(
        traceOptions.tracerType(),
        OpCodeTracerConfigBuilder.createFrom(traceOptions.opCodeTracerConfig())
            .limit(effectiveLimit)
            .build(),
        traceOptions.tracerConfig(),
        traceOptions.stateOverrides());
  }
}
