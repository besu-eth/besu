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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.precompile.PrecompileContractRegistry;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.tuweni.bytes.Bytes;

/**
 * Native {@code 4byteTracer} implementation built directly on {@link OperationTracer} context hooks
 * ({@link #traceContextEnter}) instead of post-processing a full opcode-level trace.
 *
 * <p>Collects function selectors (the first 4 bytes of call data) from all non-precompile message
 * calls entered during transaction execution, along with the size of the remaining call data.
 *
 * <p>One instance traces exactly one transaction and is not thread-safe.
 *
 * @see <a
 *     href="https://geth.ethereum.org/docs/developers/evm-tracing/built-in-tracers#4byte-tracer">
 *     Geth 4byteTracer Documentation</a>
 */
public class FourByteTracer implements OperationTracer {

  private static final int FUNCTION_SELECTOR_LENGTH = 4;

  private final PrecompileContractRegistry precompiles;
  private final Map<String, Integer> selectorCounts = new HashMap<>();

  public FourByteTracer(final PrecompileContractRegistry precompiles) {
    this.precompiles = precompiles;
  }

  @Override
  public void traceContextEnter(final MessageFrame frame) {
    if (frame.getType() == MessageFrame.Type.CONTRACT_CREATION) {
      return;
    }
    if (precompiles.get(frame.getContractAddress()) != null) {
      return;
    }
    final Bytes input = frame.getInputData();
    if (input.size() < FUNCTION_SELECTOR_LENGTH) {
      return;
    }
    selectorCounts.merge(
        input.slice(0, FUNCTION_SELECTOR_LENGTH).toHexString()
            + "-"
            + (input.size() - FUNCTION_SELECTOR_LENGTH),
        1,
        Integer::sum);
  }

  public FourByteTracerResult buildResult() {
    return new FourByteTracerResult(new TreeMap<>(selectorCounts));
  }
}
