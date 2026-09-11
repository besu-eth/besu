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

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Represents the result format for Ethereum's 4byteTracer as specified in the Geth documentation.
 *
 * <p>The 4byteTracer collects function selectors (the first 4 bytes of call data) from all calls
 * made during transaction execution, along with the size of the supplied call data. This is useful
 * for analyzing which contract functions were invoked during a transaction.
 *
 * <p>The JSON output is a simple map where keys are in the format
 * "0x[4-byte-selector]-[calldata-size]" and values are the number of times that combination was
 * called. For example:
 *
 * <pre>{@code
 * {
 *   "0x27dc297e-128": 1,
 *   "0x38cc4831-0": 2,
 *   "0x524f3889-96": 1
 * }
 * }</pre>
 *
 * @param selectorCounts map of selector-size keys to occurrence counts
 * @see <a
 *     href="https://geth.ethereum.org/docs/developers/evm-tracing/built-in-tracers#4byte-tracer">
 *     Geth 4byteTracer Documentation</a>
 */
public record FourByteTracerResult(@JsonValue Map<String, Integer> selectorCounts) {
  public FourByteTracerResult(final Map<String, Integer> selectorCounts) {
    this.selectorCounts = selectorCounts == null ? Map.of() : selectorCounts;
  }
}
