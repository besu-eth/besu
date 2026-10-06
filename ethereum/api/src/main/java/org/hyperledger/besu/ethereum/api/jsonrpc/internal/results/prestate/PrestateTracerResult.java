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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.prestate;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Result model for Geth's {@code prestateTracer} output.
 *
 * <p>Non-diff mode serializes as the bare address→account map ({@link Prestate}); diff mode as
 * {@code {"post": ..., "pre": ...}} ({@link Diff}). Account fields are emitted in Geth's order
 * {@code balance, code, codeHash, nonce, storage}; a {@code null} field is omitted (Geth's {@code
 * omitempty} semantics), and {@code balance} is always present in pre-state entries.
 *
 * @see <a
 *     href="https://geth.ethereum.org/docs/developers/evm-tracing/built-in-tracers#prestate-tracer">
 *     Geth prestateTracer Documentation</a>
 */
public sealed interface PrestateTracerResult
    permits PrestateTracerResult.Prestate, PrestateTracerResult.Diff {

  /** Non-diff mode: the address→account map is the whole result. */
  record Prestate(@JsonValue Map<String, Account> accounts) implements PrestateTracerResult {}

  /** Diff mode: post-transaction changed state alongside the pre-state snapshot. */
  @JsonPropertyOrder({"post", "pre"})
  record Diff(Map<String, Account> post, Map<String, Account> pre)
      implements PrestateTracerResult {}

  /** A single account entry in the pre- or post-state map. */
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @JsonPropertyOrder({"balance", "code", "codeHash", "nonce", "storage"})
  record Account(
      String balance, String code, String codeHash, Long nonce, Map<String, String> storage) {}
}
