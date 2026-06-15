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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus;

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonUnwrapped;

/**
 * Engine API result for {@code engine_newPayloadWithWitnessV5}: the canonical payload-status fields
 * ({@code status}, {@code latestValidHash}, {@code validationError}) are flattened into the
 * top-level JSON object via {@link JsonUnwrapped}, and a {@code witness} object is appended when
 * the payload was imported successfully.
 *
 * <p>Example success response (status {@code VALID}):
 *
 * <pre>{@code
 * {
 *   "status": "VALID",
 *   "latestValidHash": "0xabc123…",
 *   "validationError": null,
 *   "witness": {
 *     "state":   ["0x…", "0x…"],
 *     "codes":   ["0x…"],
 *     "headers": ["0x…", "0x…"]
 *   }
 * }
 * }</pre>
 *
 * <p>The {@code witness} field is omitted ({@link Include#NON_NULL}) when the payload was not
 * VALID, matching the standard {@code engine_newPayloadV5} wire format for non-success responses.
 */
@JsonPropertyOrder({"status", "latestValidHash", "validationError", "witness"})
public class EnginePayloadWithWitnessResult {

  @JsonUnwrapped private final EnginePayloadStatusResult status;

  @JsonProperty("witness")
  @JsonInclude(Include.NON_NULL)
  private final ExecutionWitnessResult witness;

  public EnginePayloadWithWitnessResult(
      final EngineStatus status,
      final Hash latestValidHash,
      final Optional<String> validationError,
      final ExecutionWitnessResult witness) {
    this.status = new EnginePayloadStatusResult(status, latestValidHash, validationError);
    this.witness = witness;
  }

  @JsonGetter
  public EnginePayloadStatusResult getStatus() {
    return status;
  }

  public ExecutionWitnessResult getWitness() {
    return witness;
  }
}
