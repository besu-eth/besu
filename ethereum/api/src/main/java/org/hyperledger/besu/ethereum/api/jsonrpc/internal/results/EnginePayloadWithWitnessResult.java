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
 *   "witness": "0x…"
 * }
 * }</pre>
 *
 * <p>The {@code witness} value is currently RLP-encoded (see {@link EngineExecutionWitnessResult}).
 * EIP-8025 as of {@code tests-zkevm@v0.8.0} specifies progressive SSZ lists (EIP-7688) for the
 * witness state, codes and public keys, so this encoding still needs to be migrated.
 *
 * <p>This result is only produced for a VALID payload that was executed by this node; every other
 * outcome responds with a bare {@link PayloadStatusV1}, so a witness is always present here.
 */
@JsonPropertyOrder({"status", "latestValidHash", "validationError", "witness"})
public class EnginePayloadWithWitnessResult {

  @JsonUnwrapped private final PayloadStatusV1 status;

  @JsonProperty("witness")
  @JsonInclude(Include.NON_NULL)
  private final EngineExecutionWitnessResult witness;

  public EnginePayloadWithWitnessResult(
      final EngineStatus status,
      final Hash latestValidHash,
      final Optional<String> validationError,
      final EngineExecutionWitnessResult witness) {
    this.status = new PayloadStatusV1(status, latestValidHash, validationError);
    this.witness = witness;
  }

  @JsonGetter
  public PayloadStatusV1 getStatus() {
    return status;
  }

  public EngineExecutionWitnessResult getWitness() {
    return witness;
  }
}
