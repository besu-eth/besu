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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

// TODO: duplicate of PayloadStatusV1 — remove once EngineNewPayloadV* methods are refactored
@JsonPropertyOrder({"status", "latestValidHash", "validationError"})
public class EnginePayloadStatusResult {
  EngineStatus status;
  Optional<Hash> latestValidHash;
  Optional<String> validationError;

  @JsonCreator
  public EnginePayloadStatusResult(
      @JsonProperty("status") final EngineStatus status,
      @JsonProperty("latestValidHash") final Hash latestValidHash,
      @JsonProperty("validationError") final Optional<String> validationError) {
    this.status = status;
    this.latestValidHash = Optional.ofNullable(latestValidHash);
    this.validationError = validationError;
  }

  @JsonGetter(value = "status")
  public String getStatusAsString() {
    return status.name();
  }

  public EngineStatus getStatus() {
    return status;
  }

  @JsonGetter(value = "latestValidHash")
  public String getLatestValidHashAsString() {
    return latestValidHash.map(h -> h.getBytes().toHexString()).orElse(null);
  }

  public Optional<Hash> getLatestValidHash() {
    return latestValidHash;
  }

  @JsonGetter(value = "validationError")
  public String getError() {
    return validationError.orElse(null);
  }
}
