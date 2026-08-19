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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({"status", "latestValidHash", "validationError", "inclusionListSatisfied"})
public class PayloadStatusV2 extends PayloadStatusV1 {
  Boolean inclusionListSatisfied;

  @JsonCreator
  public PayloadStatusV2(
      @JsonProperty("status") final EngineStatus status,
      @JsonProperty("latestValidHash") final Hash latestValidHash,
      @JsonProperty("validationError") final String validationError,
      @JsonProperty("inclusionListSatisfied") final Boolean inclusionListSatisfied) {
    super(status, latestValidHash, validationError);
    this.inclusionListSatisfied = inclusionListSatisfied;
  }

  public PayloadStatusV2(final EngineStatus status) {
    this(status, null, null, null);
  }

  public PayloadStatusV2(
      final EngineStatus status, final Hash latestValidHash, final String validationError) {
    this(status, latestValidHash, validationError, null);
  }

  public PayloadStatusV2(
      final EngineStatus status, final Hash latestValidHash, final Boolean inclusionListSatisfied) {
    this(status, latestValidHash, null, inclusionListSatisfied);
  }

  @JsonGetter(value = "inclusionListSatisfied")
  public Boolean getInclusionListSatisfied() {
    return inclusionListSatisfied;
  }
}
