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

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV1;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonValue;

@JsonPropertyOrder({
  "executionPayload",
  "blockValue",
})
public sealed class EngineGetPayloadResultV2 extends EngineGetPayloadResultV1
    permits EngineGetPayloadResultV3 {
  private final Wei blockValue;

  public EngineGetPayloadResultV2(final ExecutionPayloadV1 executionPayload, final Wei blockValue) {
    super(executionPayload);
    this.blockValue = blockValue;
  }

  @Override
  @JsonValue(false)
  @JsonGetter(value = "executionPayload")
  public ExecutionPayloadV1 getExecutionPayload() {
    return executionPayload;
  }

  @JsonGetter(value = "blockValue")
  public Wei getBlockValue() {
    return blockValue;
  }
}
