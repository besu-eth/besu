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

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV1;

import com.fasterxml.jackson.annotation.JsonValue;

public sealed class EngineGetPayloadResultV1 permits EngineGetPayloadResultV2 {
  protected final ExecutionPayloadV1 executionPayload;

  public EngineGetPayloadResultV1(final ExecutionPayloadV1 executionPayload) {
    this.executionPayload = executionPayload;
  }

  @JsonValue
  public ExecutionPayloadV1 getExecutionPayload() {
    return executionPayload;
  }
}
