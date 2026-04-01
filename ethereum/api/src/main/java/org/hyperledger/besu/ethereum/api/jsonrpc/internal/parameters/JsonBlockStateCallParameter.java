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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters;

import org.hyperledger.besu.datatypes.StateOverrideMap;
import org.hyperledger.besu.ethereum.transaction.BlockStateCall;
import org.hyperledger.besu.ethereum.transaction.CallParameter;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonIgnoreProperties(ignoreUnknown = true)
public class JsonBlockStateCallParameter extends BlockStateCall {
  @JsonCreator
  public JsonBlockStateCallParameter(
      @JsonProperty("calls")
          @JsonDeserialize(contentUsing = SimulateCallParameterDeserializer.class)
          final List<CallParameter> calls,
      @JsonProperty("blockOverrides") final BlockOverridesParameter blockOverrides,
      @JsonProperty("stateOverrides") final StateOverrideMap stateOverrides) {
    super(calls, blockOverrides, stateOverrides);
  }
}
