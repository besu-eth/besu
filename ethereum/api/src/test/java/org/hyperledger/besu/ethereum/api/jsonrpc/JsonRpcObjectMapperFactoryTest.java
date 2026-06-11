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
package org.hyperledger.besu.ethereum.api.jsonrpc;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.junit.jupiter.api.Test;

class JsonRpcObjectMapperFactoryTest {

  @Test
  void shouldUseBesuJsonModuleWhenSerializingResponses() throws Exception {
    final Hash hash = Hash.ZERO;
    final String json =
        JsonRpcObjectMapperFactory.createResponseMapper()
            .writeValueAsString(new JsonRpcSuccessResponse(1, hash));

    assertThat(json).isEqualTo("{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"" + hash + "\"}");
  }

  @Test
  void shouldUseBesuJsonModuleWhenDeserializingParameters() throws Exception {
    final String json = "{\"hash\":\"" + Hash.ZERO + "\"}";

    final HashParameter parameter =
        JsonRpcObjectMapperFactory.createParameterMapper().readValue(json, HashParameter.class);

    assertThat(parameter.hash()).isEqualTo(Hash.ZERO);
  }

  private record HashParameter(Hash hash) {
    @JsonCreator
    private HashParameter(@JsonProperty("hash") final Hash hash) {
      this.hash = hash;
    }
  }
}
