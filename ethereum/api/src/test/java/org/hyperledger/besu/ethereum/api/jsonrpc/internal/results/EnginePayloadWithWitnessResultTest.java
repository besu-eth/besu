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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.VALID;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcObjectMapperFactory;

import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class EnginePayloadWithWitnessResultTest {

  private static final Hash BLOCK_HASH = Hash.wrap(Bytes32.fromHexStringLenient("0x1234"));

  @Test
  void shouldFlattenPayloadStatusAndAppendWitness() throws Exception {
    final EnginePayloadWithWitnessResult result =
        new EnginePayloadWithWitnessResult(
            VALID,
            BLOCK_HASH,
            Optional.empty(),
            new EngineExecutionWitnessResult(List.of(), List.of(), List.of()));

    final JsonNode json =
        JsonRpcObjectMapperFactory.getResponseMapper()
            .readTree(JsonRpcObjectMapperFactory.getResponseMapper().writeValueAsString(result));

    assertThat(json.get("status").isTextual()).isTrue();
    assertThat(json.get("status").asText()).isEqualTo("VALID");
    assertThat(json.get("latestValidHash").asText()).isEqualTo(BLOCK_HASH.toHexString());
    assertThat(json.get("witness").asText()).isEqualTo("0xc3c0c0c0");
  }
}
