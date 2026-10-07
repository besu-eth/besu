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

import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcObjectMapperFactory;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class EngineExecutionWitnessResultTest {

  @Test
  void shouldEncodeEmptyWitnessAsListOfThreeEmptyLists() {
    final EngineExecutionWitnessResult result =
        new EngineExecutionWitnessResult(List.of(), List.of(), List.of());

    assertThat(result.getValue()).isEqualTo("0xc3c0c0c0");
  }

  @Test
  void shouldEncodeHeadersAsRawRlpAndCodesAndStateAsByteStrings() {
    final BlockHeader header = new BlockHeaderTestFixture().number(7).buildHeader();
    final Bytes headerRlp = RLP.encode(header::writeTo);
    final Bytes code = Bytes.fromHexString("0x6001600101");
    final Bytes stateNode = Bytes.fromHexString("0xdeadbeef");

    final EngineExecutionWitnessResult result =
        new EngineExecutionWitnessResult(
            List.of(stateNode.toHexString()),
            List.of(code.toHexString()),
            List.of(headerRlp.toHexString()));

    final RLPInput in = RLP.input(Bytes.fromHexString(result.getValue()));
    in.enterList();
    final List<Bytes> headers = in.readList(item -> item.readAsRlp().raw());
    final List<Bytes> codes = in.readList(RLPInput::readBytes);
    final List<Bytes> state = in.readList(RLPInput::readBytes);
    in.leaveList();

    // headers are embedded as RLP lists, not wrapped in a byte string
    assertThat(headers).containsExactly(headerRlp);
    assertThat(codes).containsExactly(code);
    assertThat(state).containsExactly(stateNode);
  }

  @Test
  void shouldSerializeAsHexString() throws JsonProcessingException {
    final EngineExecutionWitnessResult result =
        new EngineExecutionWitnessResult(List.of(), List.of(), List.of());

    assertThat(JsonRpcObjectMapperFactory.getResponseMapper().writeValueAsString(result))
        .isEqualTo("\"0xc3c0c0c0\"");
  }
}
