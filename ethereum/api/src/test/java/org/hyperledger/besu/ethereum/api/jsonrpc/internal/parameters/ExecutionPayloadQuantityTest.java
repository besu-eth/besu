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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcObjectMapperFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

/**
 * Engine API QUANTITYs are {@code uint64}, so the whole range must deserialize rather than be
 * rejected as invalid params. Besu carries them as longs with unsigned semantics.
 */
class ExecutionPayloadQuantityTest {

  private static final String MAX_UINT64 = "0xffffffffffffffff";

  private final ObjectMapper mapper = JsonRpcObjectMapperFactory.getParameterMapper();

  @Test
  void deserializesMaxUint64SlotNumber() throws Exception {
    final ExecutionPayloadV4 payload =
        mapper.readValue("{\"slotNumber\":\"" + MAX_UINT64 + "\"}", ExecutionPayloadV4.class);

    assertThat(payload.getSlotNumber()).isEqualTo(-1L);
    assertThat(Long.toUnsignedString(payload.getSlotNumber()))
        .isEqualTo(Long.toUnsignedString(-1L));
  }

  @Test
  void deserializesMaxUint64BlockNumberGasAndTimestamp() throws Exception {
    final ExecutionPayloadV4 payload =
        mapper.readValue(
            "{\"blockNumber\":\""
                + MAX_UINT64
                + "\",\"gasLimit\":\""
                + MAX_UINT64
                + "\",\"gasUsed\":\""
                + MAX_UINT64
                + "\",\"timestamp\":\""
                + MAX_UINT64
                + "\",\"blobGasUsed\":\""
                + MAX_UINT64
                + "\"}",
            ExecutionPayloadV4.class);

    assertThat(payload.getBlockNumber()).isEqualTo(-1L);
    assertThat(payload.getGasLimit()).isEqualTo(-1L);
    assertThat(payload.getGasUsed()).isEqualTo(-1L);
    assertThat(payload.getTimestamp()).isEqualTo(-1L);
    assertThat(payload.getBlobGasUsed()).isEqualTo(-1L);
  }

  @Test
  void deserializesSmallQuantitiesUnchanged() throws Exception {
    final ExecutionPayloadV4 payload =
        mapper.readValue(
            "{\"blockNumber\":\"0x1\",\"slotNumber\":\"0x0\"}", ExecutionPayloadV4.class);

    assertThat(payload.getBlockNumber()).isEqualTo(1L);
    assertThat(payload.getSlotNumber()).isZero();
  }

  @Test
  void rejectsQuantityWiderThanUint64() {
    assertThatThrownBy(
            () ->
                mapper.readValue(
                    "{\"slotNumber\":\"0x1ffffffffffffffff\"}", ExecutionPayloadV4.class))
        .isInstanceOf(JsonProcessingException.class)
        .rootCause()
        .isInstanceOf(IllegalArgumentException.class);
  }
}
