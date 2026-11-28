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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * PayloadAttributes.timestamp is an Engine API uint64 QUANTITY. A JSON number or decimal digit
 * string must not be read as hex digits (regression for #12026).
 */
class PayloadAttributesQuantityTest {

  private final ObjectMapper mapper = JsonRpcObjectMapperFactory.getParameterMapper();

  @Test
  void deserializesHexTimestamp() throws Exception {
    final PayloadAttributesV1 attrs = deserializeV1("\"0x6ae89c4e\"");
    assertThat(attrs.getTimestamp()).isEqualTo(0x6ae89c4eL);
  }

  @Test
  void deserializesMaxUint64Timestamp() throws Exception {
    final PayloadAttributesV1 attrs = deserializeV1("\"0xffffffffffffffff\"");
    assertThat(attrs.getTimestamp()).isEqualTo(-1L);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "1793629250",
        "\"1793629250\"",
        "\"6ae89c4e\"",
        "\"0X6ae89c4e\"",
        "\"0x06ae89c4e\"",
        "\"0x\"",
        "\"0x10000000000000000\""
      })
  void rejectsNonQuantityTimestamp(final String timestampJson) {
    assertThatThrownBy(() -> deserializeV1(timestampJson))
        .isInstanceOf(JsonProcessingException.class)
        .rootCause()
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsDecimalTimestampOnV3() {
    assertThatThrownBy(
            () ->
                mapper.readValue(
                    "{\"timestamp\":1793629250,"
                        + "\"prevRandao\":\"0x2222222222222222222222222222222222222222222222222222222222222222\","
                        + "\"suggestedFeeRecipient\":\"0x1111111111111111111111111111111111111111\","
                        + "\"withdrawals\":[],"
                        + "\"parentBeaconBlockRoot\":\"0x3333333333333333333333333333333333333333333333333333333333333333\"}",
                    PayloadAttributesV3.class))
        .isInstanceOf(JsonProcessingException.class)
        .rootCause()
        .isInstanceOf(IllegalArgumentException.class);
  }

  private PayloadAttributesV1 deserializeV1(final String timestampJson) throws Exception {
    return mapper.readValue(
        "{\"timestamp\":"
            + timestampJson
            + ",\"prevRandao\":\"0x2222222222222222222222222222222222222222222222222222222222222222\","
            + "\"suggestedFeeRecipient\":\"0x1111111111111111111111111111111111111111\"}",
        PayloadAttributesV1.class);
  }
}
