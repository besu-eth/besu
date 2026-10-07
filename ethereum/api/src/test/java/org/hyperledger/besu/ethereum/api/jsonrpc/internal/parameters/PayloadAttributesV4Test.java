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
import org.junit.jupiter.params.provider.CsvSource;

class PayloadAttributesV4Test {

  private final ObjectMapper mapper = JsonRpcObjectMapperFactory.getParameterMapper();

  @ParameterizedTest
  @CsvSource({
    "0x0, 0",
    "0x1, 1",
    "0x1c9c380, 30000000",
    "0x7fffffffffffffff, 9223372036854775807",
    "0x8000000000000000, 9223372036854775807",
    "0xfffffffffffffffe, 9223372036854775807",
    "0xffffffffffffffff, 9223372036854775807"
  })
  void deserializesWholeUint64TargetGasLimitRange(final String target, final long expected)
      throws Exception {
    assertThat(deserialize(target).getTargetGasLimit()).isEqualTo(expected);
  }

  @Test
  void rejectsTargetGasLimitWiderThanUint64() {
    assertThatThrownBy(() -> deserialize("0x10000000000000000"))
        .isInstanceOf(JsonProcessingException.class)
        .rootCause()
        .isInstanceOf(IllegalArgumentException.class);
  }

  private PayloadAttributesV4 deserialize(final String targetGasLimit) throws Exception {
    return mapper.readValue(
        "{\"timestamp\":\"0x1\","
            + "\"prevRandao\":\"0x0000000000000000000000000000000000000000000000000000000000000000\","
            + "\"suggestedFeeRecipient\":\"0x0000000000000000000000000000000000000000\","
            + "\"withdrawals\":[],"
            + "\"parentBeaconBlockRoot\":\"0x0000000000000000000000000000000000000000000000000000000000000000\","
            + "\"slotNumber\":\"0x1\","
            + "\"targetGasLimit\":\""
            + targetGasLimit
            + "\"}",
        PayloadAttributesV4.class);
  }
}
