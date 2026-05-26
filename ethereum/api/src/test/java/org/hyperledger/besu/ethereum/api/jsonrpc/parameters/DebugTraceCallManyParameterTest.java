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
package org.hyperledger.besu.ethereum.api.jsonrpc.parameters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.DebugTraceCallManyParameter;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.junit.jupiter.api.Test;

public class DebugTraceCallManyParameterTest {

  private static final String EMPTY_JSON = "[]";

  private static final String INVALID_JSON = "[[\"invalid\"]]";

  private static final String CALLS_WITH_TRACE_CONFIG_JSON =
      """
          [
            [
              {
                "from": "0xfe3b557e8fb62b89f4916b721be55ceb828dbd73",
                "to":   "0x0010000000000000000000000000000000000000",
                "value":"0x0",
                "gas":  "0xfffff2",
                "gasPrice":"0xef",
                "data": "0x"
              },
              {
                "disableStorage": true,
                "disableStack":   true
              }
            ],
            [
              {
                "from": "0x627306090abab3a6e1400e9345bc60c78a8bef57",
                "to":   "0x0010000000000000000000000000000000000000",
                "value":"0x0",
                "gas":  "0xfffff2",
                "gasPrice":"0xef",
                "data": "0x"
              },
              null
            ]
          ]""";

  private static final String CALL_WITHOUT_TRACE_CONFIG_SLOT_JSON =
      """
          [
            [
              {
                "from": "0xfe3b557e8fb62b89f4916b721be55ceb828dbd73",
                "to":   "0x0010000000000000000000000000000000000000",
                "value":"0x0",
                "gas":  "0xfffff2",
                "gasPrice":"0xef",
                "data": "0x"
              }
            ]
          ]""";

  private final ObjectMapper mapper = new ObjectMapper().registerModule(new Jdk8Module());

  @Test
  public void emptyArrayParsesToEmpty() throws IOException {
    final DebugTraceCallManyParameter[] parsed =
        mapper.readValue(EMPTY_JSON, DebugTraceCallManyParameter[].class);
    assertThat(parsed).isNullOrEmpty();
  }

  @Test
  public void mixedTraceConfigsParseCorrectly() throws IOException {
    final DebugTraceCallManyParameter[] parsed =
        mapper.readValue(CALLS_WITH_TRACE_CONFIG_JSON, DebugTraceCallManyParameter[].class);

    assertThat(parsed).hasSize(2);

    assertThat(parsed[0].getTuple().getCallParameter().getSender())
        .contains(Address.fromHexString("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73"));
    assertThat(parsed[0].getTuple().getTraceParams()).isNotNull();
    assertThat(parsed[0].getTuple().getTraceParams().disableStorage()).isTrue();
    assertThat(parsed[0].getTuple().getTraceParams().disableStack()).isTrue();

    assertThat(parsed[1].getTuple().getCallParameter().getSender())
        .contains(Address.fromHexString("0x627306090abab3a6e1400e9345bc60c78a8bef57"));
    assertThat(parsed[1].getTuple().getTraceParams()).isNull();
  }

  @Test
  public void tupleWithoutTraceConfigSlotIsAccepted() throws IOException {
    final DebugTraceCallManyParameter[] parsed =
        mapper.readValue(CALL_WITHOUT_TRACE_CONFIG_SLOT_JSON, DebugTraceCallManyParameter[].class);

    assertThat(parsed).hasSize(1);
    assertThat(parsed[0].getTuple().getCallParameter().getTo())
        .contains(Address.fromHexString("0x0010000000000000000000000000000000000000"));
    assertThat(parsed[0].getTuple().getTraceParams()).isNull();
  }

  @Test
  public void malformedCallObjectFailsToParse() {
    assertThatExceptionOfType(MismatchedInputException.class)
        .isThrownBy(() -> mapper.readValue(INVALID_JSON, DebugTraceCallManyParameter[].class));
  }

  @Test
  public void emptyTupleFailsToParse() {
    assertThatExceptionOfType(MismatchedInputException.class)
        .isThrownBy(() -> mapper.readValue("[[]]", DebugTraceCallManyParameter[].class));
  }

  @Test
  public void nullCallParamsSlotFailsToParse() {
    assertThatExceptionOfType(MismatchedInputException.class)
        .isThrownBy(() -> mapper.readValue("[[null]]", DebugTraceCallManyParameter[].class));
  }

  @Test
  public void nullCallParamsWithTraceConfigFailsToParse() {
    assertThatExceptionOfType(MismatchedInputException.class)
        .isThrownBy(
            () -> mapper.readValue("[[null, {}]]", DebugTraceCallManyParameter[].class));
  }
}
