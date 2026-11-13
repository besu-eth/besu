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
package org.hyperledger.besu.ethereum.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StateOverride;
import org.hyperledger.besu.datatypes.StateOverrideMap;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;

import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

public class StateOverrideParameterTest {

  private static final String ADDRESS_HEX1 = "0xd9c9cd5f6779558b6e0ed4e6acf6b1947e7fa1f3";
  private static final String ADDRESS_HEX2 = "0xd5E23607D5d73ff2293152f464C3caB005f87696";
  private static final String STORAGE_KEY =
      "0x1cf7945003fc5b59d2f6736f0704557aa805c4f2844084ccd1173b8d56946962";
  private static final String STORAGE_VALUE =
      "0x000000000000000000000000000000000000000000000000000000110ed03bf7";
  private static final String CODE_STRING =
      "0xdbf4257000000000000000000000000000000000000000000000000000000000";

  @Test
  public void jsonDeserializesCorrectly() throws Exception {
    final String json =
        "{\"jsonrpc\":\"2.0\",\"method\":\"eth_call\",\"params\":[{"
            + "\"from\":\"0x0000000000000000000000000000000000000000\", \"to\": \"0x0000000000000000000000000000000000000000\"}, "
            + "\"latest\","
            + "{\""
            + ADDRESS_HEX1
            + "\":"
            + "{"
            + "\"balance\": \"0x01\","
            + "\"nonce\": \"0x9e\""
            + "}}],\"id\":1}";

    final JsonRpcRequestContext request = new JsonRpcRequestContext(readJsonAsJsonRpcRequest(json));
    final StateOverrideMap stateOverrideParam =
        request.getRequiredParameter(2, StateOverrideMap.class);

    final StateOverride stateOverride = stateOverrideParam.get(Address.fromHexString(ADDRESS_HEX1));

    assertThat(stateOverride.getNonce().get()).isEqualTo(158);
    assertThat(stateOverride.getBalance()).isEqualTo(Optional.of(Wei.of(1)));
    assertFalse(stateOverride.getState().isPresent());
    assertFalse(stateOverride.getStateDiff().isPresent());
  }

  @Test
  public void jsonWithCodeDeserializesCorrectly() throws Exception {
    final String json =
        "{\"jsonrpc\":\"2.0\",\"method\":\"eth_call\",\"params\":[{"
            + "\"from\":\"0x0000000000000000000000000000000000000000\", \"to\": \"0x0000000000000000000000000000000000000000\"}, "
            + "\"latest\","
            + "{\""
            + ADDRESS_HEX1
            + "\":"
            + "{"
            + "\"balance\": \"0x01\","
            + "\"code\": \""
            + CODE_STRING
            + "\""
            + "}}],\"id\":1}";

    final JsonRpcRequestContext request = new JsonRpcRequestContext(readJsonAsJsonRpcRequest(json));
    final StateOverrideMap stateOverrideParam =
        request.getRequiredParameter(2, StateOverrideMap.class);

    final StateOverride stateOverride = stateOverrideParam.get(Address.fromHexString(ADDRESS_HEX1));

    assertFalse(stateOverride.getNonce().isPresent());
    assertThat(stateOverride.getBalance()).isEqualTo(Optional.of(Wei.of(1)));
    assertThat(stateOverride.getCode()).isEqualTo(Optional.of(CODE_STRING));
    assertFalse(stateOverride.getState().isPresent());
    assertFalse(stateOverride.getStateDiff().isPresent());
  }

  @Test
  public void jsonWithHexNonceDeserializesCorrectly() throws Exception {
    final String json =
        "{\"jsonrpc\":\"2.0\",\"method\":\"eth_call\",\"params\":[{"
            + "\"from\":\"0x0000000000000000000000000000000000000000\", \"to\": \"0x0000000000000000000000000000000000000000\"}, "
            + "\"latest\","
            + "{\""
            + ADDRESS_HEX1
            + "\":"
            + "{"
            + "\"balance\": \"0x01\","
            + "\"nonce\": \""
            + "0x9e"
            + "\""
            + "}}],\"id\":1}";

    final JsonRpcRequestContext request = new JsonRpcRequestContext(readJsonAsJsonRpcRequest(json));
    final StateOverrideMap stateOverrideParam =
        request.getRequiredParameter(2, StateOverrideMap.class);

    final StateOverride stateOverride = stateOverrideParam.get(Address.fromHexString(ADDRESS_HEX1));

    assertThat(stateOverride.getBalance()).isEqualTo(Optional.of(Wei.of(1)));
    assertThat(stateOverride.getNonce().get()).isEqualTo(158); // 0x9e
    assertFalse(stateOverride.getState().isPresent());
    assertFalse(stateOverride.getStateDiff().isPresent());
  }

  @Test
  public void jsonWithStorageOverridesDeserializesCorrectly() throws Exception {
    final String json =
        "{\"jsonrpc\":\"2.0\",\"method\":\"eth_call\",\"params\":[{"
            + "\"from\":\"0x0000000000000000000000000000000000000000\", \"to\": \"0x0000000000000000000000000000000000000000\"}, "
            + "\"latest\","
            + "{\""
            + ADDRESS_HEX1
            + "\":"
            + "{"
            + "\"balance\": \"0x01\","
            + "\"nonce\": \"0x9E\","
            + "\"state\": {"
            + "\""
            + STORAGE_KEY
            + "\": \""
            + STORAGE_VALUE
            + "\""
            + "}}}],\"id\":1}";

    final JsonRpcRequestContext request = new JsonRpcRequestContext(readJsonAsJsonRpcRequest(json));

    final StateOverrideMap stateOverrideParam =
        request.getRequiredParameter(2, StateOverrideMap.class);
    assertThat(stateOverrideParam.size()).isEqualTo(1);

    final StateOverride stateOverride = stateOverrideParam.get(Address.fromHexString(ADDRESS_HEX1));
    assertThat(stateOverride.getNonce().get()).isEqualTo(158);

    assertTrue(stateOverride.getState().isPresent());
    assertThat(stateOverride.getState().get().get(STORAGE_KEY)).isEqualTo(STORAGE_VALUE);
    assertFalse(stateOverride.getStateDiff().isPresent());
  }

  @Test
  public void jsonWithMultipleStateOverridesDeserializesCorrectly() throws Exception {
    final String json =
        "{\"jsonrpc\":\"2.0\",\"method\":\"eth_call\",\"params\":[{"
            + "\"from\":\"0x0000000000000000000000000000000000000000\", \"to\": \"0x0000000000000000000000000000000000000000\"}, "
            + "\"latest\","
            + "{\""
            + ADDRESS_HEX1
            + "\":"
            + "{"
            + "\"balance\": \"0x01\","
            + "\"nonce\": \"0x9E\","
            + "\"state\": {"
            + "\""
            + STORAGE_KEY
            + "\": \""
            + STORAGE_VALUE
            + "\""
            + "}},"
            + "\""
            + ADDRESS_HEX2
            + "\":"
            + "{"
            + "\"balance\": \"0xFF\","
            + "\"nonce\": \"0x9D\","
            + "\"state\": {"
            + "\""
            + STORAGE_KEY
            + "\": \""
            + STORAGE_VALUE
            + "\""
            + "}}}],\"id\":1}";

    final JsonRpcRequestContext request = new JsonRpcRequestContext(readJsonAsJsonRpcRequest(json));

    final StateOverrideMap stateOverrideParam =
        request.getRequiredParameter(2, StateOverrideMap.class);
    assertThat(stateOverrideParam.size()).isEqualTo(2);

    final StateOverride stateOverride1 =
        stateOverrideParam.get(Address.fromHexString(ADDRESS_HEX1));
    assertThat(stateOverride1.getNonce().get()).isEqualTo(158);
    assertThat(stateOverride1.getBalance()).isEqualTo(Optional.of(Wei.fromHexString("0x01")));
    assertTrue(stateOverride1.getState().isPresent());
    assertThat(stateOverride1.getState().get().get(STORAGE_KEY)).isEqualTo(STORAGE_VALUE);
    assertFalse(stateOverride1.getStateDiff().isPresent());

    final StateOverride stateOverride2 =
        stateOverrideParam.get(Address.fromHexString(ADDRESS_HEX2));
    assertThat(stateOverride2.getNonce().get()).isEqualTo(157);
    assertThat(stateOverride2.getBalance()).isEqualTo(Optional.of(Wei.fromHexString("0xFF")));
    assertTrue(stateOverride2.getState().isPresent());
    assertThat(stateOverride2.getState().get().get(STORAGE_KEY)).isEqualTo(STORAGE_VALUE);
    assertFalse(stateOverride2.getStateDiff().isPresent());
  }

  private JsonRpcRequest readJsonAsJsonRpcRequest(final String json) throws java.io.IOException {
    return new ObjectMapper().readValue(json, JsonRpcRequest.class);
  }

  @Test
  public void shouldThrowExceptionWhenStateAndStateDiffAreBothPresent() {
    Exception exception =
        assertThrows(
            IllegalStateException.class,
            () ->
                new StateOverride.Builder()
                    .withState(Map.of("0x1234", "0x5678"))
                    .withStateDiff(Map.of("0x1234", "0x5678"))
                    .build());

    final String expectedMessage = "Cannot set both state and stateDiff";
    assertThat(exception.getMessage()).isEqualTo(expectedMessage);
  }
}
