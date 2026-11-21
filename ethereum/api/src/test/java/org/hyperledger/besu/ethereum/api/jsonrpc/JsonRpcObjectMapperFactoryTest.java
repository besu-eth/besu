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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StateOverrideMap;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.SimulateV1Parameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.transaction.CallParameter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class JsonRpcObjectMapperFactoryTest {

  private static final String ADDRESS = "0x6295eE1B4F6dD65047762F924Ecd367c17eaBf8f";

  static Stream<ObjectMapper> parameterMappers() {
    return Stream.of(
        JsonRpcObjectMapperFactory.getParameterMapper(),
        JsonRpcObjectMapperFactory.getParameterMapperIgnoringUnknownNulls(),
        JsonRpcObjectMapperFactory.getParameterMapperIgnoringUnknownEmpties());
  }

  static Stream<Arguments> parameterMappersAndMalformedAddresses() {
    return parameterMappers()
        .flatMap(
            mapper ->
                Stream.of(
                        "",
                        "0x",
                        "0x0",
                        "0x123",
                        "0x1234",
                        "0x" + "1".repeat(36),
                        "0x" + "1".repeat(37),
                        "0x" + "1".repeat(39),
                        "0x" + "1".repeat(41),
                        "0x" + "1".repeat(42),
                        "0x" + "g".repeat(40),
                        "1".repeat(40),
                        "x" + "1".repeat(40),
                        "0" + "1".repeat(40),
                        " " + ADDRESS,
                        ADDRESS + " ",
                        1,
                        true,
                        List.of(ADDRESS))
                    .map(value -> Arguments.of(mapper, value)));
  }

  @ParameterizedTest
  @MethodSource("parameterMappers")
  void parameterMappersShouldAcceptFullAddresses(final ObjectMapper mapper) {
    assertThat(mapper.convertValue(ADDRESS, Address.class))
        .isEqualTo(Address.fromHexString(ADDRESS));
    assertThat(mapper.convertValue("0x" + "0".repeat(40), Address.class)).isEqualTo(Address.ZERO);
    assertThat(mapper.convertValue("0X" + ADDRESS.substring(2), Address.class))
        .isEqualTo(Address.fromHexString(ADDRESS));

    final CallParameter callParameter =
        mapper.convertValue(Map.of("from", ADDRESS, "to", ADDRESS), CallParameter.class);
    assertThat(callParameter.getSender()).contains(Address.fromHexString(ADDRESS));
    assertThat(callParameter.getTo()).contains(Address.fromHexString(ADDRESS));
  }

  @ParameterizedTest
  @MethodSource("parameterMappersAndMalformedAddresses")
  void parameterMappersShouldRejectMalformedAddresses(
      final ObjectMapper mapper, final Object address) {
    assertThatThrownBy(() -> mapper.convertValue(address, Address.class))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> mapper.convertValue(Map.of("from", address), CallParameter.class))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> mapper.convertValue(Map.of("to", address), CallParameter.class))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @MethodSource("parameterMappers")
  void parameterMappersShouldAcceptFullStateOverrideAccountKeys(final ObjectMapper mapper) {
    final Map<String, Object> override = Map.of("balance", "0x1");

    final StateOverrideMap lowerCasePrefix =
        mapper.convertValue(Map.of(ADDRESS, override), StateOverrideMap.class);
    final StateOverrideMap upperCasePrefix =
        mapper.convertValue(Map.of("0X" + ADDRESS.substring(2), override), StateOverrideMap.class);

    assertThat(lowerCasePrefix).containsOnlyKeys(Address.fromHexString(ADDRESS));
    assertThat(upperCasePrefix).containsOnlyKeys(Address.fromHexString(ADDRESS));
  }

  @ParameterizedTest
  @MethodSource("parameterMappersAndMalformedAddresses")
  void parameterMappersShouldRejectMalformedStateOverrideAccountKeys(
      final ObjectMapper mapper, final Object address) {
    // only strings can be JSON object keys
    if (!(address instanceof String key)) {
      return;
    }
    final Map<String, Object> overrides = Map.of(key, Map.of("balance", "0x1"));

    assertThatThrownBy(() -> mapper.convertValue(overrides, StateOverrideMap.class))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                mapper.convertValue(
                    Map.of("blockStateCalls", List.of(Map.of("stateOverrides", overrides))),
                    SimulateV1Parameter.class))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void parameterMapperShouldRejectShortStateOverrideAccountKey() {
    assertThatThrownBy(
            () ->
                JsonRpcObjectMapperFactory.getParameterMapper()
                    .readValue("{\"0x1\": {\"balance\": \"0x1\"}}", StateOverrideMap.class))
        .isInstanceOf(InvalidFormatException.class);
  }

  @ParameterizedTest
  @MethodSource("parameterMappers")
  void parameterMappersShouldKeepMissingAndNullCallAddressesEmpty(final ObjectMapper mapper) {
    final Map<String, Object> nullAddresses = new HashMap<>();
    nullAddresses.put("from", null);
    nullAddresses.put("to", null);

    for (final Map<String, Object> json : List.of(Map.<String, Object>of(), nullAddresses)) {
      final CallParameter callParameter = mapper.convertValue(json, CallParameter.class);
      assertThat(callParameter.getSender()).isEmpty();
      assertThat(callParameter.getTo()).isEmpty();
    }
  }

  @Test
  void baseAndResponseMappersShouldKeepLenientAddressParsing() throws Exception {
    final Address shortAddress = Address.fromHexString("0x1");

    assertThat(JsonRpcObjectMapperFactory.getBaseMapper().convertValue("0x1", Address.class))
        .isEqualTo(shortAddress);
    assertThat(JsonRpcObjectMapperFactory.getResponseMapper().convertValue("0x1", Address.class))
        .isEqualTo(shortAddress);
    assertThat(JsonRpcObjectMapperFactory.getParameterMapper().writeValueAsString(shortAddress))
        .isEqualTo(JsonRpcObjectMapperFactory.getBaseMapper().writeValueAsString(shortAddress));
  }

  @Test
  void shouldUseBesuJsonModuleWhenSerializingResponses() throws Exception {
    final Hash hash = Hash.ZERO;
    final String json =
        JsonRpcObjectMapperFactory.getResponseMapper()
            .writeValueAsString(new JsonRpcSuccessResponse(1, hash));

    assertThat(json).isEqualTo("{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"" + hash + "\"}");
  }

  @Test
  void shouldUseBesuJsonModuleWhenDeserializingParameters() throws Exception {
    final String json = "{\"hash\":\"" + Hash.ZERO + "\"}";

    final HashParameter parameter =
        JsonRpcObjectMapperFactory.getParameterMapper().readValue(json, HashParameter.class);

    assertThat(parameter.hash()).isEqualTo(Hash.ZERO);
  }

  private record HashParameter(Hash hash) {
    @JsonCreator
    private HashParameter(@JsonProperty("hash") final Hash hash) {
      this.hash = hash;
    }
  }
}
