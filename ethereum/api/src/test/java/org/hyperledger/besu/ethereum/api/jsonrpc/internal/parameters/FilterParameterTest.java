/*
 * Copyright ConsenSys AG.
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

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.LogTopic;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcObjectMapperFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

public class FilterParameterTest {

  // Use the production parameter mapper (registers BesuJsonModule) since FilterParameter binds a
  // bare Hash (blockHash), whose @JsonCreator needs the Bytes32 deserializer a plain mapper lacks.
  private final ObjectMapper mapper = JsonRpcObjectMapperFactory.getParameterMapper();
  private static final String TOPIC_FORMAT = "0x%064d";
  private static final String TOPIC_TWO = String.format(TOPIC_FORMAT, 2);
  private static final String TOPIC_THREE = String.format(TOPIC_FORMAT, 3);
  private static final String TOPIC_FOUR = String.format(TOPIC_FORMAT, 4);
  private static final String TOPIC_FIVE = String.format(TOPIC_FORMAT, 5);

  private static final String TOPICS_TWO_THREE_ARRAY =
      "[\"" + TOPIC_TWO + "\", \"" + TOPIC_THREE + "\"]";
  private static final String TOPICS_FOUR_FIVE_ARRAY =
      "[\"" + TOPIC_FOUR + "\", \"" + TOPIC_FIVE + "\"]";

  @Test
  public void jsonWithArrayOfAddressesShouldSerializeSuccessfully() throws Exception {
    final String jsonWithAddressArray =
        "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getLogs\",\"params\":[{\"address\":[\"0x0\",\"0x1\"]}],\"id\":1}";
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(readJsonAsJsonRpcRequest(jsonWithAddressArray));
    final FilterParameter expectedFilterParameter = filterParameterWithAddresses("0x0", "0x1");

    final FilterParameter parsedFilterParameter =
        request.getRequiredParameter(0, FilterParameter.class);

    assertThat(parsedFilterParameter).usingRecursiveComparison().isEqualTo(expectedFilterParameter);
  }

  @Test
  public void jsonWithSingleAddressShouldSerializeSuccessfully() throws Exception {
    final String jsonWithSingleAddress =
        "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getLogs\",\"params\":[{\"address\":\"0x0\"}],\"id\":1}";
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(readJsonAsJsonRpcRequest(jsonWithSingleAddress));
    final FilterParameter expectedFilterParameter = filterParameterWithAddresses("0x0");

    final FilterParameter parsedFilterParameter =
        request.getRequiredParameter(0, FilterParameter.class);

    assertThat(parsedFilterParameter).usingRecursiveComparison().isEqualTo(expectedFilterParameter);
  }

  @Test
  public void jsonWithSingleAddressAndSingleTopicShouldSerializeSuccessfully() throws Exception {
    final String jsonWithSingleAddress =
        "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getLogs\",\"params\":[{\"address\":\"0x0\", "
            + "\"topics\":\""
            + TOPIC_TWO
            + "\" }],\"id\":1}";

    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(readJsonAsJsonRpcRequest(jsonWithSingleAddress));
    final FilterParameter expectedFilterParameter =
        filterParameterWithSingleListOfTopics(TOPIC_TWO);

    final FilterParameter parsedFilterParameter =
        request.getRequiredParameter(0, FilterParameter.class);

    assertThat(parsedFilterParameter).usingRecursiveComparison().isEqualTo(expectedFilterParameter);
  }

  @Test
  public void jsonWithSingleAddressAndMultipleTopicsShouldSerializeSuccessfully() throws Exception {
    final String jsonWithSingleAddress =
        "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getLogs\",\"params\":[{\"address\":\"0x0\", "
            + "\"topics\":["
            + TOPICS_TWO_THREE_ARRAY
            + "]}],\"id\":1}";

    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(readJsonAsJsonRpcRequest(jsonWithSingleAddress));
    final FilterParameter expectedFilterParameter =
        filterParameterWithSingleListOfTopics(TOPIC_TWO, TOPIC_THREE);

    final FilterParameter parsedFilterParameter =
        request.getRequiredParameter(0, FilterParameter.class);

    assertThat(parsedFilterParameter).usingRecursiveComparison().isEqualTo(expectedFilterParameter);
  }

  @Test
  public void jsonWithSingleAddressAndMultipleListsOfTopicsShouldSerializeSuccessfully()
      throws Exception {
    final String jsonWithSingleAddress =
        "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getLogs\",\"params\":[{\"address\":\"0x0\", "
            + "\"topics\":["
            + TOPICS_TWO_THREE_ARRAY
            + ","
            + TOPICS_TWO_THREE_ARRAY
            + "]}],\"id\":1}";

    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(readJsonAsJsonRpcRequest(jsonWithSingleAddress));
    final FilterParameter expectedFilterParameter =
        filterParameterWithListOfTopics(TOPIC_TWO, TOPIC_THREE);

    final FilterParameter parsedFilterParameter =
        request.getRequiredParameter(0, FilterParameter.class);

    assertThat(parsedFilterParameter).usingRecursiveComparison().isEqualTo(expectedFilterParameter);
  }

  @Test
  public void jsonWithFromAndToParametersDeserializesCorrectly() throws Exception {
    final String json =
        "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getLogs\",\"params\":[{"
            + "\"address\":\"0x0\", \"fromBlock\": \"0x0\", \"toBlock\": \"pending\","
            + "\"topics\":["
            + TOPICS_TWO_THREE_ARRAY
            + ","
            + TOPICS_TWO_THREE_ARRAY
            + "]}],\"id\":1}";

    final JsonRpcRequestContext request = new JsonRpcRequestContext(readJsonAsJsonRpcRequest(json));
    final FilterParameter parsedFilterParameter =
        request.getRequiredParameter(0, FilterParameter.class);

    assertThat(parsedFilterParameter.getFromBlock()).isEqualTo(new BlockParameter(0));
    assertThat(parsedFilterParameter.getToBlock()).isEqualTo(BlockParameter.PENDING);
  }

  @Test
  public void jsonWithBlockHashIncludingAliasAndFromAndToParametersIsInvalid() throws Exception {
    final String json =
        "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getLogs\",\"params\":[{"
            + "\"address\":\"0x0\", \"fromBlock\": \"0x0\", \"toBlock\": \"pending\","
            + "\"topics\":["
            + TOPICS_TWO_THREE_ARRAY
            + ","
            + TOPICS_TWO_THREE_ARRAY
            + "], \"blockHash\": \""
            + Hash.ZERO
            + "\"}],\"id\":1}";

    final String jsonUsingAlias =
        "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getLogs\",\"params\":[{"
            + "\"address\":\"0x0\", \"fromBlock\": \"0x0\", \"toBlock\": \"pending\","
            + "\"topics\":["
            + TOPICS_TWO_THREE_ARRAY
            + ","
            + TOPICS_TWO_THREE_ARRAY
            + "], \"blockhash\": \""
            + Hash.ZERO
            + "\"}],\"id\":1}";

    final JsonRpcRequestContext request = new JsonRpcRequestContext(readJsonAsJsonRpcRequest(json));
    final FilterParameter parsedFilterParameter =
        request.getRequiredParameter(0, FilterParameter.class);

    final JsonRpcRequestContext requestUsingAlias =
        new JsonRpcRequestContext(readJsonAsJsonRpcRequest(jsonUsingAlias));
    final FilterParameter parsedFilterParameterUsingAlias =
        requestUsingAlias.getRequiredParameter(0, FilterParameter.class);

    assertThat(parsedFilterParameterUsingAlias.isValid()).isFalse();
    assertThat(parsedFilterParameter)
        .usingRecursiveComparison()
        .isEqualTo(parsedFilterParameterUsingAlias);
  }

  @Test
  public void jsonBlockHashAliasSucceeds() throws Exception {
    final String json =
        "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getLogs\",\"params\":[{"
            + "\"address\":\"0x0\","
            + "\"topics\":["
            + TOPICS_TWO_THREE_ARRAY
            + ","
            + TOPICS_TWO_THREE_ARRAY
            + "], \"blockHash\": \""
            + Hash.ZERO
            + "\"}],\"id\":1}";

    final String jsonUsingAlias =
        "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getLogs\",\"params\":[{"
            + "\"address\":\"0x0\","
            + "\"topics\":["
            + TOPICS_TWO_THREE_ARRAY
            + ","
            + TOPICS_TWO_THREE_ARRAY
            + "], \"blockhash\": \""
            + Hash.ZERO
            + "\"}],\"id\":1}";

    final JsonRpcRequestContext request = new JsonRpcRequestContext(readJsonAsJsonRpcRequest(json));
    final FilterParameter parsedFilterParameter =
        request.getRequiredParameter(0, FilterParameter.class);

    final JsonRpcRequestContext requestUsingAlias =
        new JsonRpcRequestContext(readJsonAsJsonRpcRequest(jsonUsingAlias));
    final FilterParameter parsedFilterParameterUsingAlias =
        requestUsingAlias.getRequiredParameter(0, FilterParameter.class);

    assertThat(parsedFilterParameterUsingAlias.isValid()).isTrue();
    assertThat(parsedFilterParameterUsingAlias.getBlockHash()).isEqualTo(Optional.of(Hash.ZERO));

    // blockhash and blockHash should end up the same
    assertThat(parsedFilterParameter)
        .usingRecursiveComparison()
        .isEqualTo(parsedFilterParameterUsingAlias);
  }

  @Test
  public void jsonWithParamsInDifferentOrderShouldDeserializeIntoFilterParameterSuccessfully()
      throws Exception {
    final String jsonWithTopicsFirst =
        "{\"topics\":[\"0x492e34c7da2a87c57444aa0f6143558999bceec63065f04557cfb20932e0d591\"], \"address\":\"0x8CdaF0CD259887258Bc13a92C0a6dA92698644C0\",\"fromBlock\":\"earliest\",\"toBlock\":\"latest\"}";
    final String jsonWithTopicsLast =
        "{\"address\":\"0x8CdaF0CD259887258Bc13a92C0a6dA92698644C0\",\"fromBlock\":\"earliest\",\"toBlock\":\"latest\",\"topics\":[\"0x492e34c7da2a87c57444aa0f6143558999bceec63065f04557cfb20932e0d591\"]}";

    assertThat(readJsonAsFilterParameter(jsonWithTopicsFirst))
        .usingRecursiveComparison()
        .isEqualTo(readJsonAsFilterParameter(jsonWithTopicsLast));
  }

  @Test
  public void whenTopicsParameterIsArrayOfStringsFilterContainsListOfSingletonLists()
      throws JsonProcessingException {
    final List<String> topics =
        Lists.newArrayList(
            "0xce8688f853ffa65c042b72302433c25d7a230c322caba0901587534b6551091d",
            null,
            "0x000000000000000000000000244a53ab66ea8901c25efc48c8ab84662643cc74");
    final FilterParameter filter = createFilterWithTopics(topics);

    assertThat(filter.getTopics())
        .containsExactly(
            singletonList(
                LogTopic.fromHexString(
                    "0xce8688f853ffa65c042b72302433c25d7a230c322caba0901587534b6551091d")),
            singletonList(null),
            singletonList(
                LogTopic.fromHexString(
                    "0x000000000000000000000000244a53ab66ea8901c25efc48c8ab84662643cc74")));
  }

  @Test
  public void whenTopicsParameterContainsArraysFilterContainsListOfSingletonLists()
      throws JsonProcessingException {
    final List<List<String>> topics =
        Lists.newArrayList(
            singletonList("0xce8688f853ffa65c042b72302433c25d7a230c322caba0901587534b6551091d"),
            null,
            singletonList("0x000000000000000000000000244a53ab66ea8901c25efc48c8ab84662643cc74"));

    final FilterParameter filter = createFilterWithTopics(topics);

    assertThat(filter.getTopics())
        .containsExactly(
            singletonList(
                LogTopic.fromHexString(
                    "0xce8688f853ffa65c042b72302433c25d7a230c322caba0901587534b6551091d")),
            singletonList(null),
            singletonList(
                LogTopic.fromHexString(
                    "0x000000000000000000000000244a53ab66ea8901c25efc48c8ab84662643cc74")));
  }

  @Test
  public void whenTopicArrayContainsNullFilterContainsSingletonListOfAllTopics()
      throws JsonProcessingException {
    final List<List<String>> topics =
        Lists.newArrayList(
            singletonList("0xce8688f853ffa65c042b72302433c25d7a230c322caba0901587534b6551091d"),
            null,
            singletonList("0x000000000000000000000000244a53ab66ea8901c25efc48c8ab84662643cc74"));
    final FilterParameter filter = createFilterWithTopics(topics);

    assertThat(filter.getTopics())
        .containsExactly(
            singletonList(
                LogTopic.fromHexString(
                    "0xce8688f853ffa65c042b72302433c25d7a230c322caba0901587534b6551091d")),
            singletonList(null),
            singletonList(
                LogTopic.fromHexString(
                    "0x000000000000000000000000244a53ab66ea8901c25efc48c8ab84662643cc74")));
  }

  @Test
  public void emptyListDecodesCorrectly() throws JsonProcessingException {
    final List<String> topics = emptyList();
    final FilterParameter filter = createFilterWithTopics(topics);

    assertThat(filter.getTopics().size()).isZero();
  }

  @Test
  public void emptyListAsSubTopicDecodesCorrectly() throws JsonProcessingException {
    final List<List<String>> topics =
        Lists.newArrayList(
            singletonList("0xce8688f853ffa65c042b72302433c25d7a230c322caba0901587534b6551091d"),
            emptyList());
    final FilterParameter filter = createFilterWithTopics(topics);
    assertThat(filter.getTopics())
        .containsExactly(
            singletonList(
                LogTopic.fromHexString(
                    "0xce8688f853ffa65c042b72302433c25d7a230c322caba0901587534b6551091d")),
            emptyList());
  }

  @Test
  public void emptyListOfTopicsInJsonShouldDeserializeAsEmptyList() throws java.io.IOException {
    final FilterParameter filterParameter = readJsonAsFilterParameter("{\"topics\":[]}");

    assertThat(filterParameter.getTopics().size()).isEqualTo(0);
  }

  @Test
  public void nestedEmptyListOfTopicsShouldDeserializeAsEmptyList() throws java.io.IOException {
    final FilterParameter filterParameter = readJsonAsFilterParameter("{\"topics\":[[]]}");

    assertThat(filterParameter.getTopics()).containsExactly(emptyList());
  }

  @Test
  public void singleTopicShouldDeserializeAsSingleTopic() throws java.io.IOException {
    final FilterParameter filterParameter =
        readJsonAsFilterParameter("{\"topics\":[\"" + TOPIC_TWO + "\"]}");

    assertThat(filterParameter.getTopics())
        .containsExactly(List.of(LogTopic.fromHexString(TOPIC_TWO)));
  }

  @Test
  public void nestedSingleTopicShouldDeserializeAsSingleTopic() throws java.io.IOException {
    final FilterParameter filterParameter =
        readJsonAsFilterParameter("{\"topics\":[[\"" + TOPIC_TWO + "\"]]}");

    assertThat(filterParameter.getTopics())
        .containsExactly(List.of(LogTopic.fromHexString(TOPIC_TWO)));
  }

  @Test
  public void twoTopicsShouldDeserializeAsTwoTopicLists() throws java.io.IOException {
    final FilterParameter filterParameter =
        readJsonAsFilterParameter("{\"topics\":" + TOPICS_TWO_THREE_ARRAY + "}");

    assertThat(filterParameter.getTopics())
        .containsExactly(
            List.of(LogTopic.fromHexString(TOPIC_TWO)),
            List.of(LogTopic.fromHexString(TOPIC_THREE)));
  }

  @Test
  public void twoTopicsInFirstPositionShouldDeserializeAsOneTopicList() throws java.io.IOException {
    final FilterParameter filterParameter =
        readJsonAsFilterParameter("{\"topics\":[" + TOPICS_TWO_THREE_ARRAY + "]}");

    assertThat(filterParameter.getTopics())
        .containsExactly(
            List.of(LogTopic.fromHexString(TOPIC_TWO), LogTopic.fromHexString(TOPIC_THREE)));
  }

  @Test
  public void nullInFirstPositionShouldDeserializeAsEmptyFirstTopic() throws java.io.IOException {
    final FilterParameter filterParameter =
        readJsonAsFilterParameter("{\"topics\":[null, \"" + TOPIC_THREE + "\"]}");

    assertThat(filterParameter.getTopics())
        .containsExactly(singletonList(null), List.of(LogTopic.fromHexString(TOPIC_THREE)));
  }

  @Test
  public void twoTopicsInFirstAndSecondPositionShouldDeserializeAsTwoListsOfTopics()
      throws java.io.IOException {
    final FilterParameter filterParameter =
        readJsonAsFilterParameter(
            "{\"topics\":[" + TOPICS_TWO_THREE_ARRAY + "," + TOPICS_FOUR_FIVE_ARRAY + "]}");

    assertThat(filterParameter.getTopics())
        .containsExactly(
            List.of(LogTopic.fromHexString(TOPIC_TWO), LogTopic.fromHexString(TOPIC_THREE)),
            List.of(LogTopic.fromHexString(TOPIC_FOUR), LogTopic.fromHexString(TOPIC_FIVE)));
  }

  @Test
  public void fromAndToAddressesShouldDeserializeAsTwoListsOfAddresses()
      throws java.io.IOException {
    final FilterParameter filterParameter =
        readJsonAsFilterParameter("{\"fromAddress\":[\"0x0\"], \"toAddress\":[\"0x1\"]}");
    assertThat(filterParameter.getFromAddress()).containsExactly(Address.fromHexString("0x0"));
    assertThat(filterParameter.getToAddress()).containsExactly(Address.fromHexString("0x1"));
  }

  @Test
  public void countAndAfterShouldDeserializeCorrectly() throws java.io.IOException {
    final FilterParameter filterParameter = readJsonAsFilterParameter("{\"after\":1, \"count\":2}");
    assertThat(filterParameter.getAfter()).contains(1);
    assertThat(filterParameter.getCount()).contains(2);
  }

  private <T> FilterParameter createFilterWithTopics(final T inputTopics)
      throws JsonProcessingException {
    final Map<String, T> payload = new HashMap<>();
    payload.put("topics", inputTopics);

    final String json = new ObjectMapper().writeValueAsString(payload);
    return mapper.readValue(json, FilterParameter.class);
  }

  private FilterParameter filterParameterWithAddresses(final String... addresses) {
    return new FilterParameter(
        BlockParameter.LATEST,
        BlockParameter.LATEST,
        null,
        null,
        Arrays.stream(addresses).map(Address::fromHexString).collect(toUnmodifiableList()),
        null,
        null,
        null,
        null);
  }

  private FilterParameter filterParameterWithSingleListOfTopics(final String... topics) {
    return new FilterParameter(
        BlockParameter.LATEST,
        BlockParameter.LATEST,
        null,
        null,
        singletonList(Address.fromHexString("0x0")),
        singletonList(
            Arrays.stream(topics).map(LogTopic::fromHexString).collect(toUnmodifiableList())),
        null,
        null,
        null);
  }

  private FilterParameter filterParameterWithListOfTopics(final String... topics) {
    List<LogTopic> topicsList =
        Arrays.stream(topics).map(LogTopic::fromHexString).collect(toUnmodifiableList());
    List<List<LogTopic>> topicsListList = Arrays.asList(topicsList, topicsList);
    return new FilterParameter(
        BlockParameter.LATEST,
        BlockParameter.LATEST,
        null,
        null,
        singletonList(Address.fromHexString("0x0")),
        topicsListList,
        null,
        null,
        null);
  }

  private JsonRpcRequest readJsonAsJsonRpcRequest(final String jsonWithSingleAddress)
      throws java.io.IOException {
    return new ObjectMapper().readValue(jsonWithSingleAddress, JsonRpcRequest.class);
  }

  private FilterParameter readJsonAsFilterParameter(final String json) throws java.io.IOException {
    return mapper.readValue(json, FilterParameter.class);
  }
}
