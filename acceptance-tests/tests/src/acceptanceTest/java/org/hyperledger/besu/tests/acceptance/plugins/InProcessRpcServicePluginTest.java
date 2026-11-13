/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.tests.acceptance.plugins;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.config.JsonUtil;
import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.node.ObjectNode;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class InProcessRpcServicePluginTest extends AcceptanceTestBase {
  private static final long MIN_GAS_PRICE = 123456;
  private static final String FUNDED = "0xfe3b557e8fb62b89f4916b721be55ceb828dbd73";
  private static final String TO = "0x627306090abaB3A6e1400e9345bC60c78a8BEf57";

  protected static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
  private OkHttpClient client;

  private BesuNode node;

  @BeforeEach
  public void setUp() throws Exception {
    node =
        besu.createPluginsNode(
            "node1",
            List.of("testPlugins"),
            List.of(
                "--Xin-process-rpc-enabled=true",
                "--Xin-process-rpc-apis=MINER,ETH",
                "--plugin-test-set-min-gas-price=" + MIN_GAS_PRICE),
            "MINER", "TESTS");
    cluster.start(node);
    client = new OkHttpClient();
  }

  private ObjectNode callPlugin(final String method, final List<String> params) throws IOException {
    final String json =
        String.format(
            "{\"jsonrpc\":\"2.0\",\"method\":\"%s\",\"params\":[%s],\"id\":42}",
            method,
            params.stream().map(p -> "\"" + p + "\"").collect(Collectors.joining(",")));
    final RequestBody body = RequestBody.create(json, JSON);
    final String resultString =
        client
            .newCall(
                new Request.Builder()
                    .post(body)
                    .url(
                        "http://"
                            + node.getHostName()
                            + ":"
                            + node.getJsonRpcPort().get()
                            + "/")
                    .build())
            .execute()
            .body()
            .string();
    return JsonUtil.objectNodeFromString(resultString);
  }

  @Test
  public void smokeTest() {
    final var currMinGasPrice = node.execute(minerTransactions.minerGetMinGasPrice());
    assertThat(currMinGasPrice).isEqualTo(BigInteger.valueOf(MIN_GAS_PRICE));
  }

  @Test
  public void estimateGasStrictFundedSender() throws IOException {
    final var result = callPlugin("tests_estimateGas", List.of(FUNDED, TO, "true", "false"));
    assertThat(result.get("result").asText()).startsWith("SUCCESS");
  }

  @Test
  public void estimateGasStrictZeroBalanceFails() throws IOException {
    final var result = callPlugin("tests_estimateGas", List.of(FUNDED, TO, "true", "true"));
    assertThat(result.get("result").asText()).contains("-32004");
  }

  @Test
  public void estimateGasWithoutStrictCurrentBehavior() throws IOException {
    final var result = callPlugin("tests_estimateGas", List.of(FUNDED, TO, "false", "false"));
    System.out.println("no-strict result today: " + result.get("result").asText());
  }
}
