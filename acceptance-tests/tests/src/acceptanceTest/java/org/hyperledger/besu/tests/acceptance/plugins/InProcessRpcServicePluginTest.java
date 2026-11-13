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
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration;
import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.hyperledger.besu.tests.acceptance.dsl.node.configuration.genesis.GenesisConfigurationFactory;

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
  private static final String FUNDED = "0x8da48afC965480220a3dB9244771bd3afcB5d895";
  private static final String TO = "0xa05b21E5186Ce93d2a226722b85D6e550Ac7D6E3";
  private static final String UNFUNDED = "0x1111111111111111111111111111111111111111";

  protected static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
  private OkHttpClient client;

  private BesuNode node;

  @BeforeEach
  public void setUp() throws Exception {
    final JsonRpcConfiguration rpcConfig = JsonRpcConfiguration.createDefault();
    rpcConfig.addRpcApi("TESTS");
    node =
        besu.createPluginsNode(
            "node1",
            List.of("testPlugins"),
            builder ->
                builder
                    .devMode(false)
                    .genesisConfigProvider(
                        ignore ->
                            GenesisConfigurationFactory.createFromResource("/dev/dev_prague.json"))
                    .jsonRpcConfiguration(rpcConfig)
                    .jsonRpcEnabled()
                    .miningEnabled(false)
                    .extraCLIOptions(
                        List.of(
                            "--Xin-process-rpc-enabled=true",
                            "--Xin-process-rpc-apis=MINER,ETH",
                            "--plugin-test-set-min-gas-price=" + MIN_GAS_PRICE)));
    cluster.start(node);
    client = new OkHttpClient();
  }

  private ObjectNode callPlugin(final String method, final List<String> params) throws IOException {
    final String json =
        String.format(
            "{\"jsonrpc\":\"2.0\",\"method\":\"%s\",\"params\":[%s],\"id\":42}",
            method, params.stream().map(p -> "\"" + p + "\"").collect(Collectors.joining(",")));
    final RequestBody body = RequestBody.create(json, JSON);
    final String resultString =
        client
            .newCall(
                new Request.Builder()
                    .post(body)
                    .url("http://" + node.getHostName() + ":" + node.getJsonRpcPort().get() + "/")
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

  private String estimateGas(
      final String from, final String to, final String strict, final String gasPrice)
      throws IOException {
    final var result = callPlugin("tests_estimateGas", List.of(from, to, strict, gasPrice));
    assertThat(result.has("result")).as("response: %s", result).isTrue();
    return result.get("result").asText();
  }

  // 1. funded sender, no fee fields, strict: true -> an estimate
  @Test
  public void strictFundedSenderWithoutFeeFieldsGetsEstimate() throws IOException {
    assertThat(estimateGas(FUNDED, TO, "true", "none")).isEqualTo("SUCCESS 0x5208");
  }

  // 2. sender with no balance, gasPrice 7 (the base fee), strict: true -> -32004
  @Test
  public void strictUnfundedSenderWithGasPriceIsRejected() throws IOException {
    assertThat(estimateGas(UNFUNDED, TO, "true", "7"))
        .isEqualTo("ERROR -32004 Upfront gas cost exceeds account balance");
  }

  // 3. same call as 2 without strict: records today's behavior (strict defaults to true)
  @Test
  public void unfundedSenderWithGasPriceWithoutStrictIsAlsoRejectedToday() throws IOException {
    assertThat(estimateGas(UNFUNDED, TO, "unset", "7"))
        .isEqualTo("ERROR -32004 Upfront gas cost exceeds account balance");
  }
}
