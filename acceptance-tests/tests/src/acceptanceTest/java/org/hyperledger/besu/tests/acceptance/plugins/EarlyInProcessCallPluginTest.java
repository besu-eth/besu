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
package org.hyperledger.besu.tests.acceptance.plugins;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A plugin's RPC methods are refused while the plugin is not running: before its start() has
 * returned and once its stop() has returned.
 */
public class EarlyInProcessCallPluginTest extends AcceptanceTestBase {
  private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private BesuNode node;
  private OkHttpClient client;

  @BeforeEach
  public void setUp() throws Exception {
    node =
        besu.createPluginsNode(
            "node1",
            List.of("testPlugins"),
            List.of(
                "--Xin-process-rpc-enabled=true", "--plugin-early-in-process-call-enabled=true"),
            "TESTS");
    cluster.start(node);
    client = new OkHttpClient();
  }

  @Test
  public void aCallFromAnEarlierPluginsStartIsRefusedUntilTheTargetHasStarted() throws IOException {
    // the RPC port opens before afterMainLoop() runs, so wait until that call has been recorded
    await()
        .atMost(30, SECONDS)
        .untilAsserted(() -> assertThat(outcomes().get(1).asText()).isNotEqualTo("not called"));
    final JsonNode outcomes = outcomes();

    assertThat(outcomes.get(0).asText()).startsWith("ERROR").contains("Plugin is not running");
    assertThat(outcomes.get(1).asText()).isEqualTo("SUCCESS: ok");
    assertThat(call("tests_earlyTargetInvokedBeforeStart").get("result").asBoolean()).isFalse();
  }

  @Test
  public void aCallFromALaterPluginsStopReachesAnEarlierPluginButNotAStoppedOne()
      throws IOException {
    cluster.stopNode(node);

    waitForFile(signal("earlyInProcessTarget.stop"));
    waitForFile(signal("earlyInProcessCall.stop"));
    // the target plugin loads second, so it stops first, while the calling plugin is still running
    assertThat(Files.readAllLines(signal("earlyInProcessTarget.stop")).get(0))
        .startsWith("SUCCESS");
    // the calling plugin stops last, when the target has already stopped
    assertThat(Files.readAllLines(signal("earlyInProcessCall.stop")).get(0))
        .startsWith("ERROR")
        .contains("Plugin is not running");
  }

  private JsonNode outcomes() throws IOException {
    return call("tests_earlyCallOutcome").get("result");
  }

  private Path signal(final String name) {
    return node.homeDirectory().resolve("plugins").resolve(name);
  }

  private JsonNode call(final String method) throws IOException {
    final String request =
        String.format("{\"jsonrpc\":\"2.0\",\"method\":\"%s\",\"params\":[],\"id\":1}", method);
    final String response =
        client
            .newCall(
                new Request.Builder()
                    .post(RequestBody.create(request, JSON))
                    .url("http://" + node.getHostName() + ":" + node.getJsonRpcPort().get() + "/")
                    .build())
            .execute()
            .body()
            .string();
    return MAPPER.readTree(response);
  }
}
