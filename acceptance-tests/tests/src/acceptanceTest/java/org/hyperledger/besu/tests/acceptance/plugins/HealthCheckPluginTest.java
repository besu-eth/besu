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
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class HealthCheckPluginTest extends AcceptanceTestBase {

  private BesuNode pluginNode;
  private OkHttpClient client;

  @BeforeEach
  public void setUp() throws Exception {
    pluginNode = besu.createPluginsNode("node1", List.of("testPlugins"), List.of());
    cluster.start(pluginNode);
    client = new OkHttpClient();
  }

  @Test
  public void livenessEndpointOverridden() throws IOException {
    final Response response =
        client
            .newCall(
                new Request.Builder()
                    .get()
                    .url(
                        "http://"
                            + pluginNode.getHostName()
                            + ":"
                            + pluginNode.getJsonRpcPort().get()
                            + "/liveness")
                    .build())
            .execute();

    assertThat(response.code()).isEqualTo(200);
    final JsonNode body = new ObjectMapper().readTree(response.body().string());
    assertThat(body.get("status").asText()).isEqualTo("UP");
    assertThat(body.get("checks").get("testPlugin").asText()).isEqualTo("liveness");
    waitForFile(pluginNode.homeDirectory().resolve("plugins/liveness-plugin-called"));
  }

  @Test
  public void readinessEndpointOverridden() throws IOException {
    final Response response =
        client
            .newCall(
                new Request.Builder()
                    .get()
                    .url(
                        "http://"
                            + pluginNode.getHostName()
                            + ":"
                            + pluginNode.getJsonRpcPort().get()
                            + "/readiness")
                    .build())
            .execute();

    assertThat(response.code()).isEqualTo(200);
    final JsonNode body = new ObjectMapper().readTree(response.body().string());
    assertThat(body.get("status").asText()).isEqualTo("UP");
    assertThat(body.get("checks").get("testPlugin").asText()).isEqualTo("readiness");
    waitForFile(pluginNode.homeDirectory().resolve("plugins/readiness-plugin-called"));
  }

  @Test
  public void builtInReadinessIsUpOnceTheNodeIsRunning() throws IOException {
    final BesuNode plainNode = besu.createArchiveNode("plainNode");
    cluster.addNode(plainNode);

    // minPeers=0: the default asks for one peer, which a test node is not guaranteed to have
    final Request request =
        new Request.Builder()
            .get()
            .url(
                "http://"
                    + plainNode.getHostName()
                    + ":"
                    + plainNode.getJsonRpcPort().get()
                    + "/readiness?minPeers=0")
            .build();

    // the built-in check receives P2PService in afterMainLoop(), so it turns UP after the main loop
    await()
        .atMost(30, SECONDS)
        .untilAsserted(
            () -> {
              try (Response response = client.newCall(request).execute()) {
                assertThat(response.code()).isEqualTo(200);
                final JsonNode body = new ObjectMapper().readTree(response.body().string());
                assertThat(body.get("status").asText()).isEqualTo("UP");
                assertThat(body.get("checks").has("peers")).isTrue();
              }
            });
  }
}
