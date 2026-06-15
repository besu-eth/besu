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

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;

import java.io.IOException;
import java.util.List;

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
    waitForFile(pluginNode.homeDirectory().resolve("plugins/liveness-plugin-called"));
  }

  @Test
  public void customHealthEndpointWorks() throws IOException {
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
                            + "/custom-health")
                    .build())
            .execute();

    assertThat(response.code()).isEqualTo(200);
    assertThat(response.body().string()).contains("\"status\" : \"UP\"");
  }
}
