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
import org.hyperledger.besu.tests.acceptance.dsl.node.configuration.BesuNodeConfigurationBuilder;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.Test;

/**
 * The built-in plugins go through the plugin context like external plugins but are not subject to
 * the options that select external plugins: with external plugins disabled, and with the plugin
 * selection naming only an external plugin, the startup summary lists all four as registered, the
 * node runs on RocksDB (it could not start otherwise) and {@code /liveness} is served.
 */
public class BuiltInPluginsTest extends AcceptanceTestBase {

  private static final List<String> BUILT_IN_PLUGIN_IDS =
      List.of(
          "org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBPlugin",
          "org.hyperledger.besu.services.kvstore.InMemoryStoragePlugin",
          "org.hyperledger.besu.plugin.services.health.LivenessCheckPlugin",
          "org.hyperledger.besu.plugin.services.health.ReadinessCheckPlugin");

  private final OkHttpClient client = new OkHttpClient();

  /**
   * The node is up, so the RocksDB built-in registered; its startup summary names the four
   * built-ins as registered through the context; and the liveness endpoint answers. The endpoint
   * alone would not prove the built-in registered, since Besu's core check answers {@code UP} the
   * same way.
   */
  private void assertBuiltInsAreServing(final BesuNode node) throws IOException {
    node.verify(net.awaitPeerCount(0));
    final String console = cluster.getConsoleContents();
    assertThat(console).contains("Built-in plugins:");
    BUILT_IN_PLUGIN_IDS.forEach(id -> assertThat(console).contains(" - " + id + " ("));
    final Response response =
        client
            .newCall(
                new Request.Builder()
                    .get()
                    .url(
                        "http://"
                            + node.getHostName()
                            + ":"
                            + node.getJsonRpcPort().get()
                            + "/liveness")
                    .build())
            .execute();
    assertThat(response.code()).isEqualTo(200);
    final JsonNode body = new ObjectMapper().readTree(response.body().string());
    assertThat(body.get("status").asText()).isEqualTo("UP");
  }

  @Test
  public void nodeRunsOnRocksDbWithExternalPluginsDisabled() throws Exception {
    final BesuNode node =
        besu.create(
            new BesuNodeConfigurationBuilder()
                .name("node1")
                .extraCLIOptions(List.of("--Xplugins-external-enabled=false"))
                .build());
    cluster.startConsoleCapture();
    cluster.start(node);

    assertBuiltInsAreServing(node);
  }

  @Test
  public void nodeRunsOnRocksDbWhenThePluginSelectionNamesOnlyAnExternalPlugin() throws Exception {
    final BesuNode node =
        besu.create(
            new BesuNodeConfigurationBuilder()
                .name("node1")
                .plugins(List.of("testPlugins"))
                .requestedPlugins(List.of("TestPicoCLIPlugin"))
                .build());
    cluster.startConsoleCapture();
    cluster.start(node);

    assertBuiltInsAreServing(node);
  }
}
