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

import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration;
import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.jupiter.api.Test;

/**
 * A plugin's RPC handler must not run before the plugin's {@code start()} has returned. The test
 * plugin delays its start by two seconds and answers {@code tests_isStarted} with whether it has
 * started. The RPC port is fixed up front instead of chosen by the node, so the test can hammer it
 * from before the node process launches; no answer may ever be {@code false}. Were the ports opened
 * before the plugins started, every answer during the delay would be.
 */
public class StartBeforeServingPluginTest extends AcceptanceTestBase {
  private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
  private static final String IS_STARTED_REQUEST =
      "{\"jsonrpc\":\"2.0\",\"method\":\"tests_isStarted\",\"params\":[],\"id\":1}";

  @Test
  public void rpcHandlerNeverRunsBeforeThePluginHasStarted() throws Exception {
    final int rpcPort = freePort();
    final BesuNode node =
        besu.createPluginsNode(
            "node1",
            List.of("testPlugins"),
            builder ->
                builder
                    .jsonRpcConfiguration(jsonRpcOnFixedPort(rpcPort))
                    .extraCLIOptions(
                        List.of("--rpc-http-api=TESTS", "--plugin-start-delay-test-ms=2000")));

    final OkHttpClient client =
        new OkHttpClient.Builder()
            .connectTimeout(Duration.ofMillis(500))
            .readTimeout(Duration.ofSeconds(10))
            .build();
    final Request request =
        new Request.Builder()
            .post(RequestBody.create(IS_STARTED_REQUEST, JSON))
            .url("http://" + node.getHostName() + ":" + rpcPort + "/")
            .build();

    final List<Boolean> answers = new ArrayList<>();
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      final Future<?> nodeStarted = executor.submit(() -> cluster.start(node));
      while (!nodeStarted.isDone()) {
        isStarted(client, request).ifPresent(answers::add);
        Thread.sleep(10);
      }
      nodeStarted.get(1, TimeUnit.MINUTES);
      // the node is up, so this request cannot be refused
      answers.add(isStarted(client, request).orElseThrow());
    } finally {
      executor.shutdownNow();
    }

    assertThat(answers).isNotEmpty().doesNotContain(false);
  }

  /** Empty while the port is not accepting connections yet. */
  private static Optional<Boolean> isStarted(final OkHttpClient client, final Request request) {
    try (Response response = client.newCall(request).execute()) {
      final String body = response.body().string();
      final JsonNode result = new ObjectMapper().readTree(body).get("result");
      assertThat(result).as("tests_isStarted answered %s", body).isNotNull();
      return Optional.of(result.asBoolean());
    } catch (final IOException e) {
      return Optional.empty();
    }
  }

  private static JsonRpcConfiguration jsonRpcOnFixedPort(final int port) {
    final JsonRpcConfiguration config = JsonRpcConfiguration.createDefault();
    config.setEnabled(true);
    config.setPort(port);
    config.setHostsAllowlist(List.of("*"));
    return config;
  }

  private static int freePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
