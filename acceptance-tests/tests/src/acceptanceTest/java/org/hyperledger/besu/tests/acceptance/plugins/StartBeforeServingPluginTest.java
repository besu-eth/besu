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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * started. The node binds an ephemeral port, as every acceptance test node does, so the test learns
 * the port from the line the JSON-RPC server logs when it binds, captured live from the node's
 * output, and hammers it from that moment until the node reports up; no answer may ever be {@code
 * false}. Were the ports opened before the plugins started, the line would appear during the delay
 * and every answer during it would be.
 */
public class StartBeforeServingPluginTest extends AcceptanceTestBase {
  private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
  private static final String IS_STARTED_REQUEST =
      "{\"jsonrpc\":\"2.0\",\"method\":\"tests_isStarted\",\"params\":[],\"id\":1}";
  // logged by JsonRpcHttpService once the HTTP server has bound its port; the engine API would log
  // the same line but is disabled on this node, and the HTTP service starts first anyway
  private static final Pattern LISTENING =
      Pattern.compile("JSON-RPC service started and listening on (\\S+):(\\d+)");
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Duration STARTUP_DEADLINE = Duration.ofMinutes(2);

  @Test
  public void rpcHandlerNeverRunsBeforeThePluginHasStarted() throws Exception {
    final BesuNode node =
        besu.createPluginsNode(
            "node1",
            List.of("testPlugins"),
            List.of("--rpc-http-api=TESTS", "--plugin-start-delay-test-ms=2000"));

    final OkHttpClient client =
        new OkHttpClient.Builder()
            .connectTimeout(Duration.ofMillis(500))
            .readTimeout(Duration.ofSeconds(10))
            .build();

    final List<Boolean> answers = new ArrayList<>();
    Optional<Request> request = Optional.empty();
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      cluster.startConsoleCapture();
      final Future<?> nodeStarted = executor.submit(() -> cluster.start(node));
      final Instant deadline = Instant.now().plus(STARTUP_DEADLINE);
      while (!nodeStarted.isDone()) {
        assertThat(Instant.now())
            .as("the node did not finish starting within %s", STARTUP_DEADLINE)
            .isBefore(deadline);
        if (request.isEmpty()) {
          request = listeningAddress().map(StartBeforeServingPluginTest::isStartedRequest);
        } else {
          isStarted(client, request.get()).ifPresent(answers::add);
        }
        Thread.sleep(10);
      }
      nodeStarted.get(1, TimeUnit.MINUTES);
      assertThat(request).as("the JSON-RPC server logged the port it listens on").isPresent();
      // the node is up, so this request cannot be refused
      answers.add(isStarted(client, request.get()).orElseThrow());
    } finally {
      executor.shutdownNow();
      client.dispatcher().executorService().shutdown();
      client.connectionPool().evictAll();
    }

    assertThat(answers).isNotEmpty().doesNotContain(false);
  }

  /** Empty until the node has logged that its JSON-RPC server is listening. */
  private Optional<String> listeningAddress() {
    final Matcher matcher = LISTENING.matcher(cluster.peekConsoleContents());
    return matcher.find()
        ? Optional.of(matcher.group(1) + ":" + matcher.group(2))
        : Optional.empty();
  }

  private static Request isStartedRequest(final String address) {
    return new Request.Builder()
        .post(RequestBody.create(IS_STARTED_REQUEST, JSON))
        .url("http://" + address + "/")
        .build();
  }

  /** Empty while the port is not accepting connections yet; any answer must carry a result. */
  private static Optional<Boolean> isStarted(final OkHttpClient client, final Request request) {
    final String body;
    try (Response response = client.newCall(request).execute()) {
      body = response.body().string();
    } catch (final IOException e) {
      return Optional.empty();
    }
    final JsonNode result;
    try {
      result = MAPPER.readTree(body).get("result");
    } catch (final IOException e) {
      throw new AssertionError("tests_isStarted answered " + body, e);
    }
    assertThat(result).as("tests_isStarted answered %s", body).isNotNull();
    return Optional.of(result.asBoolean());
  }
}
