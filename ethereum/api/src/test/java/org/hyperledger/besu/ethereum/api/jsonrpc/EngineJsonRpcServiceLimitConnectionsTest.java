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
package org.hyperledger.besu.ethereum.api.jsonrpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.ethereum.api.jsonrpc.health.HealthService;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.AbortiveWebSocketClient;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.methods.WebSocketMethodsFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.SubscriptionManager;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.metrics.StubMetricsSystem;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.nat.NatService;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import io.vertx.core.Vertx;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EngineJsonRpcServiceLimitConnectionsTest {

  private static final int MAX_CONNECTIONS = 2;
  private static final int COUNT_REJECTIONS = 3;

  private Vertx vertx;
  private StubMetricsSystem metricsSystem;
  private EngineJsonRpcService engineJsonRpcService;
  private String baseUrl;
  private final List<OkHttpClient> heldClients = new ArrayList<>();

  @BeforeEach
  public void setUp() throws Exception {
    vertx = Vertx.vertx();
    metricsSystem = new StubMetricsSystem();
    final Path dataDir = Files.createTempDirectory("engine-limit-connections");
    final JsonRpcConfiguration config = JsonRpcConfiguration.createEngineDefault();
    config.setPort(0);
    config.setHostsAllowlist(List.of("*"));
    config.setMaxActiveConnections(MAX_CONNECTIONS);
    config.setAuthenticationEnabled(false);

    final Map<String, JsonRpcMethod> methods =
        new WebSocketMethodsFactory(
                new SubscriptionManager(new NoOpMetricsSystem()), new HashMap<>(), 0)
            .methods();

    engineJsonRpcService =
        new EngineJsonRpcService(
            vertx,
            dataDir,
            config,
            metricsSystem,
            new NatService(Optional.empty(), true),
            methods,
            Optional.empty(),
            new EthScheduler(1, 1, 1, new NoOpMetricsSystem()),
            Optional.empty(),
            HealthService.ALWAYS_HEALTHY,
            HealthService.ALWAYS_HEALTHY);
    engineJsonRpcService.start().join();
    baseUrl = engineJsonRpcService.url();
  }

  @AfterEach
  public void tearDown() {
    heldClients.forEach(client -> client.connectionPool().evictAll());
    heldClients.clear();
    if (engineJsonRpcService != null) {
      engineJsonRpcService.stop().join();
    }
    if (vertx != null) {
      vertx.close();
    }
  }

  @Test
  public void zeroMaxActiveConnectionsMeansNoLimit() throws Exception {
    engineJsonRpcService.stop().join();

    final JsonRpcConfiguration unlimited = JsonRpcConfiguration.createEngineDefault();
    unlimited.setPort(0);
    unlimited.setHostsAllowlist(List.of("*"));
    unlimited.setMaxActiveConnections(0);
    unlimited.setAuthenticationEnabled(false);

    final Map<String, JsonRpcMethod> methods =
        new WebSocketMethodsFactory(
                new SubscriptionManager(new NoOpMetricsSystem()), new HashMap<>(), 0)
            .methods();

    engineJsonRpcService =
        new EngineJsonRpcService(
            vertx,
            Files.createTempDirectory("engine-unlimited-connections"),
            unlimited,
            metricsSystem,
            new NatService(Optional.empty(), true),
            methods,
            Optional.empty(),
            new EthScheduler(1, 1, 1, new NoOpMetricsSystem()),
            Optional.empty(),
            HealthService.ALWAYS_HEALTHY,
            HealthService.ALWAYS_HEALTHY);
    engineJsonRpcService.start().join();
    baseUrl = engineJsonRpcService.url();

    final List<OkHttpClient> clients = new ArrayList<>();
    final int aboveFormerDefault = 5;
    for (int i = 0; i < aboveFormerDefault; i++) {
      final OkHttpClient client = new OkHttpClient();
      clients.add(client);
      try (final Response resp = client.newCall(readinessRequest()).execute()) {
        assertThat(resp.code()).isEqualTo(200);
      }
    }
    assertThat(metricsSystem.getGaugeValue("active_engine_connection_count"))
        .isEqualTo(aboveFormerDefault);
    clients.forEach(client -> client.connectionPool().evictAll());
  }

  @Test
  public void rejectedConnectionsDoNotBlockLaterAcceptedConnections() throws Exception {
    for (int i = 0; i < MAX_CONNECTIONS; i++) {
      final OkHttpClient client = new OkHttpClient();
      heldClients.add(client);
      try (final Response resp = client.newCall(readinessRequest()).execute()) {
        assertThat(resp.code()).isEqualTo(200);
      }
    }

    for (int i = 0; i < COUNT_REJECTIONS; i++) {
      final OkHttpClient rejectedClient = new OkHttpClient();
      assertThatThrownBy(() -> rejectedClient.newCall(readinessRequest()).execute())
          .isInstanceOf(IOException.class)
          .hasStackTraceContaining("unexpected end of stream");
    }

    assertThat(metricsSystem.getGaugeValue("active_engine_connection_count"))
        .as("rejected connections should not increase the active connection count")
        .isEqualTo(MAX_CONNECTIONS);

    // release held connections — if rejects leaked the counter, new accepts would still fail
    heldClients.forEach(client -> client.connectionPool().evictAll());
    heldClients.clear();

    try (final Response resp = new OkHttpClient().newCall(readinessRequest()).execute()) {
      assertThat(resp.code()).isEqualTo(200);
    }
  }

  @Test
  public void abruptWebSocketResetReleasesActiveConnectionCount() throws Exception {
    final URI uri = URI.create(baseUrl);
    final String request =
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_subscribe\",\"params\":[\"syncing\"]}";
    final int resets = 5;
    for (int i = 0; i < resets; i++) {
      AbortiveWebSocketClient.handshakeSendAndReset(uri.getHost(), uri.getPort(), request);
    }

    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (metricsSystem.getGaugeValue("active_engine_connection_count") != 0) {
      if (System.nanoTime() > deadline) {
        break;
      }
      Thread.sleep(50);
    }

    assertThat(metricsSystem.getGaugeValue("active_engine_connection_count"))
        .as("abrupt Engine WS reset must not leak active_engine_connection_count")
        .isEqualTo(0);
  }

  private Request readinessRequest() {
    return new Request.Builder().get().url(baseUrl + HealthService.READINESS_PATH).build();
  }
}
