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
package org.hyperledger.besu.ethereum.api.jsonrpc.websocket;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.spy;

import org.hyperledger.besu.ethereum.api.jsonrpc.EngineJsonRpcService;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.authentication.AuthenticationService;
import org.hyperledger.besu.ethereum.api.jsonrpc.authentication.EngineAuthService;
import org.hyperledger.besu.ethereum.api.jsonrpc.health.HealthCheckResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.health.HealthService;
import org.hyperledger.besu.ethereum.api.jsonrpc.health.HealthService.HealthCheck;
import org.hyperledger.besu.ethereum.api.jsonrpc.health.HealthService.ParamSource;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.MutableJsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.methods.WebSocketMethodsFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.SubscriptionManager;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.nat.NatService;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.core.json.Json;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class JsonRpcJWTTest {
  private static final int VERTX_AWAIT_TIMEOUT_MILLIS = 10000;
  public static final String HOSTNAME = "127.0.0.1";

  protected static Vertx vertx;
  private VertxTestContext testContext;
  private final JsonRpcConfiguration jsonRpcConfiguration =
      JsonRpcConfiguration.createEngineDefault();
  private HttpClient httpClient;
  private Optional<AuthenticationService> jwtAuth;
  private HealthService healthy;
  private EthScheduler scheduler;
  private Path bufferDir;
  private Map<String, JsonRpcMethod> websocketMethods;

  @BeforeEach
  public void initServerAndClient() {
    jsonRpcConfiguration.setPort(0);
    jsonRpcConfiguration.setHostsAllowlist(List.of("*"));
    try {
      jsonRpcConfiguration.setAuthenticationPublicKeyFile(
          new File(this.getClass().getResource("jwt.hex").toURI()));
    } catch (URISyntaxException e) {
      fail("couldn't load jwt key from jwt.hex in classpath");
    }
    vertx = Vertx.vertx();
    testContext = new VertxTestContext();

    websocketMethods =
        new WebSocketMethodsFactory(
                new SubscriptionManager(new NoOpMetricsSystem()), new HashMap<>())
            .methods();

    bufferDir = null;
    try {
      bufferDir = Files.createTempDirectory("JsonRpcJWTTest").toAbsolutePath();
    } catch (IOException e) {
      fail("can't create tempdir", e);
    }

    jwtAuth =
        Optional.of(
            new EngineAuthService(
                vertx,
                Optional.ofNullable(jsonRpcConfiguration.getAuthenticationPublicKeyFile()),
                bufferDir));

    healthy =
        new HealthService(
            new HealthCheck() {
              @Override
              public HealthCheckResult check(final ParamSource paramSource) {
                return HealthCheckResult.HEALTHY;
              }
            });

    scheduler = new EthScheduler(1, 1, 1, new NoOpMetricsSystem());
  }

  @AfterEach
  public void after() {}

  @Test
  public void unauthenticatedWebsocketAllowedWithoutJWTAuth() throws InterruptedException {

    EngineJsonRpcService engineJsonRpcService =
        new EngineJsonRpcService(
            vertx,
            bufferDir,
            jsonRpcConfiguration,
            new NoOpMetricsSystem(),
            new NatService(Optional.empty(), true),
            websocketMethods,
            Optional.empty(),
            scheduler,
            Optional.empty(),
            healthy,
            healthy);

    engineJsonRpcService.start().join();

    final InetSocketAddress inetSocketAddress = engineJsonRpcService.socketAddress();
    int listenPort = inetSocketAddress.getPort();

    final HttpClientOptions httpClientOptions =
        new HttpClientOptions().setDefaultHost(HOSTNAME).setDefaultPort(listenPort);

    httpClient = vertx.createHttpClient(httpClientOptions);

    WebSocketConnectOptions wsOpts = new WebSocketConnectOptions();
    wsOpts.setPort(listenPort);
    wsOpts.setHost(HOSTNAME);
    wsOpts.setURI("/");

    httpClient.webSocket(
        wsOpts,
        connected -> {
          if (connected.failed()) {
            connected.cause().printStackTrace();
          }
          assertThat(connected.succeeded()).isTrue();
          WebSocket ws = connected.result();

          JsonRpcRequest req =
              new JsonRpcRequest("2.0", "eth_subscribe", List.of("syncing").toArray());
          ws.frameHandler(
              resp -> {
                assertThat(resp.isText()).isTrue();
                MutableJsonRpcSuccessResponse messageReply =
                    Json.decodeValue(resp.textData(), MutableJsonRpcSuccessResponse.class);
                assertThat(messageReply.getResult()).isEqualTo("0x1");
                testContext.completeNow();
              });
          ws.writeTextMessage(Json.encode(req));
        });

    testContext.awaitCompletion(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    engineJsonRpcService.stop();
    httpClient.close();
  }

  @Test
  public void httpRequestWithDefaultHeaderAndValidJWTIsAccepted() throws InterruptedException {

    EngineJsonRpcService engineJsonRpcService =
        new EngineJsonRpcService(
            vertx,
            bufferDir,
            jsonRpcConfiguration,
            new NoOpMetricsSystem(),
            new NatService(Optional.empty(), true),
            websocketMethods,
            Optional.empty(),
            scheduler,
            jwtAuth,
            healthy,
            healthy);

    engineJsonRpcService.start().join();

    final InetSocketAddress inetSocketAddress = engineJsonRpcService.socketAddress();
    int listenPort = inetSocketAddress.getPort();

    final HttpClientOptions httpClientOptions =
        new HttpClientOptions().setDefaultHost(HOSTNAME).setDefaultPort(listenPort);

    httpClient = vertx.createHttpClient(httpClientOptions);

    WebSocketConnectOptions wsOpts = new WebSocketConnectOptions();
    wsOpts.setPort(listenPort);
    wsOpts.setHost(HOSTNAME);
    wsOpts.setURI("/");
    wsOpts.addHeader(
        "Authorization", "Bearer " + ((EngineAuthService) jwtAuth.get()).createToken());
    wsOpts.addHeader(HttpHeaders.HOST, "anything");

    httpClient.webSocket(
        wsOpts,
        connected -> {
          if (connected.failed()) {
            connected.cause().printStackTrace();
          }
          assertThat(connected.succeeded()).isTrue();
          WebSocket ws = connected.result();
          JsonRpcRequest req =
              new JsonRpcRequest("1", "eth_subscribe", List.of("syncing").toArray());
          ws.frameHandler(
              resp -> {
                assertThat(resp.isText()).isTrue();
                System.out.println(resp.textData());
                assertThat(resp.textData()).doesNotContain("error");
                MutableJsonRpcSuccessResponse messageReply =
                    Json.decodeValue(resp.textData(), MutableJsonRpcSuccessResponse.class);
                assertThat(messageReply.getResult()).isEqualTo("0x1");
                testContext.completeNow();
              });
          ws.writeTextMessage(Json.encode(req));
        });

    testContext.awaitCompletion(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    engineJsonRpcService.stop();
    httpClient.close();
  }

  @Test
  public void wsRequestFromBadHostAndValidJWTIsDenied() throws InterruptedException {

    JsonRpcConfiguration strictHost = JsonRpcConfiguration.createEngineDefault();
    strictHost.setHostsAllowlist(List.of("localhost"));
    strictHost.setPort(0);
    try {
      strictHost.setAuthenticationPublicKeyFile(
          new File(this.getClass().getResource("jwt.hex").toURI()));
    } catch (URISyntaxException e) {
      fail("didn't parse jwt");
    }

    EngineJsonRpcService engineJsonRpcService =
        spy(
            new EngineJsonRpcService(
                vertx,
                bufferDir,
                strictHost,
                new NoOpMetricsSystem(),
                new NatService(Optional.empty(), true),
                websocketMethods,
                Optional.empty(),
                scheduler,
                jwtAuth,
                healthy,
                healthy));

    engineJsonRpcService.start().join();

    final InetSocketAddress inetSocketAddress = engineJsonRpcService.socketAddress();
    int listenPort = inetSocketAddress.getPort();

    final HttpClientOptions httpClientOptions =
        new HttpClientOptions().setDefaultHost(HOSTNAME).setDefaultPort(listenPort);

    httpClient = vertx.createHttpClient(httpClientOptions);

    WebSocketConnectOptions wsOpts = new WebSocketConnectOptions();
    wsOpts.setPort(listenPort);
    wsOpts.setHost(HOSTNAME);
    wsOpts.setURI("/");
    wsOpts.addHeader(
        "Authorization", "Bearer " + ((EngineAuthService) jwtAuth.get()).createToken());
    wsOpts.addHeader(HttpHeaders.HOST, "bogushost");

    httpClient.webSocket(
        wsOpts,
        connected -> {
          if (connected.failed()) {
            connected.cause().printStackTrace();
          }
          assertThat(connected.succeeded()).isFalse();
          testContext.completeNow();
        });

    testContext.awaitCompletion(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    engineJsonRpcService.stop();

    httpClient.close();
  }

  @Test
  public void httpRequestFromBadHostAndValidJWTIsDenied() throws InterruptedException {

    JsonRpcConfiguration strictHost = JsonRpcConfiguration.createEngineDefault();
    strictHost.setHostsAllowlist(List.of("localhost"));
    strictHost.setPort(0);
    try {
      strictHost.setAuthenticationPublicKeyFile(
          new File(this.getClass().getResource("jwt.hex").toURI()));
    } catch (URISyntaxException e) {
      fail("didn't parse jwt");
    }

    EngineJsonRpcService engineJsonRpcService =
        spy(
            new EngineJsonRpcService(
                vertx,
                bufferDir,
                strictHost,
                new NoOpMetricsSystem(),
                new NatService(Optional.empty(), true),
                websocketMethods,
                Optional.empty(),
                scheduler,
                jwtAuth,
                healthy,
                healthy));

    engineJsonRpcService.start().join();

    final InetSocketAddress inetSocketAddress = engineJsonRpcService.socketAddress();
    int listenPort = inetSocketAddress.getPort();

    final HttpClientOptions httpClientOptions =
        new HttpClientOptions().setDefaultHost(HOSTNAME).setDefaultPort(listenPort);

    httpClient = vertx.createHttpClient(httpClientOptions);

    MultiMap headers =
        HttpHeaders.set(
                "Authorization", "Bearer " + ((EngineAuthService) jwtAuth.get()).createToken())
            .set(HttpHeaders.HOST, "bogushost");

    httpClient.request(
        HttpMethod.GET,
        "/",
        connected -> {
          if (connected.failed()) {
            connected.cause().printStackTrace();
          }
          HttpClientRequest request = connected.result();
          request.headers().addAll(headers);
          request.send(
              response -> {
                assertThat(response.result().statusCode()).isNotEqualTo(500);
                assertThat(response.result().statusCode()).isEqualTo(403);
                testContext.completeNow();
              });
        });

    testContext.awaitCompletion(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    engineJsonRpcService.stop();

    httpClient.close();
  }

  @Test
  public void httpRequestWithDefaultHeaderAndInvalidJWTIsDenied() throws InterruptedException {

    EngineJsonRpcService engineJsonRpcService =
        new EngineJsonRpcService(
            vertx,
            bufferDir,
            jsonRpcConfiguration,
            new NoOpMetricsSystem(),
            new NatService(Optional.empty(), true),
            websocketMethods,
            Optional.empty(),
            scheduler,
            jwtAuth,
            healthy,
            healthy);

    engineJsonRpcService.start().join();

    final InetSocketAddress inetSocketAddress = engineJsonRpcService.socketAddress();
    int listenPort = inetSocketAddress.getPort();

    final HttpClientOptions httpClientOptions =
        new HttpClientOptions().setDefaultHost(HOSTNAME).setDefaultPort(listenPort);

    httpClient = vertx.createHttpClient(httpClientOptions);

    WebSocketConnectOptions wsOpts = new WebSocketConnectOptions();
    wsOpts.setPort(listenPort);
    wsOpts.setHost(HOSTNAME);
    wsOpts.setURI("/");
    wsOpts.addHeader("Authorization", "Bearer totallyunparseablenonsense");

    httpClient.webSocket(
        wsOpts,
        connected -> {
          if (connected.failed()) {
            connected.cause().printStackTrace();
          }
          assertThat(connected.succeeded()).isFalse();
          testContext.completeNow();
        });

    testContext.awaitCompletion(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    engineJsonRpcService.stop();
    httpClient.close();
  }
}
