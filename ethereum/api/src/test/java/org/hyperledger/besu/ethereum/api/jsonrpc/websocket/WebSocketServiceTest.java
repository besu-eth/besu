/*
 * Copyright ConsenSys AG.
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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verifyNoInteractions;

import org.hyperledger.besu.ethereum.api.handlers.TimeoutOptions;
import org.hyperledger.besu.ethereum.api.jsonrpc.execution.BaseJsonRpcProcessor;
import org.hyperledger.besu.ethereum.api.jsonrpc.execution.JsonRpcExecutor;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.methods.WebSocketMethodsFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.SubscriptionManager;
import org.hyperledger.besu.ethereum.api.util.TestJsonRpcMethodsUtil;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.metrics.StubMetricsSystem;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketClient;
import io.vertx.core.http.WebSocketClientOptions;
import io.vertx.core.http.WebSocketFrame;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class WebSocketServiceTest {

  private static final int VERTX_AWAIT_TIMEOUT_MILLIS = 10000;

  private Vertx vertx;
  private VertxTestContext testContext;
  private WebSocketConfiguration websocketConfiguration;
  private WebSocketMessageHandler webSocketMessageHandlerSpy;
  private Map<String, JsonRpcMethod> websocketMethods;
  private WebSocketService websocketService;
  private StubMetricsSystem metricsSystem;
  private HttpClient httpClient;
  private WebSocketClient webSocketClient;
  private final int maxConnections = 5;
  private final int maxFrameSize = 1024 * 1024;

  @BeforeEach
  public void before() {
    vertx = Vertx.vertx();
    testContext = new VertxTestContext();
    metricsSystem = new StubMetricsSystem();

    websocketConfiguration = WebSocketConfiguration.createDefault();
    websocketConfiguration.setPort(0);
    websocketConfiguration.setHostsAllowlist(Collections.singletonList("*"));
    websocketConfiguration.setMaxActiveConnections(maxConnections);
    websocketConfiguration.setMaxFrameSize(maxFrameSize);

    startWebSocketService();

    websocketConfiguration.setPort(websocketService.socketAddress().getPort());

    final HttpClientOptions httpClientOptions =
        new HttpClientOptions()
            .setDefaultHost(websocketConfiguration.getHost())
            .setDefaultPort(websocketConfiguration.getPort());

    httpClient = vertx.createHttpClient(httpClientOptions);

    final WebSocketClientOptions webSocketClientOptions =
        new WebSocketClientOptions()
            .setDefaultHost(websocketConfiguration.getHost())
            .setDefaultPort(websocketConfiguration.getPort());

    webSocketClient = vertx.createWebSocketClient(webSocketClientOptions);
  }

  private void startWebSocketService() {
    websocketMethods =
        new WebSocketMethodsFactory(
                new SubscriptionManager(new NoOpMetricsSystem(), websocketConfiguration),
                new HashMap<>(),
                0)
            .methods();
    webSocketMessageHandlerSpy =
        spy(
            new WebSocketMessageHandler(
                vertx,
                new JsonRpcExecutor(new BaseJsonRpcProcessor(), websocketMethods),
                mock(EthScheduler.class),
                TimeoutOptions.defaultOptions().getTimeoutSeconds()));

    websocketService =
        new WebSocketService(
            vertx, websocketConfiguration, webSocketMessageHandlerSpy, metricsSystem);
    websocketService.start().join();
  }

  @AfterEach
  public void after() {
    reset(webSocketMessageHandlerSpy);
    websocketService.stop();
  }

  @Test
  public void limitActiveConnections() throws InterruptedException {
    // expecting maxConnections successful responses
    final CountDownLatch successLatch = new CountDownLatch(maxConnections);
    // and a number of rejections
    final int countRejections = 2;
    final CountDownLatch rejectionLatch = new CountDownLatch(countRejections);

    final String request = "{\"id\": 1, \"method\": \"eth_subscribe\", \"params\": [\"syncing\"]}";
    // the number in the response is the subscription ID, so in successive responses this increments
    final String expectedResponse1 = "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"0x1\"}";

    // attempt to exceed max connections - but only maxConnections should succeed
    for (int i = 0; i < maxConnections + countRejections; i++) {
      webSocketClient
          .connect("/")
          .onComplete(
              future -> {
                if (future.succeeded()) {
                  WebSocket ws = future.result();
                  ws.handler(
                      buffer -> {
                        assertNotNull(buffer.toString());
                        // assert a successful response
                        assertTrue(
                            buffer.toString().startsWith(expectedResponse1.substring(0, 36)));
                        successLatch.countDown();
                      });
                  ws.writeTextMessage(request);
                } else {
                  // count down the rejected WS connections
                  rejectionLatch.countDown();
                }
              });
    }
    // wait for successful responses AND rejected connections
    successLatch.await(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    rejectionLatch.await(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    assertThat(successLatch.getCount()).isEqualTo(0);
    assertThat(rejectionLatch.getCount()).isEqualTo(0);

    assertThat(metricsSystem.getGaugeValue("active_ws_connection_count"))
        .as("rejected connections should not increase the active connection count")
        .isEqualTo(maxConnections);
  }

  @Test
  public void abruptWebSocketResetReleasesActiveConnectionCount() throws Exception {
    final String request =
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_subscribe\",\"params\":[\"syncing\"]}";
    final int resets = 5;
    for (int i = 0; i < resets; i++) {
      AbortiveWebSocketClient.handshakeSendAndReset(
          websocketConfiguration.getHost(), websocketConfiguration.getPort(), request);
      // Release is async (Vert.x event loop); wait for the counter to drain before the
      // next handshake so a lagging release cannot get it rejected at the limit.
      final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (metricsSystem.getGaugeValue("active_ws_connection_count") != 0
          && System.nanoTime() <= deadline) {
        Thread.sleep(50);
      }
    }

    assertThat(metricsSystem.getGaugeValue("active_ws_connection_count"))
        .as("abrupt WS reset must not leak active_ws_connection_count")
        .isEqualTo(0);
  }

  @Test
  public void stalledResponseIsAbandonedWhenClientResets() throws Exception {
    assertStalledResponseIsAbandoned(AbortiveWebSocketClient::reset);
  }

  @Test
  public void stalledResponseIsAbandonedWhenClientSendsCloseFrame() throws Exception {
    assertStalledResponseIsAbandoned(AbortiveWebSocketClient::sendCloseFrame);
  }

  /**
   * Runs against the real Vert.x server rather than a mocked ServerWebSocket, so it checks what
   * JsonResponseStreamer and StreamBackpressure assume about a WebSocket that closes while its
   * write queue is full.
   */
  private void assertStalledResponseIsAbandoned(final SocketCloser closeWhileStalled)
      throws Exception {
    final EndlessResult result = new EndlessResult();
    final JsonRpcMethod method = endlessResponse(result);
    websocketMethods.put(method.getName(), method);

    final CountDownLatch subscriptionsRemoved = new CountDownLatch(1);
    vertx
        .eventBus()
        .consumer(
            SubscriptionManager.EVENTBUS_REMOVE_SUBSCRIPTIONS_ADDRESS,
            m -> subscriptionsRemoved.countDown())
        .completion()
        .toCompletionStage()
        .toCompletableFuture()
        .get(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

    final Socket socket =
        AbortiveWebSocketClient.handshakeAndSendWithoutReading(
            websocketConfiguration.getHost(),
            websocketConfiguration.getPort(),
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"" + method.getName() + "\",\"params\":[]}");
    try {
      assertThat(waitUntilStalled(result))
          .as("the response should stall once the client stops reading")
          .isTrue();

      closeWhileStalled.close(socket);

      // StreamBackpressure would otherwise wait out its 60s drain timeout
      final Throwable failure =
          result.writeFailure.get(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
      assertThat(failure)
          .as("the stalled write should be abandoned as soon as the WebSocket closes")
          .isInstanceOf(IOException.class)
          .hasMessageNotContaining("Timed out");
    } finally {
      if (!socket.isClosed()) {
        AbortiveWebSocketClient.reset(socket);
      }
      websocketMethods.remove(method.getName());
    }

    assertThat(subscriptionsRemoved.await(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
        .as("closing the connection should remove its subscriptions")
        .isTrue();
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (metricsSystem.getGaugeValue("active_ws_connection_count") != 0
        && System.nanoTime() <= deadline) {
      Thread.sleep(50);
    }
    assertThat(metricsSystem.getGaugeValue("active_ws_connection_count"))
        .as("closing a stalled WS must not leak active_ws_connection_count")
        .isEqualTo(0);
  }

  /** Waits until chunks have been written and then stop being written for a while. */
  private static boolean waitUntilStalled(final EndlessResult result) throws InterruptedException {
    final long deadline =
        System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(VERTX_AWAIT_TIMEOUT_MILLIS);
    long previous = -1;
    while (System.nanoTime() <= deadline) {
      Thread.sleep(200);
      final long current = result.chunksWritten.get();
      if (current > 0 && current == previous) {
        return true;
      }
      previous = current;
    }
    return false;
  }

  private static JsonRpcMethod endlessResponse(final EndlessResult result) {
    return new JsonRpcMethod() {
      @Override
      public String getName() {
        return "temp_endlessResponse";
      }

      @Override
      public JsonRpcResponse response(final JsonRpcRequestContext request) {
        return new JsonRpcSuccessResponse(request.getRequest().getId(), result);
      }
    };
  }

  @FunctionalInterface
  private interface SocketCloser {
    void close(Socket socket) throws IOException;
  }

  /** A result that streams chunks until writing one fails, and records that failure. */
  @JsonSerialize(using = EndlessResultSerializer.class)
  private static final class EndlessResult {
    final AtomicLong chunksWritten = new AtomicLong();
    final CompletableFuture<Throwable> writeFailure = new CompletableFuture<>();
  }

  private static final class EndlessResultSerializer extends JsonSerializer<EndlessResult> {
    private static final String CHUNK = "a".repeat(8192);

    @Override
    public void serialize(
        final EndlessResult value, final JsonGenerator gen, final SerializerProvider serializers)
        throws IOException {
      gen.writeStartArray();
      try {
        while (true) {
          gen.writeString(CHUNK);
          value.chunksWritten.incrementAndGet();
        }
      } catch (final IOException | RuntimeException e) {
        value.writeFailure.complete(e);
        throw e;
      }
    }
  }

  @Test
  public void limitSubscriptions() throws InterruptedException {
    // reset the service to start with a maxActiveSubscriptions of 3
    websocketService.stop().join();
    int maxActiveSubscriptions = 3;
    websocketConfiguration.setMaxActiveSubscriptions(maxActiveSubscriptions);
    startWebSocketService();

    // expecting maxActiveSubscriptions successful responses
    final CountDownLatch successLatch = new CountDownLatch(maxActiveSubscriptions);
    // and a number of rejections
    final int countRejections = 2;
    final CountDownLatch rejectionLatch = new CountDownLatch(countRejections);

    final String request = "{\"id\": 1, \"method\": \"eth_subscribe\", \"params\": [\"syncing\"]}";
    // the number in the response is the subscription ID, so in successive responses this increments
    final String expectedResponse1 = "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"0x1\"}";

    // attempt to exceed max subscriptions - but only maxActiveSubscriptions should succeed
    for (int i = 0; i < maxActiveSubscriptions + countRejections; i++) {
      webSocketClient
          .connect("/")
          .onComplete(
              future -> {
                if (future.succeeded()) {
                  WebSocket ws = future.result();
                  ws.handler(
                      buffer -> {
                        assertNotNull(buffer.toString());
                        // assert a successful response
                        if (buffer.toString().startsWith(expectedResponse1.substring(0, 36))) {
                          successLatch.countDown();
                        } else {
                          rejectionLatch.countDown();
                        }
                      });
                  ws.writeTextMessage(request);
                } else {
                  throw new AssertionError("test should not reach here!");
                }
              });
    }
    // wait for successful responses AND rejected connections
    successLatch.await(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    rejectionLatch.await(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    assertThat(successLatch.getCount()).isEqualTo(0);
    assertThat(rejectionLatch.getCount()).isEqualTo(0);
  }

  @Test
  public void websocketServiceExecutesHandlerOnMessage() throws InterruptedException {

    final String request = "{\"id\": 1, \"method\": \"eth_subscribe\", \"params\": [\"syncing\"]}";
    final String expectedResponse = "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"0x1\"}";

    webSocketClient
        .connect("/")
        .onComplete(
            future -> {
              if (future.succeeded()) {
                WebSocket ws = future.result();
                ws.handler(
                    buffer ->
                        testContext.verify(
                            () -> {
                              assertEquals(expectedResponse, buffer.toString());
                              testContext.completeNow();
                            }));

                ws.writeTextMessage(request);
              } else {
                testContext.failNow("websocket connection failed");
              }
            });

    assertThat(testContext.awaitCompletion(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
        .as("test context should complete before the timeout")
        .isTrue();
    if (testContext.failed()) {
      throw new AssertionError(testContext.causeOfFailure());
    }
  }

  @Test
  public void websocketServiceHandlesBinaryFrames() throws InterruptedException {

    webSocketClient
        .connect("/")
        .onComplete(
            future -> {
              if (future.succeeded()) {
                WebSocket ws = future.result();
                final JsonObject requestJson = new JsonObject().put("id", 1).put("method", "eth_x");
                ws.handler(
                    // we don't really care what the response is
                    buffer -> {
                      testContext.completeNow();
                    });
                ws.writeFinalBinaryFrame(Buffer.buffer(requestJson.toString()));
              } else {
                testContext.failNow("websocket connection failed");
              }
            });

    assertThat(testContext.awaitCompletion(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
        .as("test context should complete before the timeout")
        .isTrue();
    if (testContext.failed()) {
      throw new AssertionError(testContext.causeOfFailure());
    }
  }

  @Test
  public void websocketServiceRemoveSubscriptionOnConnectionClose() throws InterruptedException {

    vertx
        .eventBus()
        .consumer(SubscriptionManager.EVENTBUS_REMOVE_SUBSCRIPTIONS_ADDRESS)
        .handler(
            m -> {
              assertNotNull(m.body());
              testContext.completeNow();
            })
        .completion()
        .onComplete(
            v ->
                webSocketClient
                    .connect("/")
                    .onComplete(
                        websocket -> {
                          websocket.result().close();
                        }));

    assertThat(testContext.awaitCompletion(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
        .as("test context should complete before the timeout")
        .isTrue();
    if (testContext.failed()) {
      throw new AssertionError(testContext.causeOfFailure());
    }
  }

  @Test
  public void websocketServiceCloseConnectionOnUnrecoverableError() throws InterruptedException {

    final byte[] bigMessage = new byte[maxFrameSize + 1];
    Arrays.fill(bigMessage, (byte) 1);

    webSocketClient
        .connect("/")
        .onComplete(
            future -> {
              if (future.succeeded()) {
                WebSocket ws = future.result();
                ws.write(Buffer.buffer(bigMessage));
                ws.closeHandler(v -> testContext.completeNow());
              } else {
                testContext.failNow("websocket connection failed");
              }
            });

    assertThat(testContext.awaitCompletion(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
        .as("test context should complete before the timeout")
        .isTrue();
    if (testContext.failed()) {
      throw new AssertionError(testContext.causeOfFailure());
    }
  }

  @SuppressWarnings("deprecation") // No alternative available in vertx 3.
  @Test
  public void websocketServiceMustReturnErrorOnHttpRequest() throws InterruptedException {

    httpClient
        .request(
            HttpMethod.POST,
            websocketConfiguration.getPort(),
            websocketConfiguration.getHost(),
            "/")
        .onComplete(
            request -> {
              request
                  .result()
                  .send()
                  .onComplete(
                      response ->
                          response
                              .result()
                              .bodyHandler(
                                  b -> {
                                    assertEquals(400, response.result().statusCode());
                                    assertEquals(
                                        "Websocket endpoint can't handle HTTP requests",
                                        b.toString());
                                    testContext.completeNow();
                                  }));
            });
    assertThat(testContext.awaitCompletion(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
        .as("test context should complete before the timeout")
        .isTrue();
    if (testContext.failed()) {
      throw new AssertionError(testContext.causeOfFailure());
    }
  }

  @Test
  public void handleLoginRequestWithAuthDisabled() {
    httpClient
        .request(
            HttpMethod.POST,
            websocketConfiguration.getPort(),
            websocketConfiguration.getHost(),
            "/login")
        .onComplete(
            request -> {
              request.result().putHeader("Content-Type", "application/json; charset=utf-8");
              request.result().end("{\"username\":\"user\",\"password\":\"pass\"}");
              request
                  .result()
                  .send()
                  .onComplete(
                      response -> {
                        assertThat(response.result().statusCode()).isEqualTo(400);
                        assertThat(response.result().statusMessage())
                            .isEqualTo("Authentication not enabled");
                      });
            });
  }

  @Test
  public void webSocketDoesNotHandlePingPayloadAsJsonRpcRequest() throws InterruptedException {

    webSocketClient
        .connect("/")
        .onComplete(
            result -> {
              WebSocket websocket = result.result();

              websocket.handler(
                  buffer -> {
                    final String payload = buffer.toString();
                    if (!payload.equals("foo")) {
                      testContext.failNow(
                          "Only expected PONG response with same payload as PING request");
                    }
                  });

              websocket.closeHandler(
                  h -> {
                    verifyNoInteractions(webSocketMessageHandlerSpy);
                    testContext.completeNow();
                  });

              websocket.writeFrame(WebSocketFrame.pingFrame(Buffer.buffer("foo")));
              websocket.close();
            });

    assertThat(testContext.awaitCompletion(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
        .as("test context should complete before the timeout")
        .isTrue();
    if (testContext.failed()) {
      throw new AssertionError(testContext.causeOfFailure());
    }
  }

  @Test
  public void handleResponseWithOptionalEmptyValue() throws InterruptedException {
    final JsonRpcMethod method = TestJsonRpcMethodsUtil.optionalEmptyResponse();
    websocketMethods.put(method.getName(), method);

    final String request =
        "{\"id\": 1, \"method\": \"" + method.getName() + "\", \"params\": [\"syncing\"]}";
    final String expectedResponse = "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":null}";

    webSocketClient
        .connect("/")
        .onComplete(
            future -> {
              if (future.succeeded()) {
                WebSocket ws = future.result();
                ws.handler(
                    buffer -> {
                      assertEquals(expectedResponse, buffer.toString());
                      testContext.completeNow();
                    });

                ws.writeTextMessage(request);
              } else {
                testContext.failNow("websocket connection failed");
              }
            });

    assertThat(testContext.awaitCompletion(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
        .as("test context should complete before the timeout")
        .isTrue();
    if (testContext.failed()) {
      throw new AssertionError(testContext.causeOfFailure());
    }
    testContext.verify(() -> websocketMethods.remove(method.getName()));
  }

  @Test
  public void handleResponseWithOptionalExistingValue() throws InterruptedException {
    final JsonRpcMethod method = TestJsonRpcMethodsUtil.optionalResponseWithValue("foo");
    websocketMethods.put(method.getName(), method);

    final String request =
        "{\"id\": 1, \"method\": \"" + method.getName() + "\", \"params\": [\"syncing\"]}";
    final String expectedResponse = "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"foo\"}";

    webSocketClient
        .connect("/")
        .onComplete(
            future -> {
              if (future.succeeded()) {
                WebSocket ws = future.result();
                ws.handler(
                    buffer -> {
                      assertEquals(expectedResponse, buffer.toString());
                      testContext.completeNow();
                    });

                ws.writeTextMessage(request);
              } else {
                testContext.failNow("websocket connection failed");
              }
            });

    assertThat(testContext.awaitCompletion(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
        .as("test context should complete before the timeout")
        .isTrue();
    if (testContext.failed()) {
      throw new AssertionError(testContext.causeOfFailure());
    }
    testContext.verify(() -> websocketMethods.remove(method.getName()));
  }
}
