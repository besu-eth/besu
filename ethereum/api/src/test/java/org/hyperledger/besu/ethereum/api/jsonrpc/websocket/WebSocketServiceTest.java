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
import org.hyperledger.besu.ethereum.api.jsonrpc.execution.CombinedJsonRpcProcessor;
import org.hyperledger.besu.ethereum.api.jsonrpc.execution.JsonRpcExecutor;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.methods.WebSocketMethodsFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.SubscriptionManager;
import org.hyperledger.besu.ethereum.api.util.TestJsonRpcMethodsUtil;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.WebSocket;
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
  private HttpClient httpClient;
  private final int maxConnections = 3;
  private final int maxFrameSize = 1024 * 1024;

  @BeforeEach
  public void before() {
    vertx = Vertx.vertx();
    testContext = new VertxTestContext();

    websocketConfiguration = WebSocketConfiguration.createDefault();
    websocketConfiguration.setPort(0);
    websocketConfiguration.setHostsAllowlist(Collections.singletonList("*"));
    websocketConfiguration.setMaxActiveConnections(maxConnections);
    websocketConfiguration.setMaxFrameSize(maxFrameSize);

    websocketMethods =
        new WebSocketMethodsFactory(
                new SubscriptionManager(new NoOpMetricsSystem()), new HashMap<>())
            .methods();
    webSocketMessageHandlerSpy =
        spy(
            new WebSocketMessageHandler(
                vertx,
                new JsonRpcExecutor(new CombinedJsonRpcProcessor(new NoOpMetricsSystem()), websocketMethods),
                mock(EthScheduler.class),
                TimeoutOptions.defaultOptions().getTimeoutSeconds()));

    websocketService =
        new WebSocketService(
            vertx, websocketConfiguration, webSocketMessageHandlerSpy, new NoOpMetricsSystem());
    websocketService.start().join();

    websocketConfiguration.setPort(websocketService.socketAddress().getPort());

    final HttpClientOptions httpClientOptions =
        new HttpClientOptions()
            .setDefaultHost(websocketConfiguration.getHost())
            .setDefaultPort(websocketConfiguration.getPort());

    httpClient = vertx.createHttpClient(httpClientOptions);
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
      httpClient.webSocket(
          "/",
          future -> {
            if (future.succeeded()) {
              WebSocket ws = future.result();
              ws.handler(
                  buffer -> {
                    assertNotNull(buffer.toString());
                    // assert a successful response
                    assertTrue(buffer.toString().startsWith(expectedResponse1.substring(0, 36)));
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
  }

  @Test
  public void websocketServiceExecutesHandlerOnMessage() throws InterruptedException {

    final String request = "{\"id\": 1, \"method\": \"eth_subscribe\", \"params\": [\"syncing\"]}";
    final String expectedResponse = "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"0x1\"}";

    httpClient.webSocket(
        "/",
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

    testContext.awaitCompletion(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
  }

  @Test
  public void websocketServiceHandlesBinaryFrames() throws InterruptedException {

    httpClient.webSocket(
        "/",
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

    testContext.awaitCompletion(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
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
        .completionHandler(
            v ->
                httpClient.webSocket(
                    "/",
                    websocket -> {
                      websocket.result().close();
                    }));

    testContext.awaitCompletion(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
  }

  @Test
  public void websocketServiceCloseConnectionOnUnrecoverableError() throws InterruptedException {

    final byte[] bigMessage = new byte[maxFrameSize + 1];
    Arrays.fill(bigMessage, (byte) 1);

    httpClient.webSocket(
        "/",
        future -> {
          if (future.succeeded()) {
            WebSocket ws = future.result();
            ws.write(Buffer.buffer(bigMessage));
            ws.closeHandler(v -> testContext.completeNow());
          } else {
            testContext.failNow("websocket connection failed");
          }
        });

    testContext.awaitCompletion(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
  }

  @SuppressWarnings("deprecation") // No alternative available in vertx 3.
  @Test
  public void websocketServiceMustReturnErrorOnHttpRequest() throws InterruptedException {

    httpClient.request(
        HttpMethod.POST,
        websocketConfiguration.getPort(),
        websocketConfiguration.getHost(),
        "/",
        request -> {
          request
              .result()
              .send(
                  response ->
                      response
                          .result()
                          .bodyHandler(
                              b -> {
                                assertEquals(400, response.result().statusCode());
                                assertEquals(
                                    "Websocket endpoint can't handle HTTP requests", b.toString());
                                testContext.completeNow();
                              }));
        });
    testContext.awaitCompletion(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
  }

  @Test
  public void handleLoginRequestWithAuthDisabled() {
    httpClient.request(
        HttpMethod.POST,
        websocketConfiguration.getPort(),
        websocketConfiguration.getHost(),
        "/login",
        request -> {
          request.result().putHeader("Content-Type", "application/json; charset=utf-8");
          request.result().end("{\"username\":\"user\",\"password\":\"pass\"}");
          request
              .result()
              .send(
                  response -> {
                    assertThat(response.result().statusCode()).isEqualTo(400);
                    assertThat(response.result().statusMessage())
                        .isEqualTo("Authentication not enabled");
                  });
        });
  }

  @Test
  public void webSocketDoesNotHandlePingPayloadAsJsonRpcRequest() throws InterruptedException {

    httpClient.webSocket(
        "/",
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

    testContext.awaitCompletion(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
  }

  @Test
  public void handleResponseWithOptionalEmptyValue() throws InterruptedException {
    final JsonRpcMethod method = TestJsonRpcMethodsUtil.optionalEmptyResponse();
    websocketMethods.put(method.getName(), method);

    final String request =
        "{\"id\": 1, \"method\": \"" + method.getName() + "\", \"params\": [\"syncing\"]}";
    final String expectedResponse = "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":null}";

    httpClient.webSocket(
        "/",
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

    testContext.awaitCompletion(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    testContext.verify(() -> websocketMethods.remove(method.getName()));
  }

  @Test
  public void handleResponseWithOptionalExistingValue() throws InterruptedException {
    final JsonRpcMethod method = TestJsonRpcMethodsUtil.optionalResponseWithValue("foo");
    websocketMethods.put(method.getName(), method);

    final String request =
        "{\"id\": 1, \"method\": \"" + method.getName() + "\", \"params\": [\"syncing\"]}";
    final String expectedResponse = "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"foo\"}";

    httpClient.webSocket(
        "/",
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

    testContext.awaitCompletion(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    testContext.verify(() -> websocketMethods.remove(method.getName()));
  }
}
