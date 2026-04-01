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
package org.hyperledger.besu.ethereum.api.jsonrpc.websocket.methods;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.api.handlers.TimeoutOptions;
import org.hyperledger.besu.ethereum.api.jsonrpc.execution.CombinedJsonRpcProcessor;
import org.hyperledger.besu.ethereum.api.jsonrpc.execution.JsonRpcExecutor;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.WebSocketMessageHandler;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.Subscription;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.SubscriptionManager;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.request.SubscriptionType;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.syncing.SyncingSubscription;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.http.WebSocketFrame;
import io.vertx.core.json.Json;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

@ExtendWith(VertxExtension.class)
public class EthSubscribeIntegrationTest {

  private Vertx vertx;
  private VertxTestContext testContext;
  private WebSocketMessageHandler webSocketMessageHandler;
  private SubscriptionManager subscriptionManager;
  private WebSocketMethodsFactory webSocketMethodsFactory;
  private final int ASYNC_TIMEOUT = 5000;
  private final String CONNECTION_ID_1 = "test-connection-id-1";
  private final String CONNECTION_ID_2 = "test-connection-id-2";

  @BeforeEach
  public void before() {
    vertx = Vertx.vertx();
    testContext = new VertxTestContext();
    subscriptionManager = new SubscriptionManager(new NoOpMetricsSystem());
    webSocketMethodsFactory = new WebSocketMethodsFactory(subscriptionManager, new HashMap<>());
    webSocketMessageHandler =
        new WebSocketMessageHandler(
            vertx,
            new JsonRpcExecutor(new CombinedJsonRpcProcessor(new NoOpMetricsSystem()), webSocketMethodsFactory.methods()),
            Mockito.mock(EthScheduler.class),
            TimeoutOptions.defaultOptions().getTimeoutSeconds());
  }

  @Test
  public void shouldAddConnectionToMap() throws InterruptedException {

    final JsonRpcRequest subscribeRequestBody = createEthSubscribeRequestBody(CONNECTION_ID_1);

    final JsonRpcSuccessResponse expectedResponse =
        new JsonRpcSuccessResponse(subscribeRequestBody.getId(), "0x1");

    final ServerWebSocket websocketMock = mock(ServerWebSocket.class);
    when(websocketMock.textHandlerID()).thenReturn(CONNECTION_ID_1);
    when(websocketMock.writeFrame(argThat(this::isFinalFrame)))
        .then(completeOnLastFrame(testContext, websocketMock));

    webSocketMessageHandler.handle(
        websocketMock, Json.encodeToBuffer(subscribeRequestBody), Optional.empty());

    testContext.awaitCompletion(ASYNC_TIMEOUT, TimeUnit.MILLISECONDS);

    final List<SyncingSubscription> syncingSubscriptions = getSubscriptions();
    assertThat(syncingSubscriptions).hasSize(1);
    assertThat(syncingSubscriptions.get(0).getConnectionId()).isEqualTo(CONNECTION_ID_1);
    verify(websocketMock).writeFrame(argThat(isFrameWithAnyText(Json.encode(expectedResponse))));
    verify(websocketMock).writeFrame(argThat(this::isFinalFrame));
  }

  @Test
  public void shouldAddMultipleConnectionsToMap() throws InterruptedException {
    final CountDownLatch latch = new CountDownLatch(2);

    final JsonRpcRequest subscribeRequestBody1 = createEthSubscribeRequestBody(CONNECTION_ID_1);
    final JsonRpcRequest subscribeRequestBody2 = createEthSubscribeRequestBody(CONNECTION_ID_2);

    final JsonRpcSuccessResponse expectedResponse1 =
        new JsonRpcSuccessResponse(subscribeRequestBody1.getId(), "0x1");
    final JsonRpcSuccessResponse expectedResponse2 =
        new JsonRpcSuccessResponse(subscribeRequestBody2.getId(), "0x2");

    final ServerWebSocket websocketMock1 = mock(ServerWebSocket.class);
    when(websocketMock1.textHandlerID()).thenReturn(CONNECTION_ID_1);
    when(websocketMock1.writeFrame(argThat(this::isFinalFrame))).then(countDownOnLastFrame(latch));

    final ServerWebSocket websocketMock2 = mock(ServerWebSocket.class);
    when(websocketMock2.textHandlerID()).thenReturn(CONNECTION_ID_2);
    when(websocketMock2.writeFrame(argThat(this::isFinalFrame))).then(countDownOnLastFrame(latch));

    webSocketMessageHandler.handle(
        websocketMock1, Json.encodeToBuffer(subscribeRequestBody1), Optional.empty());
    webSocketMessageHandler.handle(
        websocketMock2, Json.encodeToBuffer(subscribeRequestBody2), Optional.empty());

    latch.await(ASYNC_TIMEOUT, TimeUnit.MILLISECONDS);

    final List<SyncingSubscription> updatedSubscriptions = getSubscriptions();
    assertThat(updatedSubscriptions).hasSize(2);
    final List<String> connectionIds =
        updatedSubscriptions.stream()
            .map(Subscription::getConnectionId)
            .collect(Collectors.toList());
    assertThat(connectionIds).containsExactlyInAnyOrder(CONNECTION_ID_1, CONNECTION_ID_2);

    verify(websocketMock1)
        .writeFrame(
            argThat(
                isFrameWithAnyText(
                    Json.encode(expectedResponse1), Json.encode(expectedResponse2))));
    verify(websocketMock1).writeFrame(argThat(this::isFinalFrame));

    verify(websocketMock2)
        .writeFrame(
            argThat(
                isFrameWithAnyText(
                    Json.encode(expectedResponse1), Json.encode(expectedResponse2))));
    verify(websocketMock2).writeFrame(argThat(this::isFinalFrame));
  }

  private List<SyncingSubscription> getSubscriptions() {
    return subscriptionManager.subscriptionsOfType(
        SubscriptionType.SYNCING, SyncingSubscription.class);
  }

  private WebSocketRpcRequest createEthSubscribeRequestBody(final String connectionId) {
    return Json.decodeValue(
        "{\"id\": 1, \"method\": \"eth_subscribe\", \"params\": [\"syncing\"], \"connectionId\": \""
            + connectionId
            + "\"}",
        WebSocketRpcRequest.class);
  }

  private ArgumentMatcher<WebSocketFrame> isFrameWithAnyText(final String... text) {
    return f -> f.isText() && Stream.of(text).anyMatch(t -> t.equals(f.textData()));
  }

  private boolean isFinalFrame(final WebSocketFrame frame) {
    return frame.isFinal();
  }

  private Answer<ServerWebSocket> completeOnLastFrame(
      final VertxTestContext testContext, final ServerWebSocket websocket) {
    return invocation -> {
      testContext.completeNow();
      return websocket;
    };
  }

  private Answer<Future<Void>> countDownOnLastFrame(final CountDownLatch latch) {
    return invocation -> {
      latch.countDown();
      return Future.succeededFuture();
    };
  }
}
