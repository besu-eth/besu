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
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.SubscriptionManager;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.request.SubscribeRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.request.SubscriptionType;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.http.WebSocketFrame;
import io.vertx.core.json.Json;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatcher;
import org.mockito.stubbing.Answer;

@ExtendWith(VertxExtension.class)
public class EthUnsubscribeIntegrationTest {

  private Vertx vertx;
  private VertxTestContext testContext;
  private WebSocketMessageHandler webSocketMessageHandler;
  private SubscriptionManager subscriptionManager;
  private WebSocketMethodsFactory webSocketMethodsFactory;
  private final int ASYNC_TIMEOUT = 5000;
  private final String CONNECTION_ID = "test-connection-id-1";

  @BeforeEach
  public void before() {
    vertx = Vertx.vertx(new VertxOptions().setPreferNativeTransport(true));
    testContext = new VertxTestContext();
    subscriptionManager = new SubscriptionManager(new NoOpMetricsSystem());
    webSocketMethodsFactory = new WebSocketMethodsFactory(subscriptionManager, new HashMap<>());
    webSocketMessageHandler =
        new WebSocketMessageHandler(
            vertx,
            new JsonRpcExecutor(new CombinedJsonRpcProcessor(new NoOpMetricsSystem()), webSocketMethodsFactory.methods()),
            mock(EthScheduler.class),
            TimeoutOptions.defaultOptions().getTimeoutSeconds());
  }

  @Test
  public void shouldRemoveConnectionWithSingleSubscriptionFromMap() throws InterruptedException {

    // Add the subscription we'd like to remove
    final SubscribeRequest subscribeRequest =
        new SubscribeRequest(SubscriptionType.SYNCING, null, null, CONNECTION_ID);
    final Long subscriptionId = subscriptionManager.subscribe(subscribeRequest);
    assertThat(subscriptionManager.getSubscriptionById(subscriptionId)).isNotNull();

    final JsonRpcRequest unsubscribeRequestBody =
        createEthUnsubscribeRequestBody(subscriptionId, CONNECTION_ID);

    final JsonRpcSuccessResponse expectedResponse =
        new JsonRpcSuccessResponse(unsubscribeRequestBody.getId(), Boolean.TRUE);

    final ServerWebSocket websocketMock = mock(ServerWebSocket.class);
    when(websocketMock.textHandlerID()).thenReturn(CONNECTION_ID);
    when(websocketMock.writeFrame(argThat(this::isFinalFrame)))
        .then(completeOnLastFrame(testContext));

    webSocketMessageHandler.handle(
        websocketMock, Json.encodeToBuffer(unsubscribeRequestBody), Optional.empty());

    testContext.awaitCompletion(ASYNC_TIMEOUT, TimeUnit.MILLISECONDS);
    assertThat(subscriptionManager.getSubscriptionById(subscriptionId)).isNull();
    verify(websocketMock).writeFrame(argThat(isFrameWithText(Json.encode(expectedResponse))));
    verify(websocketMock).writeFrame(argThat(this::isFinalFrame));
  }

  @Test
  public void shouldRemoveSubscriptionAndKeepConnection() throws InterruptedException {

    // Add the subscriptions we'd like to remove
    final SubscribeRequest subscribeRequest =
        new SubscribeRequest(SubscriptionType.SYNCING, null, null, CONNECTION_ID);
    final Long subscriptionId1 = subscriptionManager.subscribe(subscribeRequest);
    final Long subscriptionId2 = subscriptionManager.subscribe(subscribeRequest);

    assertThat(subscriptionManager.getSubscriptionById(subscriptionId1)).isNotNull();
    assertThat(subscriptionManager.getSubscriptionById(subscriptionId2)).isNotNull();

    final JsonRpcRequest unsubscribeRequestBody =
        createEthUnsubscribeRequestBody(subscriptionId2, CONNECTION_ID);

    final JsonRpcSuccessResponse expectedResponse =
        new JsonRpcSuccessResponse(unsubscribeRequestBody.getId(), Boolean.TRUE);

    final ServerWebSocket websocketMock = mock(ServerWebSocket.class);
    when(websocketMock.textHandlerID()).thenReturn(CONNECTION_ID);
    when(websocketMock.writeFrame(argThat(this::isFinalFrame)))
        .then(completeOnLastFrame(testContext));

    webSocketMessageHandler.handle(
        websocketMock, Json.encodeToBuffer(unsubscribeRequestBody), Optional.empty());

    testContext.awaitCompletion(ASYNC_TIMEOUT, TimeUnit.MILLISECONDS);
    assertThat(subscriptionManager.getSubscriptionById(subscriptionId1)).isNotNull();
    assertThat(subscriptionManager.getSubscriptionById(subscriptionId2)).isNull();
    verify(websocketMock).writeFrame(argThat(isFrameWithText(Json.encode(expectedResponse))));
    verify(websocketMock).writeFrame(argThat(this::isFinalFrame));
  }

  private JsonRpcRequest createEthUnsubscribeRequestBody(
      final Long subscriptionId, final String connectionId) {
    return Json.decodeValue(
        "{\"id\": 1, \"method\": \"eth_unsubscribe\", \"params\": [\""
            + subscriptionId
            + "\"], \"connectionId\": \""
            + connectionId
            + "\"}",
        WebSocketRpcRequest.class);
  }

  private ArgumentMatcher<WebSocketFrame> isFrameWithText(final String text) {
    return f -> f.isText() && f.textData().equals(text);
  }

  private boolean isFinalFrame(final WebSocketFrame frame) {
    return frame.isFinal();
  }

  private Answer<Future<Void>> completeOnLastFrame(final VertxTestContext testContext) {
    return invocation -> {
      testContext.completeNow();
      return Future.succeededFuture();
    };
  }
}
