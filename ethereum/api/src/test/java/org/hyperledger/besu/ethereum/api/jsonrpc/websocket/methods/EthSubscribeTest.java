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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.Quantity;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.MaxSubscriptionsExceededException;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.SubscriptionManager;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.request.InvalidSubscriptionRequestException;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.request.SubscribeRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.request.SubscriptionRequestMapper;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.request.SubscriptionType;

import io.vertx.core.json.Json;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EthSubscribeTest {

  private EthSubscribe ethSubscribe;
  private SubscriptionManager subscriptionManagerMock;
  private SubscriptionRequestMapper mapperMock;

  @BeforeEach
  public void before() {
    subscriptionManagerMock = mock(SubscriptionManager.class);
    mapperMock = mock(SubscriptionRequestMapper.class);
    ethSubscribe = new EthSubscribe(subscriptionManagerMock, mapperMock);
  }

  @Test
  public void nameIsEthSubscribe() {
    assertThat(ethSubscribe.getName()).isEqualTo("eth_subscribe");
  }

  @Test
  public void responseContainsSubscriptionId() {
    final WebSocketRpcRequest webSocketRequest = createWebSocketRpcRequest();
    final JsonRpcRequestContext jsonRpcrequestContext = new JsonRpcRequestContext(webSocketRequest);

    final SubscribeRequest subscribeRequest =
        new SubscribeRequest(
            SubscriptionType.SYNCING, null, null, webSocketRequest.getConnectionId());

    when(mapperMock.mapSubscribeRequest(eq(jsonRpcrequestContext))).thenReturn(subscribeRequest);
    when(subscriptionManagerMock.subscribe(eq(subscribeRequest))).thenReturn(1L);

    final JsonRpcSuccessResponse expectedResponse =
        new JsonRpcSuccessResponse(
            jsonRpcrequestContext.getRequest().getId(), Quantity.create((1L)));

    assertThat(ethSubscribe.response(jsonRpcrequestContext)).isEqualTo(expectedResponse);
  }

  @Test
  public void invalidSubscribeRequestRespondsInvalidRequestResponse() {
    final WebSocketRpcRequest webSocketRequest = createWebSocketRpcRequest();
    final JsonRpcRequestContext jsonRpcrequestContext = new JsonRpcRequestContext(webSocketRequest);

    when(mapperMock.mapSubscribeRequest(any()))
        .thenThrow(new InvalidSubscriptionRequestException());

    final JsonRpcErrorResponse expectedResponse =
        new JsonRpcErrorResponse(
            jsonRpcrequestContext.getRequest().getId(), RpcErrorType.INVALID_REQUEST);

    assertThat(ethSubscribe.response(jsonRpcrequestContext)).isEqualTo(expectedResponse);
  }

  @Test
  public void uncaughtErrorOnSubscriptionManagerShouldRespondInternalErrorResponse() {
    final WebSocketRpcRequest webSocketRequest = createWebSocketRpcRequest();
    final JsonRpcRequestContext jsonRpcrequestContext = new JsonRpcRequestContext(webSocketRequest);

    when(mapperMock.mapSubscribeRequest(any())).thenReturn(mock(SubscribeRequest.class));
    when(subscriptionManagerMock.subscribe(any())).thenThrow(new RuntimeException());

    final JsonRpcErrorResponse expectedResponse =
        new JsonRpcErrorResponse(
            jsonRpcrequestContext.getRequest().getId(), RpcErrorType.INTERNAL_ERROR);

    assertThat(ethSubscribe.response(jsonRpcrequestContext)).isEqualTo(expectedResponse);
  }

  @Test
  public void maxSubscriptionsExceededShouldRespondWithExceedsMaxActiveSubscriptionsError() {
    final WebSocketRpcRequest webSocketRequest = createWebSocketRpcRequest();
    final JsonRpcRequestContext jsonRpcrequestContext = new JsonRpcRequestContext(webSocketRequest);

    when(mapperMock.mapSubscribeRequest(any())).thenReturn(mock(SubscribeRequest.class));
    when(subscriptionManagerMock.subscribe(any()))
        .thenThrow(new MaxSubscriptionsExceededException(1));

    final JsonRpcErrorResponse expectedResponse =
        new JsonRpcErrorResponse(
            jsonRpcrequestContext.getRequest().getId(),
            RpcErrorType.EXCEEDS_RPC_MAX_ACTIVE_SUBSCRIPTIONS);

    assertThat(ethSubscribe.response(jsonRpcrequestContext)).isEqualTo(expectedResponse);
  }

  private WebSocketRpcRequest createWebSocketRpcRequest() {
    return Json.decodeValue(
        "{\"id\": 1, \"method\": \"eth_subscribe\", \"params\": [\"syncing\"], \"connectionId\": \"1\"}",
        WebSocketRpcRequest.class);
  }
}
