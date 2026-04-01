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

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.api.handlers.TimeoutOptions;
import org.hyperledger.besu.ethereum.api.jsonrpc.execution.CombinedJsonRpcProcessor;
import org.hyperledger.besu.ethereum.api.jsonrpc.execution.JsonRpcExecutor;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.methods.WebSocketRpcRequest;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.http.WebSocketFrame;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

@ExtendWith(VertxExtension.class)
public class WebSocketMessageHandlerTest {

  private static final int VERTX_AWAIT_TIMEOUT_MILLIS = 10000;
  private Vertx vertx;
  private VertxTestContext testContext;
  private WebSocketMessageHandler handler;
  private JsonRpcMethod jsonRpcMethodMock;
  private ServerWebSocket websocketMock;
  private final Map<String, JsonRpcMethod> methods = new HashMap<>();

  @BeforeEach
  public void before() {
    vertx = Vertx.vertx(new VertxOptions().setPreferNativeTransport(true));
    testContext = new VertxTestContext();

    jsonRpcMethodMock = mock(JsonRpcMethod.class);
    websocketMock = mock(ServerWebSocket.class);

    when(websocketMock.textHandlerID()).thenReturn(UUID.randomUUID().toString());

    methods.put("eth_x", jsonRpcMethodMock);
    handler =
        new WebSocketMessageHandler(
            vertx,
            new JsonRpcExecutor(new CombinedJsonRpcProcessor(new NoOpMetricsSystem()), methods),
            mock(EthScheduler.class),
            TimeoutOptions.defaultOptions().getTimeoutSeconds());
  }

  @AfterEach
  public void after() throws Throwable {
    Mockito.reset(jsonRpcMethodMock);
    Mockito.reset(websocketMock);
  }

  @Test
  public void handlerDeliversResponseSuccessfully() throws InterruptedException {

    final JsonObject requestJson = new JsonObject().put("id", 1).put("method", "eth_x");
    final JsonRpcRequest requestBody = requestJson.mapTo(WebSocketRpcRequest.class);
    final JsonRpcRequestContext expectedRequest = new JsonRpcRequestContext(requestBody);

    final JsonRpcSuccessResponse expectedResponse =
        new JsonRpcSuccessResponse(requestBody.getId(), null);

    when(jsonRpcMethodMock.response(eq(expectedRequest))).thenReturn(expectedResponse);

    when(websocketMock.writeFrame(argThat(this::isFinalFrame)))
        .then(completeOnLastFrame(testContext));

    handler.handle(websocketMock, requestJson.toBuffer(), Optional.empty());

    testContext.awaitCompletion(
        WebSocketMessageHandlerTest.VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

    // can verify only after async not before
    verify(websocketMock).writeFrame(argThat(isFrameWithText(Json.encode(expectedResponse))));
    verify(websocketMock).writeFrame(argThat(this::isFinalFrame));
    verify(jsonRpcMethodMock).response(eq(expectedRequest));
  }

  @Test
  public void handlerBatchRequestDeliversResponseSuccessfully() throws InterruptedException {

    final JsonObject requestJson = new JsonObject().put("id", 1).put("method", "eth_x");
    final JsonArray arrayJson = new JsonArray(List.of(requestJson, requestJson));
    final JsonRpcRequest requestBody = requestJson.mapTo(WebSocketRpcRequest.class);
    final JsonRpcRequestContext expectedRequest = new JsonRpcRequestContext(requestBody);
    final JsonRpcSuccessResponse expectedSingleResponse =
        new JsonRpcSuccessResponse(requestBody.getId(), null);

    final JsonArray expectedBatchResponse =
        new JsonArray(List.of(expectedSingleResponse, expectedSingleResponse));

    when(jsonRpcMethodMock.response(eq(expectedRequest))).thenReturn(expectedSingleResponse);

    when(websocketMock.writeFrame(argThat(this::isFinalFrame)))
        .then(completeOnLastFrame(testContext));

    handler.handle(websocketMock, arrayJson.toBuffer(), Optional.empty());

    testContext.awaitCompletion(
        WebSocketMessageHandlerTest.VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    // can verify only after async not before
    verify(websocketMock).writeFrame(argThat(isFrameWithText(Json.encode(expectedBatchResponse))));
    verify(websocketMock).writeFrame(argThat(this::isFinalFrame));
    verify(jsonRpcMethodMock, Mockito.times(2)).response(eq(expectedRequest));
  }

  @Test
  public void handlerBatchRequestContainingErrorsShouldRespondWithBatchErrors()
      throws InterruptedException {
    ServerWebSocket websocketMock = mock(ServerWebSocket.class);

    when(websocketMock.textHandlerID()).thenReturn(UUID.randomUUID().toString());

    WebSocketMessageHandler handleBadCalls =
        new WebSocketMessageHandler(
            vertx,
            new JsonRpcExecutor(new CombinedJsonRpcProcessor(new NoOpMetricsSystem()), methods),
            mock(EthScheduler.class),
            TimeoutOptions.defaultOptions().getTimeoutSeconds());

    final JsonObject requestJson =
        new JsonObject().put("id", 1).put("method", "eth_nonexistentMethod");
    final JsonRpcErrorResponse expectedErrorResponse1 =
        new JsonRpcErrorResponse(1, RpcErrorType.METHOD_NOT_FOUND);

    final JsonArray arrayJson = new JsonArray(List.of(requestJson, requestJson));

    final JsonArray expectedBatchResponse =
        new JsonArray(List.of(expectedErrorResponse1, expectedErrorResponse1));

    when(websocketMock.writeFrame(argThat(this::isFinalFrame)))
        .then(completeOnLastFrame(testContext));

    handleBadCalls.handle(websocketMock, arrayJson.toBuffer(), Optional.empty());

    testContext.awaitCompletion(
        WebSocketMessageHandlerTest.VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

    // can verify only after async not before
    verify(websocketMock).writeFrame(argThat(isFrameWithText(Json.encode(expectedBatchResponse))));
    verify(websocketMock).writeFrame(argThat(this::isFinalFrame));
    verifyNoInteractions(jsonRpcMethodMock);
  }

  @Test
  public void jsonDecodeFailureShouldRespondInvalidRequest() throws InterruptedException {

    final JsonRpcErrorResponse expectedResponse =
        new JsonRpcErrorResponse(null, RpcErrorType.INVALID_REQUEST);

    when(websocketMock.writeFrame(argThat(this::isFinalFrame)))
        .then(completeOnLastFrame(testContext));

    handler.handle(websocketMock, Buffer.buffer(), Optional.empty());

    testContext.awaitCompletion(
        WebSocketMessageHandlerTest.VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

    // can verify only after async not before
    verify(websocketMock).writeFrame(argThat(isFrameWithText(Json.encode(expectedResponse))));
    verify(websocketMock).writeFrame(argThat(this::isFinalFrame));
    verifyNoInteractions(jsonRpcMethodMock);
  }

  @Test
  public void objectMapperFailureShouldRespondInvalidRequest() throws InterruptedException {

    final JsonRpcErrorResponse expectedResponse =
        new JsonRpcErrorResponse(null, RpcErrorType.INVALID_REQUEST);

    when(websocketMock.writeFrame(argThat(this::isFinalFrame)))
        .then(completeOnLastFrame(testContext));

    handler.handle(websocketMock, new JsonObject().toBuffer(), Optional.empty());

    testContext.awaitCompletion(
        WebSocketMessageHandlerTest.VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

    // can verify only after async not before
    verify(websocketMock).writeFrame(argThat(isFrameWithText(Json.encode(expectedResponse))));
    verify(websocketMock).writeFrame(argThat(this::isFinalFrame));
    verifyNoInteractions(jsonRpcMethodMock);
  }

  @Test
  public void absentMethodShouldRespondMethodNotFound() throws InterruptedException {

    final JsonObject requestJson =
        new JsonObject().put("id", 1).put("method", "eth_nonexistentMethod");
    final JsonRpcErrorResponse expectedResponse =
        new JsonRpcErrorResponse(1, RpcErrorType.METHOD_NOT_FOUND);

    when(websocketMock.writeFrame(argThat(this::isFinalFrame)))
        .then(completeOnLastFrame(testContext));

    handler.handle(websocketMock, requestJson.toBuffer(), Optional.empty());

    testContext.awaitCompletion(
        WebSocketMessageHandlerTest.VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

    verify(websocketMock).writeFrame(argThat(isFrameWithText(Json.encode(expectedResponse))));
    verify(websocketMock).writeFrame(argThat(this::isFinalFrame));
    verifyNoInteractions(jsonRpcMethodMock);
  }

  @Test
  public void onInvalidJsonRpcParametersExceptionProcessingRequestShouldRespondInvalidParams()
      throws InterruptedException {

    final JsonObject requestJson = new JsonObject().put("id", 1).put("method", "eth_x");
    final JsonRpcRequestContext expectedRequest =
        new JsonRpcRequestContext(requestJson.mapTo(WebSocketRpcRequest.class));
    when(jsonRpcMethodMock.response(eq(expectedRequest)))
        .thenThrow(new InvalidJsonRpcParameters(""));
    final JsonRpcErrorResponse expectedResponse =
        new JsonRpcErrorResponse(1, RpcErrorType.INVALID_PARAMS);

    when(websocketMock.writeFrame(argThat(this::isFinalFrame)))
        .then(completeOnLastFrame(testContext));

    handler.handle(websocketMock, requestJson.toBuffer(), Optional.empty());

    testContext.awaitCompletion(
        WebSocketMessageHandlerTest.VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

    // can verify only after async not before
    verify(websocketMock).writeFrame(argThat(isFrameWithText(Json.encode(expectedResponse))));
    verify(websocketMock).writeFrame(argThat(this::isFinalFrame));
  }

  @Test
  public void onExceptionProcessingRequestShouldRespondInternalError() throws InterruptedException {

    final JsonObject requestJson = new JsonObject().put("id", 1).put("method", "eth_x");
    final JsonRpcRequestContext expectedRequest =
        new JsonRpcRequestContext(requestJson.mapTo(WebSocketRpcRequest.class));
    when(jsonRpcMethodMock.response(eq(expectedRequest))).thenThrow(new RuntimeException());
    final JsonRpcErrorResponse expectedResponse =
        new JsonRpcErrorResponse(1, RpcErrorType.INTERNAL_ERROR);

    when(websocketMock.writeFrame(argThat(this::isFinalFrame)))
        .then(completeOnLastFrame(testContext));

    handler.handle(websocketMock, requestJson.toBuffer(), Optional.empty());

    testContext.awaitCompletion(
        WebSocketMessageHandlerTest.VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

    // can verify only after async not before
    verify(websocketMock).writeFrame(argThat(isFrameWithText(Json.encode(expectedResponse))));
    verify(websocketMock).writeFrame(argThat(this::isFinalFrame));
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
