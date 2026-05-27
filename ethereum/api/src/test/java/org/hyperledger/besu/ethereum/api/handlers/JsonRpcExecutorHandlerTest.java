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
package org.hyperledger.besu.ethereum.api.handlers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.context.ContextKey;
import org.hyperledger.besu.ethereum.api.jsonrpc.execution.JsonRpcExecutor;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.opentelemetry.api.trace.Tracer;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.impl.future.SucceededFuture;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class JsonRpcExecutorHandlerTest {

  private JsonRpcExecutor mockExecutor;
  private Tracer mockTracer;
  private JsonRpcConfiguration mockConfig;
  private RoutingContext mockContext;
  private Vertx mockVertx;
  private HttpServerResponse mockResponse;
  private final long timeoutSeconds = 22;

  @BeforeEach
  void setUp() {
    mockExecutor = mock(JsonRpcExecutor.class);
    mockTracer = mock(Tracer.class);
    mockConfig = mock(JsonRpcConfiguration.class);
    mockContext = mock(RoutingContext.class);
    mockVertx = mock(Vertx.class);
    mockResponse = mock(HttpServerResponse.class);

    when(mockConfig.getHttpTimeoutSec()).thenReturn(timeoutSeconds);
    when(mockContext.vertx()).thenReturn(mockVertx);
    when(mockContext.response()).thenReturn(mockResponse);
    when(mockResponse.ended()).thenReturn(false);
    when(mockResponse.setStatusCode(anyInt())).thenReturn(mockResponse);
  }

  @Test
  void testTimeoutHandling() {
    // Arrange
    Handler<RoutingContext> handler =
        JsonRpcExecutorHandler.handler(mockExecutor, mockTracer, mockConfig);
    ArgumentCaptor<Long> delayCaptor = ArgumentCaptor.forClass(Long.class);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Handler<Long>> timerHandlerCaptor = ArgumentCaptor.forClass(Handler.class);

    when(mockContext.get(eq(ContextKey.REQUEST_BODY_AS_JSON_OBJECT.name()))).thenReturn("{}");
    when(mockVertx.setTimer(delayCaptor.capture(), timerHandlerCaptor.capture())).thenReturn(1L);
    when(mockContext.get("timerId")).thenReturn(1L);

    // Act
    handler.handle(mockContext);

    // Assert
    long timeoutMillis = timeoutSeconds * 1000;
    verify(mockVertx).setTimer(eq(timeoutMillis), any());

    // Simulate timeout
    timerHandlerCaptor.getValue().handle(1L);

    // Verify timeout handling
    verify(mockResponse, times(1))
        .setStatusCode(eq(HttpResponseStatus.REQUEST_TIMEOUT.code())); // Expect 408 Request Timeout
    verify(mockResponse, times(1)).end(contains("Timeout expired"));
    verify(mockVertx, times(1)).cancelTimer(1L);
  }

  @Test
  void testCancelTimerOnSuccessfulExecution() {
    // Arrange
    Handler<RoutingContext> handler =
        JsonRpcExecutorHandler.handler(mockExecutor, mockTracer, mockConfig);
    when(mockContext.get(eq(ContextKey.REQUEST_BODY_AS_JSON_OBJECT.name()))).thenReturn("{}");
    when(mockVertx.setTimer(anyLong(), any())).thenReturn(1L);
    when(mockContext.get("timerId")).thenReturn(1L);

    // Act
    handler.handle(mockContext);

    // Assert
    verify(mockVertx).setTimer(anyLong(), any());
    verify(mockVertx).cancelTimer(1L);
  }

  // --- Streaming error handling tests ---

  /**
   * Set up the mock context so the handler creates a JsonRpcObjectExecutor for a streaming method.
   */
  private void setUpStreamingContext() {
    final JsonObject jsonRequest =
        new JsonObject()
            .put("jsonrpc", "2.0")
            .put("id", 1)
            .put("method", "debug_traceBlockByNumber");

    // isJsonObjectRequest(ctx) checks ctx.data().containsKey(...)
    final Map<String, Object> contextData = new HashMap<>();
    contextData.put(ContextKey.REQUEST_BODY_AS_JSON_OBJECT.name(), jsonRequest);
    when(mockContext.data()).thenReturn(contextData);
    when(mockContext.get(ContextKey.REQUEST_BODY_AS_JSON_OBJECT.name())).thenReturn(jsonRequest);

    // Make the method recognized as streaming
    when(mockExecutor.isStreamingMethod("debug_traceBlockByNumber")).thenReturn(true);

    // Mock HttpServerRequest (needed by JsonResponseStreamer constructor)
    final HttpServerRequest mockRequest = mock(HttpServerRequest.class);
    when(mockContext.request()).thenReturn(mockRequest);
    when(mockRequest.remoteAddress()).thenReturn(SocketAddress.domainSocketAddress("test"));

    // Mock response methods used in the streaming path
    when(mockResponse.putHeader(any(CharSequence.class), any(CharSequence.class)))
        .thenReturn(mockResponse);
    when(mockResponse.setChunked(anyBoolean())).thenReturn(mockResponse);
    when(mockResponse.exceptionHandler(any())).thenReturn(mockResponse);
    when(mockResponse.write(any(Buffer.class))).thenReturn(new SucceededFuture<>(null, null));
    when(mockResponse.headWritten()).thenReturn(false);
    when(mockResponse.closed()).thenReturn(false);

    // Timer setup (not the focus of these tests, but required by the handler)
    when(mockVertx.setTimer(anyLong(), any())).thenReturn(1L);
    when(mockContext.get("timerId")).thenReturn(1L);
  }

  @Test
  void streamingPreStreamValidationError_sendsProperHttpErrorResponse() throws Exception {
    setUpStreamingContext();

    // executeStreaming returns a validation error (e.g., bad request) before any data is written
    when(mockExecutor.executeStreaming(any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(Optional.of(new JsonRpcErrorResponse(1, RpcErrorType.INVALID_REQUEST)));

    Handler<RoutingContext> handler =
        JsonRpcExecutorHandler.handler(mockExecutor, mockTracer, mockConfig);
    handler.handle(mockContext);

    // Should send 400 BAD_REQUEST (INVALID_REQUEST maps to BAD_REQUEST)
    verify(mockResponse).setStatusCode(eq(HttpResponseStatus.BAD_REQUEST.code()));
    // The response should be written (error body) and ended, NOT reset
    verify(mockResponse, never()).reset();
  }

  @Test
  void streamingInvalidParamsBeforeHeaders_sendsProperHttpError() throws Exception {
    setUpStreamingContext();

    // Processor chain throws InvalidJsonRpcParameters before any data is written
    when(mockExecutor.executeStreaming(any(), any(), any(), any(), any(), any(), any(), any()))
        .thenThrow(
            new InvalidJsonRpcParameters(
                "Invalid block number", RpcErrorType.INVALID_BLOCK_NUMBER_PARAMS));

    Handler<RoutingContext> handler =
        JsonRpcExecutorHandler.handler(mockExecutor, mockTracer, mockConfig);
    handler.handle(mockContext);

    // Headers not sent yet → proper error response via handleJsonRpcError
    // INVALID_BLOCK_NUMBER_PARAMS → default HTTP 200 in statusCodeFromError, with error body
    verify(mockResponse).setStatusCode(eq(HttpResponseStatus.OK.code()));
    verify(mockResponse).end(contains("Invalid block number params"));
    verify(mockResponse, never()).reset();
  }

  @Test
  void streamingExceptionAfterHeaders_resetsConnection() throws Exception {
    setUpStreamingContext();
    when(mockResponse.headWritten()).thenReturn(true);

    // Exception thrown mid-stream — headers are already flushed
    when(mockExecutor.executeStreaming(any(), any(), any(), any(), any(), any(), any(), any()))
        .thenThrow(new RuntimeException("trace execution failed"));

    Handler<RoutingContext> handler =
        JsonRpcExecutorHandler.handler(mockExecutor, mockTracer, mockConfig);
    handler.handle(mockContext);

    // Headers already sent → cannot change status code, must reset the connection
    verify(mockResponse).reset();
    // setStatusCode is only called once by prepareHttpResponse for Content-Type setup, not for
    // error
    verify(mockResponse, never()).setStatusCode(anyInt());
  }

  @Test
  void streamingTimeoutAfterHeaders_resetsInsteadOfSettingStatus() {
    // This tests the timer handler behavior when streaming is already in progress.
    // Capture the timer handler, then invoke it with headWritten=true.
    setUpStreamingContext();

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Handler<Long>> timerHandlerCaptor = ArgumentCaptor.forClass(Handler.class);
    when(mockVertx.setTimer(anyLong(), timerHandlerCaptor.capture())).thenReturn(1L);

    // Streaming completed or is in progress — headers already written
    when(mockResponse.headWritten()).thenReturn(true);

    Handler<RoutingContext> handler =
        JsonRpcExecutorHandler.handler(mockExecutor, mockTracer, mockConfig);
    handler.handle(mockContext);

    // Simulate timeout firing while streaming is in progress
    timerHandlerCaptor.getValue().handle(1L);

    // Should reset (not try to setStatusCode which would throw IllegalStateException)
    verify(mockResponse).reset();
    verify(mockResponse, never()).setStatusCode(anyInt());
  }
}
