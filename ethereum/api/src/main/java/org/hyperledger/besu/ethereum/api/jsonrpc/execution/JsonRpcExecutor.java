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
package org.hyperledger.besu.ethereum.api.jsonrpc.execution;

import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.INVALID_REQUEST;

import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestId;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcNoResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonRpcExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(JsonRpcExecutor.class);

  private final JsonRpcProcessor rpcProcessor;
  private final Map<String, JsonRpcMethod> rpcMethods;

  private record PreparedRequest(
      JsonRpcRequestId id, JsonRpcMethod method, Span span, JsonRpcRequestContext context) {}

  public JsonRpcExecutor(
      final JsonRpcProcessor rpcProcessor, final Map<String, JsonRpcMethod> rpcMethods) {
    this.rpcProcessor = rpcProcessor;
    this.rpcMethods = rpcMethods;
  }

  public JsonRpcResponse execute(
      final Optional<User> optionalUser,
      final Tracer tracer,
      final Context spanContext,
      final Supplier<Boolean> alive,
      final JsonObject jsonRpcRequest,
      final Function<JsonObject, JsonRpcRequest> requestBodyProvider) {
    final Object prepared =
        prepareExecution(
            optionalUser, tracer, spanContext, alive, jsonRpcRequest, requestBodyProvider);
    if (prepared == null) {
      return new JsonRpcNoResponse();
    }
    if (prepared instanceof JsonRpcResponse response) {
      return response;
    }
    final PreparedRequest req = (PreparedRequest) prepared;
    return rpcProcessor.process(req.id(), req.method(), req.span(), req.context());
  }

  public boolean isStreamingMethod(final String methodName) {
    final JsonRpcMethod method = rpcMethods.get(methodName);
    return method != null && method.isStreaming();
  }

  /**
   * Executes a streaming JSON-RPC method. If the request fails validation (bad params, missing
   * method, notification, etc.) the error response is returned without writing to {@code out},
   * allowing the caller to send a proper HTTP error before headers are flushed.
   *
   * @return an error/no-op response if streaming did not start, or empty on success
   */
  public Optional<JsonRpcResponse> executeStreaming(
      final Optional<User> optionalUser,
      final Tracer tracer,
      final Context spanContext,
      final Supplier<Boolean> alive,
      final JsonObject jsonRpcRequest,
      final Function<JsonObject, JsonRpcRequest> requestBodyProvider,
      final OutputStream out,
      final ObjectMapper mapper)
      throws IOException {
    final Object prepared =
        prepareExecution(
            optionalUser, tracer, spanContext, alive, jsonRpcRequest, requestBodyProvider);
    if (prepared == null) {
      return Optional.of(new JsonRpcNoResponse());
    }
    if (prepared instanceof JsonRpcResponse response) {
      return Optional.of(response);
    }
    final PreparedRequest req = (PreparedRequest) prepared;
    rpcProcessor.streamProcess(req.id(), req.method(), req.span(), req.context(), out, mapper);
    return Optional.empty();
  }

  /**
   * Shared preamble for execute/executeStreaming: parses the request, creates the span, and
   * validates method availability. Returns a {@link PreparedRequest} if ready to process, a {@link
   * JsonRpcResponse} if there was a validation error, or null for notifications.
   */
  private Object prepareExecution(
      final Optional<User> optionalUser,
      final Tracer tracer,
      final Context spanContext,
      final Supplier<Boolean> alive,
      final JsonObject jsonRpcRequest,
      final Function<JsonObject, JsonRpcRequest> requestBodyProvider) {
    try {
      final JsonRpcRequest requestBody = requestBodyProvider.apply(jsonRpcRequest);
      final JsonRpcRequestId id = new JsonRpcRequestId(requestBody.getId());
      if (requestBody.isNotification()) {
        return null;
      }
      final Span span =
          tracer != null
              ? tracer
                  .spanBuilder(requestBody.getMethod())
                  .setSpanKind(SpanKind.INTERNAL)
                  .setParent(spanContext)
                  .startSpan()
              : Span.getInvalid();

      final Optional<RpcErrorType> unavailableMethod = validateMethodAvailability(requestBody);
      if (unavailableMethod.isPresent()) {
        span.setStatus(StatusCode.ERROR, "method unavailable");
        return new JsonRpcErrorResponse(id, unavailableMethod.get());
      }

      final JsonRpcMethod method = rpcMethods.get(requestBody.getMethod());
      return new PreparedRequest(
          id, method, span, new JsonRpcRequestContext(requestBody, optionalUser, alive));
    } catch (final IllegalArgumentException e) {
      try {
        final Integer id = jsonRpcRequest.getInteger("id", null);
        return new JsonRpcErrorResponse(id, INVALID_REQUEST);
      } catch (final ClassCastException idNotIntegerException) {
        return new JsonRpcErrorResponse(null, INVALID_REQUEST);
      }
    }
  }

  private Optional<RpcErrorType> validateMethodAvailability(final JsonRpcRequest request) {
    final String name = request.getMethod();

    if (LOG.isTraceEnabled()) {
      final JsonArray params = JsonObject.mapFrom(request).getJsonArray("params");
      LOG.trace("JSON-RPC request -> {} {}", name, params);
    }

    final JsonRpcMethod method = rpcMethods.get(name);

    if (method == null) {
      if (!RpcMethod.rpcMethodExists(name)) {
        return Optional.of(RpcErrorType.METHOD_NOT_FOUND);
      }
      if (!rpcMethods.containsKey(name)) {
        return Optional.of(RpcErrorType.METHOD_NOT_ENABLED);
      }
    }

    return Optional.empty();
  }
}
