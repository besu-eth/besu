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

import static io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_JSON;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.INVALID_REQUEST;

import org.hyperledger.besu.ethereum.api.jsonrpc.JsonResponseStreamer;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.context.ContextKey;
import org.hyperledger.besu.ethereum.api.jsonrpc.execution.JsonRpcExecutor;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.io.IOException;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonRpcExecutorHandler {
  private static final Logger LOG = LoggerFactory.getLogger(JsonRpcExecutorHandler.class);

  private static final String SPAN_CONTEXT = "span_context";

  private static final ObjectMapper jsonObjectMapper =
      new ObjectMapper()
          .registerModule(new Jdk8Module()); // Handle JDK8 Optionals (de)serialization

  private JsonRpcExecutorHandler() {}

  public static Handler<RoutingContext> handler(
      final JsonRpcExecutor jsonRpcExecutor,
      final Tracer tracer,
      final JsonRpcConfiguration jsonRpcConfiguration) {
    return ctx -> {
      final long timeoutMillis = resolveTimeoutMillis(ctx, jsonRpcExecutor, jsonRpcConfiguration);
      final long timerId =
          ctx.vertx()
              .setTimer(
                  timeoutMillis,
                  id -> {
                    final String requestBodyAsJson =
                        ctx.get(ContextKey.REQUEST_BODY_AS_JSON_OBJECT.name()).toString();
                    LOG.error(
                        "Timeout ({} ms) occurred in JSON-RPC executor for method {}",
                        timeoutMillis,
                        getShortLogString(requestBodyAsJson));
                    LOG.atTrace()
                        .setMessage("Timeout ({} ms) occurred in JSON-RPC executor for method {}")
                        .addArgument(timeoutMillis)
                        .addArgument(requestBodyAsJson)
                        .log();
                    handleErrorAndEndResponse(ctx, null, RpcErrorType.TIMEOUT_ERROR);
                  });

      ctx.put("timerId", timerId);

      try {
        if (isJsonObjectRequest(ctx)) {
          executeSingleRequest(jsonRpcExecutor, tracer, ctx);
        } else if (isJsonArrayRequest(ctx)) {
          executeBatchRequest(jsonRpcExecutor, tracer, ctx, jsonRpcConfiguration);
        } else {
          handleErrorAndEndResponse(ctx, null, RpcErrorType.PARSE_ERROR);
          cancelTimer(ctx);
        }
      } catch (final IOException e) {
        final String requestBodyAsJson =
            ctx.get(ContextKey.REQUEST_BODY_AS_JSON_OBJECT.name()).toString();
        LOG.error("Error streaming JSON-RPC response", e);
        LOG.atTrace()
            .setMessage("Error streaming JSON-RPC response")
            .addArgument(requestBodyAsJson)
            .log();
        handleErrorAndEndResponse(ctx, null, RpcErrorType.INTERNAL_ERROR);
        cancelTimer(ctx);
      } catch (final RuntimeException e) {
        final String requestBodyAsJson =
            ctx.get(ContextKey.REQUEST_BODY_AS_JSON_OBJECT.name()).toString();
        LOG.error(
            "Unhandled exception in JSON-RPC executor for method {}",
            getShortLogString(requestBodyAsJson),
            e);
        LOG.atTrace()
            .setMessage("Unhandled exception in JSON-RPC executor for method {}")
            .addArgument(requestBodyAsJson)
            .log();
        handleErrorAndEndResponse(ctx, null, RpcErrorType.INTERNAL_ERROR);
        cancelTimer(ctx);
      }
    };
  }

  private static void executeSingleRequest(
      final JsonRpcExecutor jsonRpcExecutor, final Tracer tracer, final RoutingContext ctx)
      throws IOException {
    HttpServerResponse response = ctx.response();
    response = response.putHeader("Content-Type", APPLICATION_JSON);

    final JsonObject jsonRequest = ctx.get(ContextKey.REQUEST_BODY_AS_JSON_OBJECT.name());
    lazyTraceLogger(jsonRequest::toString);
    final JsonRpcResponse jsonRpcResponse =
        executeRequest(jsonRpcExecutor, tracer, jsonRequest, ctx);
    handleSingleResponse(response, jsonRpcResponse, ctx);
    cancelTimer(ctx);
  }

  private static void executeBatchRequest(
      final JsonRpcExecutor jsonRpcExecutor,
      final Tracer tracer,
      final RoutingContext ctx,
      final JsonRpcConfiguration jsonRpcConfiguration)
      throws IOException {
    HttpServerResponse response = ctx.response();
    response = response.putHeader("Content-Type", APPLICATION_JSON);

    final JsonArray batchJsonRequest = ctx.get(ContextKey.REQUEST_BODY_AS_JSON_ARRAY.name());
    lazyTraceLogger(batchJsonRequest::toString);

    if (isBatchSizeValid(batchJsonRequest, jsonRpcConfiguration)) {
      try (final JsonResponseStreamer streamer =
          new JsonResponseStreamer(response, ctx.request().remoteAddress())) {
        executeBatch(jsonRpcExecutor, tracer, batchJsonRequest, streamer, ctx);
      }
    } else {
      handleErrorAndEndResponse(ctx, null, RpcErrorType.EXCEEDS_RPC_MAX_BATCH_SIZE);
    }
    cancelTimer(ctx);
  }

  private static void executeBatch(
      final JsonRpcExecutor jsonRpcExecutor,
      final Tracer tracer,
      final JsonArray batchJsonRequest,
      final JsonResponseStreamer streamer,
      final RoutingContext ctx)
      throws IOException {
    try (JsonGenerator generator = getJsonObjectMapper().getFactory().createGenerator(streamer)) {
      generator.writeStartArray();
      for (int i = 0; i < batchJsonRequest.size(); i++) {
        JsonRpcResponse response =
            processMaybeRequest(jsonRpcExecutor, tracer, batchJsonRequest.getValue(i), ctx);
        if (response.getType() != RpcResponseType.NONE) {
          generator.writeObject(response);
        }
        lazyTraceLogger(() -> getJsonObjectMapper().writeValueAsString(response));
      }
      generator.writeEndArray();
    }
  }

  private static JsonRpcResponse processMaybeRequest(
      final JsonRpcExecutor jsonRpcExecutor,
      final Tracer tracer,
      final Object maybeRequest,
      final RoutingContext ctx) {
    if (maybeRequest instanceof JsonObject) {
      return executeRequest(jsonRpcExecutor, tracer, (JsonObject) maybeRequest, ctx);
    } else {
      return new JsonRpcErrorResponse(null, INVALID_REQUEST);
    }
  }

  private static void handleSingleResponse(
      final HttpServerResponse response,
      final JsonRpcResponse jsonRpcResponse,
      final RoutingContext ctx)
      throws IOException {
    response.setStatusCode(status(jsonRpcResponse).code());
    if (jsonRpcResponse.getType() == RpcResponseType.NONE) {
      response.end();
    } else {
      try (final JsonResponseStreamer streamer =
          new JsonResponseStreamer(response, ctx.request().remoteAddress())) {
        lazyTraceLogger(() -> getJsonObjectMapper().writeValueAsString(jsonRpcResponse));
        getJsonObjectMapper().writeValue(streamer, jsonRpcResponse);
      }
    }
  }

  private static JsonRpcResponse executeRequest(
      final JsonRpcExecutor jsonRpcExecutor,
      final Tracer tracer,
      final JsonObject jsonRequest,
      final RoutingContext ctx) {
    final Optional<User> user = ContextKey.AUTHENTICATED_USER.extractFrom(ctx, Optional::empty);
    final Context spanContext = ctx != null ? ctx.get(SPAN_CONTEXT) : Context.current();
    return jsonRpcExecutor.execute(
        user,
        tracer,
        spanContext,
        ctx != null ? () -> !ctx.response().closed() : () -> true,
        jsonRequest,
        req -> req.mapTo(JsonRpcRequest.class));
  }

  private static boolean isBatchSizeValid(
      final JsonArray batchJsonRequest, final JsonRpcConfiguration jsonRpcConfiguration) {
    return !(jsonRpcConfiguration.getMaxBatchSize() > 0
        && batchJsonRequest.size() > jsonRpcConfiguration.getMaxBatchSize());
  }

  private static HttpResponseStatus status(final JsonRpcResponse response) {
    return switch (response.getType()) {
      case UNAUTHORIZED -> HttpResponseStatus.UNAUTHORIZED;
      case ERROR -> statusCodeFromError(((JsonRpcErrorResponse) response).getErrorType());
      default -> HttpResponseStatus.OK;
    };
  }

  private static HttpResponseStatus statusCodeFromError(final RpcErrorType error) {
    return switch (error) {
      case INVALID_REQUEST, PARSE_ERROR -> HttpResponseStatus.BAD_REQUEST;
      case TIMEOUT_ERROR -> HttpResponseStatus.REQUEST_TIMEOUT;
      default -> HttpResponseStatus.OK;
    };
  }

  private static void handleErrorAndEndResponse(
      final RoutingContext ctx, final Object id, final RpcErrorType errorType) {
    if (!ctx.response().ended()) {
      final HttpServerResponse response = ctx.response();
      response
          .setStatusCode(statusCodeFromError(errorType).code())
          .end(Json.encode(new JsonRpcErrorResponse(id, errorType)));
    }
  }

  private static Object getShortLogString(final String requestBodyAsJson) {
    final int maxLogLength = 256;
    return requestBodyAsJson == null || requestBodyAsJson.length() < maxLogLength
        ? requestBodyAsJson
        : requestBodyAsJson.substring(0, maxLogLength).concat("...");
  }

  private static void cancelTimer(final RoutingContext ctx) {
    Long timerId = ctx.get("timerId");
    if (timerId != null) {
      ctx.vertx().cancelTimer(timerId);
    }
  }

  private static boolean isJsonObjectRequest(final RoutingContext ctx) {
    return ctx.data().containsKey(ContextKey.REQUEST_BODY_AS_JSON_OBJECT.name());
  }

  private static boolean isJsonArrayRequest(final RoutingContext ctx) {
    return ctx.data().containsKey(ContextKey.REQUEST_BODY_AS_JSON_ARRAY.name());
  }

  private static long resolveTimeoutMillis(
      final RoutingContext ctx,
      final JsonRpcExecutor jsonRpcExecutor,
      final JsonRpcConfiguration config) {
    if (isJsonObjectRequest(ctx)) {
      final JsonObject req = ctx.get(ContextKey.REQUEST_BODY_AS_JSON_OBJECT.name());
      if (req != null && jsonRpcExecutor.isStreamingMethod(req.getString("method"))) {
        return config.getHttpStreamingTimeoutSec() * 1000;
      }
    }
    return config.getHttpTimeoutSec() * 1000;
  }

  private static ObjectMapper getJsonObjectMapper() {
    return jsonObjectMapper;
  }

  @FunctionalInterface
  private interface ExceptionThrowingSupplier<T> {
    T get() throws Exception;
  }

  private static void lazyTraceLogger(final ExceptionThrowingSupplier<String> logMessageSupplier) {
    if (LOG.isTraceEnabled()) {
      try {
        LOG.trace(logMessageSupplier.get());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
