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

import static org.hyperledger.besu.ethereum.api.handlers.AbstractJsonRpcExecutor.handleJsonRpcError;

import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.context.ContextKey;
import org.hyperledger.besu.ethereum.api.jsonrpc.execution.JsonRpcExecutor;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;

import java.io.IOException;
import java.util.Optional;

import io.opentelemetry.api.trace.Tracer;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonRpcExecutorHandler {
  private static final Logger LOG = LoggerFactory.getLogger(JsonRpcExecutorHandler.class);

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
        createExecutor(jsonRpcExecutor, tracer, ctx, jsonRpcConfiguration)
            .ifPresentOrElse(
                executor -> {
                  try {
                    executor.execute();
                  } catch (IOException e) {
                    final String method = executor.getRpcMethodName(ctx);
                    final String requestBodyAsJson =
                        ctx.get(ContextKey.REQUEST_BODY_AS_JSON_OBJECT.name()).toString();
                    LOG.error("{} - Error streaming JSON-RPC response", method, e);
                    LOG.atTrace()
                        .setMessage("{} - Error streaming JSON-RPC response")
                        .addArgument(requestBodyAsJson)
                        .log();
                    handleErrorAndEndResponse(ctx, null, RpcErrorType.INTERNAL_ERROR);
                  } finally {
                    cancelTimer(ctx);
                  }
                },
                () -> {
                  handleErrorAndEndResponse(ctx, null, RpcErrorType.PARSE_ERROR);
                  cancelTimer(ctx);
                });
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

  private static void handleErrorAndEndResponse(
      final RoutingContext ctx, final Object id, final RpcErrorType errorType) {
    if (!ctx.response().ended()) {
      handleJsonRpcError(ctx, id, errorType);
    }
  }

  private static Optional<AbstractJsonRpcExecutor> createExecutor(
      final JsonRpcExecutor jsonRpcExecutor,
      final Tracer tracer,
      final RoutingContext ctx,
      final JsonRpcConfiguration jsonRpcConfiguration) {
    if (isJsonObjectRequest(ctx)) {
      return Optional.of(
          new JsonRpcObjectExecutor(jsonRpcExecutor, tracer, ctx, jsonRpcConfiguration));
    }
    if (isJsonArrayRequest(ctx)) {
      return Optional.of(
          new JsonRpcArrayExecutor(jsonRpcExecutor, tracer, ctx, jsonRpcConfiguration));
    }
    return Optional.empty();
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
}
