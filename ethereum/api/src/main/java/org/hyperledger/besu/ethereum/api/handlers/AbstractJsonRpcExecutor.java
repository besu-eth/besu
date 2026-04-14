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

import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.context.ContextKey;
import org.hyperledger.besu.ethereum.api.jsonrpc.execution.JsonRpcExecutor;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;

import java.io.IOException;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractJsonRpcExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractJsonRpcExecutor.class);

  protected static final String SPAN_CONTEXT = "span_context";
  final JsonRpcExecutor jsonRpcExecutor;
  final Tracer tracer;
  final RoutingContext ctx;
  final JsonRpcConfiguration jsonRpcConfiguration;

  private static final ObjectMapper jsonObjectMapper =
      new ObjectMapper()
          .registerModule(new Jdk8Module()); // Handle JDK8 Optionals (de)serialization

  /**
   * Creates a new AbstractJsonRpcExecutor.
   *
   * @param jsonRpcExecutor The executor used to process the JSON RPC requests.
   * @param tracer The tracer used for monitoring and debugging purposes.
   * @param ctx The context of the routing, containing information about the HTTP request and
   *     response.
   * @param jsonRpcConfiguration The configuration for JSON RPC operations
   */
  public AbstractJsonRpcExecutor(
      final JsonRpcExecutor jsonRpcExecutor,
      final Tracer tracer,
      final RoutingContext ctx,
      final JsonRpcConfiguration jsonRpcConfiguration) {
    this.jsonRpcExecutor = jsonRpcExecutor;
    this.tracer = tracer;
    this.ctx = ctx;
    this.jsonRpcConfiguration = jsonRpcConfiguration;
  }

  abstract void execute() throws IOException;

  abstract String getRpcMethodName(final RoutingContext ctx);

  protected static JsonRpcResponse executeRequest(
      final JsonRpcExecutor jsonRpcExecutor,
      final Tracer tracer,
      final JsonObject jsonRequest,
      final RoutingContext ctx) {
    final Optional<User> user = ContextKey.AUTHENTICATED_USER.extractFrom(ctx, Optional::empty);
    final Context spanContext = ctx.get(SPAN_CONTEXT);
    return jsonRpcExecutor.execute(
        user,
        tracer,
        spanContext,
        () -> !ctx.response().closed(),
        jsonRequest,
        req -> req.mapTo(JsonRpcRequest.class));
  }

  protected static void handleJsonRpcError(
      final RoutingContext routingContext, final Object id, final RpcErrorType error) {
    final HttpServerResponse response = routingContext.response();
    if (!response.closed()) {
      if (response.headWritten()) {
        // Streaming already started — cannot change status code or headers.
        // Reset the connection so the client sees a transport error rather than
        // silently receiving truncated JSON.
        response.reset();
      } else {
        response
            .setStatusCode(statusCodeFromError(error).code())
            .end(Json.encode(new JsonRpcErrorResponse(id, error)));
      }
    }
  }

  private static HttpResponseStatus statusCodeFromError(final RpcErrorType error) {
    return switch (error) {
      case INVALID_REQUEST, PARSE_ERROR -> HttpResponseStatus.BAD_REQUEST;
      case TIMEOUT_ERROR -> HttpResponseStatus.REQUEST_TIMEOUT;
      case UNAUTHORIZED -> HttpResponseStatus.UNAUTHORIZED;
      default -> HttpResponseStatus.OK;
    };
  }

  protected HttpServerResponse prepareHttpResponse(final RoutingContext ctx) {
    HttpServerResponse response = ctx.response();
    response = response.putHeader("Content-Type", APPLICATION_JSON);
    return response;
  }

  protected static ObjectMapper getJsonObjectMapper() {
    return jsonObjectMapper;
  }

  @FunctionalInterface
  protected interface ExceptionThrowingSupplier<T> {
    T get() throws Exception;
  }

  protected static void lazyTraceLogger(
      final ExceptionThrowingSupplier<String> logMessageSupplier) {
    if (LOG.isTraceEnabled()) {
      try {
        LOG.trace(logMessageSupplier.get());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
