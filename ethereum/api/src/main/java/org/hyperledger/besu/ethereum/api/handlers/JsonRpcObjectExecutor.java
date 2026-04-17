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

import org.hyperledger.besu.ethereum.api.jsonrpc.JsonResponseStreamer;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.context.ContextKey;
import org.hyperledger.besu.ethereum.api.jsonrpc.execution.JsonRpcExecutor;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcRequestException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.io.IOException;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;

public class JsonRpcObjectExecutor extends AbstractJsonRpcExecutor {
  private final ObjectWriter jsonObjectWriter = createObjectWriter();

  public JsonRpcObjectExecutor(
      final JsonRpcExecutor jsonRpcExecutor,
      final Tracer tracer,
      final RoutingContext ctx,
      final JsonRpcConfiguration jsonRpcConfiguration) {
    super(jsonRpcExecutor, tracer, ctx, jsonRpcConfiguration);
  }

  @Override
  void execute() throws IOException {
    final HttpServerResponse response = prepareHttpResponse(ctx);
    final JsonObject jsonRequest = ctx.get(ContextKey.REQUEST_BODY_AS_JSON_OBJECT.name());

    if (jsonRpcExecutor.isStreamingMethod(jsonRequest.getString("method"))) {
      executeStreamingMethod(response, jsonRequest);
      return;
    }

    lazyTraceLogger(jsonRequest::toString);
    final JsonRpcResponse jsonRpcResponse =
        executeRequest(jsonRpcExecutor, tracer, jsonRequest, ctx);
    handleJsonObjectResponse(response, jsonRpcResponse, ctx);
  }

  private void executeStreamingMethod(
      final HttpServerResponse response, final JsonObject jsonRequest) throws IOException {
    // Do NOT set the status code eagerly — let JsonResponseStreamer flush headers
    // on first write.  This keeps the response uncommitted so that pre-stream
    // errors (bad params, auth failures, missing blocks) can still produce a
    // proper HTTP error with the correct status code.
    final JsonResponseStreamer streamer =
        new JsonResponseStreamer(response, ctx.request().remoteAddress());
    try {
      final Optional<User> user = ContextKey.AUTHENTICATED_USER.extractFrom(ctx, Optional::empty);
      final Context spanContext = ctx.get(SPAN_CONTEXT);
      final Optional<JsonRpcResponse> preStreamError =
          jsonRpcExecutor.executeStreaming(
              user,
              tracer,
              spanContext,
              () -> !ctx.response().closed(),
              jsonRequest,
              req -> req.mapTo(JsonRpcRequest.class),
              streamer,
              getJsonObjectMapper());
      if (preStreamError.isPresent()) {
        // Validation failed before any data was written to the stream.
        // The streamer's close() is a no-op (chunked never set), so we can
        // send a proper error response with the correct HTTP status code.
        handleJsonObjectResponse(response, preStreamError.get(), ctx);
        return;
      }
      // Streaming completed — end the chunked response.
      streamer.close();
    } catch (final Exception e) {
      if (!response.headWritten()) {
        // Headers not flushed yet — send a proper HTTP error response.
        final Object id = jsonRequest.getValue("id");
        final RpcErrorType errorType =
            e instanceof InvalidJsonRpcRequestException ijrp
                ? ijrp.getRpcErrorType()
                : RpcErrorType.INTERNAL_ERROR;
        handleJsonRpcError(ctx, id, errorType);
      } else if (!response.ended()) {
        // Streaming started but failed mid-stream — reset the connection so the
        // client sees a transport error rather than truncated JSON.
        response.reset();
      }
      if (e instanceof IOException ioe) {
        throw ioe;
      }
    }
  }

  @Override
  String getRpcMethodName(final RoutingContext ctx) {
    final JsonObject jsonObject = ctx.get(ContextKey.REQUEST_BODY_AS_JSON_OBJECT.name());
    return jsonObject.getString("method");
  }

  private void handleJsonObjectResponse(
      final HttpServerResponse response,
      final JsonRpcResponse jsonRpcResponse,
      final RoutingContext ctx)
      throws IOException {
    response.setStatusCode(status(jsonRpcResponse).code());
    if (jsonRpcResponse.getType() == RpcResponseType.NONE) {
      response.end();
      return;
    }

    try (final JsonResponseStreamer streamer =
        new JsonResponseStreamer(response, ctx.request().remoteAddress())) {
      lazyTraceLogger(() -> getJsonObjectMapper().writeValueAsString(jsonRpcResponse));
      jsonObjectWriter.writeValue(streamer, jsonRpcResponse);
    }
  }

  private static HttpResponseStatus status(final JsonRpcResponse response) {
    return switch (response.getType()) {
      case UNAUTHORIZED -> HttpResponseStatus.UNAUTHORIZED;
      case ERROR -> statusCodeFromError(((JsonRpcErrorResponse) response).getErrorType());
      default -> HttpResponseStatus.OK;
    };
  }

  private ObjectWriter createObjectWriter() {
    ObjectWriter writer =
        jsonRpcConfiguration.isPrettyJsonEnabled()
            ? getJsonObjectMapper().writerWithDefaultPrettyPrinter()
            : getJsonObjectMapper().writer();
    return writer
        .without(JsonGenerator.Feature.FLUSH_PASSED_TO_STREAM)
        .with(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
  }

  private static HttpResponseStatus statusCodeFromError(final RpcErrorType error) {
    return switch (error) {
      case INVALID_REQUEST, PARSE_ERROR -> HttpResponseStatus.BAD_REQUEST;
      default -> HttpResponseStatus.OK;
    };
  }
}
