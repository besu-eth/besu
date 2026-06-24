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

import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.INVALID_REQUEST;

import org.hyperledger.besu.ethereum.api.jsonrpc.JsonResponseStreamer;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.context.ContextKey;
import org.hyperledger.besu.ethereum.api.jsonrpc.execution.JsonRpcExecutor;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import io.opentelemetry.api.trace.Tracer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

public class JsonRpcArrayExecutor extends AbstractJsonRpcExecutor {
  public JsonRpcArrayExecutor(
      final JsonRpcExecutor jsonRpcExecutor,
      final Tracer tracer,
      final RoutingContext ctx,
      final JsonRpcConfiguration jsonRpcConfiguration) {
    super(jsonRpcExecutor, tracer, ctx, jsonRpcConfiguration);
  }

  /**
   * Executes the JSON-RPC request(s) associated with the current routing context.
   *
   * @throws IOException if there is an error writing the response to the client
   */
  @Override
  void execute() throws IOException {
    HttpServerResponse response = prepareHttpResponse(ctx);
    final JsonArray batchJsonRequest = getRequestBodyAsJsonArray(ctx);
    if (isBatchSizeValid(batchJsonRequest)) {
      try (final JsonResponseStreamer streamer =
          new JsonResponseStreamer(response, ctx.request().remoteAddress())) {
        executeRpcRequestBatch(batchJsonRequest, streamer);
      }
    } else {
      handleJsonRpcError(ctx, null, RpcErrorType.EXCEEDS_RPC_MAX_BATCH_SIZE);
    }
  }

  /**
   * Executes a batch of RPC requests.
   *
   * @param rpcRequestBatch the batch of RPC requests.
   * @param streamer the JsonResponseStreamer to use.
   */
  public void executeRpcRequestBatch(
      final JsonArray rpcRequestBatch, final JsonResponseStreamer streamer) throws IOException {
    try (JsonGenerator generator = getJsonObjectMapper().getFactory().createGenerator(streamer)) {
      generator.writeStartArray();
      for (int i = 0; i < rpcRequestBatch.size(); i++) {
        JsonRpcResponse response = processMaybeRequest(rpcRequestBatch.getValue(i));
        if (response.getType() != RpcResponseType.NONE) {
          generator.writeObject(response);
        }
        lazyTraceLogger(() -> getJsonObjectMapper().writeValueAsString(response));
      }
      generator.writeEndArray();
    }
  }

  /**
   * Processes a single RPC request.
   *
   * @param maybeRequest the object that might be a request.
   * @return the response from executing the request, or an error response if it wasn't a valid
   *     request.
   */
  private JsonRpcResponse processMaybeRequest(final Object maybeRequest) {
    if (maybeRequest instanceof JsonObject jsonObject) {
      return executeRequest(jsonObject);
    } else {
      return createErrorResponse();
    }
  }

  /**
   * Executes a single RPC request.
   *
   * @param request the request to execute.
   * @return the response from executing the request.
   */
  private JsonRpcResponse executeRequest(final JsonObject request) {
    return executeRequest(jsonRpcExecutor, tracer, request, ctx);
  }

  /**
   * Creates an error response for an invalid request.
   *
   * @return an error response.
   */
  private JsonRpcResponse createErrorResponse() {
    return new JsonRpcErrorResponse(null, INVALID_REQUEST);
  }

  @Override
  String getRpcMethodName(final RoutingContext ctx) {
    return "JsonArray";
  }

  private boolean isBatchSizeValid(final JsonArray batchJsonRequest) {
    return !(jsonRpcConfiguration.getMaxBatchSize() > 0
        && batchJsonRequest.size() > jsonRpcConfiguration.getMaxBatchSize());
  }

  private JsonArray getRequestBodyAsJsonArray(final RoutingContext ctx) {
    final JsonArray batchJsonRequest = ctx.get(ContextKey.REQUEST_BODY_AS_JSON_ARRAY.name());
    lazyTraceLogger(batchJsonRequest::toString);
    return batchJsonRequest;
  }
}
