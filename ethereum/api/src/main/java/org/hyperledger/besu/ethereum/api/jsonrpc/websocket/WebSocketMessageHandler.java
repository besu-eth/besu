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

import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.INVALID_REQUEST;

import org.hyperledger.besu.ethereum.api.handlers.IsAliveHandler;
import org.hyperledger.besu.ethereum.api.jsonrpc.execution.JsonRpcExecutor;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.methods.WebSocketRpcRequest;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebSocketMessageHandler {

  private static final ObjectMapper jsonObjectMapper =
      new ObjectMapper()
          .registerModule(new Jdk8Module()); // Handle JDK8 Optionals (de)serialization

  private static final Logger LOG = LoggerFactory.getLogger(WebSocketMessageHandler.class);
  private static final ObjectWriter JSON_OBJECT_WRITER =
      new ObjectMapper()
          .registerModule(new Jdk8Module()) // Handle JDK8 Optionals (de)serialization
          .writer()
          .without(Feature.FLUSH_PASSED_TO_STREAM)
          .with(Feature.AUTO_CLOSE_TARGET);

  private final Vertx vertx;
  private final JsonRpcExecutor jsonRpcExecutor;
  final EthScheduler ethScheduler;
  private final long timeoutSec;

  public WebSocketMessageHandler(
      final Vertx vertx,
      final JsonRpcExecutor jsonRpcExecutor,
      final EthScheduler ethScheduler,
      final long timeoutSec) {
    this.vertx = vertx;
    this.jsonRpcExecutor = jsonRpcExecutor;
    this.ethScheduler = ethScheduler;
    this.timeoutSec = timeoutSec;
  }

  public void handle(
      final ServerWebSocket websocket, final Buffer buffer, final Optional<User> user) {
    if (buffer.length() == 0) {
      replyToClient(websocket, errorResponse(null, RpcErrorType.INVALID_REQUEST));
    } else {
      try {
        final JsonObject jsonRpcRequest = buffer.toJsonObject();

        if (jsonRpcExecutor.isStreamingMethod(jsonRpcRequest.getString("method"))) {
          vertx
              .<Void>executeBlocking(
                  promise -> {
                    try (JsonResponseStreamer streamer = new JsonResponseStreamer(websocket)) {
                      jsonRpcExecutor.executeStreaming(
                          user,
                          null,
                          null,
                          new IsAliveHandler(ethScheduler, timeoutSec),
                          jsonRpcRequest,
                          req -> {
                            final WebSocketRpcRequest websocketRequest =
                                req.mapTo(WebSocketRpcRequest.class);
                            websocketRequest.setConnectionId(websocket.textHandlerID());
                            return websocketRequest;
                          },
                          streamer,
                          jsonObjectMapper);
                      promise.complete();
                    } catch (IOException e) {
                      promise.fail(e);
                    }
                  })
              .onFailure(
                  throwable -> {
                    LOG.error("Error streaming JSON-RPC response", throwable);
                    try {
                      final Integer id = jsonRpcRequest.getInteger("id", null);
                      replyToClient(websocket, errorResponse(id, RpcErrorType.INTERNAL_ERROR));
                    } catch (ClassCastException idNotIntegerException) {
                      replyToClient(websocket, errorResponse(null, RpcErrorType.INTERNAL_ERROR));
                    }
                  });
          return;
        }

        vertx
            .<JsonRpcResponse>executeBlocking(
                promise -> {
                  try {
                    final JsonRpcResponse jsonRpcResponse =
                        jsonRpcExecutor.execute(
                            user,
                            null,
                            null,
                            new IsAliveHandler(ethScheduler, timeoutSec),
                            jsonRpcRequest,
                            req -> {
                              final WebSocketRpcRequest websocketRequest =
                                  req.mapTo(WebSocketRpcRequest.class);
                              websocketRequest.setConnectionId(websocket.textHandlerID());
                              return websocketRequest;
                            });
                    promise.complete(jsonRpcResponse);
                  } catch (RuntimeException e) {
                    promise.fail(e);
                  }
                })
            .onSuccess(jsonRpcResponse -> replyToClient(websocket, jsonRpcResponse))
            .onFailure(
                throwable -> {
                  try {
                    final Integer id = jsonRpcRequest.getInteger("id", null);
                    replyToClient(websocket, errorResponse(id, RpcErrorType.INTERNAL_ERROR));
                  } catch (ClassCastException idNotIntegerException) {
                    replyToClient(websocket, errorResponse(null, RpcErrorType.INTERNAL_ERROR));
                  }
                });
      } catch (DecodeException jsonObjectDecodeException) {
        try {
          final JsonArray batchJsonRpcRequest = buffer.toJsonArray();
          vertx
              .<List<JsonRpcResponse>>executeBlocking(
                  promise -> {
                    List<JsonRpcResponse> responses = new ArrayList<>();
                    for (int i = 0; i < batchJsonRpcRequest.size(); i++) {
                      final JsonObject jsonRequest;
                      try {
                        jsonRequest = batchJsonRpcRequest.getJsonObject(i);
                      } catch (ClassCastException e) {
                        responses.add(new JsonRpcErrorResponse(null, INVALID_REQUEST));
                        continue;
                      }
                      responses.add(
                          jsonRpcExecutor.execute(
                              user,
                              null,
                              null,
                              new IsAliveHandler(ethScheduler, timeoutSec),
                              jsonRequest,
                              req -> {
                                final WebSocketRpcRequest websocketRequest =
                                    req.mapTo(WebSocketRpcRequest.class);
                                websocketRequest.setConnectionId(websocket.textHandlerID());
                                return websocketRequest;
                              }));
                    }
                    promise.complete(responses);
                  })
              .onSuccess(
                  jsonRpcBatchResponse -> {
                    final JsonRpcResponse[] completed =
                        jsonRpcBatchResponse.stream()
                            .filter(
                                jsonRpcResponse ->
                                    jsonRpcResponse.getType() != RpcResponseType.NONE)
                            .toArray(JsonRpcResponse[]::new);
                    replyToClient(websocket, completed);
                  })
              .onFailure(
                  throwable ->
                      replyToClient(websocket, errorResponse(null, RpcErrorType.INTERNAL_ERROR)));
        } catch (RuntimeException jsonArrayDecodeException) {
          replyToClient(websocket, errorResponse(null, RpcErrorType.INTERNAL_ERROR));
        }
      }
    }
  }

  private void replyToClient(final ServerWebSocket websocket, final Object result) {
    traceResponse(result);
    try {
      // underlying output stream lifecycle is managed by the json object writer
      JSON_OBJECT_WRITER.writeValue(new JsonResponseStreamer(websocket), result);
    } catch (IOException ex) {
      LOG.error("Error streaming JSON-RPC response", ex);
    }
  }

  private JsonRpcResponse errorResponse(final Object id, final RpcErrorType error) {
    return new JsonRpcErrorResponse(id, error);
  }

  private void traceResponse(final Object response) {
    try {
      LOG.trace(jsonObjectMapper.writeValueAsString(response));
    } catch (JsonProcessingException e) {
      LOG.error("Error tracing JSON-RPC response", e);
    }
  }
}
