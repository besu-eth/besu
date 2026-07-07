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
package org.hyperledger.besu.ethereum.api.jsonrpc.ipc;

import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.INVALID_REQUEST;

import org.hyperledger.besu.ethereum.api.handlers.JsonRpcParserHandler;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcObjectMapperFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.execution.JsonRpcExecutor;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.methods.WebSocketRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.SubscriptionManager;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetSocket;
import io.vertx.core.net.SocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonRpcIpcService {

  private static final Logger LOG = LoggerFactory.getLogger(JsonRpcIpcService.class);
  private static final ObjectWriter JSON_OBJECT_WRITER =
      JsonRpcObjectMapperFactory.getResponseMapper()
          .writer()
          .without(JsonGenerator.Feature.FLUSH_PASSED_TO_STREAM)
          .with(JsonGenerator.Feature.AUTO_CLOSE_TARGET);

  private final Vertx vertx;
  private final Path path;
  private final JsonRpcExecutor jsonRpcExecutor;
  private final Optional<SubscriptionManager> subscriptionManager;
  private NetServer netServer;

  public JsonRpcIpcService(final Vertx vertx, final Path path, final JsonRpcExecutor rpcExecutor) {
    this(vertx, path, rpcExecutor, Optional.empty());
  }

  public JsonRpcIpcService(
      final Vertx vertx,
      final Path path,
      final JsonRpcExecutor rpcExecutor,
      final Optional<SubscriptionManager> subscriptionManager) {
    this.vertx = vertx;
    this.path = path;
    this.jsonRpcExecutor = rpcExecutor;
    this.subscriptionManager = subscriptionManager;
  }

  public Future<NetServer> start() {
    netServer = vertx.createNetServer(buildNetServerOptions());
    netServer.connectHandler(socket -> handleNewConnection(socket));
    return netServer
        .listen(SocketAddress.domainSocketAddress(path.toString()))
        .onSuccess(successServer -> LOG.info("IPC endpoint opened: {}", path))
        .onFailure(throwable -> LOG.error("Unable to open IPC endpoint", throwable));
  }

  private void handleNewConnection(final NetSocket socket) {
    final AtomicBoolean closedSocket = new AtomicBoolean(false);
    final String connectionId = UUID.randomUUID().toString();

    subscriptionManager.ifPresent(
        manager ->
            vertx
                .eventBus()
                .consumer(connectionId)
                .handler(
                    msg -> {
                      if (!closedSocket.get()) {
                        socket.write(Buffer.buffer(msg.body().toString() + '\n'));
                      }
                    }));

    socket
        .closeHandler(
            unused -> {
              closedSocket.set(true);
              subscriptionManager.ifPresent(
                  manager ->
                      vertx
                          .eventBus()
                          .send(
                              SubscriptionManager.EVENTBUS_REMOVE_SUBSCRIPTIONS_ADDRESS,
                              connectionId));
            })
        .handler(
            JsonRpcParserHandler.ipcHandler(
                (jsonObj, jsonArr) -> {
                  if (jsonObj != null) {
                    handleSingleRequest(socket, jsonObj, closedSocket, connectionId);
                  } else if (jsonArr != null) {
                    handleBatchRequest(socket, jsonArr, closedSocket);
                  }
                },
                () -> errorReturn(socket, null, RpcErrorType.PARSE_ERROR)));
  }

  private void handleSingleRequest(
      final NetSocket socket,
      final JsonObject jsonRpcRequest,
      final AtomicBoolean closedSocket,
      final String connectionId) {
    vertx
        .<JsonRpcResponse>executeBlocking(
            promise -> {
              final JsonRpcResponse jsonRpcResponse =
                  jsonRpcExecutor.execute(
                      Optional.empty(),
                      null,
                      null,
                      closedSocket::get,
                      jsonRpcRequest,
                      req -> {
                        if (subscriptionManager.isPresent()) {
                          final WebSocketRpcRequest websocketRequest =
                              req.mapTo(WebSocketRpcRequest.class);
                          websocketRequest.setConnectionId(connectionId);
                          return websocketRequest;
                        } else {
                          return req.mapTo(JsonRpcRequest.class);
                        }
                      });
              promise.complete(jsonRpcResponse);
            })
        .onSuccess(
            jsonRpcResponse -> {
              if (!closedSocket.get()) {
                writeResponse(socket, jsonRpcResponse);
              }
            })
        .onFailure(
            throwable -> {
              if (!closedSocket.get()) {
                try {
                  final Integer id = jsonRpcRequest.getInteger("id", null);
                  errorReturn(socket, id, RpcErrorType.INTERNAL_ERROR);
                } catch (ClassCastException idNotIntegerException) {
                  errorReturn(socket, null, RpcErrorType.INTERNAL_ERROR);
                }
              }
            });
  }

  private void handleBatchRequest(
      final NetSocket socket,
      final JsonArray batchJsonRpcRequest,
      final AtomicBoolean closedSocket) {
    if (batchJsonRpcRequest.isEmpty()) {
      errorReturn(socket, null, RpcErrorType.INVALID_REQUEST);
    } else {
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
                          Optional.empty(),
                          null,
                          null,
                          closedSocket::get,
                          jsonRequest,
                          req -> req.mapTo(JsonRpcRequest.class)));
                }
                promise.complete(responses);
              })
          .onSuccess(
              jsonRpcBatchResponse -> {
                if (!closedSocket.get()) {
                  try {
                    final JsonRpcResponse[] completed =
                        jsonRpcBatchResponse.stream()
                            .filter(
                                jsonRpcResponse ->
                                    jsonRpcResponse.getType() != RpcResponseType.NONE)
                            .toArray(JsonRpcResponse[]::new);

                    socket.write(JSON_OBJECT_WRITER.writeValueAsString(completed) + '\n');
                  } catch (JsonProcessingException e) {
                    LOG.error("Error streaming JSON-RPC response", e);
                  }
                }
              })
          .onFailure(
              throwable -> {
                if (!closedSocket.get()) {
                  errorReturn(socket, null, RpcErrorType.INTERNAL_ERROR);
                }
              });
    }
  }

  private void writeResponse(final NetSocket socket, final JsonRpcResponse response) {
    try {
      socket.write(JSON_OBJECT_WRITER.writeValueAsString(response) + '\n');
    } catch (JsonProcessingException e) {
      LOG.error("Error streaming JSON-RPC response", e);
    }
  }

  public Future<Void> stop() {
    if (netServer == null) {
      return Future.succeededFuture();
    } else {
      return netServer
          .close()
          .onComplete(
              closeResult -> {
                try {
                  Files.deleteIfExists(path);
                } catch (IOException e) {
                  LOG.error("Unable to delete IPC file", e);
                }
              });
    }
  }

  private Future<Void> errorReturn(
      final NetSocket socket, final Integer id, final RpcErrorType rpcError) {
    return socket.write(Buffer.buffer(Json.encode(new JsonRpcErrorResponse(id, rpcError)) + '\n'));
  }

  private NetServerOptions buildNetServerOptions() {
    return new NetServerOptions();
  }
}
