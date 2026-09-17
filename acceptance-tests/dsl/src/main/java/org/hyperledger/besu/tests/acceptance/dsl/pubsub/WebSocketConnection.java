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
package org.hyperledger.besu.tests.acceptance.dsl.pubsub;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.tests.acceptance.dsl.WaitUtils;
import org.hyperledger.besu.tests.acceptance.dsl.node.configuration.NodeConfiguration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

import io.vertx.core.Vertx;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketClient;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebSocketConnection {

  private static final Logger LOG = LoggerFactory.getLogger(WebSocketConnection.class);

  private final WebSocketConnectOptions options;
  private final ConcurrentLinkedDeque<SubscriptionEvent> subscriptionEvents;

  // Vert.x wraps the client in a Cleaner that shuts it down (closing its connections) once the
  // client itself becomes unreachable, so a strong reference must be kept for the connection's
  // lifetime rather than letting the client returned by createWebSocketClient() go out of scope.
  private final WebSocketClient webSocketClient;

  private volatile String error;
  private volatile boolean receivedResponse;
  private volatile JsonRpcSuccessEvent latestEvent;
  private volatile WebSocket connection;

  public WebSocketConnection(final Vertx vertx, final NodeConfiguration node) {
    if (!node.getJsonRpcWebSocketPort().isPresent()) {
      throw new IllegalStateException(
          "Can't start websocket connection for node with RPC disabled");
    }
    subscriptionEvents = new ConcurrentLinkedDeque<>();
    options = new WebSocketConnectOptions();
    options.setPort(node.getJsonRpcWebSocketPort().get());
    options.setHost(node.getHostName());
    webSocketClient = vertx.createWebSocketClient();

    connect();
  }

  public JsonRpcSuccessEvent subscribe(final String params) {
    resetLatestResult();
    return send(
        String.format("{\"id\": 1, \"method\": \"eth_subscribe\", \"params\": [\"%s\"]}", params));
  }

  public JsonRpcSuccessEvent unsubscribe(final Subscription subscription) {
    resetLatestResult();
    return send(
        String.format(
            "{\"id\": 2, \"method\": \"eth_unsubscribe\", \"params\": [\"%s\"]}", subscription));
  }

  private JsonRpcSuccessEvent send(final String json) {

    connection.writeTextMessage(json);

    WaitUtils.waitFor(() -> assertThat(receivedResponse).isEqualTo(true));

    assertThat(latestEvent)
        .as(
            "Expecting a JSON-RPC success response to message: %s, instead received: %s",
            json, error)
        .isNotNull();

    return latestEvent;
  }

  private void connect() {
    webSocketClient
        .connect(options)
        .onSuccess(
            websocket -> {
              webSocketConnection(websocket);

              websocket.handler(
                  data -> {
                    try {
                      final WebSocketEvent eventType = Json.decodeValue(data, WebSocketEvent.class);

                      if (eventType.isSubscription()) {
                        success(Json.decodeValue(data, SubscriptionEvent.class));
                      } else {
                        success(Json.decodeValue(data, JsonRpcSuccessEvent.class));
                      }

                    } catch (final DecodeException e) {
                      error(
                          "Data: "
                              + data.toString()
                              + "\nException: "
                              + ExceptionUtils.getStackTrace(e));
                    }
                  });
            })
        .onFailure(cause -> LOG.error("Websocket connect failed", cause));

    WaitUtils.waitFor(() -> assertThat(connection).isNotNull());
  }

  private void webSocketConnection(final WebSocket connection) {
    this.connection = connection;
  }

  private void resetLatestResult() {
    this.error = null;
    this.latestEvent = null;
    this.receivedResponse = false;
  }

  private void error(final String response) {
    this.error = response;
    this.receivedResponse = true;
  }

  private void success(final JsonRpcSuccessEvent result) {
    this.latestEvent = result;
    this.receivedResponse = true;
  }

  private void success(final SubscriptionEvent result) {
    this.subscriptionEvents.add(result);
    this.receivedResponse = true;
  }

  public List<SubscriptionEvent> getSubscriptionEvents() {
    return new ArrayList<>(subscriptionEvents);
  }
}
