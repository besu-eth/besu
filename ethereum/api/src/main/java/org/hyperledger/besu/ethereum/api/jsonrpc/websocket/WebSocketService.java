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

import static org.hyperledger.besu.ethereum.api.jsonrpc.authentication.AuthenticationUtils.truncToken;

import org.hyperledger.besu.ethereum.api.jsonrpc.authentication.AuthenticationService;
import org.hyperledger.besu.ethereum.api.jsonrpc.authentication.AuthenticationUtils;
import org.hyperledger.besu.ethereum.api.jsonrpc.authentication.DefaultAuthenticationService;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.Logging403ErrorHandler;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.SubscriptionManager;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.net.InetSocketAddress;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.ClientAuth;
import io.vertx.core.http.HttpConnection;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.ServerWebSocketHandshake;
import io.vertx.core.net.HostAndPort;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebSocketService {

  private static final Logger LOG = LoggerFactory.getLogger(WebSocketService.class);

  private static final InetSocketAddress EMPTY_SOCKET_ADDRESS = new InetSocketAddress("0.0.0.0", 0);
  private static final String APPLICATION_JSON = "application/json";

  private final int maxActiveConnections;
  private final AtomicInteger activeConnectionsCount = new AtomicInteger();

  private final Vertx vertx;
  private final WebSocketConfiguration configuration;
  private final WebSocketMessageHandler websocketMessageHandler;

  private HttpServer httpServer;

  @VisibleForTesting public final Optional<AuthenticationService> authenticationService;

  public WebSocketService(
      final Vertx vertx,
      final WebSocketConfiguration configuration,
      final WebSocketMessageHandler websocketMessageHandler,
      final MetricsSystem metricsSystem) {
    this(
        vertx,
        configuration,
        websocketMessageHandler,
        DefaultAuthenticationService.create(vertx, configuration),
        metricsSystem);
  }

  public WebSocketService(
      final Vertx vertx,
      final WebSocketConfiguration configuration,
      final WebSocketMessageHandler websocketMessageHandler,
      final Optional<AuthenticationService> authenticationService,
      final MetricsSystem metricsSystem) {
    this.vertx = vertx;
    this.configuration = configuration;
    this.websocketMessageHandler = websocketMessageHandler;
    this.authenticationService = authenticationService;
    this.maxActiveConnections = configuration.getMaxActiveConnections();

    metricsSystem.createIntegerGauge(
        BesuMetricCategory.RPC,
        "active_ws_connection_count",
        "Total no of active rpc ws connections",
        activeConnectionsCount::intValue);
  }

  public CompletableFuture<?> start() {
    LOG.info(
        "Starting Websocket service on {}:{}", configuration.getHost(), configuration.getPort());

    final CompletableFuture<?> resultFuture = new CompletableFuture<>();
    HttpServerOptions serverOptions =
        new HttpServerOptions()
            .setHost(configuration.getHost())
            .setPort(configuration.getPort())
            .setHandle100ContinueAutomatically(true)
            .setCompressionSupported(true)
            .addWebSocketSubProtocol("undefined")
            .setMaxWebSocketFrameSize(configuration.getMaxFrameSize())
            .setMaxWebSocketMessageSize(configuration.getMaxFrameSize() * 4)
            .setRegisterWebSocketWriteHandlers(true);

    // Check if SSL/TLS is enabled in the configuration
    if (configuration.isSslEnabled()) {
      serverOptions.setSsl(true);
      String keystorePassword = null;

      String keystorePath = configuration.getKeyStorePath().orElse(null);
      try {
        keystorePassword = configuration.getKeyStorePassword().orElse(null);
      } catch (Exception e) {
        LOG.error("Error reading keystore password", e);
        resultFuture.completeExceptionally(e);
        return resultFuture;
      }
      String keyPath = configuration.getKeyPath().orElse(null);
      String certPath = configuration.getCertPath().orElse(null);

      String keystoreType = configuration.getKeyStoreType().orElse("JKS");

      switch (keystoreType.toUpperCase(Locale.getDefault())) {
        case "PEM":
          serverOptions.setKeyCertOptions(
              new PemKeyCertOptions().setKeyPath(keyPath).setCertPath(certPath));
          break;
        case "JKS":
        default:
          serverOptions.setKeyCertOptions(
              new JksOptions().setPath(keystorePath).setPassword(keystorePassword));
          break;
      }
    }

    //  Set up truststore for client authentication (mTLS)
    if (configuration.isClientAuthEnabled()) {
      serverOptions.setClientAuth(ClientAuth.REQUIRED);
      String truststorePassword;

      String truststorePath = configuration.getTrustStorePath().orElse(null);
      try {
        truststorePassword = configuration.getTrustStorePassword().orElse(null);
      } catch (Exception e) {
        LOG.error("Error reading truststore password", e);
        resultFuture.completeExceptionally(e);
        return resultFuture;
      }
      String truststoreType = configuration.getTrustStoreType().orElse("JKS");
      String trustCertPath = configuration.getTrustCertPath().orElse(null);

      switch (truststoreType.toUpperCase(Locale.getDefault())) {
        case "PEM":
          serverOptions.setTrustOptions(new PemTrustOptions().addCertPath(trustCertPath));
          break;
        case "JKS":
        default:
          serverOptions.setTrustOptions(
              new JksOptions().setPath(truststorePath).setPassword(truststorePassword));
          break;
      }
    }

    httpServer =
        vertx
            .createHttpServer(serverOptions)
            .webSocketHandshakeHandler(webSocketHandshakeHandler())
            .connectionHandler(connectionHandler())
            .requestHandler(httpHandler());

    httpServer.listen().onComplete(startHandler(resultFuture));

    return resultFuture;
  }

  private Handler<ServerWebSocketHandshake> webSocketHandshakeHandler() {
    return handshake -> {
      final String token = getAuthToken(handshake);
      if (token != null) {
        LOG.atTrace()
            .setMessage("Websocket authentication token {}")
            .addArgument(() -> truncToken(token))
            .log();
      }

      if (!checkHostInAllowlist(
          Optional.ofNullable(handshake.authority()).map(HostAndPort::host))) {
        handshake.reject(403);
        return;
      }

      handshake
          .accept()
          .onSuccess(
              websocket -> {
                final SocketAddress socketAddress = websocket.remoteAddress();
                final String connectionId = websocket.textHandlerID();

                LOG.debug("Websocket Connected ({})", socketAddressAsString(socketAddress));

                final Handler<Buffer> socketHandler =
                    buffer -> {
                      LOG.debug(
                          "Received Websocket request (binary frame) {} ({})",
                          buffer.toString(),
                          socketAddressAsString(socketAddress));

                      if (authenticationService.isPresent()) {
                        authenticationService
                            .get()
                            .authenticate(
                                token,
                                user -> websocketMessageHandler.handle(websocket, buffer, user));
                      } else {
                        websocketMessageHandler.handle(websocket, buffer, Optional.empty());
                      }
                    };
                websocket.textMessageHandler(text -> socketHandler.handle(Buffer.buffer(text)));
                websocket.binaryMessageHandler(socketHandler);

                websocket.closeHandler(
                    v -> {
                      LOG.debug(
                          "Websocket Disconnected ({})", socketAddressAsString(socketAddress));
                      vertx
                          .eventBus()
                          .publish(
                              SubscriptionManager.EVENTBUS_REMOVE_SUBSCRIPTIONS_ADDRESS,
                              connectionId);
                    });

                websocket.exceptionHandler(
                    t -> {
                      LOG.debug(
                          "Unrecoverable error on Websocket: {} ({})",
                          t.getMessage(),
                          socketAddressAsString(socketAddress));
                      websocket.close();
                    });
              })
          .onFailure(t -> LOG.debug("Failed to accept websocket handshake", t));
    };
  }

  private Handler<HttpConnection> connectionHandler() {

    return connection -> {
      if (activeConnectionsCount.get() >= maxActiveConnections) {
        // disallow new connections to prevent DoS
        LOG.warn(
            "Rejecting new connection from {}. {}/{} max active connections limit reached.",
            connection.remoteAddress(),
            activeConnectionsCount.getAndIncrement(),
            maxActiveConnections);
        connection.close();
      } else {
        LOG.debug(
            "Opened connection from {}. Total of active connections: {}/{}",
            connection.remoteAddress(),
            activeConnectionsCount.incrementAndGet(),
            maxActiveConnections);
      }
      connection.closeHandler(
          c ->
              LOG.debug(
                  "Connection closed from {}. Total of active connections: {}/{}",
                  connection.remoteAddress(),
                  activeConnectionsCount.decrementAndGet(),
                  maxActiveConnections));
    };
  }

  private Handler<HttpServerRequest> httpHandler() {
    final Router router = Router.router(vertx);

    // Verify Host header to avoid rebind attack.
    router.route().handler(checkAllowlistHostHeader());

    if (authenticationService.isPresent()) {
      router.route("/login").handler(BodyHandler.create());
      router
          .post("/login")
          .produces(APPLICATION_JSON)
          .handler(authenticationService.get()::handleLogin);
    } else {
      router
          .post("/login")
          .produces(APPLICATION_JSON)
          .handler(DefaultAuthenticationService::handleDisabledLogin);
    }
    router.errorHandler(403, new Logging403ErrorHandler());
    router.route().handler(WebSocketService::handleHttpNotSupported);
    return router;
  }

  private static void handleHttpNotSupported(final RoutingContext http) {
    final HttpServerResponse response = http.response();
    if (!response.closed()) {
      response.setStatusCode(400).end("Websocket endpoint can't handle HTTP requests");
    }
  }

  private Handler<AsyncResult<HttpServer>> startHandler(final CompletableFuture<?> resultFuture) {
    return res -> {
      if (res.succeeded()) {

        final int actualPort = res.result().actualPort();
        LOG.info(
            "Websocket service started and listening on {}:{}",
            configuration.getHost(),
            actualPort);
        configuration.setPort(actualPort);
        resultFuture.complete(null);
      } else {
        resultFuture.completeExceptionally(res.cause());
      }
    };
  }

  public CompletableFuture<?> stop() {
    if (httpServer == null) {
      return CompletableFuture.completedFuture(null);
    }

    final CompletableFuture<?> resultFuture = new CompletableFuture<>();

    httpServer
        .close()
        .onComplete(
            res -> {
              if (res.succeeded()) {
                httpServer = null;
                resultFuture.complete(null);
              } else {
                resultFuture.completeExceptionally(res.cause());
              }
            });

    return resultFuture;
  }

  public InetSocketAddress socketAddress() {
    if (httpServer == null) {
      return EMPTY_SOCKET_ADDRESS;
    }
    return new InetSocketAddress(configuration.getHost(), httpServer.actualPort());
  }

  private String socketAddressAsString(final SocketAddress socketAddress) {
    return String.format("host=%s, port=%d", socketAddress.host(), socketAddress.port());
  }

  private String getAuthToken(final ServerWebSocketHandshake handshake) {
    return AuthenticationUtils.getJwtTokenFromAuthorizationHeaderValue(
        handshake.headers().get("Authorization"));
  }

  private Handler<RoutingContext> checkAllowlistHostHeader() {
    return event -> {
      if (checkHostInAllowlist(
          Optional.ofNullable(event.request().authority()).map(HostAndPort::host))) {
        event.next();
      } else {
        final HttpServerResponse response = event.response();
        if (!response.closed()) {
          response
              .setStatusCode(403)
              .putHeader("Content-Type", "application/json; charset=utf-8")
              .end("{\"message\":\"Host not authorized.\"}");
        }
      }
    };
  }

  @VisibleForTesting
  boolean checkHostInAllowlist(final Optional<String> host) {
    return configuration.getHostsAllowlist().contains("*")
        || host.map(
                header ->
                    configuration.getHostsAllowlist().stream()
                        .anyMatch(allowListEntry -> allowListEntry.equalsIgnoreCase(header)))
            .orElse(false);
  }
}
