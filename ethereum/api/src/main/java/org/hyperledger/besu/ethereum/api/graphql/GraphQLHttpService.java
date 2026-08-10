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
package org.hyperledger.besu.ethereum.api.graphql;

import static com.google.common.base.Preconditions.checkArgument;
import static io.vertx.core.http.HttpMethod.GET;
import static io.vertx.core.http.HttpMethod.POST;

import org.hyperledger.besu.ethereum.api.graphql.internal.response.GraphQLErrorResponse;
import org.hyperledger.besu.ethereum.api.graphql.internal.response.GraphQLJsonRequest;
import org.hyperledger.besu.ethereum.api.graphql.internal.response.GraphQLResponse;
import org.hyperledger.besu.ethereum.api.graphql.internal.response.GraphQLResponseType;
import org.hyperledger.besu.ethereum.api.graphql.internal.response.GraphQLSuccessResponse;
import org.hyperledger.besu.ethereum.api.handlers.IsAliveHandler;
import org.hyperledger.besu.ethereum.api.handlers.TimeoutOptions;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.util.NetworkUtility;

import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.MediaType;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphQLError;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.ClientAuth;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.core.net.HostAndPort;
import io.vertx.core.net.JksOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.TimeoutHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class handles the HTTP service for GraphQL. It sets up the server, handles requests and
 * responses, and manages the lifecycle of the server.
 *
 * <p>It is responsible for processing GraphQL requests, executing them using the provided GraphQL
 * engine, and returning the results in the HTTP response.
 *
 * <p>It also handles errors and exceptions that may occur during the processing of a request.
 */
public class GraphQLHttpService {

  private static final Logger LOG = LoggerFactory.getLogger(GraphQLHttpService.class);

  private static final InetSocketAddress EMPTY_SOCKET_ADDRESS = new InetSocketAddress("0.0.0.0", 0);
  private static final String APPLICATION_JSON = "application/json";
  private static final String GRAPH_QL_ROUTE = "/graphql";
  private static final MediaType MEDIA_TYPE_JUST_JSON = MediaType.JSON_UTF_8.withoutParameters();
  private static final String EMPTY_RESPONSE = "";

  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

  private final Vertx vertx;
  private final GraphQLConfiguration config;
  private final Path dataDir;

  private HttpServer httpServer;

  private final GraphQL graphQL;

  private final Map<GraphQLContextType, Object> graphQlContextMap;

  private final EthScheduler scheduler;

  /**
   * Construct a GraphQLHttpService handler
   *
   * @param vertx The vertx process that will be running this service
   * @param dataDir The data directory where requests can be buffered
   * @param config Configuration for the rpc methods being loaded
   * @param graphQL GraphQL engine
   * @param graphQlContextMap GraphQlContext Map
   * @param scheduler {@link EthScheduler} used to trigger timeout on backend queries
   */
  public GraphQLHttpService(
      final Vertx vertx,
      final Path dataDir,
      final GraphQLConfiguration config,
      final GraphQL graphQL,
      final Map<GraphQLContextType, Object> graphQlContextMap,
      final EthScheduler scheduler) {
    this.dataDir = dataDir;

    validateConfig(config);
    this.config = config;
    this.vertx = vertx;
    this.graphQL = graphQL;
    this.graphQlContextMap = graphQlContextMap;
    this.scheduler = scheduler;
  }

  private void validateConfig(final GraphQLConfiguration config) {
    checkArgument(
        config.getPort() == 0 || NetworkUtility.isValidPort(config.getPort()),
        "Invalid port configuration.");
    checkArgument(config.getHost() != null, "Required host is not configured.");
  }

  /**
   * Starts the GraphQL HTTP service.
   *
   * <p>This method initializes the HTTP server and sets up the necessary routes for handling
   * GraphQL requests. It also validates the configuration and sets up the necessary handlers for
   * different types of requests.
   *
   * @return a CompletableFuture that will be completed when the server is successfully started.
   */
  public CompletableFuture<?> start() {
    LOG.info("Starting GraphQL HTTP service on {}:{}", config.getHost(), config.getPort());
    // Create the HTTP server and a router object.
    HttpServerOptions options =
        new HttpServerOptions()
            .setHost(config.getHost())
            .setPort(config.getPort())
            .setHandle100ContinueAutomatically(true)
            .setCompressionSupported(true);

    if (config.isTlsEnabled()) {
      try {
        options
            .setSsl(true)
            .setKeyCertOptions(
                new JksOptions()
                    .setPath(config.getTlsKeyStorePath())
                    .setPassword(config.getTlsKeyStorePassword()));
      } catch (Exception e) {
        LOG.error("Failed to get TLS keystore password", e);
        return CompletableFuture.failedFuture(e);
      }

      if (config.isMtlsEnabled()) {
        try {
          options
              .setTrustOptions(
                  new JksOptions()
                      .setPath(config.getTlsTrustStorePath())
                      .setPassword(config.getTlsTrustStorePassword()))
              .setClientAuth(ClientAuth.REQUIRED);
        } catch (Exception e) {
          LOG.error("Failed to get TLS truststore password", e);
          return CompletableFuture.failedFuture(e);
        }
      }
    }

    LOG.info("Options {}", options);
    httpServer = vertx.createHttpServer(options);

    // Handle graphql http requests
    final Router router = Router.router(vertx);

    // Verify Host header to avoid rebind attack.
    router.route().handler(checkAllowlistHostHeader());

    router
        .route()
        .handler(
            CorsHandler.create()
                .addOriginWithRegex(buildCorsRegexFromConfig())
                .allowedHeader("*")
                .allowedHeader("content-type"));
    router
        .route()
        .handler(
            BodyHandler.create()
                .setUploadsDirectory(dataDir.resolve("uploads").toString())
                .setDeleteUploadedFilesOnEnd(true)
                .setPreallocateBodyBuffer(true));
    router.route("/").method(GET).method(POST).handler(this::handleEmptyRequestAndRedirect);
    router
        .route(GRAPH_QL_ROUTE)
        .method(GET)
        .method(POST)
        .produces(APPLICATION_JSON)
        .handler(
            TimeoutHandler.create(
                TimeUnit.SECONDS.toMillis(config.getHttpTimeoutSec()),
                TimeoutOptions.DEFAULT_ERROR_CODE))
        .handler(this::handleGraphQLRequest);

    final CompletableFuture<?> resultFuture = new CompletableFuture<>();
    httpServer
        .requestHandler(router)
        .listen()
        .onComplete(
            res -> {
              if (!res.failed()) {
                resultFuture.complete(null);
                LOG.info(
                    "GraphQL HTTP service started and listening on {}:{}",
                    config.getHost(),
                    httpServer.actualPort());
                return;
              }
              httpServer = null;
              final Throwable cause = res.cause();
              if (cause instanceof SocketException) {
                resultFuture.completeExceptionally(
                    new GraphQLServiceException(
                        String.format(
                            "Failed to bind Ethereum GraphQL HTTP listener to %s:%s: %s",
                            config.getHost(), config.getPort(), cause.getMessage())));
                return;
              }
              resultFuture.completeExceptionally(cause);
            });

    return resultFuture;
  }

  private Handler<RoutingContext> checkAllowlistHostHeader() {
    return event -> {
      final Optional<String> hostHeader = getAndValidateHostHeader(event);
      if (config.getHostsAllowlist().contains("*")
          || (hostHeader.isPresent() && hostIsInAllowlist(hostHeader.get()))) {
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

  private Optional<String> getAndValidateHostHeader(final RoutingContext event) {
    final HostAndPort hostAndPort = event.request().authority();
    return Optional.ofNullable(hostAndPort).map(HostAndPort::host);
  }

  private boolean hostIsInAllowlist(final String hostHeader) {
    if (config.getHostsAllowlist().stream()
        .anyMatch(allowlistEntry -> allowlistEntry.equalsIgnoreCase(hostHeader))) {
      return true;
    } else {
      LOG.trace("Host not in allowlist: '{}'", hostHeader);
      return false;
    }
  }

  /**
   * Stops the GraphQL HTTP service.
   *
   * <p>This method stops the HTTP server that was created and started by the start() method. If the
   * server is not running, this method will do nothing.
   *
   * @return a CompletableFuture that will be completed when the server is successfully stopped.
   */
  public CompletableFuture<?> stop() {
    if (httpServer == null) {
      return CompletableFuture.completedFuture(null);
    }

    final CompletableFuture<?> resultFuture = new CompletableFuture<>();
    httpServer
        .close()
        .onComplete(
            res -> {
              if (res.failed()) {
                resultFuture.completeExceptionally(res.cause());
              } else {
                httpServer = null;
                resultFuture.complete(null);
              }
            });
    return resultFuture;
  }

  /**
   * Returns the socket address of the GraphQL HTTP service.
   *
   * <p>This method returns the socket address that the HTTP server is bound to. If the server is
   * not running, it returns an empty socket address.
   *
   * @return the socket address of the HTTP server, or an empty socket address if the server is not
   *     running.
   */
  public InetSocketAddress socketAddress() {
    if (httpServer == null) {
      return EMPTY_SOCKET_ADDRESS;
    }
    return new InetSocketAddress(config.getHost(), httpServer.actualPort());
  }

  /**
   * Returns the URL of the GraphQL HTTP service.
   *
   * <p>This method constructs and returns the URL that the HTTP server is bound to. If the server
   * is not running, it returns an empty string.
   *
   * @return the URL of the HTTP server, or an empty string if the server is not running.
   */
  @VisibleForTesting
  public String url() {
    if (httpServer == null) {
      return "";
    }
    String scheme = config.isTlsEnabled() ? "https" : "http";
    return NetworkUtility.urlForSocketAddress(scheme, socketAddress());
  }

  // Empty Get/Post requests to / will be redirected to /graphql using 308 Permanent Redirect
  private void handleEmptyRequestAndRedirect(final RoutingContext routingContext) {
    final HttpServerResponse response = routingContext.response();
    if (response.closed()) {
      return;
    }
    response.setStatusCode(HttpResponseStatus.PERMANENT_REDIRECT.code());
    response.putHeader("Location", "/graphql");
    response.end();
  }

  private void handleGraphQLRequest(final RoutingContext routingContext) {
    try {
      final String query;
      final String operationName;
      final Map<String, Object> variables;
      final HttpServerRequest request = routingContext.request();

      switch (request.method().name()) {
        case "GET":
          final String queryString = request.getParam("query");
          query = Objects.requireNonNullElse(queryString, "");
          operationName = request.getParam("operationName");
          final String variableString = request.getParam("variables");
          if (variableString != null) {
            try {
              variables = DatabindCodec.mapper().readValue(variableString, MAP_TYPE);
            } catch (final JsonProcessingException e) {
              throw new DecodeException(e.getMessage(), e);
            }
          } else {
            variables = Collections.emptyMap();
          }
          break;
        case "POST":
          final String contentType = request.getHeader(HttpHeaders.CONTENT_TYPE);
          if (contentType != null && MediaType.parse(contentType).is(MEDIA_TYPE_JUST_JSON)) {
            final String requestBody = routingContext.body().asString().trim();
            final GraphQLJsonRequest jsonRequest =
                Json.decodeValue(requestBody, GraphQLJsonRequest.class);
            final String jsonQuery = jsonRequest.getQuery();
            query = Objects.requireNonNullElse(jsonQuery, "");
            operationName = jsonRequest.getOperationName();
            final Map<String, Object> jsonVariables = jsonRequest.getVariables();
            variables = Objects.requireNonNullElse(jsonVariables, Collections.emptyMap());
          } else {
            // treat all else as application/graphql
            final String requestQuery = routingContext.body().asString().trim();
            query = Objects.requireNonNullElse(requestQuery, "");
            operationName = null;
            variables = Collections.emptyMap();
          }
          break;
        default:
          routingContext
              .response()
              .setStatusCode(HttpResponseStatus.METHOD_NOT_ALLOWED.code())
              .end();
          return;
      }

      final HttpServerResponse response = routingContext.response();
      vertx
          .executeBlocking(() -> process(query, operationName, variables), false)
          .onComplete(
              (res) -> {
                if (response.closed()) {
                  return;
                }
                response.putHeader("Content-Type", MediaType.JSON_UTF_8.toString());
                if (res.failed()) {
                  response.setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
                  response.end(
                      serialise(
                          new GraphQLErrorResponse(
                              Collections.singletonMap(
                                  "errors",
                                  Collections.singletonList(
                                      Collections.singletonMap(
                                          "message", res.cause().getMessage()))))));
                } else {
                  final GraphQLResponse graphQLResponse = res.result();
                  response.setStatusCode(status(graphQLResponse).code());
                  response.end(serialise(graphQLResponse));
                }
              });

    } catch (final DecodeException ex) {
      handleGraphQLError(routingContext, ex);
    }
  }

  private HttpResponseStatus status(final GraphQLResponse response) {

    switch (response.getType()) {
      case UNAUTHORIZED:
        return HttpResponseStatus.UNAUTHORIZED;
      case ERROR:
        return HttpResponseStatus.BAD_REQUEST;
      case SUCCESS:
      case NONE:
      default:
        return HttpResponseStatus.OK;
    }
  }

  private String serialise(final GraphQLResponse response) {

    if (response.getType() == GraphQLResponseType.NONE) {
      return EMPTY_RESPONSE;
    }

    return Json.encodePrettily(response.getResult());
  }

  private GraphQLResponse process(
      final String requestJson, final String operationName, final Map<String, Object> variables) {
    Map<GraphQLContextType, Object> contextMap = new ConcurrentHashMap<>();
    contextMap.putAll(graphQlContextMap);
    contextMap.put(
        GraphQLContextType.IS_ALIVE_HANDLER,
        new IsAliveHandler(scheduler, config.getHttpTimeoutSec()));
    final ExecutionInput executionInput =
        ExecutionInput.newExecutionInput()
            .query(requestJson)
            .operationName(operationName)
            .variables(variables)
            .graphQLContext(contextMap)
            .build();
    final ExecutionResult result = graphQL.execute(executionInput);
    final Map<String, Object> toSpecificationResult = result.toSpecification();
    final List<GraphQLError> errors = result.getErrors();
    if (errors.size() == 0) {
      return new GraphQLSuccessResponse(toSpecificationResult);
    } else {
      return new GraphQLErrorResponse(toSpecificationResult);
    }
  }

  private void handleGraphQLError(final RoutingContext routingContext, final Exception ex) {
    LOG.debug("Error handling GraphQL request", ex);
    final HttpServerResponse response = routingContext.response();
    if (!response.closed()) {
      response
          .setStatusCode(HttpResponseStatus.BAD_REQUEST.code())
          .end(Json.encode(new GraphQLErrorResponse(ex.getMessage())));
    }
  }

  private String buildCorsRegexFromConfig() {
    if (config.getCorsAllowedDomains().isEmpty()) {
      return "";
    }
    if (config.getCorsAllowedDomains().contains("*")) {
      return ".*";
    } else {
      final StringJoiner stringJoiner = new StringJoiner("|");
      config.getCorsAllowedDomains().stream().filter(s -> !s.isEmpty()).forEach(stringJoiner::add);
      return stringJoiner.toString();
    }
  }
}
