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
package org.hyperledger.besu.metrics.prometheus;

import static com.google.common.base.Preconditions.checkArgument;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;

import org.hyperledger.besu.metrics.MetricsService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.sun.net.httpserver.Authenticator;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpPrincipal;
import io.prometheus.metrics.exporter.httpserver.DefaultHandler;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.vertx.core.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Metrics http service. */
public class MetricsHttpService implements MetricsService {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsHttpService.class);
  private static final Authenticator.Result AUTHORIZED =
      new Authenticator.Success(new HttpPrincipal("metrics", "metrics"));
  private static final Authenticator.Result NOT_AUTHORIZED = new Authenticator.Failure(403);
  private static final InetSocketAddress EMPTY_SOCKET_ADDRESS = new InetSocketAddress("0.0.0.0", 0);

  private final MetricsConfiguration config;
  private final PrometheusMetricsSystem metricsSystem;
  private HTTPServer httpServer;

  /**
   * Instantiates a new Metrics http service.
   *
   * @param configuration the configuration
   * @param metricsSystem the metrics system
   */
  public MetricsHttpService(
      final MetricsConfiguration configuration, final PrometheusMetricsSystem metricsSystem) {
    validateConfig(configuration);
    this.config = configuration;
    this.metricsSystem = metricsSystem;
  }

  private void validateConfig(final MetricsConfiguration config) {
    checkArgument(config.getPort() >= 0 && config.getPort() < 65535, "Invalid port configuration.");
    checkArgument(config.getHost() != null, "Required host is not configured.");
    checkArgument(
        !(config.isEnabled() && config.isPushEnabled()),
        "Metrics Http Service cannot run concurrent with push metrics.");
  }

  @Override
  public CompletableFuture<?> start() {
    LOG.info("Starting metrics http service on {}:{}", config.getHost(), config.getPort());

    try {
      httpServer =
          HTTPServer.builder()
              .hostname(config.getHost())
              .port(config.getPort())
              .registry(metricsSystem.getRegistry())
              .authenticator(
                  new Authenticator() {
                    @Override
                    public Result authenticate(final HttpExchange exch) {
                      return checkAllowlistHostHeader(exch);
                    }
                  })
              .defaultHandler(new RestrictedDefaultHandler())
              .buildAndStart();

      return CompletableFuture.completedFuture(null);
    } catch (final Throwable e) {
      return CompletableFuture.failedFuture(e);
    }
  }

  private Authenticator.Result checkAllowlistHostHeader(final HttpExchange exch) {
    if (config.getHostsAllowlist().contains("*")) {
      return AUTHORIZED;
    }

    return Optional.ofNullable(exch.getRequestHeaders().getFirst("Host"))
        .map(host -> HostAndPort.parseAuthority(host, -1).host())
        .filter(this::hostIsInAllowlist)
        .map(unused -> AUTHORIZED)
        .orElse(NOT_AUTHORIZED);
  }

  private boolean hostIsInAllowlist(final String hostHeader) {
    if (config.getHostsAllowlist().stream()
        .anyMatch(
            allowlistEntry ->
                allowlistEntry
                    .toLowerCase(Locale.ROOT)
                    .equals(hostHeader.toLowerCase(Locale.ROOT)))) {
      return true;
    } else {
      LOG.trace("Host not in allowlist: '{}'", hostHeader);
      return false;
    }
  }

  @Override
  public CompletableFuture<?> stop() {
    metricsSystem.shutdown();
    if (httpServer == null) {
      return CompletableFuture.completedFuture(null);
    }

    try {
      httpServer.stop();
      return CompletableFuture.completedFuture(null);
    } catch (final Throwable e) {
      return CompletableFuture.failedFuture(e);
    }
  }

  /**
   * Socket address inet socket address.
   *
   * @return the inet socket address
   */
  InetSocketAddress socketAddress() {
    if (httpServer == null) {
      return EMPTY_SOCKET_ADDRESS;
    }
    return new InetSocketAddress(config.getHost(), httpServer.getPort());
  }

  @Override
  public Optional<Integer> getPort() {
    if (httpServer == null) {
      return Optional.empty();
    }
    return Optional.of(httpServer.getPort());
  }

  private static class RestrictedDefaultHandler extends DefaultHandler {
    RestrictedDefaultHandler() {
      super("/metrics");
    }

    @Override
    public void handle(final HttpExchange exchange) throws IOException {
      if (!exchange.getRequestURI().getPath().equals(exchange.getHttpContext().getPath())) {
        try {
          exchange.sendResponseHeaders(HTTP_NOT_FOUND, -1);
        } finally {
          exchange.close();
        }
      } else {
        super.handle(exchange);
      }
    }
  }
}
