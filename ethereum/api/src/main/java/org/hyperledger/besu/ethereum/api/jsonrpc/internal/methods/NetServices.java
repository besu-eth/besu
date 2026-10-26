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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import org.hyperledger.besu.ethereum.api.graphql.GraphQLConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.WebSocketConfiguration;
import org.hyperledger.besu.ethereum.p2p.network.P2PNetwork;
import org.hyperledger.besu.metrics.prometheus.MetricsConfiguration;
import org.hyperledger.besu.plugin.data.EnodeURL;
import org.hyperledger.besu.plugin.services.NetworkEndpointService;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetServices implements JsonRpcMethod {
  private static final Logger LOG = LoggerFactory.getLogger(NetServices.class);

  private final JsonRpcConfiguration jsonRpcConfiguration;
  private final WebSocketConfiguration webSocketConfiguration;
  private final P2PNetwork p2pNetwork;
  private final MetricsConfiguration metricsConfiguration;
  private final GraphQLConfiguration graphQLConfiguration;
  private final NetworkEndpointService networkEndpointService;
  private final Set<String> warnedNameClashes = ConcurrentHashMap.newKeySet();

  public NetServices(
      final JsonRpcConfiguration jsonRpcConfiguration,
      final WebSocketConfiguration webSocketConfiguration,
      final P2PNetwork p2pNetwork,
      final MetricsConfiguration metricsConfiguration,
      final NetworkEndpointService networkEndpointService,
      final GraphQLConfiguration graphQLConfiguration) {
    this.jsonRpcConfiguration = jsonRpcConfiguration;
    this.webSocketConfiguration = webSocketConfiguration;
    this.p2pNetwork = p2pNetwork;
    this.metricsConfiguration = metricsConfiguration;
    this.graphQLConfiguration = graphQLConfiguration;
    this.networkEndpointService = networkEndpointService;
  }

  @Override
  public String getName() {
    return RpcMethod.NET_SERVICES.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    final Map<String, ImmutableMap<String, String>> servicesMap = new LinkedHashMap<>();

    if (jsonRpcConfiguration.isEnabled()) {
      servicesMap.put(
          "jsonrpc",
          createServiceDetailsMap(jsonRpcConfiguration.getHost(), jsonRpcConfiguration.getPort()));
    }
    if (webSocketConfiguration.isEnabled()) {
      servicesMap.put(
          "ws",
          createServiceDetailsMap(
              webSocketConfiguration.getHost(), webSocketConfiguration.getPort()));
    }
    if (p2pNetwork.isP2pEnabled()) {
      p2pNetwork
          .getLocalEnode()
          .filter(EnodeURL::isListening)
          .ifPresent(
              enode ->
                  servicesMap.put(
                      "p2p",
                      createServiceDetailsMap(
                          enode.getIpAsString(), enode.getListeningPort().get())));
    }
    if (metricsConfiguration.isEnabled()) {
      servicesMap.put(
          "metrics",
          createServiceDetailsMap(
              metricsConfiguration.getHost(), metricsConfiguration.getActualPort()));
    }
    if (graphQLConfiguration.isEnabled()) {
      servicesMap.put(
          "graphQL",
          createServiceDetailsMap(graphQLConfiguration.getHost(), graphQLConfiguration.getPort()));
    }
    // Listeners owned by plugins come after the built-in services, in registration order; a
    // built-in entry wins on a name clash. A misbehaving endpoint is skipped so that it cannot
    // break net_services for every caller.
    for (final NetworkEndpointService.NetworkEndpoint endpoint :
        networkEndpointService.getEndpoints()) {
      try {
        if (!endpoint.isEnabled()) {
          continue;
        }
        final String serviceName = endpoint.getServiceName();
        if (servicesMap.containsKey(serviceName)) {
          if (warnedNameClashes.add(serviceName)) {
            LOG.warn(
                "Plugin network endpoint {} clashes with a built-in service of the same name and is"
                    + " not reported by {}",
                serviceName,
                getName());
          }
          continue;
        }
        final String host = endpoint.getHost();
        if (host == null) {
          LOG.warn("Plugin network endpoint {} has no host and is not reported", serviceName);
          continue;
        }
        servicesMap.put(serviceName, createServiceDetailsMap(host, endpoint.getConfiguredPort()));
      } catch (final RuntimeException e) {
        LOG.warn("Failed to read plugin network endpoint for {}: {}", getName(), e.toString());
      }
    }

    return new JsonRpcSuccessResponse(
        requestContext.getRequest().getId(), ImmutableMap.copyOf(servicesMap));
  }

  private ImmutableMap<String, String> createServiceDetailsMap(final String host, final int port) {
    return ImmutableMap.of("host", host, "port", String.valueOf(port));
  }
}
