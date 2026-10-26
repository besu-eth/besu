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
package org.hyperledger.besu.ethereum.api.jsonrpc.methods;

import org.hyperledger.besu.ethereum.api.graphql.GraphQLConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcApis;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.NetEnode;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.NetListening;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.NetPeerCount;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.NetServices;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.NetVersion;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.WebSocketConfiguration;
import org.hyperledger.besu.ethereum.p2p.network.P2PNetwork;
import org.hyperledger.besu.metrics.prometheus.MetricsConfiguration;
import org.hyperledger.besu.plugin.services.NetworkEndpointService;

import java.math.BigInteger;
import java.util.Map;
import java.util.Optional;

public class NetJsonRpcMethods extends ApiGroupJsonRpcMethods {

  private final P2PNetwork p2pNetwork;
  private final BigInteger networkId;
  private final JsonRpcConfiguration jsonRpcConfiguration;
  private final WebSocketConfiguration webSocketConfiguration;
  private final MetricsConfiguration metricsConfiguration;
  private final GraphQLConfiguration graphQLConfiguration;
  private final NetworkEndpointService networkEndpointService;

  public NetJsonRpcMethods(
      final P2PNetwork p2pNetwork,
      final BigInteger networkId,
      final JsonRpcConfiguration jsonRpcConfiguration,
      final WebSocketConfiguration webSocketConfiguration,
      final MetricsConfiguration metricsConfiguration,
      final NetworkEndpointService networkEndpointService,
      final GraphQLConfiguration graphQLConfiguration) {
    this.p2pNetwork = p2pNetwork;
    this.networkId = networkId;
    this.jsonRpcConfiguration = jsonRpcConfiguration;
    this.webSocketConfiguration = webSocketConfiguration;
    this.metricsConfiguration = metricsConfiguration;
    this.graphQLConfiguration = graphQLConfiguration;
    this.networkEndpointService = networkEndpointService;
  }

  @Override
  protected String getApiGroup() {
    return RpcApis.NET.name();
  }

  @Override
  protected Map<String, JsonRpcMethod> create() {
    return mapOf(
        new NetVersion(Optional.of(networkId)),
        new NetListening(p2pNetwork),
        new NetPeerCount(p2pNetwork),
        new NetEnode(p2pNetwork),
        new NetServices(
            jsonRpcConfiguration,
            webSocketConfiguration,
            p2pNetwork,
            metricsConfiguration,
            networkEndpointService,
            graphQLConfiguration));
  }
}
