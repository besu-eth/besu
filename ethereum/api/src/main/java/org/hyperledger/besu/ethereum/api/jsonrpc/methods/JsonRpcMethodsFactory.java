/*
 * Copyright contributors to Besu.
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

import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.ApiConfiguration;
import org.hyperledger.besu.ethereum.api.graphql.GraphQLConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.filter.FilterManager;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.RpcModules;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.WebSocketConfiguration;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.blockcreation.MiningCoordinator;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.p2p.network.P2PNetwork;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeDnsConfiguration;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.permissioning.AccountLocalConfigPermissioningController;
import org.hyperledger.besu.ethereum.permissioning.NodeLocalConfigPermissioningController;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.metrics.ObservableMetricsSystem;
import org.hyperledger.besu.metrics.prometheus.MetricsConfiguration;
import org.hyperledger.besu.nat.NatService;
import org.hyperledger.besu.plugin.BesuPlugin;

import java.math.BigInteger;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.vertx.core.Vertx;

public class JsonRpcMethodsFactory {

  public Map<String, JsonRpcMethod> methods(
      final String clientNodeName,
      final String clientVersion,
      final String commit,
      final BigInteger networkId,
      final GenesisConfigOptions genesisConfigOptions,
      final P2PNetwork p2pNetwork,
      final BlockchainQueries blockchainQueries,
      final Synchronizer synchronizer,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final FilterManager filterManager,
      final TransactionPool transactionPool,
      final MiningConfiguration miningConfiguration,
      final MiningCoordinator miningCoordinator,
      final ObservableMetricsSystem metricsSystem,
      final Set<Capability> supportedCapabilities,
      final Optional<AccountLocalConfigPermissioningController> accountsAllowlistController,
      final Optional<NodeLocalConfigPermissioningController> nodeAllowlistController,
      final Collection<String> rpcApis,
      final JsonRpcConfiguration jsonRpcConfiguration,
      final WebSocketConfiguration webSocketConfiguration,
      final MetricsConfiguration metricsConfiguration,
      final GraphQLConfiguration graphQLConfiguration,
      final NatService natService,
      final Map<String, BesuPlugin> namedPlugins,
      final Path dataDir,
      final EthPeers ethPeers,
      final Vertx consensusEngineServer,
      final ApiConfiguration apiConfiguration,
      final Optional<EnodeDnsConfiguration> enodeDnsConfiguration,
      final TransactionSimulator transactionSimulator,
      final EthScheduler ethScheduler) {
    final Map<String, JsonRpcMethod> enabled = new HashMap<>();
    if (!rpcApis.isEmpty()) {
      final JsonRpcMethod modules = new RpcModules(rpcApis);
      enabled.put(modules.getName(), modules);
      final List<JsonRpcMethods> availableApiGroups =
          List.of(
              new AdminJsonRpcMethods(
                  clientNodeName,
                  networkId,
                  genesisConfigOptions,
                  p2pNetwork,
                  blockchainQueries,
                  namedPlugins,
                  natService,
                  ethPeers,
                  enodeDnsConfiguration,
                  protocolSchedule),
              new DebugJsonRpcMethods(
                  blockchainQueries,
                  protocolContext,
                  protocolSchedule,
                  metricsSystem,
                  transactionPool,
                  synchronizer,
                  dataDir,
                  transactionSimulator),
              new ExecutionEngineJsonRpcMethods(
                  miningCoordinator,
                  protocolSchedule,
                  protocolContext,
                  ethPeers,
                  consensusEngineServer,
                  clientVersion,
                  commit,
                  transactionPool,
                  metricsSystem),
              new EthJsonRpcMethods(
                  blockchainQueries,
                  synchronizer,
                  protocolSchedule,
                  filterManager,
                  transactionPool,
                  miningCoordinator,
                  miningConfiguration,
                  supportedCapabilities,
                  apiConfiguration,
                  genesisConfigOptions,
                  transactionSimulator,
                  metricsSystem),
              new NetJsonRpcMethods(
                  p2pNetwork,
                  networkId,
                  jsonRpcConfiguration,
                  webSocketConfiguration,
                  metricsConfiguration,
                  graphQLConfiguration),
              new MinerJsonRpcMethods(miningConfiguration, miningCoordinator),
              new PermJsonRpcMethods(accountsAllowlistController, nodeAllowlistController),
              new Web3JsonRpcMethods(clientNodeName),
              new TraceJsonRpcMethods(
                  blockchainQueries,
                  protocolSchedule,
                  protocolContext,
                  apiConfiguration,
                  transactionSimulator,
                  metricsSystem,
                  ethScheduler),
              new TxPoolJsonRpcMethods(transactionPool),
              new PluginsJsonRpcMethods(namedPlugins),
              new TestingJsonRpcMethods(
                  protocolContext,
                  protocolSchedule,
                  miningConfiguration,
                  transactionPool,
                  ethScheduler));

      for (final JsonRpcMethods apiGroup : availableApiGroups) {
        enabled.putAll(apiGroup.create(rpcApis));
      }
    }

    return enabled;
  }
}
