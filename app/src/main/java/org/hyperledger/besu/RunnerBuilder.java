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
package org.hyperledger.besu;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.isNull;
import static java.util.function.Predicate.isEqual;
import static java.util.function.Predicate.not;
import static org.hyperledger.besu.controller.BesuController.CACHE_PATH;

import org.hyperledger.besu.cli.config.EthNetworkConfig;
import org.hyperledger.besu.cli.options.EthstatsOptions;
import org.hyperledger.besu.controller.BesuController;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.ApiConfiguration;
import org.hyperledger.besu.ethereum.api.graphql.GraphQLConfiguration;
import org.hyperledger.besu.ethereum.api.graphql.GraphQLContextType;
import org.hyperledger.besu.ethereum.api.graphql.GraphQLDataFetchers;
import org.hyperledger.besu.ethereum.api.graphql.GraphQLHttpService;
import org.hyperledger.besu.ethereum.api.graphql.GraphQLProvider;
import org.hyperledger.besu.ethereum.api.jsonrpc.EngineJsonRpcService;
import org.hyperledger.besu.ethereum.api.jsonrpc.InProcessRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcHttpService;
import org.hyperledger.besu.ethereum.api.jsonrpc.authentication.AuthenticationService;
import org.hyperledger.besu.ethereum.api.jsonrpc.authentication.DefaultAuthenticationService;
import org.hyperledger.besu.ethereum.api.jsonrpc.authentication.EngineAuthService;
import org.hyperledger.besu.ethereum.api.jsonrpc.execution.AuthenticatedJsonRpcProcessor;
import org.hyperledger.besu.ethereum.api.jsonrpc.execution.BaseJsonRpcProcessor;
import org.hyperledger.besu.ethereum.api.jsonrpc.execution.JsonRpcExecutor;
import org.hyperledger.besu.ethereum.api.jsonrpc.execution.JsonRpcProcessor;
import org.hyperledger.besu.ethereum.api.jsonrpc.health.HealthService;
import org.hyperledger.besu.ethereum.api.jsonrpc.health.LivenessCheck;
import org.hyperledger.besu.ethereum.api.jsonrpc.health.ReadinessCheck;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.filter.FilterManager;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.filter.FilterManagerBuilder;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.ipc.JsonRpcIpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.ipc.JsonRpcIpcService;
import org.hyperledger.besu.ethereum.api.jsonrpc.methods.JsonRpcMethodsFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.WebSocketConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.WebSocketMessageHandler;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.WebSocketService;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.methods.WebSocketMethodsFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.SubscriptionManager;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.blockheaders.NewBlockHeadersSubscriptionService;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.logs.LogsSubscriptionService;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.pending.PendingTransactionDroppedSubscriptionService;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.pending.PendingTransactionSubscriptionService;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.syncing.SyncingSubscriptionService;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.blockcreation.MiningCoordinator;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.p2p.config.DiscoveryConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.ImmutableNetworkingConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.NetworkingConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.RlpxConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.SubProtocolConfiguration;
import org.hyperledger.besu.ethereum.p2p.discovery.DefaultPeerDiscoveryAgentFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.DefaultRlpxAgentFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.NodeIdentifier;
import org.hyperledger.besu.ethereum.p2p.discovery.PeerDiscoveryAgentFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.RlpxAgentFactory;
import org.hyperledger.besu.ethereum.p2p.network.DefaultP2PNetwork;
import org.hyperledger.besu.ethereum.p2p.network.NetworkRunner;
import org.hyperledger.besu.ethereum.p2p.network.NetworkRunner.NetworkBuilder;
import org.hyperledger.besu.ethereum.p2p.network.NoopP2PNetwork;
import org.hyperledger.besu.ethereum.p2p.network.P2PNetwork;
import org.hyperledger.besu.ethereum.p2p.network.ProtocolManager;
import org.hyperledger.besu.ethereum.p2p.peers.DefaultPeer;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeDnsConfiguration;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissionSubnet;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissions;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissionsDenylist;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.SubProtocol;
import org.hyperledger.besu.ethereum.permissioning.AccountLocalConfigPermissioningController;
import org.hyperledger.besu.ethereum.permissioning.NodeLocalConfigPermissioningController;
import org.hyperledger.besu.ethereum.permissioning.NodePermissioningControllerFactory;
import org.hyperledger.besu.ethereum.permissioning.PermissioningConfiguration;
import org.hyperledger.besu.ethereum.permissioning.account.AccountPermissioningController;
import org.hyperledger.besu.ethereum.permissioning.account.AccountPermissioningControllerFactory;
import org.hyperledger.besu.ethereum.permissioning.node.InsufficientPeersPermissioningProvider;
import org.hyperledger.besu.ethereum.permissioning.node.NodePermissioningController;
import org.hyperledger.besu.ethereum.permissioning.node.PeerPermissionsAdapter;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.ethstats.EthStatsService;
import org.hyperledger.besu.ethstats.util.EthStatsConnectOptions;
import org.hyperledger.besu.metrics.MetricsService;
import org.hyperledger.besu.metrics.ObservableMetricsSystem;
import org.hyperledger.besu.metrics.prometheus.MetricsConfiguration;
import org.hyperledger.besu.nat.NatMethod;
import org.hyperledger.besu.nat.NatService;
import org.hyperledger.besu.nat.core.NatManager;
import org.hyperledger.besu.nat.docker.DockerDetector;
import org.hyperledger.besu.nat.docker.DockerNatManager;
import org.hyperledger.besu.nat.upnp.UpnpNatManager;
import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.data.EnodeURL;
import org.hyperledger.besu.services.BesuPluginContextImpl;
import org.hyperledger.besu.services.PermissioningServiceImpl;
import org.hyperledger.besu.services.RpcEndpointServiceImpl;
import org.hyperledger.besu.services.TransactionValidatorServiceImpl;
import org.hyperledger.besu.util.BesuVersionUtils;
import org.hyperledger.besu.util.NetworkUtility;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import graphql.GraphQL;
import inet.ipaddr.IPAddress;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The builder for Runner class. */
public class RunnerBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(RunnerBuilder.class);

  private Vertx vertx;
  private BesuController besuController;

  private NetworkingConfiguration networkingConfiguration = NetworkingConfiguration.DEFAULT;
  private final Collection<Bytes> bannedNodeIds = new ArrayList<>();
  private boolean p2pEnabled = true;
  private boolean discoveryEnabled;
  private String p2pAdvertisedHost;
  private String p2pListenInterface = NetworkUtility.INADDR_ANY;
  private int p2pListenPort;
  private Optional<String> p2pAdvertisedHostIpv6 = Optional.empty();
  private Optional<String> p2pListenInterfaceIpv6 = Optional.empty();
  private int p2pListenPortIpv6 = EnodeURLImpl.DEFAULT_LISTENING_PORT_IPV6;
  private boolean preferIpv6Outbound = false;
  private NatMethod natMethod = NatMethod.AUTO;
  private boolean natMethodFallbackEnabled;
  private EthNetworkConfig ethNetworkConfig;
  private EthstatsOptions ethstatsOptions;
  private JsonRpcConfiguration jsonRpcConfiguration;
  private Optional<JsonRpcConfiguration> engineJsonRpcConfiguration = Optional.empty();
  private GraphQLConfiguration graphQLConfiguration;
  private WebSocketConfiguration webSocketConfiguration;
  private InProcessRpcConfiguration inProcessRpcConfiguration;
  private ApiConfiguration apiConfiguration;
  private Path dataDir;
  private Optional<Path> pidPath = Optional.empty();
  private MetricsConfiguration metricsConfiguration;
  private ObservableMetricsSystem metricsSystem;
  private PermissioningServiceImpl permissioningService;
  private Optional<PermissioningConfiguration> permissioningConfiguration = Optional.empty();
  private Collection<EnodeURLImpl> staticNodes = Collections.emptyList();
  private Optional<String> identityString = Optional.empty();
  private BesuPluginContextImpl besuPluginContext;
  private boolean autoLogBloomCaching = true;
  private StorageProvider storageProvider;
  private RpcEndpointServiceImpl rpcEndpointServiceImpl;
  private JsonRpcIpcConfiguration jsonRpcIpcConfiguration;
  private Optional<EnodeDnsConfiguration> enodeDnsConfiguration;
  private List<IPAddress> allowedSubnets = new ArrayList<>();
  private boolean poaDiscoveryRetryBootnodes = true;
  private TransactionValidatorServiceImpl transactionValidatorService;

  /** Instantiates a new Runner builder. */
  public RunnerBuilder() {}

  /**
   * Add Vertx.
   *
   * @param vertx the vertx instance
   * @return runner builder
   */
  public RunnerBuilder vertx(final Vertx vertx) {
    this.vertx = vertx;
    return this;
  }

  /**
   * Add Besu controller.
   *
   * @param besuController the besu controller
   * @return the runner builder
   */
  public RunnerBuilder besuController(final BesuController besuController) {
    this.besuController = besuController;
    return this;
  }

  /**
   * P2p enabled.
   *
   * @param p2pEnabled the p 2 p enabled
   * @return the runner builder
   */
  public RunnerBuilder p2pEnabled(final boolean p2pEnabled) {
    this.p2pEnabled = p2pEnabled;
    return this;
  }

  /**
   * Enable Discovery.
   *
   * @param discoveryEnabled the discoveryEnabled
   * @return the runner builder
   */
  public RunnerBuilder discoveryEnabled(final boolean discoveryEnabled) {
    this.discoveryEnabled = discoveryEnabled;
    return this;
  }

  /**
   * Add Eth network config.
   *
   * @param ethNetworkConfig the eth network config
   * @return the runner builder
   */
  public RunnerBuilder ethNetworkConfig(final EthNetworkConfig ethNetworkConfig) {
    this.ethNetworkConfig = ethNetworkConfig;
    return this;
  }

  /**
   * Add Networking configuration.
   *
   * @param networkingConfiguration the networking configuration
   * @return the runner builder
   */
  public RunnerBuilder networkingConfiguration(
      final NetworkingConfiguration networkingConfiguration) {
    this.networkingConfiguration = networkingConfiguration;
    return this;
  }

  /**
   * Add P2p advertised host.
   *
   * @param p2pAdvertisedHost the P2P advertised host
   * @return the runner builder
   */
  public RunnerBuilder p2pAdvertisedHost(final String p2pAdvertisedHost) {
    this.p2pAdvertisedHost = p2pAdvertisedHost;
    return this;
  }

  /**
   * Add P2P Listener interface ip/host name.
   *
   * @param ip the ip
   * @return the runner builder
   */
  public RunnerBuilder p2pListenInterface(final String ip) {
    checkArgument(!isNull(ip), "Invalid null value supplied for p2pListenInterface");
    this.p2pListenInterface = ip;
    return this;
  }

  /**
   * Add P2P listen port.
   *
   * @param p2pListenPort the p 2 p listen port
   * @return the runner builder
   */
  public RunnerBuilder p2pListenPort(final int p2pListenPort) {
    this.p2pListenPort = p2pListenPort;
    return this;
  }

  /**
   * Add P2P advertised host for IPv6.
   *
   * @param p2pAdvertisedHostIpv6 optional host name
   * @return the runner builder
   */
  public RunnerBuilder p2pAdvertisedHostIpv6(final Optional<String> p2pAdvertisedHostIpv6) {
    this.p2pAdvertisedHostIpv6 = p2pAdvertisedHostIpv6;
    return this;
  }

  /**
   * Add IPv6 P2P host name/IP address for P2P to listen to.
   *
   * @param p2pListenInterfaceIpv6 optional host name/IP address
   * @return the runner builder
   */
  public RunnerBuilder p2pListenInterfaceIpv6(final Optional<String> p2pListenInterfaceIpv6) {
    this.p2pListenInterfaceIpv6 = p2pListenInterfaceIpv6;
    return this;
  }

  /**
   * Add IPv6 port for P2P.
   *
   * @param p2pListenPortIpv6 the IPv6 P2P listen port
   * @return the runner builder
   */
  public RunnerBuilder p2pListenPortIpv6(final int p2pListenPortIpv6) {
    this.p2pListenPortIpv6 = p2pListenPortIpv6;
    return this;
  }

  /**
   * Add Nat method.
   *
   * @param natMethod the nat method
   * @return the runner builder
   */
  public RunnerBuilder natMethod(final NatMethod natMethod) {
    this.natMethod = natMethod;
    return this;
  }

  /**
   * Enable Nat method fallback.
   *
   * @param natMethodFallbackEnabled the nat method fallback enabled
   * @return the runner builder
   */
  public RunnerBuilder natMethodFallbackEnabled(final boolean natMethodFallbackEnabled) {
    this.natMethodFallbackEnabled = natMethodFallbackEnabled;
    return this;
  }

  /**
   * Add EthStatsOptions
   *
   * @param ethstatsOptions the ethstats options
   * @return Runner builder instance
   */
  public RunnerBuilder ethstatsOptions(final EthstatsOptions ethstatsOptions) {
    this.ethstatsOptions = ethstatsOptions;
    return this;
  }

  /**
   * Add Json RPC configuration.
   *
   * @param jsonRpcConfiguration the json rpc configuration
   * @return the runner builder
   */
  public RunnerBuilder jsonRpcConfiguration(final JsonRpcConfiguration jsonRpcConfiguration) {
    this.jsonRpcConfiguration = jsonRpcConfiguration;
    return this;
  }

  /**
   * Add Engine json RPC configuration.
   *
   * @param engineJsonRpcConfiguration the engine json rpc configuration
   * @return the runner builder
   */
  public RunnerBuilder engineJsonRpcConfiguration(
      final JsonRpcConfiguration engineJsonRpcConfiguration) {
    this.engineJsonRpcConfiguration = Optional.ofNullable(engineJsonRpcConfiguration);
    return this;
  }

  /**
   * Add GraphQl configuration.
   *
   * @param graphQLConfiguration the graph ql configuration
   * @return the runner builder
   */
  public RunnerBuilder graphQLConfiguration(final GraphQLConfiguration graphQLConfiguration) {
    this.graphQLConfiguration = graphQLConfiguration;
    return this;
  }

  /**
   * Add Web socket configuration.
   *
   * @param webSocketConfiguration the web socket configuration
   * @return the runner builder
   */
  public RunnerBuilder webSocketConfiguration(final WebSocketConfiguration webSocketConfiguration) {
    this.webSocketConfiguration = webSocketConfiguration;
    return this;
  }

  /**
   * Add In-Process RPC configuration.
   *
   * @param inProcessRpcConfiguration the in-process RPC configuration
   * @return the runner builder
   */
  public RunnerBuilder inProcessRpcConfiguration(
      final InProcessRpcConfiguration inProcessRpcConfiguration) {
    this.inProcessRpcConfiguration = inProcessRpcConfiguration;
    return this;
  }

  /**
   * Add Api configuration.
   *
   * @param apiConfiguration the api configuration
   * @return the runner builder
   */
  public RunnerBuilder apiConfiguration(final ApiConfiguration apiConfiguration) {
    this.apiConfiguration = apiConfiguration;
    return this;
  }

  /**
   * Add Permissioning configuration.
   *
   * @param permissioningConfiguration the permissioning configuration
   * @return the runner builder
   */
  public RunnerBuilder permissioningConfiguration(
      final Optional<PermissioningConfiguration> permissioningConfiguration) {
    this.permissioningConfiguration = permissioningConfiguration;
    return this;
  }

  /**
   * Add pid path.
   *
   * @param pidPath the pid path
   * @return the runner builder
   */
  public RunnerBuilder pidPath(final Path pidPath) {
    this.pidPath = Optional.ofNullable(pidPath);
    return this;
  }

  /**
   * Add Data dir.
   *
   * @param dataDir the data dir
   * @return the runner builder
   */
  public RunnerBuilder dataDir(final Path dataDir) {
    this.dataDir = dataDir;
    return this;
  }

  /**
   * Add list of Banned node id.
   *
   * @param bannedNodeIds the banned node ids
   * @return the runner builder
   */
  public RunnerBuilder bannedNodeIds(final Collection<Bytes> bannedNodeIds) {
    this.bannedNodeIds.addAll(bannedNodeIds);
    return this;
  }

  /**
   * Add Metrics configuration.
   *
   * @param metricsConfiguration the metrics configuration
   * @return the runner builder
   */
  public RunnerBuilder metricsConfiguration(final MetricsConfiguration metricsConfiguration) {
    this.metricsConfiguration = metricsConfiguration;
    return this;
  }

  /**
   * Add Metrics system.
   *
   * @param metricsSystem the metrics system
   * @return the runner builder
   */
  public RunnerBuilder metricsSystem(final ObservableMetricsSystem metricsSystem) {
    this.metricsSystem = metricsSystem;
    return this;
  }

  /**
   * Add Permissioning service.
   *
   * @param permissioningService the permissioning service
   * @return the runner builder
   */
  public RunnerBuilder permissioningService(final PermissioningServiceImpl permissioningService) {
    this.permissioningService = permissioningService;
    return this;
  }

  /**
   * Add Static nodes collection.
   *
   * @param staticNodes the static nodes
   * @return the runner builder
   */
  public RunnerBuilder staticNodes(final Collection<EnodeURLImpl> staticNodes) {
    this.staticNodes = staticNodes;
    return this;
  }

  /**
   * Add Node identity string.
   *
   * @param identityString the identity string
   * @return the runner builder
   */
  public RunnerBuilder identityString(final Optional<String> identityString) {
    this.identityString = identityString;
    return this;
  }

  /**
   * Add Besu plugin context.
   *
   * @param besuPluginContext the besu plugin context
   * @return the runner builder
   */
  public RunnerBuilder besuPluginContext(final BesuPluginContextImpl besuPluginContext) {
    this.besuPluginContext = besuPluginContext;
    return this;
  }

  /**
   * Enable Auto log bloom caching.
   *
   * @param autoLogBloomCaching the auto log bloom caching
   * @return the runner builder
   */
  public RunnerBuilder autoLogBloomCaching(final boolean autoLogBloomCaching) {
    this.autoLogBloomCaching = autoLogBloomCaching;
    return this;
  }

  /**
   * Add Storage provider.
   *
   * @param storageProvider the storage provider
   * @return the runner builder
   */
  public RunnerBuilder storageProvider(final StorageProvider storageProvider) {
    this.storageProvider = storageProvider;
    return this;
  }

  /**
   * Add Rpc endpoint service.
   *
   * @param rpcEndpointService the rpc endpoint service
   * @return the runner builder
   */
  public RunnerBuilder rpcEndpointService(final RpcEndpointServiceImpl rpcEndpointService) {
    this.rpcEndpointServiceImpl = rpcEndpointService;
    return this;
  }

  /**
   * Add Json Rpc Ipc configuration.
   *
   * @param jsonRpcIpcConfiguration the json rpc ipc configuration
   * @return the runner builder
   */
  public RunnerBuilder jsonRpcIpcConfiguration(
      final JsonRpcIpcConfiguration jsonRpcIpcConfiguration) {
    this.jsonRpcIpcConfiguration = jsonRpcIpcConfiguration;
    return this;
  }

  /**
   * Add enode DNS configuration
   *
   * @param enodeDnsConfiguration the DNS configuration for enodes
   * @return the runner builder
   */
  public RunnerBuilder enodeDnsConfiguration(final EnodeDnsConfiguration enodeDnsConfiguration) {
    this.enodeDnsConfiguration =
        enodeDnsConfiguration != null ? Optional.of(enodeDnsConfiguration) : Optional.empty();
    return this;
  }

  /**
   * Add subnet configuration
   *
   * @param allowedSubnets the allowedSubnets
   * @return the runner builder
   */
  public RunnerBuilder allowedSubnets(final List<IPAddress> allowedSubnets) {
    this.allowedSubnets = allowedSubnets;
    return this;
  }

  /**
   * Flag to indicate if peer table refreshes should always query bootnodes
   *
   * @param poaDiscoveryRetryBootnodes whether to always query bootnodes
   * @return the runner builder
   */
  public RunnerBuilder poaDiscoveryRetryBootnodes(final boolean poaDiscoveryRetryBootnodes) {
    this.poaDiscoveryRetryBootnodes = poaDiscoveryRetryBootnodes;
    return this;
  }

  /**
   * Add IPv6 outbound preference for P2P connections.
   *
   * @param preferIpv6Outbound if true, prefer IPv6 when peer advertises both address families
   * @return the runner builder
   */
  public RunnerBuilder preferIpv6Outbound(final boolean preferIpv6Outbound) {
    this.preferIpv6Outbound = preferIpv6Outbound;
    return this;
  }

  /**
   * Set the transaction validator service.
   *
   * @param transactionValidatorService the transaction validator service
   * @return the runner builder
   */
  public RunnerBuilder transactionValidatorService(
      final TransactionValidatorServiceImpl transactionValidatorService) {
    this.transactionValidatorService = transactionValidatorService;
    return this;
  }

  /**
   * Build Runner instance.
   *
   * @return the runner
   */
  public Runner build() {

    Preconditions.checkNotNull(besuController);

    final DiscoveryConfiguration discoveryConfiguration =
        DiscoveryConfiguration.create()
            .setBindHost(p2pListenInterface)
            .setBindPort(p2pListenPort)
            .setAdvertisedHost(p2pAdvertisedHost);
    p2pListenInterfaceIpv6.ifPresent(
        iface -> {
          discoveryConfiguration.setBindHostIpv6(p2pListenInterfaceIpv6);
          discoveryConfiguration.setBindPortIpv6(p2pListenPortIpv6);
          discoveryConfiguration.setAdvertisedHostIpv6(p2pAdvertisedHostIpv6);
        });
    discoveryConfiguration.setPreferIpv6Outbound(preferIpv6Outbound);
    if (discoveryEnabled) {
      discoveryConfiguration.setEnodeBootnodes(ethNetworkConfig.enodeBootNodes());
      discoveryConfiguration.setEnrBootnodes(ethNetworkConfig.enrBootNodes());

      discoveryConfiguration.setIncludeBootnodesOnPeerRefresh(
          besuController.getGenesisConfigOptions().isPoa() && poaDiscoveryRetryBootnodes);
      LOG.info(
          "Resolved {} bootnodes.",
          discoveryConfiguration.getEnodeBootnodes().size()
              + discoveryConfiguration.getEnrBootnodes().size());
      LOG.debug(
          "Bootnodes enode={}, enr={}",
          discoveryConfiguration.getEnodeBootnodes(),
          discoveryConfiguration.getEnrBootnodes());
      discoveryConfiguration.setDnsDiscoveryURL(ethNetworkConfig.dnsDiscoveryUrl());
      discoveryConfiguration.setDiscoveryV5Enabled(
          networkingConfiguration.discoveryConfiguration().isDiscoveryV5Enabled());
      discoveryConfiguration.setFilterOnEnrForkId(
          networkingConfiguration.discoveryConfiguration().isFilterOnEnrForkIdEnabled());
      discoveryConfiguration.setDiscV5DiscoveryIntervalSeconds(
          networkingConfiguration.discoveryConfiguration().getDiscV5DiscoveryIntervalSeconds());
      discoveryConfiguration.setDiscV5DiscoveryTimeoutSeconds(
          networkingConfiguration.discoveryConfiguration().getDiscV5DiscoveryTimeoutSeconds());
      discoveryConfiguration.setDiscV5MinimumPeerRatio(
          networkingConfiguration.discoveryConfiguration().getDiscV5MinimumPeerRatio());
    } else {
      discoveryConfiguration.setEnabled(false);
    }

    final NodeKey nodeKey = besuController.getNodeKey();

    final SubProtocolConfiguration subProtocolConfiguration =
        besuController.getSubProtocolConfiguration();

    final ProtocolSchedule protocolSchedule = besuController.getProtocolSchedule();
    final ProtocolContext context = besuController.getProtocolContext();

    final List<SubProtocol> subProtocols = subProtocolConfiguration.getSubProtocols();
    final List<ProtocolManager> protocolManagers = subProtocolConfiguration.getProtocolManagers();
    final Set<Capability> supportedCapabilities =
        protocolManagers.stream()
            .flatMap(protocolManager -> protocolManager.getSupportedCapabilities().stream())
            .collect(Collectors.toSet());

    // IPv6 dual-stack support (a second UDP socket + a second TCP socket) was introduced
    // alongside DiscV5. Besu does not implement dual-stack for DiscV4, so RLPx should only
    // bind a second TCP socket when DiscV5 is active. This guard can be dropped once DiscV4
    // is removed.
    final boolean rlpxDualStackEnabled =
        discoveryEnabled && networkingConfiguration.discoveryConfiguration().isDiscoveryV5Enabled();
    final RlpxConfiguration rlpxConfiguration =
        RlpxConfiguration.create()
            .setBindHost(p2pListenInterface)
            .setBindPort(p2pListenPort)
            .setBindHostIpv6(rlpxDualStackEnabled ? p2pListenInterfaceIpv6 : Optional.empty())
            .setBindPortIpv6(
                rlpxDualStackEnabled
                    ? p2pListenInterfaceIpv6.map(ignored -> p2pListenPortIpv6)
                    : Optional.empty())
            .setSupportedProtocols(subProtocols)
            .setClientId(BesuVersionUtils.nodeName(identityString));
    networkingConfiguration =
        ImmutableNetworkingConfiguration.builder()
            .from(networkingConfiguration)
            .rlpxConfiguration(rlpxConfiguration)
            .discoveryConfiguration(discoveryConfiguration)
            .build();

    final PeerPermissionsDenylist bannedNodes = PeerPermissionsDenylist.create();
    bannedNodeIds.forEach(bannedNodes::add);

    PeerPermissionSubnet peerPermissionSubnet = new PeerPermissionSubnet(allowedSubnets);
    final PeerPermissions defaultPeerPermissions =
        PeerPermissions.combine(peerPermissionSubnet, bannedNodes);

    final List<? extends NodeIdentifier> bootnodes =
        discoveryConfiguration.getBootnodeIdentifiers();

    final Synchronizer synchronizer = besuController.getSynchronizer();

    final TransactionSimulator transactionSimulator = besuController.getTransactionSimulator();

    final Bytes localNodeId = nodeKey.getPublicKey().getEncodedBytes();
    final Optional<NodePermissioningController> nodePermissioningController =
        buildNodePermissioningController(
            bootnodes, synchronizer, transactionSimulator, localNodeId, context.getBlockchain());

    final PeerPermissions peerPermissions =
        nodePermissioningController
            .map(nodePC -> new PeerPermissionsAdapter(nodePC, context.getBlockchain()))
            .map(nodePerms -> PeerPermissions.combine(nodePerms, defaultPeerPermissions))
            .orElse(defaultPeerPermissions);

    final EthPeers ethPeers = besuController.getEthPeers();

    LOG.info("Detecting NAT service.");
    final boolean fallbackEnabled = natMethod == NatMethod.AUTO || natMethodFallbackEnabled;
    final NatService natService = new NatService(buildNatManager(natMethod), fallbackEnabled);
    final NetworkBuilder inactiveNetwork = caps -> new NoopP2PNetwork();

    PeerDiscoveryAgentFactory peerDiscoveryAgentFactory =
        DefaultPeerDiscoveryAgentFactory.builder()
            .vertx(vertx)
            .nodeKey(nodeKey)
            .config(networkingConfiguration)
            .peerPermissions(peerPermissions)
            .natService(natService)
            .metricsSystem(metricsSystem)
            .storageProvider(storageProvider)
            .blockchain(context.getBlockchain())
            .blockNumberForks(besuController.getGenesisConfigOptions().getForkBlockNumbers())
            .timestampForks(besuController.getGenesisConfigOptions().getForkBlockTimestamps())
            .build();

    RlpxAgentFactory rlpxAgentFactory =
        DefaultRlpxAgentFactory.builder()
            .nodeKey(nodeKey)
            .config(networkingConfiguration)
            .peerPermissions(peerPermissions)
            .metricsSystem(metricsSystem)
            .allConnectionsSupplier(ethPeers::streamAllConnections)
            .allActiveConnectionsSupplier(ethPeers::streamAllActiveConnections)
            .maxPeers(ethPeers.getMaxPeers())
            .build();

    final NetworkBuilder activeNetwork =
        caps ->
            DefaultP2PNetwork.builder()
                .vertx(vertx)
                .nodeKey(nodeKey)
                .config(networkingConfiguration)
                .peerPermissions(peerPermissions)
                .metricsSystem(metricsSystem)
                .supportedCapabilities(caps)
                .natService(natService)
                .peerDiscoveryAgentFactory(peerDiscoveryAgentFactory)
                .rlpxAgentFactory(rlpxAgentFactory)
                .build();

    final NetworkRunner networkRunner =
        NetworkRunner.builder()
            .protocolManagers(protocolManagers)
            .subProtocols(subProtocols)
            .network(p2pEnabled ? activeNetwork : inactiveNetwork)
            .metricsSystem(metricsSystem)
            .peerConnectionGatekeeper(ethPeers::gatePeerConnection)
            .build();

    networkRunner.getRlpxAgent().ifPresent(ethPeers::setRlpxAgent);

    final P2PNetwork network = networkRunner.getNetwork();
    // ForkId in Ethereum Node Record needs updating when we transition to a new
    // protocol spec
    context
        .getBlockchain()
        .observeBlockAdded(
            blockAddedEvent -> {
              if (protocolSchedule.isOnMilestoneBoundary(blockAddedEvent.getHeader())) {
                network.updateNodeRecord();
              }
            });
    nodePermissioningController.ifPresent(
        n ->
            n.setInsufficientPeersPermissioningProvider(
                new InsufficientPeersPermissioningProvider(network, bootnodes)));

    final TransactionPool transactionPool = besuController.getTransactionPool();
    final MiningCoordinator miningCoordinator = besuController.getMiningCoordinator();
    final MiningConfiguration miningConfiguration = besuController.getMiningParameters();

    final BlockchainQueries blockchainQueries =
        new BlockchainQueries(
            protocolSchedule,
            context.getBlockchain(),
            context.getWorldStateArchive(),
            Optional.of(dataDir.resolve(CACHE_PATH)),
            Optional.of(besuController.getProtocolManager().ethContext().getScheduler()),
            apiConfiguration,
            miningConfiguration);

    final FilterManager filterManager =
        new FilterManagerBuilder()
            .blockchainQueries(blockchainQueries)
            .transactionPool(transactionPool)
            .build();
    vertx.deployVerticle(filterManager);

    final P2PNetwork peerNetwork = networkRunner.getNetwork();

    sanitizePeers(network, staticNodes)
        .map(DefaultPeer::fromEnodeURL)
        .forEach(peerNetwork::addMaintainedConnectionPeer);

    protocolSchedule.setAdditionalValidationRules(
        transactionValidatorService.getTransactionValidatorRules());

    final Optional<NodeLocalConfigPermissioningController> nodeLocalConfigPermissioningController =
        nodePermissioningController.flatMap(NodePermissioningController::localConfigController);

    final Optional<AccountPermissioningController> accountPermissioningController =
        buildAccountPermissioningController(permissioningConfiguration, transactionSimulator);

    final Optional<AccountLocalConfigPermissioningController>
        accountLocalConfigPermissioningController =
            accountPermissioningController.flatMap(
                AccountPermissioningController::getAccountLocalConfigPermissioningController);

    Optional<JsonRpcHttpService> jsonRpcHttpService = Optional.empty();

    if (jsonRpcConfiguration.isEnabled()) {
      final Map<String, JsonRpcMethod> nonEngineMethods =
          jsonRpcMethods(
              protocolSchedule,
              context,
              besuController,
              peerNetwork,
              blockchainQueries,
              synchronizer,
              transactionPool,
              miningConfiguration,
              miningCoordinator,
              metricsSystem,
              supportedCapabilities,
              jsonRpcConfiguration.getRpcApis().stream()
                  .filter(apiGroup -> !apiGroup.toLowerCase(Locale.ROOT).startsWith("engine"))
                  .collect(Collectors.toList()),
              filterManager,
              accountLocalConfigPermissioningController,
              nodeLocalConfigPermissioningController,
              jsonRpcConfiguration,
              webSocketConfiguration,
              metricsConfiguration,
              graphQLConfiguration,
              natService,
              besuPluginContext.getPluginsByName(),
              dataDir,
              rpcEndpointServiceImpl,
              transactionSimulator,
              besuController.getProtocolManager().ethContext().getScheduler());

      jsonRpcHttpService =
          Optional.of(
              new JsonRpcHttpService(
                  vertx,
                  dataDir,
                  jsonRpcConfiguration,
                  metricsSystem,
                  natService,
                  nonEngineMethods,
                  new HealthService(new LivenessCheck()),
                  new HealthService(new ReadinessCheck(peerNetwork, synchronizer))));
    }

    final SubscriptionManager subscriptionManager =
        createSubscriptionManager(vertx, transactionPool, blockchainQueries);

    if (webSocketConfiguration.isEnabled()
        || (jsonRpcIpcConfiguration != null && jsonRpcIpcConfiguration.isEnabled())) {
      createLogsSubscriptionService(context.getBlockchain(), subscriptionManager);

      createNewBlockHeadersSubscriptionService(
          context.getBlockchain(), blockchainQueries, subscriptionManager);

      createSyncingSubscriptionService(synchronizer, subscriptionManager);
    }

    Optional<EngineJsonRpcService> engineJsonRpcService = Optional.empty();
    if (engineJsonRpcConfiguration.isPresent() && engineJsonRpcConfiguration.get().isEnabled()) {
      final Map<String, JsonRpcMethod> engineMethods =
          jsonRpcMethods(
              protocolSchedule,
              context,
              besuController,
              peerNetwork,
              blockchainQueries,
              synchronizer,
              transactionPool,
              miningConfiguration,
              miningCoordinator,
              metricsSystem,
              supportedCapabilities,
              engineJsonRpcConfiguration.get().getRpcApis(),
              filterManager,
              accountLocalConfigPermissioningController,
              nodeLocalConfigPermissioningController,
              engineJsonRpcConfiguration.get(),
              webSocketConfiguration,
              metricsConfiguration,
              graphQLConfiguration,
              natService,
              besuPluginContext.getPluginsByName(),
              dataDir,
              rpcEndpointServiceImpl,
              transactionSimulator,
              besuController.getProtocolManager().ethContext().getScheduler());

      final Optional<AuthenticationService> authToUse =
          engineJsonRpcConfiguration.get().isAuthenticationEnabled()
              ? Optional.of(
                  new EngineAuthService(
                      vertx,
                      Optional.ofNullable(
                          engineJsonRpcConfiguration.get().getAuthenticationPublicKeyFile()),
                      dataDir))
              : Optional.empty();

      final WebSocketConfiguration engineSocketConfig =
          webSocketConfiguration.isEnabled()
              ? webSocketConfiguration
              : WebSocketConfiguration.createEngineDefault();

      final WebSocketMethodsFactory websocketMethodsFactory =
          new WebSocketMethodsFactory(subscriptionManager, engineMethods);

      engineJsonRpcService =
          Optional.of(
              new EngineJsonRpcService(
                  vertx,
                  dataDir,
                  engineJsonRpcConfiguration.orElse(JsonRpcConfiguration.createEngineDefault()),
                  metricsSystem,
                  natService,
                  websocketMethodsFactory.methods(),
                  Optional.ofNullable(engineSocketConfig),
                  besuController.getProtocolManager().ethContext().getScheduler(),
                  authToUse,
                  new HealthService(new LivenessCheck()),
                  new HealthService(new ReadinessCheck(peerNetwork, synchronizer))));
    }

    Optional<GraphQLHttpService> graphQLHttpService = Optional.empty();
    if (graphQLConfiguration.isEnabled()) {
      final GraphQLDataFetchers fetchers = new GraphQLDataFetchers(supportedCapabilities);
      final Map<GraphQLContextType, Object> graphQlContextMap = new ConcurrentHashMap<>();
      graphQlContextMap.putIfAbsent(GraphQLContextType.BLOCKCHAIN_QUERIES, blockchainQueries);
      graphQlContextMap.putIfAbsent(GraphQLContextType.PROTOCOL_SCHEDULE, protocolSchedule);
      graphQlContextMap.putIfAbsent(GraphQLContextType.TRANSACTION_POOL, transactionPool);
      graphQlContextMap.putIfAbsent(GraphQLContextType.MINING_COORDINATOR, miningCoordinator);
      graphQlContextMap.putIfAbsent(GraphQLContextType.SYNCHRONIZER, synchronizer);
      graphQlContextMap.putIfAbsent(
          GraphQLContextType.CHAIN_ID, protocolSchedule.getChainId().map(UInt256::valueOf));
      graphQlContextMap.putIfAbsent(GraphQLContextType.TRANSACTION_SIMULATOR, transactionSimulator);
      final GraphQL graphQL;
      try {
        graphQL = GraphQLProvider.buildGraphQL(fetchers);
      } catch (final IOException ioe) {
        throw new RuntimeException(ioe);
      }

      graphQLHttpService =
          Optional.of(
              new GraphQLHttpService(
                  vertx,
                  dataDir,
                  graphQLConfiguration,
                  graphQL,
                  graphQlContextMap,
                  besuController.getProtocolManager().ethContext().getScheduler()));
    }

    Optional<WebSocketService> webSocketService = Optional.empty();
    if (webSocketConfiguration.isEnabled()) {
      final Map<String, JsonRpcMethod> nonEngineMethods =
          jsonRpcMethods(
              protocolSchedule,
              context,
              besuController,
              peerNetwork,
              blockchainQueries,
              synchronizer,
              transactionPool,
              miningConfiguration,
              miningCoordinator,
              metricsSystem,
              supportedCapabilities,
              webSocketConfiguration.getRpcApis().stream()
                  .filter(apiGroup -> !apiGroup.toLowerCase(Locale.ROOT).startsWith("engine"))
                  .collect(Collectors.toList()),
              filterManager,
              accountLocalConfigPermissioningController,
              nodeLocalConfigPermissioningController,
              jsonRpcConfiguration,
              webSocketConfiguration,
              metricsConfiguration,
              graphQLConfiguration,
              natService,
              besuPluginContext.getPluginsByName(),
              dataDir,
              rpcEndpointServiceImpl,
              transactionSimulator,
              besuController.getProtocolManager().ethContext().getScheduler());

      webSocketService =
          Optional.of(
              createWebsocketService(
                  vertx,
                  webSocketConfiguration,
                  subscriptionManager,
                  nonEngineMethods,
                  DefaultAuthenticationService.create(vertx, webSocketConfiguration),
                  metricsSystem));
    }

    final Optional<MetricsService> metricsService = createMetricsService(metricsConfiguration);

    final Optional<EthStatsService> ethStatsService;
    if (isEthStatsEnabled()) {
      ethStatsService =
          Optional.of(
              new EthStatsService(
                  EthStatsConnectOptions.fromParams(
                      ethstatsOptions.getEthstatsUrl(),
                      ethstatsOptions.getEthstatsContact(),
                      ethstatsOptions.getEthstatsCaCert(),
                      ethstatsOptions.getEthstatsReportInterval()),
                  blockchainQueries,
                  besuController.getProtocolManager(),
                  transactionPool,
                  miningCoordinator,
                  besuController.getSyncState(),
                  vertx,
                  BesuVersionUtils.nodeName(identityString),
                  besuController.getGenesisConfigOptions(),
                  network));
    } else {
      ethStatsService = Optional.empty();
    }

    final Optional<JsonRpcIpcService> jsonRpcIpcService;
    if (jsonRpcIpcConfiguration.isEnabled()) {
      final Map<String, JsonRpcMethod> ipcMethods =
          jsonRpcMethods(
              protocolSchedule,
              context,
              besuController,
              peerNetwork,
              blockchainQueries,
              synchronizer,
              transactionPool,
              miningConfiguration,
              miningCoordinator,
              metricsSystem,
              supportedCapabilities,
              jsonRpcIpcConfiguration.getEnabledApis().stream()
                  .filter(apiGroup -> !apiGroup.toLowerCase(Locale.ROOT).startsWith("engine"))
                  .collect(Collectors.toList()),
              filterManager,
              accountLocalConfigPermissioningController,
              nodeLocalConfigPermissioningController,
              jsonRpcConfiguration,
              webSocketConfiguration,
              metricsConfiguration,
              graphQLConfiguration,
              natService,
              besuPluginContext.getPluginsByName(),
              dataDir,
              rpcEndpointServiceImpl,
              transactionSimulator,
              besuController.getProtocolManager().ethContext().getScheduler());

      final WebSocketMethodsFactory ipcMethodsFactory =
          new WebSocketMethodsFactory(subscriptionManager, ipcMethods);

      jsonRpcIpcService =
          Optional.of(
              new JsonRpcIpcService(
                  vertx,
                  jsonRpcIpcConfiguration.getPath(),
                  new JsonRpcExecutor(new BaseJsonRpcProcessor(), ipcMethodsFactory.methods()),
                  Optional.of(subscriptionManager)));
    } else {
      jsonRpcIpcService = Optional.empty();
    }

    final Map<String, JsonRpcMethod> inProcessRpcMethods;
    if (inProcessRpcConfiguration.isEnabled()) {
      inProcessRpcMethods =
          jsonRpcMethods(
              protocolSchedule,
              context,
              besuController,
              peerNetwork,
              blockchainQueries,
              synchronizer,
              transactionPool,
              miningConfiguration,
              miningCoordinator,
              metricsSystem,
              supportedCapabilities,
              inProcessRpcConfiguration.getInProcessRpcApis(),
              filterManager,
              accountLocalConfigPermissioningController,
              nodeLocalConfigPermissioningController,
              jsonRpcConfiguration,
              webSocketConfiguration,
              metricsConfiguration,
              graphQLConfiguration,
              natService,
              besuPluginContext.getPluginsByName(),
              dataDir,
              rpcEndpointServiceImpl,
              transactionSimulator,
              besuController.getProtocolManager().ethContext().getScheduler());
    } else {
      inProcessRpcMethods = Map.of();
    }

    return new Runner(
        vertx,
        networkRunner,
        natService,
        jsonRpcHttpService,
        engineJsonRpcService,
        graphQLHttpService,
        webSocketService,
        jsonRpcIpcService,
        inProcessRpcMethods,
        metricsService,
        ethStatsService,
        besuController,
        dataDir,
        pidPath,
        autoLogBloomCaching ? blockchainQueries.getTransactionLogBloomCacher() : Optional.empty(),
        context.getBlockchain());
  }

  private boolean isEthStatsEnabled() {
    return ethstatsOptions != null && !Strings.isNullOrEmpty(ethstatsOptions.getEthstatsUrl());
  }

  private Stream<EnodeURLImpl> sanitizePeers(
      final P2PNetwork network, final Collection<EnodeURLImpl> enodeURLS) {
    if (network.getLocalEnode().isEmpty()) {
      return enodeURLS.stream();
    }
    final EnodeURL localEnodeURL = network.getLocalEnode().get();
    return enodeURLS.stream()
        .filter(enodeURL -> !enodeURL.getNodeId().equals(localEnodeURL.getNodeId()));
  }

  private Optional<NodePermissioningController> buildNodePermissioningController(
      final List<? extends NodeIdentifier> bootnodesIdentifiers,
      final Synchronizer synchronizer,
      final TransactionSimulator transactionSimulator,
      final Bytes localNodeId,
      final Blockchain blockchain) {
    final Collection<NodeIdentifier> fixedNodes = getFixedNodes(bootnodesIdentifiers, staticNodes);

    if (permissioningConfiguration.isPresent()) {
      final PermissioningConfiguration configuration = this.permissioningConfiguration.get();
      final NodePermissioningController nodePermissioningController =
          new NodePermissioningControllerFactory()
              .create(
                  configuration,
                  synchronizer,
                  fixedNodes,
                  localNodeId,
                  transactionSimulator,
                  metricsSystem,
                  blockchain,
                  permissioningService.getConnectionPermissioningProviders());

      return Optional.of(nodePermissioningController);
    } else if (permissioningService.getConnectionPermissioningProviders().size() > 0) {
      final NodePermissioningController nodePermissioningController =
          new NodePermissioningControllerFactory()
              .create(
                  new PermissioningConfiguration(Optional.empty()),
                  synchronizer,
                  fixedNodes,
                  localNodeId,
                  transactionSimulator,
                  metricsSystem,
                  blockchain,
                  permissioningService.getConnectionPermissioningProviders());

      return Optional.of(nodePermissioningController);
    } else {
      return Optional.empty();
    }
  }

  private Optional<AccountPermissioningController> buildAccountPermissioningController(
      final Optional<PermissioningConfiguration> permissioningConfiguration,
      final TransactionSimulator transactionSimulator) {

    if (permissioningConfiguration.isPresent()
        || permissioningService.getTransactionPermissioningProviders().size() > 0) {
      final PermissioningConfiguration configuration =
          permissioningConfiguration.orElse(new PermissioningConfiguration(Optional.empty()));
      final Optional<AccountPermissioningController> accountPermissioningController =
          AccountPermissioningControllerFactory.create(
              configuration,
              transactionSimulator,
              metricsSystem,
              permissioningService.getTransactionPermissioningProviders());

      accountPermissioningController.ifPresent(
          permissioningController ->
              besuController
                  .getProtocolSchedule()
                  .setPermissionTransactionFilter(permissioningController::isPermitted));

      return accountPermissioningController;
    } else {
      return Optional.empty();
    }
  }

  private Optional<NatManager> buildNatManager(final NatMethod natMethod) {

    final NatMethod detectedNatMethod =
        Optional.of(natMethod)
            .filter(not(isEqual(NatMethod.AUTO)))
            .orElse(NatService.autoDetectNatMethod(new DockerDetector()));
    switch (detectedNatMethod) {
      case UPNP:
        return Optional.of(new UpnpNatManager());
      case DOCKER:
        return Optional.of(
            new DockerNatManager(p2pAdvertisedHost, p2pListenPort, jsonRpcConfiguration.getPort()));
      case NONE:
      default:
        return Optional.empty();
    }
  }

  /**
   * Gets fixed nodes. Visible for testing.
   *
   * @param someFixedNodes the fixed nodes
   * @param moreFixedNodes nodes added to fixed nodes
   * @return the fixed and more nodes combined
   */
  @VisibleForTesting
  public static Collection<NodeIdentifier> getFixedNodes(
      final Collection<? extends NodeIdentifier> someFixedNodes,
      final Collection<EnodeURLImpl> moreFixedNodes) {
    final Collection<NodeIdentifier> fixedNodes = new ArrayList<>();
    fixedNodes.addAll(someFixedNodes);
    fixedNodes.addAll(moreFixedNodes);
    return fixedNodes;
  }

  private Map<String, JsonRpcMethod> jsonRpcMethods(
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final BesuController besuController,
      final P2PNetwork network,
      final BlockchainQueries blockchainQueries,
      final Synchronizer synchronizer,
      final TransactionPool transactionPool,
      final MiningConfiguration miningConfiguration,
      final MiningCoordinator miningCoordinator,
      final ObservableMetricsSystem metricsSystem,
      final Set<Capability> supportedCapabilities,
      final Collection<String> jsonRpcApis,
      final FilterManager filterManager,
      final Optional<AccountLocalConfigPermissioningController> accountAllowlistController,
      final Optional<NodeLocalConfigPermissioningController> nodeAllowlistController,
      final JsonRpcConfiguration jsonRpcConfiguration,
      final WebSocketConfiguration webSocketConfiguration,
      final MetricsConfiguration metricsConfiguration,
      final GraphQLConfiguration graphQLConfiguration,
      final NatService natService,
      final Map<String, BesuPlugin> namedPlugins,
      final Path dataDir,
      final RpcEndpointServiceImpl rpcEndpointServiceImpl,
      final TransactionSimulator transactionSimulator,
      final EthScheduler ethScheduler) {
    // sync vertx for engine consensus API, to process requests in FIFO order;
    final Vertx consensusEngineServer = Vertx.vertx(new VertxOptions().setWorkerPoolSize(1));

    final Map<String, JsonRpcMethod> methods =
        new JsonRpcMethodsFactory()
            .methods(
                BesuVersionUtils.nodeName(identityString),
                BesuVersionUtils.shortVersion(),
                BesuVersionUtils.commit(),
                ethNetworkConfig.networkId(),
                besuController.getGenesisConfigOptions(),
                network,
                blockchainQueries,
                synchronizer,
                protocolSchedule,
                protocolContext,
                filterManager,
                transactionPool,
                miningConfiguration,
                miningCoordinator,
                metricsSystem,
                supportedCapabilities,
                accountAllowlistController,
                nodeAllowlistController,
                jsonRpcApis,
                jsonRpcConfiguration,
                webSocketConfiguration,
                metricsConfiguration,
                graphQLConfiguration,
                natService,
                namedPlugins,
                dataDir,
                besuController.getProtocolManager().ethContext().getEthPeers(),
                consensusEngineServer,
                apiConfiguration,
                enodeDnsConfiguration,
                transactionSimulator,
                ethScheduler);
    methods.putAll(besuController.getAdditionalJsonRpcMethods(jsonRpcApis));

    final var pluginMethods =
        rpcEndpointServiceImpl.getPluginMethods(jsonRpcConfiguration.getRpcApis());

    final var overriddenMethods =
        methods.keySet().stream().filter(pluginMethods::containsKey).collect(Collectors.toList());
    if (overriddenMethods.size() > 0) {
      throw new RuntimeException("You can not override built in methods " + overriddenMethods);
    }

    methods.putAll(pluginMethods);
    return methods;
  }

  private SubscriptionManager createSubscriptionManager(
      final Vertx vertx,
      final TransactionPool transactionPool,
      final BlockchainQueries blockchainQueries) {
    final SubscriptionManager subscriptionManager =
        new SubscriptionManager(metricsSystem, blockchainQueries.getBlockchain());
    final PendingTransactionSubscriptionService pendingTransactions =
        new PendingTransactionSubscriptionService(subscriptionManager);
    final PendingTransactionDroppedSubscriptionService pendingTransactionsRemoved =
        new PendingTransactionDroppedSubscriptionService(subscriptionManager);
    transactionPool.subscribePendingTransactions(pendingTransactions);
    transactionPool.subscribeDroppedTransactions(pendingTransactionsRemoved);
    vertx.deployVerticle(subscriptionManager);

    return subscriptionManager;
  }

  private void createLogsSubscriptionService(
      final Blockchain blockchain, final SubscriptionManager subscriptionManager) {

    final LogsSubscriptionService logsSubscriptionService =
        new LogsSubscriptionService(subscriptionManager);

    // monitoring public logs
    blockchain.observeLogs(logsSubscriptionService);
  }

  private void createSyncingSubscriptionService(
      final Synchronizer synchronizer, final SubscriptionManager subscriptionManager) {
    new SyncingSubscriptionService(subscriptionManager, synchronizer);
  }

  private void createNewBlockHeadersSubscriptionService(
      final Blockchain blockchain,
      final BlockchainQueries blockchainQueries,
      final SubscriptionManager subscriptionManager) {
    final NewBlockHeadersSubscriptionService newBlockHeadersSubscriptionService =
        new NewBlockHeadersSubscriptionService(subscriptionManager, blockchainQueries);

    blockchain.observeBlockAdded(newBlockHeadersSubscriptionService);
  }

  private WebSocketService createWebsocketService(
      final Vertx vertx,
      final WebSocketConfiguration configuration,
      final SubscriptionManager subscriptionManager,
      final Map<String, JsonRpcMethod> jsonRpcMethods,
      final Optional<AuthenticationService> authenticationService,
      final ObservableMetricsSystem metricsSystem) {

    final WebSocketMethodsFactory websocketMethodsFactory =
        new WebSocketMethodsFactory(subscriptionManager, jsonRpcMethods);

    rpcEndpointServiceImpl
        .getPluginMethods(configuration.getRpcApis())
        .values()
        .forEach(websocketMethodsFactory::addMethods);

    final JsonRpcProcessor jsonRpcProcessor;
    if (authenticationService.isPresent()) {
      jsonRpcProcessor =
          new AuthenticatedJsonRpcProcessor(
              new BaseJsonRpcProcessor(),
              authenticationService.get(),
              configuration.getRpcApisNoAuth());
    } else {
      jsonRpcProcessor = new BaseJsonRpcProcessor();
    }
    final JsonRpcExecutor jsonRpcExecutor =
        new JsonRpcExecutor(jsonRpcProcessor, websocketMethodsFactory.methods());
    final WebSocketMessageHandler websocketMessageHandler =
        new WebSocketMessageHandler(
            vertx,
            jsonRpcExecutor,
            besuController.getProtocolManager().ethContext().getScheduler(),
            webSocketConfiguration.getTimeoutSec());

    return new WebSocketService(
        vertx, configuration, websocketMessageHandler, authenticationService, metricsSystem);
  }

  private Optional<MetricsService> createMetricsService(final MetricsConfiguration configuration) {
    return MetricsService.create(configuration, metricsSystem);
  }
}
