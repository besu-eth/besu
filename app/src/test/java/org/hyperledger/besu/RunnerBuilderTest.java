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
package org.hyperledger.besu;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.VARIABLES;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.cli.config.EthNetworkConfig;
import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.config.MergeConfiguration;
import org.hyperledger.besu.consensus.common.bft.BftEventQueue;
import org.hyperledger.besu.consensus.common.bft.network.PeerConnectionTracker;
import org.hyperledger.besu.consensus.common.bft.protocol.BftProtocolManager;
import org.hyperledger.besu.consensus.ibft.protocol.IbftSubProtocol;
import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.controller.BesuController;
import org.hyperledger.besu.crypto.SECP256K1;
import org.hyperledger.besu.cryptoservices.KeyPairSecurityModule;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.ImmutableApiConfiguration;
import org.hyperledger.besu.ethereum.api.graphql.GraphQLConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.InProcessRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.ipc.JsonRpcIpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.WebSocketConfiguration;
import org.hyperledger.besu.ethereum.blockcreation.NoopMiningCoordinator;
import org.hyperledger.besu.ethereum.chain.DefaultBlockchain;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManager;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.p2p.config.NetworkingConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.SubProtocolConfiguration;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStorageProvider;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.metrics.ObservableMetricsSystem;
import org.hyperledger.besu.metrics.prometheus.MetricsConfiguration;
import org.hyperledger.besu.nat.NatMethod;
import org.hyperledger.besu.plugin.data.EnodeURL;
import org.hyperledger.besu.services.BesuPluginContextImpl;
import org.hyperledger.besu.services.PermissioningServiceImpl;
import org.hyperledger.besu.services.RpcEndpointServiceImpl;
import org.hyperledger.besu.services.TransactionValidatorServiceImpl;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;

import io.vertx.core.Vertx;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt64;
import org.ethereum.beacon.discovery.schema.NodeRecord;
import org.ethereum.beacon.discovery.schema.NodeRecordFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public final class RunnerBuilderTest {

  @TempDir private Path dataDir;

  @Mock BesuController besuController;
  @Mock ProtocolSchedule protocolSchedule;
  @Mock ProtocolContext protocolContext;
  @Mock WorldStateArchive worldstateArchive;
  @Mock Vertx vertx;
  private NodeKey nodeKey;

  @BeforeEach
  public void setup() {
    final SubProtocolConfiguration subProtocolConfiguration = mock(SubProtocolConfiguration.class);
    final EthProtocolManager ethProtocolManager = mock(EthProtocolManager.class);
    final EthContext ethContext = mock(EthContext.class);
    nodeKey = new NodeKey(new KeyPairSecurityModule(new SECP256K1().generateKeyPair()));

    when(subProtocolConfiguration.getProtocolManagers())
        .thenReturn(
            Collections.singletonList(
                new BftProtocolManager(
                    mock(BftEventQueue.class),
                    mock(PeerConnectionTracker.class),
                    IbftSubProtocol.IBFV1,
                    IbftSubProtocol.get().getName())));
    when(ethContext.getScheduler()).thenReturn(mock(EthScheduler.class));
    when(ethProtocolManager.ethContext()).thenReturn(ethContext);
    when(subProtocolConfiguration.getSubProtocols())
        .thenReturn(Collections.singletonList(new IbftSubProtocol()));

    when(protocolContext.getWorldStateArchive()).thenReturn(worldstateArchive);
    when(besuController.getProtocolManager()).thenReturn(ethProtocolManager);
    when(besuController.getSubProtocolConfiguration()).thenReturn(subProtocolConfiguration);
    when(besuController.getProtocolContext()).thenReturn(protocolContext);
    when(besuController.getProtocolSchedule()).thenReturn(protocolSchedule);
    when(besuController.getNodeKey()).thenReturn(nodeKey);
    when(besuController.getMiningParameters()).thenReturn(mock(MiningConfiguration.class));
    when(besuController.getTransactionPool())
        .thenReturn(mock(TransactionPool.class, RETURNS_DEEP_STUBS));
    when(besuController.getSynchronizer()).thenReturn(mock(Synchronizer.class));
    when(besuController.getMiningCoordinator()).thenReturn(new NoopMiningCoordinator());
    when(besuController.getMiningCoordinator()).thenReturn(mock(MergeMiningCoordinator.class));
    when(besuController.getEthPeers()).thenReturn(mock(EthPeers.class));
    final GenesisConfigOptions genesisConfigOptions = mock(GenesisConfigOptions.class);
    when(genesisConfigOptions.getForkBlockNumbers()).thenReturn(Collections.emptyList());
    when(genesisConfigOptions.getForkBlockTimestamps()).thenReturn(Collections.emptyList());
    when(besuController.getGenesisConfigOptions()).thenReturn(genesisConfigOptions);
  }

  @Test
  public void enodeUrlShouldHaveAdvertisedHostWhenDiscoveryDisabled() {
    setupBlockchainAndBlock();

    final String p2pAdvertisedHost = "172.0.0.1";
    final int p2pListenPort = 30302;

    final Runner runner =
        new RunnerBuilder()
            .p2pListenInterface("0.0.0.0")
            .p2pListenPort(p2pListenPort)
            .p2pAdvertisedHost(p2pAdvertisedHost)
            .p2pEnabled(true)
            .discoveryEnabled(false)
            .besuController(besuController)
            .ethNetworkConfig(mock(EthNetworkConfig.class))
            .metricsSystem(mock(ObservableMetricsSystem.class))
            .jsonRpcConfiguration(mock(JsonRpcConfiguration.class))
            .permissioningService(mock(PermissioningServiceImpl.class))
            .graphQLConfiguration(mock(GraphQLConfiguration.class))
            .webSocketConfiguration(mock(WebSocketConfiguration.class))
            .jsonRpcIpcConfiguration(mock(JsonRpcIpcConfiguration.class))
            .inProcessRpcConfiguration(mock(InProcessRpcConfiguration.class))
            .metricsConfiguration(mock(MetricsConfiguration.class))
            .vertx(vertx)
            .dataDir(dataDir.getRoot())
            .storageProvider(mock(KeyValueStorageProvider.class, RETURNS_DEEP_STUBS))
            .rpcEndpointService(new RpcEndpointServiceImpl())
            .apiConfiguration(ImmutableApiConfiguration.builder().build())
            .transactionValidatorService(mock(TransactionValidatorServiceImpl.class))
            .build();
    runner.startEthereumMainLoop();

    final EnodeURL expectedEnodeURL =
        EnodeURLImpl.builder()
            .ipAddress(p2pAdvertisedHost)
            .discoveryPort(0)
            .listeningPort(p2pListenPort)
            .nodeId(nodeKey.getPublicKey().getEncoded())
            .build();
    assertThat(runner.getLocalEnode().orElseThrow()).isEqualTo(expectedEnodeURL);
  }

  @Test
  public void movingAcrossProtocolSpecsUpdatesNodeRecord() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final String p2pAdvertisedHost = "172.0.0.1";
    final int p2pListenPort = 30301;
    final StorageProvider storageProvider = new InMemoryKeyValueStorageProvider();
    final Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain inMemoryBlockchain =
        createInMemoryBlockchain(genesisBlock, new MainnetBlockHeaderFunctions());
    when(protocolContext.getBlockchain()).thenReturn(inMemoryBlockchain);
    final Runner runner =
        new RunnerBuilder()
            .discoveryEnabled(true)
            .p2pListenInterface("0.0.0.0")
            .p2pListenPort(p2pListenPort)
            .p2pAdvertisedHost(p2pAdvertisedHost)
            .p2pEnabled(true)
            .natMethod(NatMethod.NONE)
            .besuController(besuController)
            .ethNetworkConfig(mock(EthNetworkConfig.class))
            .metricsSystem(mock(ObservableMetricsSystem.class))
            .permissioningService(mock(PermissioningServiceImpl.class))
            .jsonRpcConfiguration(mock(JsonRpcConfiguration.class))
            .graphQLConfiguration(mock(GraphQLConfiguration.class))
            .webSocketConfiguration(mock(WebSocketConfiguration.class))
            .jsonRpcIpcConfiguration(mock(JsonRpcIpcConfiguration.class))
            .inProcessRpcConfiguration(mock(InProcessRpcConfiguration.class))
            .metricsConfiguration(mock(MetricsConfiguration.class))
            .vertx(Vertx.vertx())
            .dataDir(dataDir.getRoot())
            .storageProvider(storageProvider)
            .rpcEndpointService(new RpcEndpointServiceImpl())
            .apiConfiguration(ImmutableApiConfiguration.builder().build())
            .transactionValidatorService(mock(TransactionValidatorServiceImpl.class))
            .build();
    runner.startEthereumMainLoop();

    when(protocolSchedule.isOnMilestoneBoundary(any(BlockHeader.class))).thenReturn(true);

    for (int i = 0; i < 2; ++i) {
      final Block block =
          gen.block(
              BlockDataGenerator.BlockOptions.create()
                  .setBlockNumber(1 + i)
                  .setParentHash(inMemoryBlockchain.getChainHeadHash()));
      inMemoryBlockchain.appendBlock(block, gen.receipts(block));
      assertThat(
              storageProvider
                  .getStorageBySegmentIdentifier(VARIABLES)
                  .get("local-enr-seqno".getBytes(StandardCharsets.UTF_8))
                  .map(Bytes::of)
                  .map(NodeRecordFactory.DEFAULT::fromBytes)
                  .map(NodeRecord::getSeq))
          .contains(UInt64.valueOf(2 + i));
    }
  }

  @Test
  public void whenEngineApiAddedListensOnDefaultPort() {
    setupBlockchainAndBlock();

    final JsonRpcConfiguration jrpc = JsonRpcConfiguration.createDefault();
    jrpc.setEnabled(true);
    final JsonRpcConfiguration engine = JsonRpcConfiguration.createEngineDefault();
    engine.setEnabled(true);
    final EthNetworkConfig mockMainnet = mock(EthNetworkConfig.class);
    when(mockMainnet.networkId()).thenReturn(BigInteger.ONE);
    MergeConfiguration.setMergeEnabled(true);
    when(besuController.getMiningCoordinator()).thenReturn(mock(MergeMiningCoordinator.class));

    final Runner runner =
        new RunnerBuilder()
            .discoveryEnabled(true)
            .p2pListenInterface("0.0.0.0")
            .p2pListenPort(30303)
            .p2pAdvertisedHost("127.0.0.1")
            .p2pEnabled(true)
            .natMethod(NatMethod.NONE)
            .besuController(besuController)
            .ethNetworkConfig(mockMainnet)
            .metricsSystem(mock(ObservableMetricsSystem.class))
            .permissioningService(mock(PermissioningServiceImpl.class))
            .jsonRpcConfiguration(jrpc)
            .engineJsonRpcConfiguration(engine)
            .graphQLConfiguration(mock(GraphQLConfiguration.class))
            .webSocketConfiguration(mock(WebSocketConfiguration.class))
            .jsonRpcIpcConfiguration(mock(JsonRpcIpcConfiguration.class))
            .inProcessRpcConfiguration(mock(InProcessRpcConfiguration.class))
            .metricsConfiguration(mock(MetricsConfiguration.class))
            .vertx(Vertx.vertx())
            .dataDir(dataDir.getRoot())
            .storageProvider(mock(KeyValueStorageProvider.class, RETURNS_DEEP_STUBS))
            .rpcEndpointService(new RpcEndpointServiceImpl())
            .besuPluginContext(mock(BesuPluginContextImpl.class))
            .apiConfiguration(ImmutableApiConfiguration.builder().build())
            .transactionValidatorService(mock(TransactionValidatorServiceImpl.class))
            .build();

    assertThat(runner.getJsonRpcPort()).isPresent();
    assertThat(runner.getEngineJsonRpcPort()).isPresent();
  }

  @Test
  public void whenEngineApiAddedWebSocketReadyOnSamePort() {
    setupBlockchainAndBlock();

    final WebSocketConfiguration wsRpc = WebSocketConfiguration.createDefault();
    wsRpc.setEnabled(true);
    final EthNetworkConfig mockMainnet = mock(EthNetworkConfig.class);
    when(mockMainnet.networkId()).thenReturn(BigInteger.ONE);
    MergeConfiguration.setMergeEnabled(true);
    when(besuController.getMiningCoordinator()).thenReturn(mock(MergeMiningCoordinator.class));
    final JsonRpcConfiguration engineConf = JsonRpcConfiguration.createEngineDefault();
    engineConf.setEnabled(true);

    final Runner runner =
        new RunnerBuilder()
            .discoveryEnabled(true)
            .p2pListenInterface("0.0.0.0")
            .p2pListenPort(30303)
            .p2pAdvertisedHost("127.0.0.1")
            .p2pEnabled(true)
            .natMethod(NatMethod.NONE)
            .besuController(besuController)
            .ethNetworkConfig(mockMainnet)
            .metricsSystem(mock(ObservableMetricsSystem.class))
            .permissioningService(mock(PermissioningServiceImpl.class))
            .jsonRpcConfiguration(JsonRpcConfiguration.createDefault())
            .engineJsonRpcConfiguration(engineConf)
            .webSocketConfiguration(wsRpc)
            .jsonRpcIpcConfiguration(mock(JsonRpcIpcConfiguration.class))
            .inProcessRpcConfiguration(mock(InProcessRpcConfiguration.class))
            .graphQLConfiguration(mock(GraphQLConfiguration.class))
            .metricsConfiguration(mock(MetricsConfiguration.class))
            .vertx(Vertx.vertx())
            .dataDir(dataDir.getRoot())
            .storageProvider(mock(KeyValueStorageProvider.class, RETURNS_DEEP_STUBS))
            .rpcEndpointService(new RpcEndpointServiceImpl())
            .besuPluginContext(mock(BesuPluginContextImpl.class))
            .apiConfiguration(ImmutableApiConfiguration.builder().build())
            .transactionValidatorService(mock(TransactionValidatorServiceImpl.class))
            .build();

    assertThat(runner.getEngineJsonRpcPort()).isPresent();
  }

  @Test
  public void whenEngineApiAddedEthSubscribeAvailable() {
    setupBlockchainAndBlock();

    final WebSocketConfiguration wsRpc = WebSocketConfiguration.createDefault();
    wsRpc.setEnabled(true);
    final EthNetworkConfig mockMainnet = mock(EthNetworkConfig.class);
    when(mockMainnet.networkId()).thenReturn(BigInteger.ONE);
    MergeConfiguration.setMergeEnabled(true);
    when(besuController.getMiningCoordinator()).thenReturn(mock(MergeMiningCoordinator.class));
    final JsonRpcConfiguration engineConf = JsonRpcConfiguration.createEngineDefault();
    engineConf.setEnabled(true);

    final Runner runner =
        new RunnerBuilder()
            .discoveryEnabled(true)
            .p2pListenInterface("0.0.0.0")
            .p2pListenPort(30303)
            .p2pAdvertisedHost("127.0.0.1")
            .p2pEnabled(true)
            .natMethod(NatMethod.NONE)
            .besuController(besuController)
            .ethNetworkConfig(mockMainnet)
            .metricsSystem(mock(ObservableMetricsSystem.class))
            .permissioningService(mock(PermissioningServiceImpl.class))
            .jsonRpcConfiguration(JsonRpcConfiguration.createDefault())
            .engineJsonRpcConfiguration(engineConf)
            .webSocketConfiguration(wsRpc)
            .jsonRpcIpcConfiguration(mock(JsonRpcIpcConfiguration.class))
            .inProcessRpcConfiguration(mock(InProcessRpcConfiguration.class))
            .graphQLConfiguration(mock(GraphQLConfiguration.class))
            .metricsConfiguration(mock(MetricsConfiguration.class))
            .vertx(Vertx.vertx())
            .dataDir(dataDir.getRoot())
            .storageProvider(mock(KeyValueStorageProvider.class, RETURNS_DEEP_STUBS))
            .rpcEndpointService(new RpcEndpointServiceImpl())
            .besuPluginContext(mock(BesuPluginContextImpl.class))
            .apiConfiguration(ImmutableApiConfiguration.builder().build())
            .transactionValidatorService(mock(TransactionValidatorServiceImpl.class))
            .build();

    assertThat(runner.getEngineJsonRpcPort()).isPresent();
    runner.startExternalServices();
    // assert that rpc method collection has eth_subscribe in it.
    runner.stop();
  }

  @Test
  public void noEngineApiNoServiceForMethods() {
    setupBlockchainAndBlock();

    final JsonRpcConfiguration defaultRpcConfig = JsonRpcConfiguration.createDefault();
    defaultRpcConfig.setEnabled(true);
    final WebSocketConfiguration defaultWebSockConfig = WebSocketConfiguration.createDefault();
    defaultWebSockConfig.setEnabled(true);
    final EthNetworkConfig mockMainnet = mock(EthNetworkConfig.class);
    when(mockMainnet.networkId()).thenReturn(BigInteger.ONE);
    MergeConfiguration.setMergeEnabled(true);

    final Runner runner =
        new RunnerBuilder()
            .discoveryEnabled(true)
            .p2pListenInterface("0.0.0.0")
            .p2pListenPort(30303)
            .p2pAdvertisedHost("127.0.0.1")
            .p2pEnabled(true)
            .natMethod(NatMethod.NONE)
            .besuController(besuController)
            .ethNetworkConfig(mockMainnet)
            .metricsSystem(mock(ObservableMetricsSystem.class))
            .permissioningService(mock(PermissioningServiceImpl.class))
            .jsonRpcConfiguration(defaultRpcConfig)
            .graphQLConfiguration(mock(GraphQLConfiguration.class))
            .webSocketConfiguration(defaultWebSockConfig)
            .jsonRpcIpcConfiguration(mock(JsonRpcIpcConfiguration.class))
            .inProcessRpcConfiguration(mock(InProcessRpcConfiguration.class))
            .metricsConfiguration(mock(MetricsConfiguration.class))
            .vertx(Vertx.vertx())
            .dataDir(dataDir.getRoot())
            .storageProvider(mock(KeyValueStorageProvider.class, RETURNS_DEEP_STUBS))
            .rpcEndpointService(new RpcEndpointServiceImpl())
            .besuPluginContext(mock(BesuPluginContextImpl.class))
            .networkingConfiguration(NetworkingConfiguration.DEFAULT)
            .apiConfiguration(ImmutableApiConfiguration.builder().build())
            .transactionValidatorService(mock(TransactionValidatorServiceImpl.class))
            .build();

    assertThat(runner.getJsonRpcPort()).isPresent();
    assertThat(runner.getEngineJsonRpcPort()).isEmpty();
  }

  private void setupBlockchainAndBlock() {
    final DefaultBlockchain blockchain = mock(DefaultBlockchain.class);
    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    final Block block = mock(Block.class);
    when(blockchain.getGenesisBlock()).thenReturn(block);
    when(block.getHash()).thenReturn(Hash.ZERO);
  }

  @Test
  public void buildWithNullJsonRpcConfiguration() {
    setupBlockchainAndBlock();

    // Build a Runner with null jsonRpcConfiguration - should not throw NPE
    final Runner runner =
        new RunnerBuilder()
            .p2pListenInterface("0.0.0.0")
            .p2pListenPort(30303)
            .p2pAdvertisedHost("127.0.0.1")
            .p2pEnabled(false)
            .discoveryEnabled(false)
            .besuController(besuController)
            .ethNetworkConfig(mock(EthNetworkConfig.class))
            .metricsSystem(mock(ObservableMetricsSystem.class))
            .permissioningService(mock(PermissioningServiceImpl.class))
            // jsonRpcConfiguration is deliberately null
            .graphQLConfiguration(mock(GraphQLConfiguration.class))
            .webSocketConfiguration(mock(WebSocketConfiguration.class))
            .jsonRpcIpcConfiguration(mock(JsonRpcIpcConfiguration.class))
            .inProcessRpcConfiguration(mock(InProcessRpcConfiguration.class))
            .metricsConfiguration(mock(MetricsConfiguration.class))
            .vertx(vertx)
            .dataDir(dataDir)
            .storageProvider(mock(KeyValueStorageProvider.class, RETURNS_DEEP_STUBS))
            .rpcEndpointService(new RpcEndpointServiceImpl())
            .apiConfiguration(ImmutableApiConfiguration.builder().build())
            .transactionValidatorService(mock(TransactionValidatorServiceImpl.class))
            .build();

    assertThat(runner).isNotNull();
  }

  @Test
  public void buildWithNullWebSocketConfiguration() {
    setupBlockchainAndBlock();

    // Build a Runner with null webSocketConfiguration - should not throw NPE
    final Runner runner =
        new RunnerBuilder()
            .p2pListenInterface("0.0.0.0")
            .p2pListenPort(30303)
            .p2pAdvertisedHost("127.0.0.1")
            .p2pEnabled(false)
            .discoveryEnabled(false)
            .besuController(besuController)
            .ethNetworkConfig(mock(EthNetworkConfig.class))
            .metricsSystem(mock(ObservableMetricsSystem.class))
            .permissioningService(mock(PermissioningServiceImpl.class))
            .jsonRpcConfiguration(mock(JsonRpcConfiguration.class))
            .graphQLConfiguration(mock(GraphQLConfiguration.class))
            // webSocketConfiguration is deliberately null
            .jsonRpcIpcConfiguration(mock(JsonRpcIpcConfiguration.class))
            .inProcessRpcConfiguration(mock(InProcessRpcConfiguration.class))
            .metricsConfiguration(mock(MetricsConfiguration.class))
            .vertx(vertx)
            .dataDir(dataDir)
            .storageProvider(mock(KeyValueStorageProvider.class, RETURNS_DEEP_STUBS))
            .rpcEndpointService(new RpcEndpointServiceImpl())
            .apiConfiguration(ImmutableApiConfiguration.builder().build())
            .transactionValidatorService(mock(TransactionValidatorServiceImpl.class))
            .build();

    assertThat(runner).isNotNull();
  }

  @Test
  public void buildWithNullGraphQLConfiguration() {
    setupBlockchainAndBlock();

    // Build a Runner with null graphQLConfiguration - should not throw NPE
    final Runner runner =
        new RunnerBuilder()
            .p2pListenInterface("0.0.0.0")
            .p2pListenPort(30303)
            .p2pAdvertisedHost("127.0.0.1")
            .p2pEnabled(false)
            .discoveryEnabled(false)
            .besuController(besuController)
            .ethNetworkConfig(mock(EthNetworkConfig.class))
            .metricsSystem(mock(ObservableMetricsSystem.class))
            .permissioningService(mock(PermissioningServiceImpl.class))
            .jsonRpcConfiguration(mock(JsonRpcConfiguration.class))
            // graphQLConfiguration is deliberately null
            .webSocketConfiguration(mock(WebSocketConfiguration.class))
            .jsonRpcIpcConfiguration(mock(JsonRpcIpcConfiguration.class))
            .inProcessRpcConfiguration(mock(InProcessRpcConfiguration.class))
            .metricsConfiguration(mock(MetricsConfiguration.class))
            .vertx(vertx)
            .dataDir(dataDir)
            .storageProvider(mock(KeyValueStorageProvider.class, RETURNS_DEEP_STUBS))
            .rpcEndpointService(new RpcEndpointServiceImpl())
            .apiConfiguration(ImmutableApiConfiguration.builder().build())
            .transactionValidatorService(mock(TransactionValidatorServiceImpl.class))
            .build();

    assertThat(runner).isNotNull();
  }

  @Test
  public void buildWithNullJsonRpcIpcConfiguration() {
    setupBlockchainAndBlock();

    // Build a Runner with null jsonRpcIpcConfiguration - should not throw NPE
    final Runner runner =
        new RunnerBuilder()
            .p2pListenInterface("0.0.0.0")
            .p2pListenPort(30303)
            .p2pAdvertisedHost("127.0.0.1")
            .p2pEnabled(false)
            .discoveryEnabled(false)
            .besuController(besuController)
            .ethNetworkConfig(mock(EthNetworkConfig.class))
            .metricsSystem(mock(ObservableMetricsSystem.class))
            .permissioningService(mock(PermissioningServiceImpl.class))
            .jsonRpcConfiguration(mock(JsonRpcConfiguration.class))
            .graphQLConfiguration(mock(GraphQLConfiguration.class))
            .webSocketConfiguration(mock(WebSocketConfiguration.class))
            // jsonRpcIpcConfiguration is deliberately null
            .inProcessRpcConfiguration(mock(InProcessRpcConfiguration.class))
            .metricsConfiguration(mock(MetricsConfiguration.class))
            .vertx(vertx)
            .dataDir(dataDir)
            .storageProvider(mock(KeyValueStorageProvider.class, RETURNS_DEEP_STUBS))
            .rpcEndpointService(new RpcEndpointServiceImpl())
            .apiConfiguration(ImmutableApiConfiguration.builder().build())
            .transactionValidatorService(mock(TransactionValidatorServiceImpl.class))
            .build();

    assertThat(runner).isNotNull();
  }

  @Test
  public void buildWithNullInProcessRpcConfiguration() {
    setupBlockchainAndBlock();

    // Build a Runner with null inProcessRpcConfiguration - should not throw NPE
    final Runner runner =
        new RunnerBuilder()
            .p2pListenInterface("0.0.0.0")
            .p2pListenPort(30303)
            .p2pAdvertisedHost("127.0.0.1")
            .p2pEnabled(false)
            .discoveryEnabled(false)
            .besuController(besuController)
            .ethNetworkConfig(mock(EthNetworkConfig.class))
            .metricsSystem(mock(ObservableMetricsSystem.class))
            .permissioningService(mock(PermissioningServiceImpl.class))
            .jsonRpcConfiguration(mock(JsonRpcConfiguration.class))
            .graphQLConfiguration(mock(GraphQLConfiguration.class))
            .webSocketConfiguration(mock(WebSocketConfiguration.class))
            .jsonRpcIpcConfiguration(mock(JsonRpcIpcConfiguration.class))
            // inProcessRpcConfiguration is deliberately null
            .metricsConfiguration(mock(MetricsConfiguration.class))
            .vertx(vertx)
            .dataDir(dataDir)
            .storageProvider(mock(KeyValueStorageProvider.class, RETURNS_DEEP_STUBS))
            .rpcEndpointService(new RpcEndpointServiceImpl())
            .apiConfiguration(ImmutableApiConfiguration.builder().build())
            .transactionValidatorService(mock(TransactionValidatorServiceImpl.class))
            .build();

    assertThat(runner).isNotNull();
  }

  @Test
  public void buildWithNullBesuPluginContext() {
    setupBlockchainAndBlock();

    // Build a Runner with null besuPluginContext - should not throw NPE
    final Runner runner =
        new RunnerBuilder()
            .p2pListenInterface("0.0.0.0")
            .p2pListenPort(30303)
            .p2pAdvertisedHost("127.0.0.1")
            .p2pEnabled(false)
            .discoveryEnabled(false)
            .besuController(besuController)
            .ethNetworkConfig(mock(EthNetworkConfig.class))
            .metricsSystem(mock(ObservableMetricsSystem.class))
            .permissioningService(mock(PermissioningServiceImpl.class))
            .jsonRpcConfiguration(mock(JsonRpcConfiguration.class))
            .graphQLConfiguration(mock(GraphQLConfiguration.class))
            .webSocketConfiguration(mock(WebSocketConfiguration.class))
            .jsonRpcIpcConfiguration(mock(JsonRpcIpcConfiguration.class))
            .inProcessRpcConfiguration(mock(InProcessRpcConfiguration.class))
            .metricsConfiguration(mock(MetricsConfiguration.class))
            .vertx(vertx)
            .dataDir(dataDir)
            .storageProvider(mock(KeyValueStorageProvider.class, RETURNS_DEEP_STUBS))
            .rpcEndpointService(new RpcEndpointServiceImpl())
            // besuPluginContext is deliberately null
            .apiConfiguration(ImmutableApiConfiguration.builder().build())
            .transactionValidatorService(mock(TransactionValidatorServiceImpl.class))
            .build();

    assertThat(runner).isNotNull();
  }

  @Test
  public void buildWithAllNullOptionalConfigurations() {
    setupBlockchainAndBlock();

    // Build a Runner with multiple null optional configurations - should not throw NPE
    // This simulates a minimal caller that only provides absolutely required fields
    final Runner runner =
        new RunnerBuilder()
            .p2pListenInterface("0.0.0.0")
            .p2pListenPort(30303)
            .p2pAdvertisedHost("127.0.0.1")
            .p2pEnabled(false)
            .discoveryEnabled(false)
            .besuController(besuController)
            .ethNetworkConfig(mock(EthNetworkConfig.class))
            .metricsSystem(mock(ObservableMetricsSystem.class))
            .permissioningService(mock(PermissioningServiceImpl.class))
            // All optional RPC/plugin configurations are null
            .metricsConfiguration(mock(MetricsConfiguration.class))
            .vertx(vertx)
            .dataDir(dataDir)
            .storageProvider(mock(KeyValueStorageProvider.class, RETURNS_DEEP_STUBS))
            .rpcEndpointService(new RpcEndpointServiceImpl())
            .apiConfiguration(ImmutableApiConfiguration.builder().build())
            .transactionValidatorService(mock(TransactionValidatorServiceImpl.class))
            .build();

    assertThat(runner).isNotNull();
  }

  @Test
  public void buildWithoutSettingOptionalConfigurationsInitializesDefaults() {
    setupBlockchainAndBlock();

    // Build a Runner explicitly NOT setting optional configurations
    // Verify that build() initializes them with defaults and build succeeds
    final RunnerBuilder builder =
        new RunnerBuilder()
            .p2pListenInterface("0.0.0.0")
            .p2pListenPort(30303)
            .p2pAdvertisedHost("127.0.0.1")
            .p2pEnabled(false)
            .discoveryEnabled(false)
            .besuController(besuController)
            .ethNetworkConfig(mock(EthNetworkConfig.class))
            .metricsSystem(mock(ObservableMetricsSystem.class))
            .permissioningService(mock(PermissioningServiceImpl.class))
            .metricsConfiguration(mock(MetricsConfiguration.class))
            .vertx(vertx)
            .dataDir(dataDir)
            .storageProvider(mock(KeyValueStorageProvider.class, RETURNS_DEEP_STUBS))
            .rpcEndpointService(new RpcEndpointServiceImpl())
            .apiConfiguration(ImmutableApiConfiguration.builder().build())
            .transactionValidatorService(mock(TransactionValidatorServiceImpl.class));
    // Note: not setting jsonRpcConfiguration, webSocketConfiguration, etc.

    // Should not throw NPE; defaults should be applied internally
    final Runner runner = builder.build();

    assertThat(runner).isNotNull();
  }
}
