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
package org.hyperledger.besu.ethereum.api.jsonrpc;

import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryWorldStateArchive;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.config.StubGenesisConfigOptions;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.ImmutableApiConfiguration;
import org.hyperledger.besu.ethereum.api.graphql.GraphQLConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.filter.FilterManager;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.filter.FilterManagerBuilder;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.methods.JsonRpcMethodsFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.WebSocketConfiguration;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.blockcreation.NoopMiningCoordinator;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockImporter;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.p2p.network.P2PNetwork;
import org.hyperledger.besu.ethereum.permissioning.AccountLocalConfigPermissioningController;
import org.hyperledger.besu.ethereum.permissioning.NodeLocalConfigPermissioningController;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.metrics.ObservableMetricsSystem;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.metrics.prometheus.MetricsConfiguration;
import org.hyperledger.besu.nat.NatService;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.math.BigInteger;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;

/** Provides a facade to construct the JSON-RPC component. */
public class JsonRpcTestMethodsFactory {

  private static final String CLIENT_NODE_NAME = "TestClientVersion/0.1.0";
  private static final String CLIENT_VERSION = "0.1.0";
  private static final String CLIENT_COMMIT = "12345678";

  private final BlockchainImporter importer;
  private final MutableBlockchain blockchain;
  private final WorldStateArchive stateArchive;
  private final ProtocolContext context;
  private final BlockchainQueries blockchainQueries;
  private final Synchronizer synchronizer;
  private final ProtocolSchedule protocolSchedule;
  private final TransactionSimulator transactionSimulator;
  private Path dataDir = mock(Path.class);

  public JsonRpcTestMethodsFactory(final BlockchainImporter importer, final Path dataDir) {
    this(importer);
    this.dataDir = dataDir;
  }

  public JsonRpcTestMethodsFactory(final BlockchainImporter importer) {
    this.importer = importer;
    this.blockchain = createInMemoryBlockchain(importer.getGenesisBlock());
    this.stateArchive = createInMemoryWorldStateArchive();
    this.importer.getGenesisState().writeStateTo(stateArchive.getWorldState());
    this.context =
        new ProtocolContext.Builder()
            .withBlockchain(blockchain)
            .withWorldStateArchive(stateArchive)
            .build();

    this.protocolSchedule = importer.getProtocolSchedule();
    this.synchronizer = mock(Synchronizer.class);

    for (final Block block : importer.getBlocks()) {
      final ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(block.getHeader());
      final BlockImporter blockImporter = protocolSpec.getBlockImporter();
      blockImporter.importBlock(context, block, HeaderValidationMode.FULL);
    }
    final var miningConfiguration = MiningConfiguration.newDefault().setCoinbase(Address.ZERO);
    this.blockchainQueries =
        new BlockchainQueries(protocolSchedule, blockchain, stateArchive, miningConfiguration);

    this.transactionSimulator =
        new TransactionSimulator(
            blockchain, stateArchive, protocolSchedule, miningConfiguration, 0L);
  }

  public JsonRpcTestMethodsFactory(
      final BlockchainImporter importer,
      final MutableBlockchain blockchain,
      final WorldStateArchive stateArchive,
      final ProtocolContext context) {
    this.importer = importer;
    this.blockchain = blockchain;
    this.stateArchive = stateArchive;
    this.context = context;
    this.protocolSchedule = importer.getProtocolSchedule();
    final var miningConfiguration = MiningConfiguration.newDefault();
    this.blockchainQueries =
        new BlockchainQueries(
            importer.getProtocolSchedule(), blockchain, stateArchive, miningConfiguration);
    this.synchronizer = mock(Synchronizer.class);
    this.transactionSimulator =
        new TransactionSimulator(
            blockchain, stateArchive, protocolSchedule, miningConfiguration, 0L);
  }

  public JsonRpcTestMethodsFactory(
      final BlockchainImporter importer,
      final MutableBlockchain blockchain,
      final WorldStateArchive stateArchive,
      final ProtocolContext context,
      final Synchronizer synchronizer) {
    this.importer = importer;
    this.blockchain = blockchain;
    this.stateArchive = stateArchive;
    this.context = context;
    this.synchronizer = synchronizer;
    this.protocolSchedule = importer.getProtocolSchedule();
    final var miningConfiguration = MiningConfiguration.newDefault();
    this.blockchainQueries =
        new BlockchainQueries(
            importer.getProtocolSchedule(), blockchain, stateArchive, miningConfiguration);
    this.transactionSimulator =
        new TransactionSimulator(
            blockchain, stateArchive, protocolSchedule, miningConfiguration, 0L);
  }

  public BlockchainQueries getBlockchainQueries() {
    return blockchainQueries;
  }

  public WorldStateArchive getStateArchive() {
    return stateArchive;
  }

  public BigInteger getChainId() {
    return protocolSchedule.getChainId().get();
  }

  public ProtocolSchedule getProtocolSchedule() {
    return protocolSchedule;
  }

  public TransactionSimulator getTransactionSimulator() {
    return transactionSimulator;
  }

  public Map<String, JsonRpcMethod> methods() {
    final P2PNetwork peerDiscovery = mock(P2PNetwork.class);
    final EthPeers ethPeers = mock(EthPeers.class);
    final TransactionPool transactionPool = mock(TransactionPool.class);
    final MiningConfiguration miningConfiguration = mock(MiningConfiguration.class);
    final ObservableMetricsSystem metricsSystem = new NoOpMetricsSystem();
    final Optional<AccountLocalConfigPermissioningController> accountWhitelistController =
        Optional.of(mock(AccountLocalConfigPermissioningController.class));
    final Optional<NodeLocalConfigPermissioningController> nodeWhitelistController =
        Optional.of(mock(NodeLocalConfigPermissioningController.class));

    final FilterManager filterManager =
        new FilterManagerBuilder()
            .blockchainQueries(blockchainQueries)
            .transactionPool(transactionPool)
            .build();

    final JsonRpcConfiguration jsonRpcConfiguration = mock(JsonRpcConfiguration.class);
    final WebSocketConfiguration webSocketConfiguration = mock(WebSocketConfiguration.class);
    final MetricsConfiguration metricsConfiguration = mock(MetricsConfiguration.class);
    final GraphQLConfiguration graphQLConfiguration = mock(GraphQLConfiguration.class);

    final NatService natService = new NatService(Optional.empty());

    final List<String> apis = new ArrayList<>();
    apis.add(RpcApis.ETH.name());
    apis.add(RpcApis.NET.name());
    apis.add(RpcApis.WEB3.name());
    apis.add(RpcApis.DEBUG.name());

    return new JsonRpcMethodsFactory()
        .methods(
            CLIENT_NODE_NAME,
            CLIENT_VERSION,
            CLIENT_COMMIT,
            getChainId(),
            new StubGenesisConfigOptions(),
            peerDiscovery,
            blockchainQueries,
            synchronizer,
            importer.getProtocolSchedule(),
            context,
            filterManager,
            transactionPool,
            miningConfiguration,
            new NoopMiningCoordinator(),
            metricsSystem,
            new HashSet<>(),
            accountWhitelistController,
            nodeWhitelistController,
            apis,
            jsonRpcConfiguration,
            webSocketConfiguration,
            metricsConfiguration,
            graphQLConfiguration,
            natService,
            new HashMap<>(),
            dataDir,
            ethPeers,
            Vertx.vertx(new VertxOptions().setWorkerPoolSize(1)),
            ImmutableApiConfiguration.builder().build(),
            Optional.empty(),
            transactionSimulator,
            new DeterministicEthScheduler());
  }
}
