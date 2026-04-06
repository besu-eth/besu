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
package org.hyperledger.besu.ethereum.eth.transactions;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryWorldStateArchive;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.GenesisState;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.difficulty.fixed.FixedDifficultyProtocolSchedule;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthMessages;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManager;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskRequestSender;
import org.hyperledger.besu.ethereum.eth.sync.ChainHeadTracker;
import org.hyperledger.besu.ethereum.eth.sync.SyncMode;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.forkid.ForkIdManager;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ScheduleBasedBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.p2p.config.DiscoveryConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.ImmutableNetworkingConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.NetworkingConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.RlpxConfiguration;
import org.hyperledger.besu.ethereum.p2p.discovery.DefaultPeerDiscoveryAgentFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.DefaultRlpxAgentFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.PeerDiscoveryAgentFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.RlpxAgentFactory;
import org.hyperledger.besu.ethereum.p2p.network.DefaultP2PNetwork;
import org.hyperledger.besu.ethereum.p2p.network.NetworkRunner;
import org.hyperledger.besu.ethereum.p2p.network.P2PNetwork;
import org.hyperledger.besu.ethereum.p2p.peers.DefaultPeer;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissions;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.permissioning.NodeMessagePermissioningProvider;
import org.hyperledger.besu.testutil.TestClock;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigInteger;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import io.vertx.core.Vertx;
import org.apache.tuweni.bytes.Bytes;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestNode implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(TestNode.class);
  private static final MetricsSystem metricsSystem = new NoOpMetricsSystem();

  protected final NodeKey nodeKey;
  protected final P2PNetwork network;
  protected final Peer selfPeer;
  protected final Map<PeerConnection, DisconnectReason> disconnections = new HashMap<>();
  private final TransactionPool transactionPool;

  public TestNode(
      final Vertx vertx,
      final Integer port,
      final KeyPair kp,
      final DiscoveryConfiguration discoveryCfg) {
    requireNonNull(vertx);
    requireNonNull(discoveryCfg);

    final int listenPort = port != null ? port : 0;
    this.nodeKey = kp != null ? NodeKeyUtils.createFrom(kp) : NodeKeyUtils.generate();

    final NetworkingConfiguration networkingConfiguration =
        ImmutableNetworkingConfiguration.builder()
            .discoveryConfiguration(discoveryCfg)
            .rlpxConfiguration(
                RlpxConfiguration.create()
                    .setBindPort(listenPort)
                    .setSupportedProtocols(EthProtocol.get()))
            .build();

    final GenesisConfig genesisConfig = GenesisConfig.fromResource("/dev.json");
    final ProtocolSchedule protocolSchedule =
        FixedDifficultyProtocolSchedule.create(
            GenesisConfig.fromResource("/dev.json").getConfigOptions(),
            false,
            EvmConfiguration.DEFAULT,
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem());

    final GenesisState genesisState =
        GenesisState.fromConfig(genesisConfig, protocolSchedule, new CodeCache());
    final BlockHeaderFunctions blockHeaderFunctions =
        ScheduleBasedBlockHeaderFunctions.create(protocolSchedule);
    final MutableBlockchain blockchain =
        createInMemoryBlockchain(genesisState.getBlock(), blockHeaderFunctions);
    final WorldStateArchive worldStateArchive = createInMemoryWorldStateArchive();
    genesisState.writeStateTo(worldStateArchive.getWorldState());
    final ProtocolContext protocolContext =
        new ProtocolContext.Builder()
            .withBlockchain(blockchain)
            .withWorldStateArchive(worldStateArchive)
            .build();

    final SyncState syncState = mock(SyncState.class);
    final SynchronizerConfiguration syncConfig = mock(SynchronizerConfiguration.class);
    when(syncConfig.getSyncMode()).thenReturn(SyncMode.FULL);
    when(syncState.isInSync(anyLong())).thenReturn(true);
    when(syncState.isInitialSyncPhaseDone()).thenReturn(true);

    final EthMessages ethMessages = new EthMessages();
    final NodeMessagePermissioningProvider nmpp = (destinationEnode, code) -> true;
    final EthPeers ethPeers =
        new EthPeers(
            () -> protocolSchedule.getByBlockHeader(blockchain.getChainHeadHeader()),
            TestClock.fixed(),
            metricsSystem,
            EthProtocolConfiguration.DEFAULT_MAX_MESSAGE_SIZE,
            Collections.singletonList(nmpp),
            Bytes.random(64),
            25,
            25,
            false,
            SyncMode.SNAP,
            new ForkIdManager(blockchain, Collections.emptyList(), Collections.emptyList()));

    final ChainHeadTracker mockCHT = getChainHeadTracker();
    ethPeers.setChainHeadTracker(mockCHT);

    final EthScheduler scheduler = new EthScheduler(1, 1, 1, metricsSystem);
    final EthContext ethContext =
        new EthContext(
            ethPeers,
            ethMessages,
            scheduler,
            new PeerTaskExecutor(ethPeers, new PeerTaskRequestSender(), metricsSystem));

    final EthProtocolConfiguration ethProtocolConfiguration = EthProtocolConfiguration.DEFAULT;
    transactionPool =
        TransactionPoolFactory.createTransactionPool(
            protocolSchedule,
            protocolContext,
            ethContext,
            TestClock.system(ZoneId.systemDefault()),
            metricsSystem,
            syncState,
            TransactionPoolConfiguration.DEFAULT,
            ethProtocolConfiguration,
            new BlobCache(),
            MiningConfiguration.newDefault());

    final EthProtocolManager ethProtocolManager =
        new EthProtocolManager(
            blockchain,
            BigInteger.ONE,
            worldStateArchive,
            transactionPool,
            ethProtocolConfiguration,
            ethPeers,
            ethMessages,
            ethContext,
            Collections.emptyList(),
            Optional.empty(),
            syncConfig,
            scheduler);

    final NetworkRunner networkRunner =
        NetworkRunner.builder()
            .subProtocols(EthProtocol.get())
            .protocolManagers(singletonList(ethProtocolManager))
            .peerConnectionGatekeeper((p, d) -> Optional.empty())
            .network(
                capabilities ->
                    createP2PNetwork(
                        networkingConfiguration,
                        vertx,
                        nodeKey,
                        blockchain,
                        capabilities,
                        ethPeers))
            .metricsSystem(new NoOpMetricsSystem())
            .build();
    network = networkRunner.getNetwork();
    network.getRlpxAgent().ifPresent(ethPeers::setRlpxAgent);
    network.subscribeDisconnect(
        (connection, reason, initiatedByPeer) -> disconnections.put(connection, reason));

    networkRunner.start();
    selfPeer = DefaultPeer.fromEnodeURL(network.getLocalEnode().get());
  }

  private P2PNetwork createP2PNetwork(
      final NetworkingConfiguration networkingConfiguration,
      final Vertx vertx,
      final NodeKey nodeKey,
      final Blockchain blockchain,
      final List<Capability> capabilities,
      final EthPeers ethPeers) {
    final PeerDiscoveryAgentFactory peerDiscoveryAgentFactory =
        DefaultPeerDiscoveryAgentFactory.builder()
            .vertx(vertx)
            .nodeKey(nodeKey)
            .config(networkingConfiguration)
            .peerPermissions(PeerPermissions.noop())
            .metricsSystem(new NoOpMetricsSystem())
            .storageProvider(new InMemoryKeyValueStorageProvider())
            .blockchain(blockchain)
            .blockNumberForks(Collections.emptyList())
            .timestampForks(Collections.emptyList())
            .build();

    final RlpxAgentFactory defaultRlpxFactory =
        DefaultRlpxAgentFactory.builder()
            .nodeKey(nodeKey)
            .config(networkingConfiguration)
            .peerPermissions(PeerPermissions.noop())
            .metricsSystem(new NoOpMetricsSystem())
            .allConnectionsSupplier(ethPeers::streamAllConnections)
            .allActiveConnectionsSupplier(ethPeers::streamAllActiveConnections)
            .maxPeers(ethPeers.getMaxPeers())
            .build();

    return DefaultP2PNetwork.builder()
        .vertx(vertx)
        .nodeKey(nodeKey)
        .config(networkingConfiguration)
        .metricsSystem(new NoOpMetricsSystem())
        .supportedCapabilities(capabilities)
        .rlpxAgentFactory(defaultRlpxFactory)
        .peerDiscoveryAgentFactory(peerDiscoveryAgentFactory)
        .build();
  }

  private static ChainHeadTracker getChainHeadTracker() {
    final ChainHeadTracker mockCHT = mock(ChainHeadTracker.class);
    final BlockHeader mockBlockHeader = mock(BlockHeader.class);
    Mockito.lenient().when(mockBlockHeader.getNumber()).thenReturn(0L);
    Mockito.lenient()
        .when(mockCHT.getBestHeaderFromPeer(any()))
        .thenReturn(CompletableFuture.completedFuture(mockBlockHeader));
    return mockCHT;
  }

  public Bytes id() {
    return nodeKey.getPublicKey().getEncodedBytes();
  }

  public static String shortId(final Bytes id) {
    return id.slice(62).toString().substring(2);
  }

  public String shortId() {
    return shortId(id());
  }

  public Peer selfPeer() {
    return selfPeer;
  }

  public CompletableFuture<PeerConnection> connect(final TestNode remoteNode) {
    return network.connect(remoteNode.selfPeer());
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public void close() throws IOException {
    IOException firstEx = null;
    try {
      network.close();
    } catch (final IOException e) {
      if (firstEx == null) {
        firstEx = e;
      }
      LOG.warn("Error closing.  Continuing", e);
    }

    if (firstEx != null) {
      throw new IOException("Unable to close successfully.  Wrapping first exception.", firstEx);
    }
  }

  @Override
  public String toString() {
    return shortId()
        + "@"
        + selfPeer.getEnodeURL().getIpAsString()
        + ':'
        + selfPeer.getEnodeURL().getListeningPortOrZero();
  }

  public void receiveRemoteTransaction(final Transaction transaction) {
    transactionPool.addRemoteTransactions(singletonList(transaction));
  }

  public void receiveLocalTransaction(final Transaction transaction) {
    transactionPool.addTransactionViaApi(transaction);
  }

  public int getPendingTransactionCount() {
    return transactionPool.count();
  }
}
