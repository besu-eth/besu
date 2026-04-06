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
package org.hyperledger.besu.ethereum.p2p.network;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.ethereum.core.Util;
import org.hyperledger.besu.ethereum.p2p.config.DiscoveryConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.NetworkingConfiguration;
import org.hyperledger.besu.ethereum.p2p.discovery.DiscoveryPeer;
import org.hyperledger.besu.ethereum.p2p.discovery.DiscoveryPeerFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.PeerDiscoveryAgent;
import org.hyperledger.besu.ethereum.p2p.discovery.PeerDiscoveryAgentFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.RlpxAgentFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.dns.DNSDaemon;
import org.hyperledger.besu.ethereum.p2p.discovery.dns.DNSDaemonListener;
import org.hyperledger.besu.ethereum.p2p.discovery.dns.EthereumNodeRecord;
import org.hyperledger.besu.ethereum.p2p.peers.DefaultPeerPrivileges;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.p2p.peers.MaintainedPeers;
import org.hyperledger.besu.ethereum.p2p.peers.MutableLocalNode;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.peers.PeerPrivileges;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissions;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissionsDenylist;
import org.hyperledger.besu.ethereum.p2p.rlpx.ConnectCallback;
import org.hyperledger.besu.ethereum.p2p.rlpx.DisconnectCallback;
import org.hyperledger.besu.ethereum.p2p.rlpx.MessageCallback;
import org.hyperledger.besu.ethereum.p2p.rlpx.RlpxAgent;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerLookup;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;
import org.hyperledger.besu.nat.NatMethod;
import org.hyperledger.besu.nat.NatService;
import org.hyperledger.besu.nat.core.NatManager;
import org.hyperledger.besu.nat.core.domain.NatServiceType;
import org.hyperledger.besu.nat.core.domain.NetworkProtocol;
import org.hyperledger.besu.nat.upnp.UpnpNatManager;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.net.InetAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.ThreadingModel;
import io.vertx.core.Vertx;
import org.apache.tuweni.bytes.Bytes;
import org.ethereum.beacon.discovery.schema.NodeRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The peer network service (defunct PeerNetworkingService) is the entrypoint to the peer-to-peer
 * components of the Ethereum client. It implements the devp2p framework from the Ethereum
 * specifications.
 *
 * <p>This component manages the peer discovery layer, the RLPx wire protocol and the subprotocols
 * supported by this client.
 *
 * <h2>Peer Discovery</h2>
 *
 * Ethereum nodes discover one another via a simple UDP protocol that follows some of the techniques
 * described in the Kademlia DHT paper. Particularly nodes are classified in a k-bucket table
 * composed of 256 buckets, where each bucket contains at most 16 peers whose <i>XOR(SHA3(x))</i>
 * distance from us is equal to the index of the bucket. The value <i>x</i> in the distance function
 * corresponds to our node ID (public key).
 *
 * <p>Upper layers in the stack subscribe to events from the peer discovery layer and initiate/drop
 * connections accordingly.
 *
 * <h2>RLPx Wire Protocol</h2>
 *
 * The RLPx wire protocol is responsible for selecting peers to engage with, authenticating and
 * encrypting communications with peers, multiplexing subprotocols, framing messages, controlling
 * legality of messages, keeping connections alive, and keeping track of peer reputation.
 *
 * <h2>Subprotocols</h2>
 *
 * Subprotocols are pluggable elements on top of the RLPx framework, which can handle a specific set
 * of messages. Each subprotocol has a 3-char ASCII denominator and a version number, and statically
 * defines a count of messages it can handle.
 *
 * <p>The RLPx wire protocol dispatches messages to subprotocols based on the capabilities agreed by
 * each of the two peers during the protocol handshake.
 *
 * @see <a href="https://pdos.csail.mit.edu/~petar/papers/maymounkov-kademlia-lncs.pdf">Kademlia DHT
 *     paper</a>
 * @see <a href="https://github.com/ethereum/wiki/wiki/Kademlia-Peer-Selection">Kademlia Peer
 *     Selection</a>
 * @see <a href="https://github.com/ethereum/devp2p/blob/master/rlpx.md">devp2p RLPx</a>
 */
public class DefaultP2PNetwork implements P2PNetwork {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultP2PNetwork.class);

  private final ScheduledExecutorService peerConnectionScheduler =
      Executors.newSingleThreadScheduledExecutor();
  private final PeerDiscoveryAgent peerDiscoveryAgent;
  private final RlpxAgent rlpxAgent;

  private final NetworkingConfiguration config;

  private final Bytes nodeId;
  private final MutableLocalNode localNode;

  private final PeerPermissions peerPermissions;
  private final MaintainedPeers maintainedPeers;

  private final NatService natService;

  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private final CountDownLatch shutdownLatch = new CountDownLatch(2);
  private final Duration shutdownTimeout = Duration.ofSeconds(15);
  private final Vertx vertx;
  private final AtomicReference<Optional<DNSDaemon>> dnsDaemonRef =
      new AtomicReference<>(Optional.empty());

  /**
   * Creates a peer networking service for production purposes.
   *
   * <p>The caller is expected to provide the IP address to be advertised (normally this node's
   * public IP address), as well as TCP and UDP port numbers for the RLPx agent and the discovery
   * agent, respectively.
   *
   * @param localNode A representation of the local node
   * @param peerDiscoveryAgent The agent responsible for discovering peers on the network.
   * @param nodeKey The node key through which cryptographic operations can be performed
   * @param config The network configuration to use.
   * @param peerPermissions An object that determines whether peers are allowed to connect
   * @param natService The NAT environment manager.
   * @param maintainedPeers A collection of peers for which we are expected to maintain connections
   * @param reputationManager An object that inspect disconnections for misbehaving peers that can
   *     then be blacklisted.
   * @param vertx the Vert.x instance managing network resources
   */
  DefaultP2PNetwork(
      final MutableLocalNode localNode,
      final PeerDiscoveryAgent peerDiscoveryAgent,
      final RlpxAgent rlpxAgent,
      final NodeKey nodeKey,
      final NetworkingConfiguration config,
      final PeerPermissions peerPermissions,
      final NatService natService,
      final MaintainedPeers maintainedPeers,
      final PeerDenylistManager reputationManager,
      final Vertx vertx) {
    this.localNode = localNode;
    this.peerDiscoveryAgent = peerDiscoveryAgent;
    this.rlpxAgent = rlpxAgent;
    this.config = config;
    this.natService = natService;
    this.maintainedPeers = maintainedPeers;

    this.nodeId = nodeKey.getPublicKey().getEncodedBytes();
    this.peerPermissions = peerPermissions;
    this.vertx = vertx;

    subscribeDisconnect(reputationManager);
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public void start() {
    if (!started.compareAndSet(false, true)) {
      LOG.warn("Attempted to start an already started " + getClass().getSimpleName());
      return;
    }

    if (config.discoveryConfiguration().isDiscoveryV5Enabled()) {
      LOG.warn(
          "Discovery Protocol v5 is enabled via --Xv5-discovery-enabled. This is an experimental feature and may not be fully stable.");
    } else {
      warnIfIpv6OptionsWithDiscV4();
    }

    final String address = config.discoveryConfiguration().getAdvertisedHost();

    Optional.ofNullable(config.discoveryConfiguration().getDNSDiscoveryURL())
        .ifPresent(
            disco -> {
              // These lists are updated every 12h
              // We retrieve the list every 10 minutes (600000 msec)
              LOG.info("Starting DNS discovery with URL {}", disco);
              config
                  .dnsDiscoveryServerOverride()
                  .ifPresent(
                      dnsServer ->
                          LOG.info(
                              "Starting DNS discovery with DNS Server override {}", dnsServer));

              final DNSDaemon dnsDaemon =
                  new DNSDaemon(
                      disco,
                      createDaemonListener(),
                      0L,
                      1000L, // start after 1 second
                      600000L,
                      config.dnsDiscoveryServerOverride().orElse(null));

              // Use Java 21 virtual thread to deploy verticle
              final DeploymentOptions options =
                  new DeploymentOptions()
                      .setThreadingModel(ThreadingModel.VIRTUAL_THREAD)
                      .setInstances(1)
                      .setWorkerPoolSize(1);

              final Future<String> deployId = vertx.deployVerticle(dnsDaemon, options);
              deployId.toCompletionStage().toCompletableFuture().join();
              dnsDaemonRef.set(Optional.of(dnsDaemon));
            });

    final int listeningPort;
    try {
      listeningPort = rlpxAgent.start().join();
    } catch (final Exception e) {
      LOG.error("Failed to start RLPx agent", e);
      // Discovery agent will not be started, count down its latch position
      shutdownLatch.countDown();
      // Ensure any partially started RLPx agent is stopped and count down its latch position
      rlpxAgent.stop().whenComplete((res, err) -> shutdownLatch.countDown());
      throw e;
    }

    // Pass the effective RLPx TCP port so that the discovery agent can write the correct tcp/tcp6
    // values into the local ENR.  The discovery agent reads its own UDP bind port independently.
    final int discoveryPort;
    try {
      discoveryPort = peerDiscoveryAgent.start(listeningPort).join();
    } catch (final Exception e) {
      LOG.error("Failed to start peer discovery agent", e);
      // Stop the partially-started discovery agent and count down its latch position on completion
      peerDiscoveryAgent.stop().whenComplete((r, err) -> shutdownLatch.countDown());
      // Stop the already-started RLPx agent and count down the remaining latch position on
      // completion
      rlpxAgent.stop().whenComplete((res, err) -> shutdownLatch.countDown());
      throw e;
    }

    final Consumer<? super NatManager> natAction =
        natManager -> {
          final UpnpNatManager upnpNatManager = (UpnpNatManager) natManager;
          upnpNatManager.requestPortForward(
              discoveryPort, NetworkProtocol.UDP, NatServiceType.DISCOVERY);
          upnpNatManager.requestPortForward(
              listeningPort, NetworkProtocol.TCP, NatServiceType.RLPX);
        };

    natService.ifNatEnvironment(NatMethod.UPNP, natAction);
    natService.ifNatEnvironment(NatMethod.UPNPP2PONLY, natAction);

    setLocalNode(address, listeningPort, discoveryPort);

    // Call checkMaintainedConnectionPeers() now that the local node is up, for immediate peer
    // additions
    checkMaintainedConnectionPeers();

    // Periodically check maintained connections
    final long checkMaintainedConnectionsSec =
        config.checkMaintainedConnectionsFrequency().toSeconds();
    peerConnectionScheduler.scheduleWithFixedDelay(
        this::checkMaintainedConnectionPeers, 2, checkMaintainedConnectionsSec, TimeUnit.SECONDS);
    // Periodically initiate outgoing connections to discovered peers
    final long checkConnectionsSec = config.initiateConnectionsFrequency().toSeconds();
    peerConnectionScheduler.scheduleWithFixedDelay(
        this::attemptPeerConnections, checkConnectionsSec, checkConnectionsSec, TimeUnit.SECONDS);
  }

  @Override
  public void stop() {
    if (!this.started.get() || !stopped.compareAndSet(false, true)) {
      // We haven't started, or we've started and stopped already
      return;
    }

    // since dnsDaemon is a vertx verticle, vertx.close will undeploy it.
    // However, we can safely call stop as well.
    dnsDaemonRef.get().ifPresent(DNSDaemon::stop);

    peerConnectionScheduler.shutdownNow();
    peerDiscoveryAgent.stop().whenComplete((res, err) -> shutdownLatch.countDown());
    rlpxAgent.stop().whenComplete((res, err) -> shutdownLatch.countDown());
    peerPermissions.close();
  }

  @Override
  public void awaitStop() {
    try {
      if (!peerConnectionScheduler.awaitTermination(
          shutdownTimeout.toSeconds(), TimeUnit.SECONDS)) {
        LOG.error(
            "{} did not shutdown cleanly: peerConnectionScheduler executor did not fully terminate.",
            this.getClass().getSimpleName());
      }
      if (!shutdownLatch.await(shutdownTimeout.toSeconds(), TimeUnit.SECONDS)) {
        LOG.error(
            "{} did not shutdown cleanly: some internal services failed to fully terminate.",
            this.getClass().getSimpleName());
      }
    } catch (final InterruptedException ex) {
      throw new IllegalStateException(ex);
    }
  }

  @Override
  public Optional<RlpxAgent> getRlpxAgent() {
    return Optional.of(rlpxAgent);
  }

  @Override
  public boolean addMaintainedConnectionPeer(final Peer peer) {
    if (localNode.isReady()
        && localNode.getPeer() != null
        && localNode.getPeer().getEnodeURL() != null
        && peer.getEnodeURL().getNodeId().equals(localNode.getPeer().getEnodeURL().getNodeId())) {
      return false;
    }
    final boolean wasAdded = maintainedPeers.add(peer);
    peerDiscoveryAgent.addPeer(peer);
    rlpxAgent.connect(peer);
    return wasAdded;
  }

  @Override
  public boolean removeMaintainedConnectionPeer(final Peer peer) {
    final boolean wasRemoved = maintainedPeers.remove(peer);
    peerDiscoveryAgent.dropPeer(peer);
    LOG.debug("Disconnect requested for peer {}.", peer);
    rlpxAgent.disconnect(peer.getId(), DisconnectReason.REQUESTED);
    return wasRemoved;
  }

  @Override
  public Collection<Peer> getMaintainedConnectionPeers() {
    return maintainedPeers.streamPeers().toList();
  }

  @VisibleForTesting
  Optional<DNSDaemon> getDnsDaemon() {
    return dnsDaemonRef.get();
  }

  @VisibleForTesting
  DNSDaemonListener createDaemonListener() {
    return (seq, records) ->
        records.stream()
            .map(DiscoveryPeerFactory::fromEthereumNodeRecord)
            .forEach(peerDiscoveryAgent::addPeer);
  }

  @VisibleForTesting
  void checkMaintainedConnectionPeers() {
    if (!localNode.isReady()) {
      return;
    }
    final List<Bytes> doNotConnectTo =
        rlpxAgent
            .streamActiveConnections()
            .map(c -> c.getPeer().getId())
            .collect(Collectors.toList());
    doNotConnectTo.add(localNode.getPeer().getEnodeURL().getNodeId());
    maintainedPeers
        .streamPeers()
        .filter(p -> !doNotConnectTo.contains(p.getId()))
        .forEach(rlpxAgent::connect);
  }

  @VisibleForTesting
  void attemptPeerConnections() {
    LOG.trace("Initiating connections to discovered peers.");
    final Stream<DiscoveryPeer> toTry =
        streamDiscoveredPeers()
            .filter(DiscoveryPeer::isReadyForConnections)
            .filter(peerDiscoveryAgent::checkForkId)
            .sorted(Comparator.comparing(DiscoveryPeer::getLastAttemptedConnection));
    toTry.forEach(rlpxAgent::connect);
  }

  @Override
  public Collection<PeerConnection> getPeers() {
    return rlpxAgent.streamConnections().collect(Collectors.toList());
  }

  @Override
  public int getPeerCount() {
    return rlpxAgent.getConnectionCount();
  }

  @Override
  public Stream<DiscoveryPeer> streamDiscoveredPeers() {
    return peerDiscoveryAgent.streamDiscoveredPeers().map(p -> p);
  }

  @Override
  public CompletableFuture<PeerConnection> connect(final Peer peer) {
    return rlpxAgent.connect(peer);
  }

  @Override
  public void subscribe(final Capability capability, final MessageCallback callback) {
    rlpxAgent.subscribeMessage(capability, callback);
  }

  @Override
  public void subscribeConnect(final ConnectCallback callback) {
    rlpxAgent.subscribeConnect(callback);
  }

  @Override
  public void subscribeDisconnect(final DisconnectCallback callback) {
    rlpxAgent.subscribeDisconnect(callback);
  }

  @Override
  public void close() {
    stop();
  }

  @Override
  public boolean isListening() {
    return localNode.isReady();
  }

  @Override
  public boolean isP2pEnabled() {
    return true;
  }

  @Override
  public boolean isDiscoveryEnabled() {
    return peerDiscoveryAgent.isEnabled();
  }

  @Override
  public boolean isStopped() {
    return peerDiscoveryAgent.isStopped();
  }

  @Override
  public Optional<EnodeURLImpl> getLocalEnode() {
    if (!localNode.isReady()) {
      return Optional.empty();
    }
    return Optional.of(localNode.getPeer().getEnodeURL());
  }

  private void warnIfIpv6OptionsWithDiscV4() {
    final DiscoveryConfiguration disc = config.discoveryConfiguration();
    if (disc.getAdvertisedHostIpv6().isPresent() || disc.isDualStackEnabled()) {
      LOG.warn(
          "--p2p-host-ipv6 and --p2p-interface-ipv6 are only supported with DiscV5 "
              + "(--Xv5-discovery-enabled). These options are ignored by DiscV4.");
    }
  }

  private void setLocalNode(
      final String address, final int listeningPort, final int discoveryPort) {
    if (localNode.isReady()) {
      // Already set
      return;
    }

    // override advertised host if we detect an external IP address via NAT manager
    final String advertisedAddress = natService.queryExternalIPAddress(address);

    final EnodeURLImpl localEnode =
        EnodeURLImpl.builder()
            .nodeId(nodeId)
            .ipAddress(advertisedAddress)
            .listeningPort(listeningPort)
            .discoveryPort(discoveryPort)
            .build();

    LOG.info("Enode URL {}", localEnode.toString());
    getLocalEnr().ifPresent(enr -> LOG.info("ENR URL {}", enr));
    LOG.info("Node address {}", Util.publicKeyToAddress(localEnode.getNodeId()));
    localNode.setEnode(localEnode);
  }

  @Override
  public Optional<String> getLocalEnr() {
    return peerDiscoveryAgent.getLocalNodeRecord().map(NodeRecord::asEnr);
  }

  @Override
  public Optional<IPv6AddressInfo> getIPv6AddressInfo() {
    try {
      return peerDiscoveryAgent
          .getLocalNodeRecord()
          .map(EthereumNodeRecord::fromNodeRecord)
          .flatMap(
              enr ->
                  enr.getIpV6Address()
                      .map(InetAddress::getHostAddress)
                      .map(
                          addr ->
                              new IPv6AddressInfo(
                                  addr,
                                  enr.getIpV6TcpListeningPort(),
                                  enr.getIpV6UdpDiscoveryPort())));
    } catch (final IllegalArgumentException e) {
      LOG.debug("Failed to parse local Ethereum Node Record; IPv6 fields will be unavailable", e);
      return Optional.empty();
    }
  }

  @Override
  public void updateNodeRecord() {
    peerDiscoveryAgent.updateNodeRecord();
  }

  public static class Builder {

    private Vertx vertx;

    private NetworkingConfiguration config = NetworkingConfiguration.DEFAULT;
    private List<Capability> supportedCapabilities;
    private NodeKey nodeKey;

    private MaintainedPeers maintainedPeers = new MaintainedPeers();
    private PeerPermissions peerPermissions = PeerPermissions.noop();

    private NatService natService = new NatService(Optional.empty());
    private MetricsSystem metricsSystem;

    private PeerDiscoveryAgentFactory peerDiscoveryAgentFactory;
    private RlpxAgentFactory rlpxAgentFactory;

    public P2PNetwork build() {
      validate();
      return doBuild();
    }

    private P2PNetwork doBuild() {
      // Set up permissions
      // Fold peer reputation into permissions
      final PeerPermissionsDenylist misbehavingPeers = PeerPermissionsDenylist.create(500);
      final PeerDenylistManager reputationManager =
          new PeerDenylistManager(misbehavingPeers, maintainedPeers);
      peerPermissions = PeerPermissions.combine(peerPermissions, misbehavingPeers);

      final MutableLocalNode localNode =
          MutableLocalNode.create(
              config.rlpxConfiguration().getClientId(), 5, supportedCapabilities);
      final PeerPrivileges peerPrivileges = new DefaultPeerPrivileges(maintainedPeers);
      final PeerLookup peerLookup = new PeerLookup();
      RlpxAgent rlpxAgent = rlpxAgentFactory.create(localNode, peerPrivileges, peerLookup);
      PeerDiscoveryAgent peerDiscoveryAgent = peerDiscoveryAgentFactory.create(rlpxAgent);
      peerLookup.set(peerDiscoveryAgent::getPeer);
      return new DefaultP2PNetwork(
          localNode,
          peerDiscoveryAgent,
          rlpxAgent,
          nodeKey,
          config,
          peerPermissions,
          natService,
          maintainedPeers,
          reputationManager,
          vertx);
    }

    private void validate() {
      checkState(nodeKey != null, "NodeKey must be set.");
      checkState(config != null, "NetworkingConfiguration must be set.");
      checkState(
          supportedCapabilities != null && !supportedCapabilities.isEmpty(),
          "Supported capabilities must be set and non-empty.");
      checkState(metricsSystem != null, "MetricsSystem must be set.");
      checkState(rlpxAgentFactory != null, "RlpxAgentFactory must be set.");
      checkState(peerDiscoveryAgentFactory != null, "DiscoveryAgentFactory must be set.");
    }

    public Builder vertx(final Vertx vertx) {
      checkNotNull(vertx);
      this.vertx = vertx;
      return this;
    }

    public Builder nodeKey(final NodeKey nodeKey) {
      checkNotNull(nodeKey);
      this.nodeKey = nodeKey;
      return this;
    }

    public Builder config(final NetworkingConfiguration config) {
      checkNotNull(config);
      this.config = config;
      return this;
    }

    public Builder supportedCapabilities(final List<Capability> supportedCapabilities) {
      checkNotNull(supportedCapabilities);
      this.supportedCapabilities = supportedCapabilities;
      return this;
    }

    public Builder supportedCapabilities(final Capability... supportedCapabilities) {
      this.supportedCapabilities = Arrays.asList(supportedCapabilities);
      return this;
    }

    public Builder peerPermissions(final PeerPermissions peerPermissions) {
      checkNotNull(peerPermissions);
      this.peerPermissions = peerPermissions;
      return this;
    }

    public Builder metricsSystem(final MetricsSystem metricsSystem) {
      checkNotNull(metricsSystem);
      this.metricsSystem = metricsSystem;
      return this;
    }

    public Builder natService(final NatService natService) {
      checkNotNull(natService);
      this.natService = natService;
      return this;
    }

    public Builder maintainedPeers(final MaintainedPeers maintainedPeers) {
      checkNotNull(maintainedPeers);
      this.maintainedPeers = maintainedPeers;
      return this;
    }

    public Builder peerDiscoveryAgentFactory(final PeerDiscoveryAgentFactory factory) {
      this.peerDiscoveryAgentFactory = factory;
      return this;
    }

    public Builder rlpxAgentFactory(final RlpxAgentFactory factory) {
      this.rlpxAgentFactory = factory;
      return this;
    }
  }
}
