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
package org.hyperledger.besu.ethereum.p2p.discovery.discv4;

import static com.google.common.base.Preconditions.checkArgument;

import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.ethereum.forkid.ForkIdManager;
import org.hyperledger.besu.ethereum.p2p.config.DiscoveryConfiguration;
import org.hyperledger.besu.ethereum.p2p.discovery.DiscoveryPeer;
import org.hyperledger.besu.ethereum.p2p.discovery.HostEndpoint;
import org.hyperledger.besu.ethereum.p2p.discovery.NodeRecordManager;
import org.hyperledger.besu.ethereum.p2p.discovery.PeerDiscoveryAgent;
import org.hyperledger.besu.ethereum.p2p.discovery.PeerDiscoveryPacketDecodingException;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.DiscoveryPeerV4;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.PeerDiscoveryController;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.PeerRequirement;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.PeerTable;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.TimerUtil;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.Packet;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.PacketDeserializer;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.PacketSerializer;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.ping.PingPacketData;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.peers.PeerId;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissions;
import org.hyperledger.besu.ethereum.p2p.rlpx.RlpxAgent;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.util.NetworkUtility;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.InetAddresses;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.rlp.EndOfRLPException;
import org.ethereum.beacon.discovery.schema.NodeRecord;
import org.ethereum.beacon.discovery.util.DecodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The peer discovery agent is the network component that sends and receives peer discovery messages
 * via UDP.
 */
public abstract class PeerDiscoveryAgentV4 implements PeerDiscoveryAgent {
  private static final Logger LOG = LoggerFactory.getLogger(PeerDiscoveryAgentV4.class);

  // The devp2p specification says only accept packets up to 1280, but some
  // clients ignore that, so we add in a little extra padding. Also used by NettyTransport to
  // drop oversized datagrams before copying them off the channel's buffer.
  public static final int MAX_PACKET_SIZE_BYTES = 1600;
  protected final List<DiscoveryPeerV4> bootstrapPeers;
  private final List<PeerRequirement> peerRequirements = new CopyOnWriteArrayList<>();
  private final PeerPermissions peerPermissions;
  private final MetricsSystem metricsSystem;
  private final RlpxAgent rlpxAgent;
  private final ForkIdManager forkIdManager;
  private final PeerTable peerTable;
  private static final boolean isIpv6Available = NetworkUtility.isIPv6Available();

  /* The peer controller, which takes care of the state machine of peers. */
  protected Optional<PeerDiscoveryController> controller = Optional.empty();

  /* The keypair used to sign messages. */
  protected final NodeKey nodeKey;
  private final Bytes id;
  protected final DiscoveryConfiguration config;

  protected boolean isStopped = false;

  private final NodeRecordManager nodeRecordManager;

  protected final Transport transport;
  protected final PacketSerializer packetSerializer;
  protected final PacketDeserializer packetDeserializer;

  // Cached so handleRawIncoming doesn't allocate a wrapper per inbound packet. Lazily
  // initialised at the start of start() — before the inbound handler is wired — so the
  // subclass constructor has run and createWorkerExecutor() is safe to call.
  private volatile PeerDiscoveryController.AsyncExecutor workerExecutor;

  // Dedicated, single-threaded executor for inbound packet decode only (never shared with
  // outbound packet signing). Preserves the arrival-order guarantee the Vert.x implementation
  // provided via an ordered executeBlocking for decode, distinct from its unordered worker pool
  // used for signing.
  private volatile PeerDiscoveryController.AsyncExecutor decodeExecutor;

  // Single-threaded executor used to serialise all PeerDiscoveryController state mutation:
  // inbound packet handling and timer callbacks both run here. Restores the Vert.x event-loop
  // ordering guarantee the migration to Netty removed.
  private volatile Executor dispatchExecutor;

  // Idempotency guard for stop(): once stopped, subsequent stop() calls are no-ops.
  // No corresponding start gate at this level — see start() for rationale.
  protected final AtomicBoolean stopGate = new AtomicBoolean(false);

  protected PeerDiscoveryAgentV4(
      final NodeKey nodeKey,
      final DiscoveryConfiguration config,
      final PeerPermissions peerPermissions,
      final MetricsSystem metricsSystem,
      final ForkIdManager forkIdManager,
      final NodeRecordManager nodeRecordManager,
      final RlpxAgent rlpxAgent,
      final PeerTable peerTable,
      final Transport transport,
      final PacketSerializer packetSerializer,
      final PacketDeserializer packetDeserializer) {
    this.metricsSystem = metricsSystem;
    checkArgument(nodeKey != null, "nodeKey cannot be null");
    checkArgument(config != null, "provided configuration cannot be null");

    validateConfiguration(config);

    this.peerPermissions = peerPermissions;
    this.bootstrapPeers =
        config.getEnodeBootnodes().stream()
            .map(DiscoveryPeerV4::fromEnode)
            .collect(Collectors.toList());

    this.config = config;
    this.nodeKey = nodeKey;

    this.id = nodeKey.getPublicKey().getEncodedBytes();

    this.forkIdManager = forkIdManager;
    this.rlpxAgent = rlpxAgent;
    this.peerTable = peerTable;
    this.nodeRecordManager = nodeRecordManager;
    this.transport = transport;
    this.packetSerializer = packetSerializer;
    this.packetDeserializer = packetDeserializer;
  }

  protected abstract TimerUtil createTimer();

  protected abstract PeerDiscoveryController.AsyncExecutor createWorkerExecutor();

  protected abstract PeerDiscoveryController.AsyncExecutor createDecodeExecutor();

  /**
   * Single-threaded executor that serialises {@link PeerDiscoveryController} state mutation.
   * Implementations must return the same single-threaded executor used to back {@link
   * #createTimer()} so timer callbacks and decoded packet handling share one thread.
   */
  protected abstract Executor createDispatchExecutor();

  /**
   * Wires the V4 inbound handler and lazily initialises worker + dispatch executors. Idempotent.
   * The composite agent calls this on every sub-agent before binding the shared UDP transport so
   * packets arriving during the bind → start window are not dropped.
   */
  @Override
  public void prepareHandlers() {
    if (workerExecutor == null) {
      workerExecutor = createWorkerExecutor();
    }
    if (decodeExecutor == null) {
      decodeExecutor = createDecodeExecutor();
    }
    if (dispatchExecutor == null) {
      dispatchExecutor = createDispatchExecutor();
    }
    transport.setInboundHandler(this::handleRawIncoming);
  }

  protected CompletableFuture<InetSocketAddress> listenForConnections() {
    return transport.start();
  }

  protected CompletableFuture<Void> sendOutgoingPacket(
      final DiscoveryPeerV4 peer, final Packet packet) {
    if (stopGate.get()) {
      return CompletableFuture.completedFuture(null);
    }
    final InetSocketAddress recipient =
        new InetSocketAddress(peer.getEnodeURL().getIpAsString(), peer.getEndpoint().getUdpPort());
    return transport.send(recipient, packetSerializer.encode(packet));
  }

  private void handleRawIncoming(final InetSocketAddress sender, final Bytes data) {
    // After stop() is called, the transport may still deliver queued packets. Drop them quietly
    // instead of letting workerExecutor.submit throw RejectedExecutionException.
    if (stopGate.get()) {
      return;
    }
    if (!validatePacketSize(data.size())) {
      LOG.trace("Discarding over-sized packet. Actual size (bytes): {}", data.size());
      return;
    }
    decodeExecutor
        .<Packet>execute(() -> packetDeserializer.decode(data))
        .whenCompleteAsync(
            (packet, err) -> {
              if (stopGate.get()) {
                // stop() was called after this decode was already queued; drop the late
                // completion instead of forwarding it into a PeerDiscoveryController that may
                // already be stopped.
                return;
              }
              if (err == null) {
                final Endpoint endpoint =
                    new Endpoint(sender.getHostString(), sender.getPort(), Optional.empty());
                try {
                  handleIncomingPacket(endpoint, packet);
                } catch (final RuntimeException e) {
                  LOG.error("Encountered error while handling packet", e);
                }
              } else {
                if (err instanceof PeerDiscoveryPacketDecodingException
                    || err instanceof DecodeException
                    || err instanceof EndOfRLPException) {
                  LOG.trace(
                      "Discarding invalid peer discovery packet: {}, {}", err.getMessage(), err);
                } else {
                  LOG.error("Encountered error while handling packet", err);
                }
              }
            },
            dispatchExecutor);
  }

  @Override
  public CompletableFuture<Integer> start(final int rlpxTcpPort) {
    // Note: no idempotency guard at this level. Tests legitimately re-invoke start() with a
    // different rlpxTcpPort to control the NodeRecord, and production paths only ever single-start.
    // The transport itself enforces a single-bind invariant via its own start guard.
    if (config.isEnabled()) {
      final String host = config.getBindHost();
      final int port = config.getBindPort();
      LOG.info(
          "Starting peer discovery agent on host={}, port={}. IPv6 {}.",
          host,
          port,
          NetworkUtility.isIPv6Available() ? "available" : "not available");

      // Idempotent — wires the inbound handler before the transport binds so no packets are lost.
      prepareHandlers();

      return listenForConnections()
          .thenApply(
              (InetSocketAddress localAddress) -> {
                // Once listener is set up, finish initializing
                final int discoveryPort = localAddress.getPort();
                // In BOTH mode, whichever agent starts first performs the one real
                // initialization on the shared manager - a no-op check for the normal
                // V4-only (dedicated, never pre-initialized) case.
                if (!nodeRecordManager.isInitialized()) {
                  nodeRecordManager.initializeLocalNode(
                      new HostEndpoint(config.getAdvertisedHost(), discoveryPort, rlpxTcpPort),
                      config
                          .getAdvertisedHostIpv6()
                          .map(
                              v6Host ->
                                  new HostEndpoint(
                                      v6Host,
                                      resolvedIpv6DiscoveryPort(),
                                      rlpxAgent.getIpv6ListeningPort().orElse(rlpxTcpPort))));
                }
                startController(
                    nodeRecordManager
                        .getLocalNode()
                        .orElseThrow(
                            () -> new IllegalStateException("Local node not initialized")));
                LOG.info("P2P peer discovery agent started and listening on {}", localAddress);
                return discoveryPort;
              });
    } else {
      return CompletableFuture.completedFuture(0);
    }
  }

  @Override
  public void updateNodeRecord() {
    if (!config.isEnabled()) {
      return;
    }
    nodeRecordManager.updateNodeRecord();
  }

  public void addPeerRequirement(final PeerRequirement peerRequirement) {
    this.peerRequirements.add(peerRequirement);
  }

  @Override
  public boolean checkForkId(final DiscoveryPeer peer) {
    return peer.getForkId().map(forkIdManager::peerCheck).orElse(true);
  }

  private void startController(final DiscoveryPeerV4 localNode) {
    final PeerDiscoveryController controller = createController(localNode);
    this.controller = Optional.of(controller);
    controller.start();
  }

  private PeerDiscoveryController createController(final DiscoveryPeerV4 localNode) {
    final Optional<Endpoint> localPeerV6Endpoint =
        config
            .getAdvertisedHostIpv6()
            .map(
                v6Host ->
                    new Endpoint(
                        v6Host,
                        resolvedIpv6DiscoveryPort(),
                        Optional.of(
                            rlpxAgent
                                .getIpv6ListeningPort()
                                .orElse(localNode.getEndpoint().getFunctionalTcpPort()))));
    return PeerDiscoveryController.builder()
        .nodeKey(nodeKey)
        .localPeer(localNode)
        .localPeerV6Endpoint(localPeerV6Endpoint)
        .bootstrapNodes(bootstrapPeers)
        .outboundMessageHandler(this::handleOutgoingPacket)
        .timerUtil(createTimer())
        .workerExecutor(workerExecutor)
        .dispatchExecutor(dispatchExecutor)
        .peerRequirement(PeerRequirement.combine(peerRequirements))
        .peerPermissions(peerPermissions)
        .metricsSystem(metricsSystem)
        .filterOnEnrForkId((config.isFilterOnEnrForkIdEnabled()))
        .rlpxAgent(rlpxAgent)
        .peerTable(peerTable)
        .includeBootnodesOnPeerRefresh(config.getIncludeBootnodesOnPeerRefresh())
        .build();
  }

  /**
   * Returns the actual bound IPv6 discovery UDP port if a shared dual-stack socket has one bound,
   * else falls back to the configured port (e.g. ephemeral {@code --p2p-port-ipv6} before bind).
   */
  private int resolvedIpv6DiscoveryPort() {
    return transport
        .getIpv6BoundAddress()
        .map(InetSocketAddress::getPort)
        .orElse(config.getBindPortIpv6());
  }

  protected boolean validatePacketSize(final int packetSize) {
    return packetSize <= MAX_PACKET_SIZE_BYTES;
  }

  protected void handleIncomingPacket(final Endpoint sourceEndpoint, final Packet packet) {
    final int udpPort = sourceEndpoint.getUdpPort();
    final int tcpPort =
        packet
            .getPacketData(PingPacketData.class)
            .flatMap(PingPacketData::getFrom)
            .flatMap(Endpoint::getTcpPort)
            .orElse(udpPort);

    final String host = deriveHost(sourceEndpoint, packet);

    // Notify the peer controller.
    final DiscoveryPeerV4 peer =
        DiscoveryPeerV4.fromEnode(
            EnodeURLImpl.builder()
                .nodeId(packet.getNodeId())
                .ipAddress(host)
                .listeningPort(tcpPort)
                .discoveryPort(udpPort)
                .build());

    controller.ifPresent(c -> c.onMessage(packet, peer));
  }

  /**
   * method to derive the host from the source endpoint and the P2P PING packet. If the host is
   * present in the P2P PING packet itself, use that as the endpoint. If the P2P PING packet
   * specifies 127.0.0.1 (the default if a custom value is not specified with --p2p-host or via a
   * suitable --nat-method) we ignore it in favour of the UDP source address. Some implementations
   * send 127.0.0.1 or 255.255.255.255 anyway, but this reduces the chance of an unexpected change
   * in behaviour as a result of https://github.com/hyperledger/besu/issues/6224 being fixed.
   *
   * @param sourceEndpoint source endpoint of the packet
   * @param packet P2P PING packet
   * @return host address as string
   */
  static String deriveHost(final Endpoint sourceEndpoint, final Packet packet) {
    final Optional<String> pingPacketHost =
        packet
            .getPacketData(PingPacketData.class)
            .flatMap(PingPacketData::getFrom)
            .map(Endpoint::getHost);

    return pingPacketHost
        // fall back to source endpoint "from" if ping packet from address does not satisfy filters
        .filter(InetAddresses::isInetAddress)
        .filter(h -> !NetworkUtility.isUnspecifiedAddress(h))
        .filter(h -> !NetworkUtility.isLocalhostAddress(h))
        .filter(h -> isIpv6Available || !NetworkUtility.isIpV6Address(h))
        .stream()
        .peek(
            h ->
                LOG.atTrace()
                    .setMessage(
                        "Using \"From\" endpoint {} specified in ping packet. Ignoring UDP source host {}")
                    .addArgument(h)
                    .addArgument(sourceEndpoint::getHost)
                    .log())
        .findFirst()
        .orElseGet(
            () -> {
              LOG.atTrace()
                  .setMessage(
                      "Ignoring \"From\" endpoint {} in ping packet. Using UDP source host {}")
                  .addArgument(pingPacketHost.orElse("not specified"))
                  .addArgument(sourceEndpoint.getHost())
                  .log();
              return sourceEndpoint.getHost();
            });
  }

  /**
   * Send a packet to the given recipient.
   *
   * @param peer the recipient
   * @param packet the packet to send
   */
  protected void handleOutgoingPacket(final DiscoveryPeerV4 peer, final Packet packet) {
    sendOutgoingPacket(peer, packet)
        .whenComplete(
            (res, err) -> {
              if (err != null) {
                handleOutgoingPacketError(err, peer, packet);
              }
            });
  }

  protected abstract void handleOutgoingPacketError(
      final Throwable err, final DiscoveryPeerV4 peer, final Packet packet);

  @Override
  public Stream<DiscoveryPeerV4> streamDiscoveredPeers() {
    return controller.map(PeerDiscoveryController::streamDiscoveredPeers).orElse(Stream.empty());
  }

  @Override
  public void dropPeer(final PeerId peer) {
    controller.ifPresent(c -> c.dropPeer(peer));
  }

  @VisibleForTesting
  public Optional<DiscoveryPeerV4> getAdvertisedPeer() {
    return nodeRecordManager.getLocalNode();
  }

  @VisibleForTesting
  public Bytes getId() {
    return id;
  }

  private static void validateConfiguration(final DiscoveryConfiguration config) {
    checkArgument(
        config.getBindHost() != null && InetAddresses.isInetAddress(config.getBindHost()),
        "valid bind host required");
    checkArgument(
        config.getAdvertisedHost() != null
            && InetAddresses.isInetAddress(config.getAdvertisedHost()),
        "valid advertisement host required");
    checkArgument(
        config.getBindPort() == 0 || NetworkUtility.isValidPort(config.getBindPort()),
        "valid port number required");
    checkArgument(config.getEnodeBootnodes() != null, "bootstrapPeers cannot be null");
    checkArgument(config.getBucketSize() > 0, "bucket size cannot be negative nor zero");
  }

  @Override
  public boolean isEnabled() {
    return config.isEnabled();
  }

  /**
   * Returns the current state of the PeerDiscoveryAgent.
   *
   * <p>If true, the node is actively listening for new connections. If false, discovery has been
   * turned off and the node is not listening for connections.
   *
   * @return true, if the {@link PeerDiscoveryAgentV4} is active on this node, false, otherwise.
   */
  @Override
  public boolean isStopped() {
    return isStopped;
  }

  @Override
  public void addPeer(final Peer peer) {
    controller.ifPresent(c -> DiscoveryPeerV4.from(peer).ifPresent(c::handleBondingRequest));
  }

  @Override
  public Optional<NodeRecord> getLocalNodeRecord() {
    return nodeRecordManager.getLocalNode().flatMap(DiscoveryPeerV4::getNodeRecord);
  }

  @VisibleForTesting
  public Optional<DiscoveryPeerV4> getLocalNode() {
    return nodeRecordManager.getLocalNode();
  }

  @Override
  public Optional<Peer> getPeer(final PeerId peerId) {
    return peerTable.get(peerId).map(peer -> peer);
  }
}
