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
package org.hyperledger.besu.ethereum.p2p.discovery.discv5;

import org.hyperledger.besu.ethereum.forkid.ForkIdManager;
import org.hyperledger.besu.ethereum.p2p.config.DiscoveryConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.NetworkingConfiguration;
import org.hyperledger.besu.ethereum.p2p.discovery.DiscoveryPeer;
import org.hyperledger.besu.ethereum.p2p.discovery.DiscoveryPeerFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.HostEndpoint;
import org.hyperledger.besu.ethereum.p2p.discovery.NodeRecordManager;
import org.hyperledger.besu.ethereum.p2p.discovery.PeerDiscoveryAgent;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.peers.PeerId;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissions;
import org.hyperledger.besu.ethereum.p2p.rlpx.RlpxAgent;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.ethereum.beacon.discovery.MutableDiscoverySystem;
import org.ethereum.beacon.discovery.schema.NodeRecord;
import org.ethereum.beacon.discovery.storage.NodeRecordListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DiscV5 implementation of {@link PeerDiscoveryAgent} that actively drives peer discovery and
 * outbound RLPx connection attempts.
 *
 * <p>This agent owns the lifecycle of the DiscV5 {@link MutableDiscoverySystem} and executes
 * periodic peer discovery using an adaptive cadence based on current peer connectivity.
 *
 * <p>Discovery cadence:
 *
 * <ul>
 *   <li>Fast (1 second) while the node is under-connected
 *   <li>Slow (30 seconds) once a sufficient number of peers has been reached
 * </ul>
 *
 * <p>Discovered peers are filtered for readiness, fork compatibility, and reachability before
 * connection attempts are delegated to the {@link RlpxAgent}.
 */
public final class PeerDiscoveryAgentV5 implements PeerDiscoveryAgent {

  private static final Logger LOG = LoggerFactory.getLogger(PeerDiscoveryAgentV5.class);

  /**
   * Factory for creating a {@link MutableDiscoverySystem}. The default implementation uses {@link
   * org.ethereum.beacon.discovery.DiscoverySystemBuilder}; tests can inject a mock.
   */
  @FunctionalInterface
  interface DiscoverySystemFactory {
    /**
     * Creates a new {@link MutableDiscoverySystem}.
     *
     * @param localNodeRecord the local node record to use
     * @param nodeRecordListener listener invoked when the discovery library resolves bound ports
     * @return a configured but not yet started discovery system
     */
    MutableDiscoverySystem create(
        NodeRecord localNodeRecord, NodeRecordListener nodeRecordListener);
  }

  private final DiscoveryConfiguration discoveryConfig;
  private final PeerPermissions peerPermissions;
  private final ForkIdManager forkIdManager;
  private final NodeRecordManager nodeRecordManager;
  private final RlpxAgent rlpxAgent;
  private final MetricsSystem metricsSystem;
  private final boolean preferIpv6Outbound;
  private final DiscoverySystemFactory discoverySystemFactory;

  // Initialized lazily in start() once the RLPx TCP port is known.
  private final AtomicReference<MutableDiscoverySystem> discoverySystem = new AtomicReference<>();

  private final ScheduledExecutorService scheduler =
      Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "discv5-peer-discovery"));

  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  // Indicates whether a discovery operation is currently in progress
  private final AtomicBoolean discoveryInProgress = new AtomicBoolean(false);

  /**
   * Creates a new DiscV5 peer discovery agent.
   *
   * <p>The {@link MutableDiscoverySystem} is not built at construction time. It is built lazily
   * during {@link #start(int)} once the actual RLPx TCP port is known, so that the local node
   * record (ENR) carries the correct {@code tcp}/{@code tcp6} values.
   *
   * @param config the full networking configuration
   * @param peerPermissions peer permissions to enforce on discovered peers
   * @param forkIdManager manager used to validate fork compatibility with peers
   * @param nodeRecordManager manager responsible for maintaining the local node record
   * @param rlpxAgent RLPx agent used to initiate outbound peer connections
   * @param metricsSystem metrics system for registering DiscV5 metrics
   * @param preferIpv6Outbound if true, prefer IPv6 when a peer advertises both address families
   * @param discoverySystemFactory factory for creating the {@link MutableDiscoverySystem}
   */
  PeerDiscoveryAgentV5(
      final NetworkingConfiguration config,
      final PeerPermissions peerPermissions,
      final ForkIdManager forkIdManager,
      final NodeRecordManager nodeRecordManager,
      final RlpxAgent rlpxAgent,
      final MetricsSystem metricsSystem,
      final boolean preferIpv6Outbound,
      final DiscoverySystemFactory discoverySystemFactory) {

    this.discoveryConfig =
        Objects.requireNonNull(config, "config must not be null").discoveryConfiguration();
    this.peerPermissions =
        Objects.requireNonNull(peerPermissions, "peerPermissions must not be null");
    this.forkIdManager = Objects.requireNonNull(forkIdManager, "forkIdManager must not be null");
    this.nodeRecordManager =
        Objects.requireNonNull(nodeRecordManager, "nodeRecordManager must not be null");
    this.rlpxAgent = Objects.requireNonNull(rlpxAgent, "rlpxAgent must not be null");
    this.metricsSystem = Objects.requireNonNull(metricsSystem, "metricsSystem must not be null");
    this.preferIpv6Outbound = preferIpv6Outbound;
    this.discoverySystemFactory =
        Objects.requireNonNull(discoverySystemFactory, "discoverySystemFactory must not be null");
  }

  /**
   * Starts the DiscV5 discovery system and the adaptive discovery loop.
   *
   * <p>The local node record (ENR) is initialized here using the supplied {@code tcpPort}, ensuring
   * the {@code tcp} and {@code tcp6} ENR fields reflect the actual RLPx listening port rather than
   * the discovery bind port.
   *
   * @param tcpPort the local RLPx TCP port used for inbound peer connections
   * @return a future completed with the UDP discovery port once discovery has started
   */
  @Override
  public CompletableFuture<Integer> start(final int tcpPort) {
    if (!isEnabled()) {
      LOG.debug("DiscV5 peer discovery is disabled; not starting agent");
      return CompletableFuture.completedFuture(0);
    }
    if (stopped.get()) {
      return CompletableFuture.failedFuture(
          new IllegalStateException(
              "Unable to start PeerDiscoveryAgentV5 after it has been stopped"));
    }
    if (!started.compareAndSet(false, true)) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("Unable to start an already started PeerDiscoveryAgentV5"));
    }
    LOG.info("Starting DiscV5 peer discovery agent ...");

    final MutableDiscoverySystem system;
    try {
      final NodeRecord localNodeRecord = initializeLocalNodeRecord(tcpPort);
      system = discoverySystemFactory.create(localNodeRecord, this::handleBoundPortResolved);
      discoverySystem.set(system);
      registerMetrics(system);
    } catch (final Exception e) {
      started.set(false);
      return CompletableFuture.failedFuture(e);
    }

    return system
        .start()
        .thenApply(
            v -> {
              try {
                if (!stopped.get()) {
                  scheduler.scheduleAtFixedRate(
                      this::discoveryTick,
                      0,
                      discoveryConfig.getDiscV5DiscoveryIntervalSeconds(),
                      TimeUnit.SECONDS);
                }
              } catch (final RejectedExecutionException e) {
                // Benign: stop() shut down the scheduler between the stopped check and the
                // schedule call. The agent is stopping so there is nothing to schedule.
                LOG.debug("Scheduler already shut down; skipping discovery tick scheduling", e);
              }
              if (stopped.get()) {
                throw new IllegalStateException(
                    "DiscV5 peer discovery agent was stopped during startup");
              }
              final NodeRecord startedNodeRecord = system.getLocalNodeRecord();
              // Return the IPv4 UDP port when available (single-stack IPv4 or dual-stack).
              // The caller uses this port for UPnP IPv4 port forwarding and for the local
              // enode URL, both of which are IPv4-only concerns. The fallback to udp6 covers
              // single-stack IPv6 mode where no IPv4 udp field exists in the ENR.
              final int localPort =
                  startedNodeRecord
                      .getUdpAddress()
                      .or(startedNodeRecord::getUdp6Address)
                      .orElseThrow(
                          () ->
                              new IllegalStateException(
                                  "Local ENR has neither udp nor udp6 address after start"))
                      .getPort();
              LOG.info("P2P DiscV5 peer discovery agent started and listening on {}", localPort);
              return localPort;
            })
        .whenComplete(
            (port, error) -> {
              if (error != null) {
                LOG.error("Failed to start DiscV5 peer discovery agent", error);
                started.set(false);
                try {
                  system.stop();
                } catch (final Exception e) {
                  LOG.debug("Error while stopping discovery system after failed start", e);
                }
                discoverySystem.set(null);
              }
            });
  }

  /**
   * Stops peer discovery, terminates scheduled tasks, and shuts down the discovery system.
   *
   * @return a completed future once shutdown has completed
   */
  @Override
  public CompletableFuture<?> stop() {
    LOG.info("Stopping DiscV5 Peer Discovery Agent");
    stopped.set(true);
    scheduler.shutdownNow();
    final MutableDiscoverySystem system = discoverySystem.getAndSet(null);
    if (system != null) {
      system.stop();
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Updates the local node record in the discovery system.
   *
   * <p>This method is typically called when network parameters change that affect the node record
   * (e.g., advertised IP address or ports).
   */
  @Override
  public void updateNodeRecord() {
    if (!isEnabled()) {
      return;
    }
    nodeRecordManager.updateNodeRecord();
  }

  /**
   * Checks whether a discovered peer is compatible with the local fork ID.
   *
   * @param peer the discovered peer
   * @return {@code true} if the peer is fork-compatible or does not advertise a fork ID
   */
  @Override
  public boolean checkForkId(final DiscoveryPeer peer) {
    return peer.getForkId().map(forkIdManager::peerCheck).orElse(true);
  }

  /**
   * Streams peers discovered by the DiscV5 discovery system.
   *
   * @return a stream of discovered peers
   */
  @Override
  public Stream<DiscoveryPeer> streamDiscoveredPeers() {
    final MutableDiscoverySystem system = discoverySystem.get();
    if (system == null) {
      return Stream.empty();
    }
    return system
        .streamLiveNodes()
        .map(nr -> DiscoveryPeerFactory.fromNodeRecord(nr, preferIpv6Outbound));
  }

  /**
   * Removes a peer from the discovery system.
   *
   * @param peerId the identifier of the peer to drop
   */
  @Override
  public void dropPeer(final PeerId peerId) {
    final MutableDiscoverySystem system = discoverySystem.get();
    if (system != null) {
      system.deleteNodeRecord(peerId.getId());
    }
  }

  /**
   * Indicates whether peer discovery is enabled via configuration.
   *
   * @return {@code true} if discovery is enabled
   */
  @Override
  public boolean isEnabled() {
    return discoveryConfig.isEnabled();
  }

  /**
   * Indicates whether the discovery agent has been stopped.
   *
   * @return {@code true} if the agent has been stopped
   */
  @Override
  public boolean isStopped() {
    return stopped.get();
  }

  /**
   * Adds a peer to the discovery system.
   *
   * @param peer the peer to add
   */
  @Override
  public void addPeer(final Peer peer) {
    final MutableDiscoverySystem system = discoverySystem.get();
    if (system != null) {
      peer.getNodeRecord().ifPresent(system::addNodeRecord);
    }
  }

  @Override
  public Optional<NodeRecord> getLocalNodeRecord() {
    return nodeRecordManager.getLocalNode().flatMap(DiscoveryPeer::getNodeRecord);
  }

  /**
   * Looks up a peer by its identifier.
   *
   * @param peerId the peer identifier
   * @return the peer if known to the discovery system
   */
  @Override
  public Optional<Peer> getPeer(final PeerId peerId) {
    final MutableDiscoverySystem system = discoverySystem.get();
    if (system == null) {
      return Optional.empty();
    }
    return system
        .lookupNode(peerId.getId())
        .map(nr -> DiscoveryPeerFactory.fromNodeRecord(nr, preferIpv6Outbound));
  }

  /**
   * Handles a {@code localNodeRecordListener} callback from the discovery library.
   *
   * <p>When {@code onBoundPortResolved} resolves ephemeral (port 0) UDP ports, this method
   * propagates the resolved ports to {@link NodeRecordManager}. {@code onDiscoveryPortResolved}
   * writes the ENR atomically once all configured endpoints are non-zero, so concurrent dual-stack
   * callbacks cannot race a write against an endpoint update.
   *
   * <p>{@code resolvedUdpPort} corresponds to the ENR {@code udp} field (IPv4 or IPv4-primary);
   * {@code resolvedUdp6Port} corresponds to the ENR {@code udp6} field (IPv6 primary or dual-stack
   * secondary). {@link NodeRecordManager} routes each to the correct endpoint based on the
   * primary's address family.
   */
  private void handleBoundPortResolved(final NodeRecord previous, final NodeRecord updated) {
    final Optional<Integer> resolvedUdpPort =
        extractResolvedPort(previous.getUdpAddress(), updated.getUdpAddress());
    final Optional<Integer> resolvedUdp6Port =
        extractResolvedPort(previous.getUdp6Address(), updated.getUdp6Address());
    if (resolvedUdpPort.isPresent() || resolvedUdp6Port.isPresent()) {
      nodeRecordManager.onDiscoveryPortResolved(resolvedUdpPort, resolvedUdp6Port);
    }
  }

  /**
   * Returns the resolved port if the address transitioned from unresolved (absent or port 0) to a
   * real port.
   */
  private static Optional<Integer> extractResolvedPort(
      final Optional<InetSocketAddress> previous, final Optional<InetSocketAddress> updated) {
    return isUnresolvedPort(previous)
        ? updated.filter(a -> a.getPort() != 0).map(InetSocketAddress::getPort)
        : Optional.empty();
  }

  /**
   * Returns {@code true} if the address is absent or bound to an ephemeral (port 0) port. Both
   * states are treated as unresolved.
   */
  private static boolean isUnresolvedPort(final Optional<InetSocketAddress> address) {
    return address.map(a -> a.getPort() == 0).orElse(true);
  }

  /** Determines whether the RLPx agent has reached a sufficient number of connected peers. */
  private boolean hasSufficientPeers() {
    return rlpxAgent.getConnectionCount()
        >= rlpxAgent.getMaxPeers() * discoveryConfig.getDiscV5MinimumPeerRatio();
  }

  /** Periodic discovery task that enforces adaptive cadence and triggers peer discovery. */
  private void discoveryTick() {
    if (stopped.get() || hasSufficientPeers()) {
      return;
    }
    discoverAndConnect();
  }

  /** Executes a DiscV5 peer search and attempts outbound connections to suitable peers. */
  private void discoverAndConnect() {
    if (!discoveryInProgress.compareAndSet(false, true)) {
      return;
    }
    final MutableDiscoverySystem system = discoverySystem.get();
    if (system == null) {
      discoveryInProgress.set(false);
      return;
    }
    system
        .searchForNewPeers()
        .orTimeout(discoveryConfig.getDiscV5DiscoveryTimeoutSeconds(), TimeUnit.SECONDS)
        .whenComplete(
            (nodeRecords, error) -> {
              try {
                if (error != null) {
                  LOG.warn("DiscV5 peer discovery failed", error);
                  return;
                }
                candidatePeers(nodeRecords).forEach(rlpxAgent::connect);
              } finally {
                discoveryInProgress.set(false);
              }
            });
  }

  /** Builds a stream of candidate peers suitable for outbound connection attempts. */
  private Stream<DiscoveryPeer> candidatePeers(final Collection<NodeRecord> newPeers) {
    if (LOG.isTraceEnabled() && !newPeers.isEmpty()) {
      LOG.trace("Discovered {} new peers", newPeers.size());
    }

    final MutableDiscoverySystem system = discoverySystem.get();
    if (system == null) {
      return Stream.empty();
    }

    final Optional<? extends DiscoveryPeer> maybeLocalNode = nodeRecordManager.getLocalNode();
    final Peer localNode = maybeLocalNode.orElse(null);
    final Bytes localNodeId =
        maybeLocalNode
            .flatMap(DiscoveryPeer::getNodeRecord)
            .map(NodeRecord::getNodeId)
            .orElse(Bytes.EMPTY);

    // Combine newly discovered peers with known peers and filter for suitability
    final Stream<NodeRecord> knownPeers = system.streamLiveNodes();
    final List<DiscoveryPeer> candidates =
        Stream.concat(newPeers.stream(), knownPeers)
            .distinct()
            // Defensive: exclude the local node record that streamLiveNodes may include.
            // The discovery library currently excludes it, but this is not an API guarantee.
            .filter(nr -> !nr.getNodeId().equals(localNodeId))
            .map(nr -> DiscoveryPeerFactory.fromNodeRecord(nr, preferIpv6Outbound))
            // Use isListening() instead of isReadyForConnections() because
            // DiscoveryPeerV4.isReadyForConnections() requires DiscV4 bonding status,
            // which is never set for DiscV5-discovered peers.
            .filter(DiscoveryPeer::isListening)
            .filter(peer -> peer.getForkId().map(forkIdManager::peerCheck).orElse(true))
            .filter(peer -> isPeerPermitted(localNode, peer))
            .toList();
    if (LOG.isTraceEnabled() && !candidates.isEmpty()) {
      LOG.trace("Total unique peers eligible for connection: {}", candidates.size());
    }
    return candidates.stream();
  }

  /**
   * Checks whether a discovered peer is permitted by the configured peer permissions.
   *
   * @param localNode the local node, or {@code null} if not yet initialized
   * @param remotePeer the remote peer to check
   * @return {@code true} if the peer is permitted
   */
  private boolean isPeerPermitted(final Peer localNode, final DiscoveryPeer remotePeer) {
    if (localNode == null) {
      // Local node not yet initialized — reject rather than bypass identity checks.
      // The peer will be re-discovered on the next FINDNODE round.
      return false;
    }
    final boolean permitted =
        peerPermissions.isPermitted(
            localNode, remotePeer, PeerPermissions.Action.DISCOVERY_ALLOW_IN_PEER_TABLE);
    if (!permitted) {
      LOG.trace("DiscV5: Peer {} rejected by peer permissions", remotePeer.getEnodeURL());
    }
    return permitted;
  }

  /**
   * Initializes the local node record with the correct RLPx TCP ports.
   *
   * <p>The {@code tcp} and {@code tcp6} ENR fields are set from the effective ports returned by
   * {@link RlpxAgent#start()} and {@link RlpxAgent#getIpv6ListeningPort()} respectively. These are
   * always the real OS-assigned ports, so ephemeral port configuration (port 0) is handled
   * correctly for TCP.
   *
   * <p>The {@code udp} and {@code udp6} fields are initially set from the configured discovery bind
   * ports. When port 0 is configured, the discovery library resolves the actual OS-assigned port
   * after bind via {@code onBoundPortResolved} and updates the ENR automatically.
   *
   * <p>The IPv6 {@link HostEndpoint} is only included when <em>both</em> an IPv6 advertised host is
   * configured <em>and</em> an IPv6 RLPx socket is actually bound. If discovery dual-stack is
   * active but RLPx dual-stack is not, the IPv6 ENR fields are omitted rather than advertising an
   * incorrect port.
   *
   * @param tcpPort the effective IPv4 RLPx TCP port returned by {@link RlpxAgent#start()}
   * @return the initialized local {@link NodeRecord}
   */
  private NodeRecord initializeLocalNodeRecord(final int tcpPort) {
    final Optional<Integer> ipv6TcpPort = rlpxAgent.getIpv6ListeningPort();

    // Include IPv6 ENR fields only when the discovery layer has an active IPv6 UDP socket
    // (isDualStackEnabled), an IPv6 host is advertised, and an IPv6 RLPx TCP socket was bound.
    // Omitting them when any of those conditions is absent avoids advertising inconsistent ENR
    // fields (e.g. an ip6/udp6 without a live UDP socket, or a tcp6 without a live TCP socket).
    final Optional<HostEndpoint> ipv6Endpoint =
        discoveryConfig.isDualStackEnabled()
            ? discoveryConfig
                .getAdvertisedHostIpv6()
                .flatMap(
                    host ->
                        ipv6TcpPort.map(
                            port ->
                                new HostEndpoint(host, discoveryConfig.getBindPortIpv6(), port)))
            : Optional.empty();

    nodeRecordManager.initializeLocalNode(
        new HostEndpoint(
            discoveryConfig.getAdvertisedHost(), discoveryConfig.getBindPort(), tcpPort),
        ipv6Endpoint);

    return nodeRecordManager
        .getLocalNode()
        .flatMap(DiscoveryPeer::getNodeRecord)
        .orElseThrow(() -> new IllegalStateException("Local node record not initialized"));
  }

  private void registerMetrics(final MutableDiscoverySystem system) {
    metricsSystem.createIntegerGauge(
        BesuMetricCategory.NETWORK,
        "discv5_live_nodes_current",
        "Current number of live nodes tracked by the DiscV5 discovery system",
        () -> system.getBucketStats().getTotalLiveNodeCount());
    metricsSystem.createIntegerGauge(
        BesuMetricCategory.NETWORK,
        "discv5_total_nodes_current",
        "Current number of total nodes tracked by the DiscV5 discovery system",
        () -> system.getBucketStats().getTotalNodeCount());
  }
}
