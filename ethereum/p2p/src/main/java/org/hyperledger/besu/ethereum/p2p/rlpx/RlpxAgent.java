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
package org.hyperledger.besu.ethereum.p2p.rlpx;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.ethereum.p2p.config.RlpxConfiguration;
import org.hyperledger.besu.ethereum.p2p.discovery.DiscoveryPeer;
import org.hyperledger.besu.ethereum.p2p.peers.LocalNode;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.peers.PeerPrivileges;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissions;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.ConnectionInitializer;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnectionEvents;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerLookup;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerRlpxPermissions;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.netty.NettyConnectionInitializer;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.PeerConnectionGatekeeper;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;
import org.hyperledger.besu.plugin.data.EnodeURL;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.util.Subscribers;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import jakarta.validation.constraints.NotNull;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RlpxAgent {
  private static final Logger LOG = LoggerFactory.getLogger(RlpxAgent.class);

  private final LocalNode localNode;
  private final PeerConnectionEvents connectionEvents;
  private final ConnectionInitializer connectionInitializer;
  private final Subscribers<ConnectCallback> connectSubscribers = Subscribers.create();
  private volatile PeerConnectionGatekeeper peerConnectionGatekeeper;
  private final PeerRlpxPermissions peerPermissions;
  private final PeerPrivileges peerPrivileges;
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private volatile ConnectionInitializer.ListeningAddresses listeningAddresses;
  private final int maxPeers;
  private final Supplier<Stream<PeerConnection>> allConnectionsSupplier;
  private final Supplier<Stream<PeerConnection>> allActiveConnectionsSupplier;
  private final Cache<Bytes, CompletableFuture<PeerConnection>> peersConnectingCache =
      CacheBuilder.newBuilder()
          .expireAfterWrite(
              Duration.ofSeconds(30L)) // we will at most try to connect every 30 seconds
          .concurrencyLevel(1)
          .build();

  private RlpxAgent(
      final LocalNode localNode,
      final PeerConnectionEvents connectionEvents,
      final ConnectionInitializer connectionInitializer,
      final PeerRlpxPermissions peerPermissions,
      final PeerPrivileges peerPrivileges,
      final int maxPeers,
      final Supplier<Stream<PeerConnection>> allConnectionsSupplier,
      final Supplier<Stream<PeerConnection>> allActiveConnectionsSupplier) {
    this.localNode = localNode;
    this.connectionEvents = connectionEvents;
    this.connectionInitializer = connectionInitializer;
    this.peerPermissions = peerPermissions;
    this.peerPrivileges = peerPrivileges;
    this.maxPeers = maxPeers;
    this.allConnectionsSupplier = allConnectionsSupplier;
    this.allActiveConnectionsSupplier = allActiveConnectionsSupplier;
  }

  public static Builder builder() {
    return new Builder();
  }

  public CompletableFuture<Integer> start() {
    if (!started.compareAndSet(false, true)) {
      return CompletableFuture.failedFuture(
          new IllegalStateException(
              "Unable to start an already started " + getClass().getSimpleName()));
    }

    setupListeners();
    return connectionInitializer
        .start()
        .thenApply(
            (addresses) -> {
              this.listeningAddresses = addresses;
              LOG.info("P2P RLPx agent started and listening on {}.", addresses.ipv4Address());
              addresses
                  .ipv6Address()
                  .ifPresent(ipv6 -> LOG.info("P2P RLPx agent also listening on IPv6: {}", ipv6));
              return addresses.ipv4Address().getPort();
            })
        .whenComplete(
            (res, err) -> {
              if (err != null) {
                // the detail of this error is already logged by the completeExceptionally() call in
                // NettyConnectionInitializer
                LOG.error("Failed to start P2P RLPx agent. Check for port conflicts.");
              }
            });
  }

  /**
   * Returns the local IPv6 listening port after startup, if dual-stack is active.
   *
   * <p>Only valid after {@link #start()} has completed.
   *
   * @return the bound IPv6 port, or empty if no IPv6 socket was bound
   */
  public Optional<Integer> getIpv6ListeningPort() {
    if (listeningAddresses == null) {
      return Optional.empty();
    }
    return listeningAddresses.ipv6Address().map(InetSocketAddress::getPort);
  }

  public CompletableFuture<Void> stop() {
    if (!started.get() || !stopped.compareAndSet(false, true)) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("Illegal attempt to stop " + getClass().getSimpleName()));
    }

    streamConnections().forEach((conn) -> conn.disconnect(DisconnectReason.CLIENT_QUITTING));
    return connectionInitializer.stop();
  }

  public Stream<PeerConnection> streamConnections() {
    try {
      return allConnectionsSupplier.get();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Stream<PeerConnection> streamActiveConnections() {
    try {
      return allActiveConnectionsSupplier.get();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  public int getConnectionCount() {
    try {
      return (int) allActiveConnectionsSupplier.get().count();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void disconnect(final Bytes peerId, final DisconnectReason reason) {
    try {
      allActiveConnectionsSupplier
          .get()
          .filter(c -> c.getPeer().getId().equals(peerId))
          .forEach(c -> c.disconnect(reason));
      final CompletableFuture<PeerConnection> peerConnectionCompletableFuture =
          getMapOfCompletableFutures().get(peerId);
      if (peerConnectionCompletableFuture != null) {
        if (!peerConnectionCompletableFuture.isDone()) {
          peerConnectionCompletableFuture.cancel(true);
        } else if (!peerConnectionCompletableFuture.isCompletedExceptionally()
            && !peerConnectionCompletableFuture.isCancelled()) {
          peerConnectionCompletableFuture.get().disconnect(reason);
        }
      }
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Connect to the peer
   *
   * @param peer The peer to connect to
   * @return A future that will resolve to the existing or newly-established connection with this
   *     peer.
   */
  public CompletableFuture<PeerConnection> connect(final Peer peer) {
    // Check if we're ready to establish connections
    if (!localNode.isReady()) {
      return CompletableFuture.failedFuture(
          new IllegalStateException(
              "Cannot connect before "
                  + this.getClass().getSimpleName()
                  + " has finished starting"));
    }

    // Check peer is valid
    final EnodeURL enode = peer.getEnodeURL();
    if (!enode.isListening()) {
      final String errorMsg = "Attempt to connect to peer with no listening port: " + enode;
      LOG.warn(errorMsg);
      return CompletableFuture.failedFuture((new IllegalArgumentException(errorMsg)));
    }

    // Check permissions
    if (!peerPermissions.allowNewOutboundConnectionTo(peer)) {
      return CompletableFuture.failedFuture(peerPermissions.newOutboundConnectionException(peer));
    }

    final CompletableFuture<PeerConnection> peerConnectionCompletableFuture;
    final Optional<DisconnectReason> maybeDisconnectReason = gatePeerConnection(peer, false);

    if (maybeDisconnectReason.isEmpty()) {
      try {
        synchronized (this) {
          peerConnectionCompletableFuture =
              peersConnectingCache.get(
                  peer.getId(), () -> createPeerConnectionCompletableFuture(peer));
        }
      } catch (final ExecutionException e) {
        throw new RuntimeException(e);
      }
    } else {
      final String errorMsg =
          "None of the ProtocolManagers wants to connect to peer " + peer.getId();
      LOG.trace(errorMsg);
      return CompletableFuture.failedFuture((new RuntimeException(errorMsg)));
    }

    return peerConnectionCompletableFuture;
  }

  @NotNull
  private CompletableFuture<PeerConnection> createPeerConnectionCompletableFuture(final Peer peer) {
    final CompletableFuture<PeerConnection> peerConnectionCompletableFuture =
        initiateOutboundConnection(peer);
    peerConnectionCompletableFuture.whenComplete(
        (peerConnection, throwable) -> {
          if (throwable == null) {
            dispatchConnect(peerConnection);
          }
        });
    return peerConnectionCompletableFuture;
  }

  private Optional<DisconnectReason> gatePeerConnection(final Peer peer, final boolean incoming) {
    return peerConnectionGatekeeper != null
        ? peerConnectionGatekeeper.checkPeerConnection(peer, incoming)
        : Optional.empty();
  }

  private void setupListeners() {
    connectionInitializer.subscribeIncomingConnect(this::handleIncomingConnection);
    peerPermissions.subscribeUpdate(this::handlePermissionsUpdate);
  }

  private void handlePermissionsUpdate(
      final boolean permissionsRestricted, final Optional<List<Peer>> peers) {
    if (!permissionsRestricted) {
      // Nothing to do
      return;
    }

    final Stream<PeerConnection> connectionsToCheck;
    if (peers.isPresent()) {
      final List<Bytes> changedPeersIds =
          peers.get().stream().map(p -> p.getId()).collect(Collectors.toList());
      connectionsToCheck =
          streamConnections().filter(c -> changedPeersIds.contains(c.getPeer().getId()));
    } else {
      connectionsToCheck = streamConnections();
    }

    connectionsToCheck.forEach(
        connection -> {
          if (!peerPermissions.allowOngoingConnection(
              connection.getPeer(), connection.inboundInitiated())) {
            LOG.debug(
                "Disconnecting from peer that is not permitted to maintain ongoing connection: {}",
                connection);
            connection.disconnect(DisconnectReason.REQUESTED);
          }
        });
  }

  private CompletableFuture<PeerConnection> initiateOutboundConnection(final Peer peer) {
    LOG.trace("Initiating connection to peer: {}", peer.getEnodeURL());
    if (peer instanceof DiscoveryPeer) {
      ((DiscoveryPeer) peer).setLastAttemptedConnection(System.currentTimeMillis());
    }

    return connectionInitializer
        .connect(peer)
        .whenComplete(
            (conn, err) -> {
              if (err != null) {
                LOG.debug("Failed to connect to peer {}: {}", peer.getId(), err);
              } else {
                LOG.debug("Outbound connection established to peer: {}", peer.getId());
              }
            });
  }

  public boolean canExceedConnectionLimits(final Bytes peerId) {
    return peerPrivileges.canExceedConnectionLimits(peerId);
  }

  private void handleIncomingConnection(final PeerConnection peerConnection) {
    final Peer peer = peerConnection.getPeer();
    // Deny connection if our local node isn't ready
    if (!localNode.isReady()) {
      LOG.debug("Node is not ready. Disconnect incoming connection: {}", peerConnection);
      peerConnection.disconnect(DisconnectReason.UNKNOWN);
      return;
    }

    // Disconnect if not permitted
    if (!peerPermissions.allowNewInboundConnectionFrom(peer)) {
      LOG.debug(
          "Node is not permitted to connect. Disconnect incoming connection: {}", peerConnection);
      peerConnection.disconnect(DisconnectReason.UNKNOWN);
      return;
    }

    final Optional<DisconnectReason> maybeDisconnectReason = gatePeerConnection(peer, true);
    if (maybeDisconnectReason.isEmpty()) {
      dispatchConnect(peerConnection);
    } else {
      peerConnection.disconnect(maybeDisconnectReason.get());
    }
  }

  public void subscribeMessage(final Capability capability, final MessageCallback callback) {
    connectionEvents.subscribeMessage(capability, callback);
  }

  public void subscribeConnect(final ConnectCallback callback) {
    connectSubscribers.subscribe(callback);
  }

  public void setPeerConnectionGatekeeper(final PeerConnectionGatekeeper gatekeeper) {
    this.peerConnectionGatekeeper = gatekeeper;
  }

  public void subscribeDisconnect(final DisconnectCallback callback) {
    connectionEvents.subscribeDisconnect(callback);
  }

  private void dispatchConnect(final PeerConnection connection) {
    connectSubscribers.forEach(c -> c.onConnect(connection));
  }

  @VisibleForTesting
  public ConcurrentMap<Bytes, CompletableFuture<PeerConnection>> getMapOfCompletableFutures() {
    return peersConnectingCache.asMap();
  }

  public int getMaxPeers() {
    return maxPeers;
  }

  public static class Builder {
    private NodeKey nodeKey;
    private LocalNode localNode;
    private RlpxConfiguration config;
    private PeerPrivileges peerPrivileges;
    private PeerPermissions peerPermissions;
    private ConnectionInitializer connectionInitializer;
    private PeerConnectionEvents connectionEvents;
    private MetricsSystem metricsSystem;
    private Supplier<Stream<PeerConnection>> allConnectionsSupplier;
    private Supplier<Stream<PeerConnection>> allActiveConnectionsSupplier;
    private int maxPeers;
    private PeerLookup peerLookup;

    private Builder() {}

    public RlpxAgent build() {
      validate();

      if (connectionEvents == null) {
        connectionEvents = new PeerConnectionEvents(metricsSystem);
      }
      if (connectionInitializer == null) {
        LOG.debug("Using default NettyConnectionInitializer");
        connectionInitializer =
            new NettyConnectionInitializer(
                nodeKey, config, localNode, connectionEvents, metricsSystem, peerLookup);
      }

      final PeerRlpxPermissions rlpxPermissions =
          new PeerRlpxPermissions(localNode, peerPermissions);
      return new RlpxAgent(
          localNode,
          connectionEvents,
          connectionInitializer,
          rlpxPermissions,
          peerPrivileges,
          maxPeers,
          allConnectionsSupplier,
          allActiveConnectionsSupplier);
    }

    private void validate() {
      checkState(nodeKey != null, "NodeKey must be configured");
      checkState(localNode != null, "LocalNode must be configured");
      checkState(config != null, "RlpxConfiguration must be set");
      checkState(peerPrivileges != null, "PeerPrivileges must be configured");
      checkState(peerPermissions != null, "PeerPermissions must be configured");
      checkState(metricsSystem != null, "MetricsSystem must be configured");
    }

    public Builder nodeKey(final NodeKey nodeKey) {
      checkNotNull(nodeKey);
      this.nodeKey = nodeKey;
      return this;
    }

    public Builder localNode(final LocalNode localNode) {
      checkNotNull(localNode);
      this.localNode = localNode;
      return this;
    }

    public Builder connectionInitializer(final ConnectionInitializer connectionInitializer) {
      checkNotNull(connectionInitializer);
      this.connectionInitializer = connectionInitializer;
      return this;
    }

    public Builder config(final RlpxConfiguration config) {
      checkNotNull(config);
      this.config = config;
      return this;
    }

    public Builder peerPrivileges(final PeerPrivileges peerPrivileges) {
      checkNotNull(peerPrivileges);
      this.peerPrivileges = peerPrivileges;
      return this;
    }

    public Builder peerPermissions(final PeerPermissions peerPermissions) {
      checkNotNull(peerPermissions);
      this.peerPermissions = peerPermissions;
      return this;
    }

    public Builder connectionEvents(final PeerConnectionEvents connectionEvents) {
      checkNotNull(connectionEvents);
      this.connectionEvents = connectionEvents;
      return this;
    }

    public Builder metricsSystem(final MetricsSystem metricsSystem) {
      checkNotNull(metricsSystem);
      this.metricsSystem = metricsSystem;
      return this;
    }

    public Builder allConnectionsSupplier(
        final Supplier<Stream<PeerConnection>> allConnectionsSupplier) {
      this.allConnectionsSupplier = allConnectionsSupplier;
      return this;
    }

    public Builder allActiveConnectionsSupplier(
        final Supplier<Stream<PeerConnection>> allActiveConnectionsSupplier) {
      this.allActiveConnectionsSupplier = allActiveConnectionsSupplier;
      return this;
    }

    public Builder maxPeers(final int maxPeers) {
      this.maxPeers = maxPeers;
      return this;
    }

    public Builder peerLookup(final PeerLookup peerLookup) {
      this.peerLookup = peerLookup;
      return this;
    }
  }
}
