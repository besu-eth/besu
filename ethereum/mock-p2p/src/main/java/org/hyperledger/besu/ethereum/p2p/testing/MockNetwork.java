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
package org.hyperledger.besu.ethereum.p2p.testing;

import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.DiscoveryPeerV4;
import org.hyperledger.besu.ethereum.p2p.network.P2PNetwork;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.rlpx.ConnectCallback;
import org.hyperledger.besu.ethereum.p2p.rlpx.DisconnectCallback;
import org.hyperledger.besu.ethereum.p2p.rlpx.MessageCallback;
import org.hyperledger.besu.ethereum.p2p.rlpx.RlpxAgent;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.DefaultMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Message;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.PeerInfo;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;
import org.hyperledger.besu.util.Subscribers;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Mock network implementation that allows passing {@link MessageData} between arbitrary peers. This
 * completely bypasses the TCP layer by directly passing {@link MessageData} from {@link
 * MockNetwork.MockPeerConnection#send(Capability, MessageData)} to callbacks registered on {@link
 * MockNetwork.MockP2PNetwork}s.
 */
public final class MockNetwork {

  private final Map<Peer, MockNetwork.MockP2PNetwork> nodes = new HashMap<>();
  private final List<Capability> capabilities;

  /**
   * Constructs a new MockNetwork with the specified capabilities.
   *
   * @param capabilities a list of capabilities that the mock network should have.
   */
  public MockNetwork(final List<Capability> capabilities) {
    this.capabilities = capabilities;
  }

  /**
   * Get the {@link P2PNetwork} that assumes a given {@link Peer} as the local node. This does not
   * connect {@link Peer} to any other peer. Any connections established by {@link #connect(Peer,
   * Peer)} require that both participating {@link Peer} have previously been passed to this method.
   *
   * @param peer Peer to get {@link P2PNetwork} for
   * @return P2PNetwork as seen by {@link Peer}
   */
  public P2PNetwork setup(final Peer peer) {
    synchronized (this) {
      return nodes.computeIfAbsent(peer, p -> new MockNetwork.MockP2PNetwork(peer, this));
    }
  }

  private PeerConnection connect(final Peer source, final Peer target) {
    synchronized (this) {
      final MockNetwork.MockPeerConnection establishedConnection =
          new MockNetwork.MockPeerConnection(source, target, this);
      final MockP2PNetwork sourceNode = nodes.get(source);
      final MockP2PNetwork targetNode = nodes.get(target);
      sourceNode.connections.put(target, establishedConnection);
      final MockNetwork.MockPeerConnection backChannel =
          new MockNetwork.MockPeerConnection(target, source, this);
      targetNode.connections.put(source, backChannel);
      sourceNode.connectCallbacks.forEach(c -> c.onConnect(establishedConnection));
      targetNode.connectCallbacks.forEach(c -> c.onConnect(backChannel));
      return establishedConnection;
    }
  }

  private void disconnect(
      final MockNetwork.MockPeerConnection connection, final DisconnectReason reason) {
    synchronized (this) {
      final MockP2PNetwork sourceNode = nodes.get(connection.from);
      final MockP2PNetwork targetNode = nodes.get(connection.to);
      if (targetNode.connections.remove(connection.from) == null
          || sourceNode.connections.remove(connection.to) == null) {
        throw new IllegalStateException(
            String.format("No connection between %s and %s", connection.from, connection.to));
      }
      targetNode.disconnectCallbacks.forEach(c -> c.onDisconnect(connection, reason, true));
      sourceNode.disconnectCallbacks.forEach(
          c -> c.onDisconnect(connection, DisconnectReason.REQUESTED, false));
    }
  }

  private static final class MockP2PNetwork implements P2PNetwork {

    private final MockNetwork network;

    private final Map<Peer, MockNetwork.MockPeerConnection> connections = new HashMap<>();

    private final Peer self;

    private final Map<Capability, Subscribers<MessageCallback>> protocolCallbacks =
        new ConcurrentHashMap<>();

    private final Subscribers<ConnectCallback> connectCallbacks = Subscribers.create();

    private final Subscribers<DisconnectCallback> disconnectCallbacks = Subscribers.create();

    MockP2PNetwork(final Peer self, final MockNetwork network) {
      this.self = self;
      this.network = network;
    }

    @Override
    public Collection<PeerConnection> getPeers() {
      synchronized (network) {
        return new ArrayList<>(connections.values());
      }
    }

    @Override
    public Stream<DiscoveryPeerV4> streamDiscoveredPeers() {
      return Stream.empty();
    }

    @Override
    public CompletableFuture<PeerConnection> connect(final Peer peer) {
      synchronized (network) {
        if (network.nodes.containsKey(peer)) {
          final PeerConnection connection = connections.get(peer);
          if (connection == null) {
            return CompletableFuture.completedFuture(network.connect(self, peer));
          } else {
            return CompletableFuture.completedFuture(connection);
          }
        } else {
          return CompletableFuture.supplyAsync(
              () -> {
                throw new IllegalStateException(
                    String.format("Tried to connect to unknown peer %s", peer));
              });
        }
      }
    }

    @Override
    public void subscribe(final Capability capability, final MessageCallback callback) {
      protocolCallbacks
          .computeIfAbsent(capability, key -> Subscribers.create())
          .subscribe(callback);
    }

    @Override
    public void subscribeConnect(final ConnectCallback callback) {
      connectCallbacks.subscribe(callback);
    }

    @Override
    public void subscribeDisconnect(final DisconnectCallback callback) {
      disconnectCallbacks.subscribe(callback);
    }

    @Override
    public boolean addMaintainedConnectionPeer(final Peer peer) {
      return true;
    }

    @Override
    public boolean removeMaintainedConnectionPeer(final Peer peer) {
      return true;
    }

    @Override
    public Collection<Peer> getMaintainedConnectionPeers() {
      return List.of();
    }

    @Override
    public void stop() {}

    @Override
    public void awaitStop() {}

    @Override
    public void start() {}

    @Override
    public void close() {}

    @Override
    public boolean isListening() {
      return true;
    }

    @Override
    public boolean isP2pEnabled() {
      return true;
    }

    @Override
    public boolean isDiscoveryEnabled() {
      return true;
    }

    @Override
    public boolean isStopped() {
      return true;
    }

    @Override
    public Optional<EnodeURLImpl> getLocalEnode() {
      return Optional.empty();
    }

    @Override
    public void updateNodeRecord() {}

    @Override
    public Optional<RlpxAgent> getRlpxAgent() {
      return Optional.empty();
    }
  }

  /**
   * A mock connection between two peers that simply invokes the callbacks on the other side's
   * {@link MockNetwork.MockP2PNetwork}.
   */
  private final class MockPeerConnection implements PeerConnection {

    /** {@link Peer} that this connection originates from. */
    private final Peer from;

    private boolean disconnected = false;

    /**
     * Peer that this connection targets and that will receive {@link Message}s sent via {@link
     * #send(Capability, MessageData)}.
     */
    private final Peer to;

    private final MockNetwork network;
    private boolean statusSent;
    private boolean statusReceived;

    MockPeerConnection(final Peer source, final Peer target, final MockNetwork network) {
      from = source;
      to = target;
      this.network = network;
    }

    @Override
    public void send(final Capability capability, final MessageData message)
        throws PeerNotConnected {
      synchronized (network) {
        final MockNetwork.MockP2PNetwork target = network.nodes.get(to);
        final MockNetwork.MockPeerConnection backChannel = target.connections.get(from);
        if (backChannel != null) {
          final Message msg = new DefaultMessage(backChannel, message);
          final Subscribers<MessageCallback> callbacks = target.protocolCallbacks.get(capability);
          if (callbacks != null) {
            callbacks.forEach(c -> c.onMessage(capability, msg));
          }
        } else {
          throw new PeerNotConnected(String.format("%s not connected to %s", to, from));
        }
      }
    }

    @Override
    public Set<Capability> getAgreedCapabilities() {
      return new HashSet<>(capabilities);
    }

    @Override
    public Peer getPeer() {
      return to;
    }

    @Override
    public PeerInfo getPeerInfo() {
      return new PeerInfo(
          5,
          "mock-network-client",
          capabilities,
          to.getEnodeURL().getListeningPortOrZero(),
          to.getId());
    }

    @Override
    public void terminateConnection(final DisconnectReason reason, final boolean peerInitiated) {
      disconnected = true;
      network.disconnect(this, reason);
    }

    @Override
    public void disconnect(final DisconnectReason reason) {
      disconnected = true;
      network.disconnect(this, reason);
    }

    @Override
    public boolean isDisconnected() {
      return disconnected;
    }

    @Override
    public InetSocketAddress getLocalAddress() {
      throw new UnsupportedOperationException();
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getInitiatedAt() {
      return 0;
    }

    @Override
    public boolean inboundInitiated() {
      return false;
    }

    @Override
    public void setStatusSent() {
      this.statusSent = true;
    }

    @Override
    public void setStatusReceived() {
      this.statusReceived = true;
    }

    @Override
    public boolean getStatusExchanged() {
      return statusSent && statusReceived;
    }
  }
}
