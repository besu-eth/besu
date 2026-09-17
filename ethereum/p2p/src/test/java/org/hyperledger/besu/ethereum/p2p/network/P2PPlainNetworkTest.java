/*
 * Copyright contributors to Hyperledger Besu.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.ethereum.p2p.EthProtocolHelper;
import org.hyperledger.besu.ethereum.p2p.config.DiscoveryConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.ImmutableNetworkingConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.NetworkingConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.RlpxConfiguration;
import org.hyperledger.besu.ethereum.p2p.network.exceptions.IncompatiblePeerException;
import org.hyperledger.besu.ethereum.p2p.peers.DefaultPeer;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissions;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissionsDenylist;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MockSubProtocol;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.PeerConnectionGatekeeper;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.SubProtocol;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;
import org.hyperledger.besu.plugin.data.EnodeURL;

import java.net.InetAddress;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.vertx.core.Vertx;
import org.apache.tuweni.bytes.Bytes;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class P2PPlainNetworkTest {
  // See ethereum/p2p/src/test/resources/keys/README.md for certificates setup.
  private final Vertx vertx = Vertx.vertx();
  private final NetworkingConfiguration config =
      ImmutableNetworkingConfiguration.builder()
          .discoveryConfiguration(DiscoveryConfiguration.create().setEnabled(false))
          .rlpxConfiguration(
              RlpxConfiguration.create()
                  .setBindPort(0)
                  .setSupportedProtocols(MockSubProtocol.create()))
          .build();

  @AfterEach
  public void closeVertx() {
    vertx.close();
  }

  @Test
  public void handshaking() throws Exception {
    final NodeKey nodeKey = NodeKeyUtils.generate();
    try (final P2PNetwork listener = createP2PNetwork(nodeKey);
        final P2PNetwork connector = createP2PNetwork()) {
      listener.getRlpxAgent().ifPresent(agent -> agent.setPeerConnectionGatekeeper(testCallback));
      connector.getRlpxAgent().ifPresent(agent -> agent.setPeerConnectionGatekeeper(testCallback));

      listener.start();
      connector.start();
      final EnodeURL listenerEnode = listener.getLocalEnode().get();
      final Bytes listenId = listenerEnode.getNodeId();
      final int listenPort = listenerEnode.getListeningPort().get();

      Assertions.assertThat(
              connector
                  .connect(createPeer(listenId, listenPort))
                  .get(30L, TimeUnit.SECONDS)
                  .getPeerInfo()
                  .getNodeId())
          .isEqualTo(listenId);
    }
  }

  @Test
  public void preventMultipleConnections() throws Exception {
    final NodeKey listenNodeKey = NodeKeyUtils.generate();
    try (final P2PNetwork listener = createP2PNetwork(listenNodeKey);
        final P2PNetwork connector = createP2PNetwork()) {
      listener.getRlpxAgent().ifPresent(agent -> agent.setPeerConnectionGatekeeper(testCallback));
      connector.getRlpxAgent().ifPresent(agent -> agent.setPeerConnectionGatekeeper(testCallback));

      listener.start();
      connector.start();
      final EnodeURL listenerEnode = listener.getLocalEnode().get();
      final Bytes listenId = listenerEnode.getNodeId();
      final int listenPort = listenerEnode.getListeningPort().get();

      final CompletableFuture<PeerConnection> firstFuture =
          connector.connect(createPeer(listenId, listenPort));
      final CompletableFuture<PeerConnection> secondFuture =
          connector.connect(createPeer(listenId, listenPort));

      final PeerConnection firstConnection = firstFuture.get(30L, TimeUnit.SECONDS);
      final PeerConnection secondConnection = secondFuture.get(30L, TimeUnit.SECONDS);
      Assertions.assertThat(firstConnection.getPeerInfo().getNodeId()).isEqualTo(listenId);

      // Connections should reference the same instance - i.e. we shouldn't create 2 distinct
      // connections
      assertThat(firstConnection == secondConnection).isTrue();
    }
  }

  /**
   * Tests that max peers setting is honoured and inbound connections that would exceed the limit
   * are correctly disconnected.
   *
   * @throws Exception On Failure
   */
  @Test
  public void limitMaxPeers() throws Exception {
    final NodeKey nodeKey = NodeKeyUtils.generate();
    final NetworkingConfiguration listenerConfig =
        ImmutableNetworkingConfiguration.builder()
            .discoveryConfiguration(DiscoveryConfiguration.create().setEnabled(false))
            .rlpxConfiguration(
                RlpxConfiguration.create()
                    .setBindPort(0)
                    .setSupportedProtocols(MockSubProtocol.create()))
            .build();
    try (final P2PNetwork listener = createP2PNetwork(listenerConfig, nodeKey);
        final P2PNetwork connector1 = createP2PNetwork();
        final P2PNetwork connector2 = createP2PNetwork()) {
      listener.getRlpxAgent().ifPresent(agent -> agent.setPeerConnectionGatekeeper(testCallback));
      connector1.getRlpxAgent().ifPresent(agent -> agent.setPeerConnectionGatekeeper(testCallback));
      connector2
          .getRlpxAgent()
          .ifPresent(
              agent ->
                  agent.setPeerConnectionGatekeeper(
                      (p, d) -> Optional.of(DisconnectReason.TOO_MANY_PEERS)));

      // Setup listener and first connection
      listener.start();
      connector1.start();
      final EnodeURL listenerEnode = listener.getLocalEnode().get();
      final Bytes listenId = listenerEnode.getNodeId();
      final int listenPort = listenerEnode.getListeningPort().get();

      final Peer listeningPeer = createPeer(listenId, listenPort);
      Assertions.assertThat(
              connector1
                  .connect(listeningPeer)
                  .get(30L, TimeUnit.SECONDS)
                  .getPeerInfo()
                  .getNodeId())
          .isEqualTo(listenId);

      // Setup second connection and check that connection is not accepted
      final CompletableFuture<PeerConnection> peerFuture = new CompletableFuture<>();
      final CompletableFuture<DisconnectReason> reasonFuture = new CompletableFuture<>();
      connector2.subscribeDisconnect(
          (peerConnection, reason, initiatedByPeer) -> {
            peerFuture.complete(peerConnection);
            reasonFuture.complete(reason);
          });
      connector2.start();
      Assertions.assertThat(connector2.connect(listeningPeer)).isCompletedExceptionally();
    }
  }

  @Test
  public void rejectPeerWithNoSharedCaps() throws Exception {
    final NodeKey listenerNodeKey = NodeKeyUtils.generate();
    final NodeKey connectorNodeKey = NodeKeyUtils.generate();

    final SubProtocol subprotocol1 = MockSubProtocol.create("eth");
    final Capability cap1 = Capability.create(subprotocol1.getName(), 68);
    final SubProtocol subprotocol2 = MockSubProtocol.create("oth");
    final Capability cap2 = Capability.create(subprotocol2.getName(), 68);
    try (final P2PNetwork listener = createP2PNetwork(listenerNodeKey, List.of(cap1));
        final P2PNetwork connector = createP2PNetwork(connectorNodeKey, List.of(cap2))) {
      listener.getRlpxAgent().ifPresent(agent -> agent.setPeerConnectionGatekeeper(testCallback));
      connector.getRlpxAgent().ifPresent(agent -> agent.setPeerConnectionGatekeeper(testCallback));

      listener.start();
      connector.start();
      final EnodeURL listenerEnode = listener.getLocalEnode().get();
      final Bytes listenId = listenerEnode.getNodeId();
      final int listenPort = listenerEnode.getListeningPort().get();

      final Peer listenerPeer = createPeer(listenId, listenPort);
      final CompletableFuture<PeerConnection> connectFuture = connector.connect(listenerPeer);
      assertThatThrownBy(connectFuture::get).hasCauseInstanceOf(IncompatiblePeerException.class);
    }
  }

  @Test
  public void rejectIncomingConnectionFromDenylistedPeer() throws Exception {
    final PeerPermissionsDenylist localDenylist = PeerPermissionsDenylist.create();

    try (final P2PNetwork localNetwork = createP2PNetwork(localDenylist);
        final P2PNetwork remoteNetwork = createP2PNetwork()) {
      localNetwork
          .getRlpxAgent()
          .ifPresent(agent -> agent.setPeerConnectionGatekeeper(testCallback));
      remoteNetwork
          .getRlpxAgent()
          .ifPresent(agent -> agent.setPeerConnectionGatekeeper(testCallback));

      localNetwork.start();
      remoteNetwork.start();

      final EnodeURL localEnode = localNetwork.getLocalEnode().get();
      final Bytes localId = localEnode.getNodeId();
      final int localPort = localEnode.getListeningPort().get();

      final EnodeURL remoteEnode = remoteNetwork.getLocalEnode().get();
      final Bytes remoteId = remoteEnode.getNodeId();
      final int remotePort = remoteEnode.getListeningPort().get();

      final Peer localPeer = createPeer(localId, localPort);
      final Peer remotePeer = createPeer(remoteId, remotePort);

      // Denylist the remote peer
      localDenylist.add(remotePeer);

      // Setup disconnect listener
      final CompletableFuture<PeerConnection> peerFuture = new CompletableFuture<>();
      final CompletableFuture<DisconnectReason> reasonFuture = new CompletableFuture<>();
      remoteNetwork.subscribeDisconnect(
          (peerConnection, reason, initiatedByPeer) -> {
            peerFuture.complete(peerConnection);
            reasonFuture.complete(reason);
          });

      // Remote connect to local
      final CompletableFuture<PeerConnection> connectFuture = remoteNetwork.connect(localPeer);

      // Check connection is made, and then a disconnect is registered at remote
      Assertions.assertThat(connectFuture.get(5L, TimeUnit.SECONDS).getPeerInfo().getNodeId())
          .isEqualTo(localId);
      Assertions.assertThat(peerFuture.get(30L, TimeUnit.SECONDS).getPeerInfo().getNodeId())
          .isEqualTo(localId);
      assertThat(reasonFuture.get(30L, TimeUnit.SECONDS))
          .isEqualByComparingTo(DisconnectReason.UNKNOWN);
    }
  }

  @Test
  public void rejectIncomingConnectionFromDisallowedPeer() throws Exception {
    final PeerPermissions peerPermissions = mock(PeerPermissions.class);
    when(peerPermissions.isPermitted(any(), any(), any())).thenReturn(true);

    try (final P2PNetwork localNetwork = createP2PNetwork(peerPermissions);
        final P2PNetwork remoteNetwork = createP2PNetwork()) {
      localNetwork
          .getRlpxAgent()
          .ifPresent(agent -> agent.setPeerConnectionGatekeeper(testCallback));
      remoteNetwork
          .getRlpxAgent()
          .ifPresent(agent -> agent.setPeerConnectionGatekeeper(testCallback));

      localNetwork.start();
      remoteNetwork.start();

      final EnodeURLImpl localEnode = localNetwork.getLocalEnode().get();
      final Peer localPeer = DefaultPeer.fromEnodeURL(localEnode);
      final Peer remotePeer = DefaultPeer.fromEnodeURL(remoteNetwork.getLocalEnode().get());

      // Deny incoming connection permissions for remotePeer
      when(peerPermissions.isPermitted(
              eq(localPeer),
              eq(remotePeer),
              eq(PeerPermissions.Action.RLPX_ALLOW_NEW_INBOUND_CONNECTION)))
          .thenReturn(false);

      // Setup disconnect listener
      final CompletableFuture<PeerConnection> peerFuture = new CompletableFuture<>();
      final CompletableFuture<DisconnectReason> reasonFuture = new CompletableFuture<>();
      remoteNetwork.subscribeDisconnect(
          (peerConnection, reason, initiatedByPeer) -> {
            peerFuture.complete(peerConnection);
            reasonFuture.complete(reason);
          });

      // Remote connect to local
      final CompletableFuture<PeerConnection> connectFuture = remoteNetwork.connect(localPeer);

      // Check connection is made, and then a disconnect is registered at remote
      final Bytes localId = localEnode.getNodeId();
      Assertions.assertThat(connectFuture.get(5L, TimeUnit.SECONDS).getPeerInfo().getNodeId())
          .isEqualTo(localId);
      Assertions.assertThat(peerFuture.get(30L, TimeUnit.SECONDS).getPeerInfo().getNodeId())
          .isEqualTo(localId);
      assertThat(reasonFuture.get(30L, TimeUnit.SECONDS))
          .isEqualByComparingTo(DisconnectReason.UNKNOWN);
    }
  }

  private final PeerConnectionGatekeeper testCallback = (p, d) -> Optional.empty();

  private Peer createPeer(final Bytes nodeId, final int listenPort) {
    return DefaultPeer.fromEnodeURL(
        EnodeURLImpl.builder()
            .ipAddress(InetAddress.getLoopbackAddress().getHostAddress())
            .nodeId(nodeId)
            .discoveryAndListeningPorts(listenPort)
            .build());
  }

  private P2PNetwork createP2PNetwork() {
    return DefaultP2PNetworkTestBuilder.builder(config, vertx, NodeKeyUtils.generate()).build();
  }

  private P2PNetwork createP2PNetwork(final NetworkingConfiguration config, final NodeKey nodeKey) {
    return DefaultP2PNetworkTestBuilder.builder(config, vertx, nodeKey).build();
  }

  private P2PNetwork createP2PNetwork(final NodeKey nodeKey) {
    return DefaultP2PNetworkTestBuilder.builder(config, vertx, nodeKey).build();
  }

  private P2PNetwork createP2PNetwork(
      final NodeKey nodeKey, final List<Capability> supportedCapabilities) {
    return DefaultP2PNetworkTestBuilder.builder(
            config, vertx, nodeKey, PeerPermissions.noop(), supportedCapabilities)
        .build();
  }

  private P2PNetwork createP2PNetwork(final PeerPermissions peerPermissions) {
    return DefaultP2PNetworkTestBuilder.builder(
            config,
            vertx,
            NodeKeyUtils.generate(),
            peerPermissions,
            List.of(EthProtocolHelper.LATEST))
        .build();
  }
}
