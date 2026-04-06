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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.p2p.peers.PeerTestHelper.createMutableLocalNode;
import static org.hyperledger.besu.ethereum.p2p.peers.PeerTestHelper.createPeer;
import static org.hyperledger.besu.ethereum.p2p.peers.PeerTestHelper.enode;
import static org.hyperledger.besu.ethereum.p2p.peers.PeerTestHelper.enodeBuilder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.ethereum.p2p.config.RlpxConfiguration;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.DiscoveryPeerV4;
import org.hyperledger.besu.ethereum.p2p.peers.DefaultPeer;
import org.hyperledger.besu.ethereum.p2p.peers.MutableLocalNode;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.peers.PeerPrivileges;
import org.hyperledger.besu.ethereum.p2p.peers.PeerTestHelper;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissions;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissionsException;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.MockConnectionInitializer;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.MockPeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnectionEvents;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.PingMessage;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RlpxAgentTest {
  private static final KeyPair KEY_PAIR = SignatureAlgorithmFactory.getInstance().generateKeyPair();
  private final RlpxConfiguration config = RlpxConfiguration.create();
  private final TestPeerPermissions peerPermissions = spy(new TestPeerPermissions());
  private final PeerPrivileges peerPrivileges = mock(PeerPrivileges.class);
  private final MutableLocalNode localNode = createMutableLocalNode();
  private final MetricsSystem metrics = new NoOpMetricsSystem();
  private final PeerConnectionEvents peerConnectionEvents = new PeerConnectionEvents(metrics);
  private final MockConnectionInitializer connectionInitializer =
      new MockConnectionInitializer(peerConnectionEvents);
  private List<PeerConnection> connections = Collections.emptyList();
  private final Supplier<Stream<PeerConnection>> allConnectionsSupplier = this::streamConnections;
  private final Supplier<Stream<PeerConnection>> allActiveConnectionsSupplier = Stream::empty;
  private RlpxAgent agent = agent();

  @BeforeEach
  public void setup() {
    // Set basic defaults
    when(peerPrivileges.canExceedConnectionLimits(any())).thenReturn(false);
    agent.setPeerConnectionGatekeeper((a, b) -> Optional.empty());
  }

  @AfterEach
  public void after() {
    connections = Collections.emptyList();
  }

  @Test
  public void start() {
    // Initial call to start should work
    final CompletableFuture<Integer> future = agent.start();
    assertThat(future).isDone();
    assertThat(future).isNotCompletedExceptionally();

    // Subsequent call to start should fail
    final CompletableFuture<Integer> future2 = agent.start();
    assertThat(future2).isDone();
    assertThat(future2).isCompletedExceptionally();
    assertThatThrownBy(future2::get)
        .hasCauseInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Unable to start an already started");
  }

  @Test
  public void stop() throws ExecutionException, InterruptedException {
    // Cannot stop before starting
    final CompletableFuture<Void> future = agent.stop();
    assertThat(future).isDone();
    assertThat(future).isCompletedExceptionally();
    assertThatThrownBy(future::get)
        .hasCauseInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Illegal attempt to stop");

    // Stop after starting should succeed
    startAgent();

    final MockPeerConnection connection = (MockPeerConnection) agent.connect(createPeer()).get();
    connections = List.of(connection);

    final CompletableFuture<Void> future2 = agent.stop();
    assertThat(future2).isDone();
    assertThat(future2).isNotCompletedExceptionally();
    // Check peer was disconnected
    assertThat(connection.getDisconnectReason()).contains(DisconnectReason.CLIENT_QUITTING);

    // Cannot stop again
    final CompletableFuture<Void> future3 = agent.stop();
    assertThat(future3).isDone();
    assertThat(future3).isCompletedExceptionally();
    assertThatThrownBy(future3::get)
        .hasCauseInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Illegal attempt to stop");
  }

  @Test
  public void connect_succeeds() {
    startAgent();
    final Peer peer = createPeer();

    final CompletableFuture<PeerConnection> connection = agent.connect(peer);

    assertThat(connection).isDone();
    assertThat(connection).isNotCompletedExceptionally();

    assertThat(agent.getMapOfCompletableFutures().values()).contains(connection);
  }

  @Test
  public void connect_fails() {
    connectionInitializer.setAutocompleteConnections(false);
    startAgent();

    final Peer peer = createPeer();
    final CompletableFuture<PeerConnection> connection = agent.connect(peer);

    // Fail connection
    connection.completeExceptionally(new RuntimeException());

    agent.getMapOfCompletableFutures().get(peer.getId()).isCompletedExceptionally();
  }

  @Test
  public void connect_toDiscoveryPeerUpdatesStats() {
    startAgent();

    final DiscoveryPeerV4 peer = DiscoveryPeerV4.fromEnode(enode());
    assertThat(peer.getLastAttemptedConnection()).isEqualTo(0);
    final CompletableFuture<PeerConnection> connection = agent.connect(peer);

    assertThat(connection).isDone();
    assertThat(connection).isNotCompletedExceptionally();
    assertThat(peer.getLastAttemptedConnection()).isGreaterThan(0);
  }

  @Test
  public void incomingConnect_causesDispatch() {
    startAgent();

    final ConnectionDispatch connectionDispatch = new ConnectionDispatch();
    agent.subscribeConnect(connectionDispatch::onConnect);

    final Peer peer = createPeer();
    final PeerConnection connection = connection(peer, true);
    connectionInitializer.simulateIncomingConnection(connection);

    assertThat(connectionDispatch.getDispatchCount()).isEqualTo(1);
  }

  @Test
  public void connect_failsIfAgentIsNotReady() {
    // Don't start agent or set localNode
    final Peer peer = createPeer();
    final CompletableFuture<PeerConnection> connection = agent.connect(peer);

    assertThat(connection).isDone();
    assertThat(connection).isCompletedExceptionally();

    assertThatThrownBy(connection::get)
        .hasCauseInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Cannot connect before")
        .hasMessageContaining("has finished starting");
  }

  @Test
  public void incomingConnect_failsIfAgentIsNotReady() {
    // Don't start agent or set localNode
    final ConnectionDispatch connectionDispatch = new ConnectionDispatch();
    agent.subscribeConnect(connectionDispatch::onConnect);
    final Peer peer = createPeer();
    final PeerConnection connection = connection(peer, true);
    connectionInitializer.simulateIncomingConnection(connection);

    assertThat(connectionDispatch.getDispatchCount()).isEqualTo(0);
  }

  @Test
  public void connect_failsIfPeerIsNotListening() {
    startAgent();

    final Peer peer = DefaultPeer.fromEnodeURL(enodeBuilder().disableListening().build());
    final CompletableFuture<PeerConnection> connection = agent.connect(peer);

    assertThat(connection).isDone();
    assertThat(connection).isCompletedExceptionally();

    assertThatThrownBy(connection::get)
        .hasCauseInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Attempt to connect to peer with no listening port");
  }

  @Test
  public void connect_doesNotCreateDuplicateConnections() {
    startAgent();

    final Peer peer = createPeer();
    final CompletableFuture<PeerConnection> connection1 = agent.connect(peer);
    final CompletableFuture<PeerConnection> connection2 = agent.connect(peer);

    assertThat(connection2).isEqualTo(connection1);
  }

  @Test
  public void connect_failsIfPeerIsNotPermitted() {
    startAgent();

    final Peer peer = createPeer();
    doReturn(false)
        .when(peerPermissions)
        .isPermitted(
            eq(localNode.getPeer()),
            eq(peer),
            eq(PeerPermissions.Action.RLPX_ALLOW_NEW_OUTBOUND_CONNECTION));
    final CompletableFuture<PeerConnection> connection = agent.connect(peer);

    assertThat(connection).isDone();
    assertThat(connection).isCompletedExceptionally();

    assertThatThrownBy(connection::get).hasCauseInstanceOf(PeerPermissionsException.class);
    assertThat(agent.getMapOfCompletableFutures().keySet().contains(peer)).isFalse();
  }

  @Test
  public void incomingConnection_disconnectedIfPeerIsNotPermitted() {
    startAgent();

    final Peer peer = createPeer();
    final MockPeerConnection connection = connection(peer, true);
    doReturn(false)
        .when(peerPermissions)
        .isPermitted(
            eq(localNode.getPeer()),
            eq(peer),
            eq(PeerPermissions.Action.RLPX_ALLOW_NEW_INBOUND_CONNECTION));
    connectionInitializer.simulateIncomingConnection(connection);

    assertThat(agent.getMapOfCompletableFutures().keySet().contains(peer)).isFalse();
    assertThat(connection.getDisconnectReason()).contains(DisconnectReason.UNKNOWN);
  }

  @Test
  public void connect_largeStreamOfPeers() {
    startAgent();

    final int peerNo = 20;
    final Stream<? extends Peer> peerStream =
        Stream.generate(PeerTestHelper::createPeer).limit(peerNo);

    agent = spy(agent);
    peerStream.forEach(agent::connect);

    assertThat(agent.getMapOfCompletableFutures().size()).isEqualTo(peerNo);
    verify(agent, times(peerNo)).connect(any(Peer.class));
  }

  @Test
  public void disconnect() throws ExecutionException, InterruptedException {
    startAgent();

    final Peer peer = spy(createPeer());
    final CompletableFuture<PeerConnection> future = agent.connect(peer);

    // Sanity check connection was established
    assertThat(agent.getMapOfCompletableFutures().size()).isEqualTo(1);
    assertThat(agent.getMapOfCompletableFutures().keySet()).contains(peer.getId());

    // Disconnect
    final DisconnectReason reason = DisconnectReason.REQUESTED;
    agent.disconnect(peer.getId(), reason);

    // Validate peer was disconnected
    final PeerConnection actual = agent.getMapOfCompletableFutures().get(peer.getId()).get();
    assertThat(actual).isEqualTo(future.get());
    assertThat(actual.isDisconnected()).isTrue();
    assertThat(((MockPeerConnection) actual).getDisconnectReason()).contains(reason);
  }

  @Test
  public void permissionsUpdate_permissionsRestrictedWithNoListOfPeers()
      throws ExecutionException, InterruptedException {
    final Peer permittedPeer = createPeer();
    final Peer nonPermittedPeer = createPeer();
    startAgent();

    final CompletableFuture<PeerConnection> permittedConnectionFuture =
        this.agent.connect(permittedPeer);
    final PeerConnection permittedConnection = permittedConnectionFuture.get();
    final PeerConnection nonPermittedConnection = spy(this.agent.connect(nonPermittedPeer).get());

    connections = List.of(permittedConnection, nonPermittedConnection);

    // Sanity check
    assertThat(agent.getMapOfCompletableFutures().size()).isEqualTo(2);

    doReturn(false)
        .when(peerPermissions)
        .isPermitted(
            eq(localNode.getPeer()),
            eq(nonPermittedPeer),
            eq(PeerPermissions.Action.RLPX_ALLOW_ONGOING_LOCALLY_INITIATED_CONNECTION));
    peerPermissions.testDispatchUpdate(true, Optional.empty());

    verify(nonPermittedConnection, times(1)).disconnect(any());
    assertThat(agent.getMapOfCompletableFutures().values()).contains(permittedConnectionFuture);
    assertThat(permittedConnection.isDisconnected()).isFalse();
    assertThat(nonPermittedConnection.isDisconnected()).isTrue();
  }

  @Test
  public void permissionsUpdate_permissionsRestrictedWithListOfPeers()
      throws ExecutionException, InterruptedException {
    final Peer permittedPeer = createPeer();
    final Peer nonPermittedPeer = createPeer();
    startAgent();

    final PeerConnection permittedConnection = agent.connect(permittedPeer).get();
    final PeerConnection nonPermittedConnection = agent.connect(nonPermittedPeer).get();

    connections = List.of(permittedConnection, nonPermittedConnection);

    // Sanity check
    assertThat(agent.getMapOfCompletableFutures().size()).isEqualTo(2);

    doReturn(false)
        .when(peerPermissions)
        .isPermitted(
            eq(localNode.getPeer()),
            eq(nonPermittedPeer),
            eq(PeerPermissions.Action.RLPX_ALLOW_ONGOING_LOCALLY_INITIATED_CONNECTION));
    peerPermissions.testDispatchUpdate(true, Optional.of(List.of(nonPermittedPeer)));

    assertThat(agent.getMapOfCompletableFutures().get(permittedPeer.getId())).isNotNull();
    assertThat(permittedConnection.isDisconnected()).isFalse();
    assertThat(nonPermittedConnection.isDisconnected()).isTrue();
  }

  @Test
  public void permissionsUpdate_permissionsRestrictedForRemotelyInitiatedConnections()
      throws ExecutionException, InterruptedException {
    final Peer locallyConnectedPeer = createPeer();
    final Peer remotelyConnectedPeer = createPeer();
    startAgent();

    final ConnectionDispatch connectionDispatch = new ConnectionDispatch();
    agent.subscribeConnect(connectionDispatch::onConnect);

    final PeerConnection locallyInitiatedConnection = agent.connect(locallyConnectedPeer).get();
    final PeerConnection remotelyInitiatedConnection = connection(remotelyConnectedPeer, true);
    connections = List.of(locallyInitiatedConnection, remotelyInitiatedConnection);
    connectionInitializer.simulateIncomingConnection(remotelyInitiatedConnection);

    // Sanity check
    assertThat(agent.getMapOfCompletableFutures().size()).isEqualTo(1); // outgoing connection
    assertThat(connectionDispatch.getDispatchCount())
        .isEqualTo(2); // incoming and outgoing connection

    doReturn(false)
        .when(peerPermissions)
        .isPermitted(
            eq(localNode.getPeer()),
            eq(remotelyConnectedPeer),
            eq(PeerPermissions.Action.RLPX_ALLOW_ONGOING_REMOTELY_INITIATED_CONNECTION));
    peerPermissions.testDispatchUpdate(true, Optional.empty());

    assertThat(locallyInitiatedConnection.isDisconnected()).isFalse();
    assertThat(remotelyInitiatedConnection.isDisconnected()).isTrue();
  }

  @Test
  public void permissionsUpdate_permissionsRestrictedForLocallyInitiatedConnections()
      throws ExecutionException, InterruptedException {
    final Peer locallyConnectedPeer = createPeer();
    final Peer remotelyConnectedPeer = createPeer();
    startAgent();

    final ConnectionDispatch connectionDispatch = new ConnectionDispatch();
    agent.subscribeConnect(connectionDispatch::onConnect);

    final PeerConnection locallyInitiatedConnection = agent.connect(locallyConnectedPeer).get();
    final PeerConnection remotelyInitiatedConnection = connection(remotelyConnectedPeer, true);
    connections = List.of(locallyInitiatedConnection, remotelyInitiatedConnection);
    connectionInitializer.simulateIncomingConnection(remotelyInitiatedConnection);

    // Sanity check
    assertThat(agent.getMapOfCompletableFutures().size()).isEqualTo(1); // outgoing connection
    assertThat(connectionDispatch.getDispatchCount())
        .isEqualTo(2); // incoming and outgoing connection

    doReturn(false)
        .when(peerPermissions)
        .isPermitted(
            eq(localNode.getPeer()),
            eq(locallyConnectedPeer),
            eq(PeerPermissions.Action.RLPX_ALLOW_ONGOING_LOCALLY_INITIATED_CONNECTION));
    peerPermissions.testDispatchUpdate(true, Optional.empty());

    assertThat(locallyInitiatedConnection.isDisconnected()).isTrue();
    assertThat(remotelyInitiatedConnection.isDisconnected()).isFalse();
  }

  @Test
  public void permissionsUpdate_permissionsRelaxedWithNoListOfPeers()
      throws ExecutionException, InterruptedException {
    final Peer peerA = createPeer();
    final Peer peerB = createPeer();
    startAgent();

    final PeerConnection connectionA = agent.connect(peerA).get();
    final PeerConnection connectionB = agent.connect(peerB).get();

    // Sanity check
    assertThat(agent.getMapOfCompletableFutures().size()).isEqualTo(2); // outgoing connections

    doReturn(false).when(peerPermissions).isPermitted(any(), any(), any());
    peerPermissions.testDispatchUpdate(false, Optional.empty());

    assertThat(connectionA.isDisconnected()).isFalse();
    assertThat(connectionB.isDisconnected()).isFalse();
  }

  @Test
  public void permissionsUpdate_permissionsRelaxedWithListOfPeers()
      throws ExecutionException, InterruptedException {
    final Peer peerA = createPeer();
    final Peer peerB = createPeer();
    startAgent();

    final PeerConnection connectionA = agent.connect(peerA).get();
    final PeerConnection connectionB = agent.connect(peerB).get();

    // Sanity check
    assertThat(agent.getMapOfCompletableFutures().size()).isEqualTo(2); // outgoing connections

    doReturn(false).when(peerPermissions).isPermitted(any(), any(), any());
    peerPermissions.testDispatchUpdate(false, Optional.of(List.of(peerA)));

    assertThat(connectionA.isDisconnected()).isFalse();
    assertThat(connectionB.isDisconnected()).isFalse();
  }

  @Test
  public void subscribeConnect_firesForOutgoingConnection() {
    final Peer peer = createPeer();
    startAgent();

    final AtomicReference<PeerConnection> connection = new AtomicReference<>();
    agent.subscribeConnect(connection::set);

    agent.connect(peer);

    assertThat(connection.get()).isNotNull();
    assertThat(connection.get().getPeer()).isEqualTo(peer);
  }

  @Test
  public void subscribeConnect_firesForIncomingConnection() {
    final Peer peer = createPeer();
    startAgent();

    final AtomicReference<PeerConnection> connection = new AtomicReference<>();
    agent.subscribeConnect(connection::set);

    connectionInitializer.simulateIncomingConnection(connection(peer, true));

    assertThat(connection.get()).isNotNull();
    assertThat(connection.get().getPeer()).isEqualTo(peer);
  }

  @Test
  public void subscribeDisconnect() {
    final Peer peer = createPeer();
    startAgent();

    final AtomicReference<PeerConnection> connection = new AtomicReference<>();
    agent.subscribeDisconnect((conn, reason, initiatedByPeer) -> connection.set(conn));

    agent.connect(peer);
    agent.disconnect(peer.getId(), DisconnectReason.REQUESTED);

    assertThat(connection.get()).isNotNull();
    assertThat(connection.get().getPeer()).isEqualTo(peer);
  }

  @Test
  public void subscribeMessage()
      throws ExecutionException, InterruptedException, PeerConnection.PeerNotConnected {
    final Peer peer = createPeer();
    final Capability cap = Capability.create("eth", 68);
    startAgent();

    final AtomicBoolean eventFired = new AtomicBoolean(false);
    agent.subscribeMessage(cap, (capability, msg) -> eventFired.set(true));

    final PeerConnection connection = agent.connect(peer).get();
    peerConnectionEvents.dispatchMessage(
        Capability.create("eth", 68), connection, PingMessage.get());

    assertThat(eventFired).isTrue();
  }

  @Test
  public void getPeerConnection_pending() {
    connectionInitializer.setAutocompleteConnections(false);
    startAgent();

    final Peer peer = createPeer();
    final CompletableFuture<PeerConnection> future = agent.connect(peer);
    assertThat(future).isNotDone();

    assertThat(agent.getMapOfCompletableFutures().values()).contains(future);
  }

  @Test
  public void getPeerConnection_established() {
    startAgent();

    final Peer peer = createPeer();
    final CompletableFuture<PeerConnection> future = agent.connect(peer);
    assertThat(future).isDone();

    assertThat(agent.getMapOfCompletableFutures().values()).contains(future);
  }

  private void startAgent() {
    startAgent(Peer.randomId());
  }

  private void startAgent(final Bytes nodeId) {
    agent.start();
    localNode.setEnode(enodeBuilder().nodeId(nodeId).build());
  }

  private RlpxAgent agent() {
    return agent(Function.identity(), __ -> {});
  }

  private RlpxAgent agent(
      final Function<RlpxAgent.Builder, RlpxAgent.Builder> buildCustomization,
      final Consumer<RlpxConfiguration> rlpxConfigurationModifier) {
    rlpxConfigurationModifier.accept(config);
    return buildCustomization
        .apply(
            RlpxAgent.builder()
                .nodeKey(NodeKeyUtils.createFrom(KEY_PAIR))
                .config(config)
                .peerPermissions(peerPermissions)
                .peerPrivileges(peerPrivileges)
                .localNode(localNode)
                .metricsSystem(metrics)
                .connectionInitializer(connectionInitializer)
                .connectionEvents(peerConnectionEvents)
                .allConnectionsSupplier(allConnectionsSupplier)
                .allActiveConnectionsSupplier(allActiveConnectionsSupplier))
        .build();
  }

  private MockPeerConnection connection(final Peer peer, final boolean inboundInitiated) {
    return MockPeerConnection.create(peer, peerConnectionEvents, inboundInitiated);
  }

  private static class TestPeerPermissions extends PeerPermissions {

    @Override
    public boolean isPermitted(final Peer localNode, final Peer remotePeer, final Action action) {
      return true;
    }

    public void testDispatchUpdate(
        final boolean permissionsRestricted, final Optional<List<Peer>> affectedPeers) {
      this.dispatchUpdate(permissionsRestricted, affectedPeers);
    }
  }

  private static class ConnectionDispatch {
    private int dispatchCount;

    public void onConnect(final PeerConnection pc) {
      this.dispatchCount++;
    }

    public int getDispatchCount() {
      return dispatchCount;
    }
  }

  private Stream<PeerConnection> streamConnections() {
    return connections.stream();
  }
}
