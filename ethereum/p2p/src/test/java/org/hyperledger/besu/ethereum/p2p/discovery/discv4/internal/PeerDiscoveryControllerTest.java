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
package org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.Hash;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.Endpoint;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.PeerDiscoveryTestHelper;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.DaggerPacketPackage;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.Packet;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.PacketData;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.PacketPackage;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.enrrequest.EnrRequestPacketData;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.enrresponse.EnrResponsePacketData;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.findneighbors.FindNeighborsPacketData;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.neighbors.NeighborsPacketData;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.ping.PingPacketData;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.ping.PingPacketDataFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.pong.PongPacketData;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.validation.EndpointValidator;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.validation.ExpiryValidator;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissions;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissions.Action;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissionsDenylist;
import org.hyperledger.besu.ethereum.p2p.rlpx.ConnectSource;
import org.hyperledger.besu.ethereum.p2p.rlpx.RlpxAgent;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.net.InetAddresses;
import jakarta.validation.constraints.NotNull;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.apache.tuweni.units.bigints.UInt64;
import org.assertj.core.api.Assertions;
import org.ethereum.beacon.discovery.schema.EnrField;
import org.ethereum.beacon.discovery.schema.IdentitySchema;
import org.ethereum.beacon.discovery.schema.IdentitySchemaInterpreter;
import org.ethereum.beacon.discovery.schema.NodeRecord;
import org.ethereum.beacon.discovery.schema.NodeRecordFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class PeerDiscoveryControllerTest {

  private static final byte MOST_SIGNIFICANT_BIT_MASK = -128;
  private static final PeerRequirement PEER_REQUIREMENT = () -> true;
  private static final long TABLE_REFRESH_INTERVAL_MS = TimeUnit.HOURS.toMillis(1);
  private PeerDiscoveryController controller;
  private DiscoveryPeerV4 localPeer;
  private PeerTable peerTable;
  private NodeKey localNodeKey;
  private final AtomicInteger counter = new AtomicInteger(1);
  private final PeerDiscoveryTestHelper helper = new PeerDiscoveryTestHelper();
  private PacketPackage packetPackage;

  private static Long longDelayFunction(final Long prev) {
    return 999999999L;
  }

  private static Long shortDelayFunction(final Long prev) {
    return Math.max(100, prev * 2);
  }

  @BeforeEach
  public void initializeMocks() {
    final List<NodeKey> nodeKeys = PeerDiscoveryTestHelper.generateNodeKeys(1);
    localNodeKey = nodeKeys.get(0);
    localPeer = helper.createDiscoveryPeer(localNodeKey);
    peerTable = new PeerTable(localPeer.getId());
    packetPackage = DaggerPacketPackage.create();
  }

  @AfterEach
  public void stopTable() {
    if (controller != null) {
      controller.stop().join();
    }
  }

  @Test
  public void bootstrapPeersRetriesSent() {
    // Create peers.
    final int peerCount = 3;
    final List<NodeKey> nodeKeys = PeerDiscoveryTestHelper.generateNodeKeys(peerCount);
    final List<DiscoveryPeerV4> peers = helper.createDiscoveryPeers(nodeKeys);

    final MockTimerUtil timer = spy(new MockTimerUtil());
    final OutboundMessageHandler outboundMessageHandler = mock(OutboundMessageHandler.class);
    controller =
        getControllerBuilder()
            .peers(peers)
            .timerUtil(timer)
            .outboundMessageHandler(outboundMessageHandler)
            .build();
    controller.setRetryDelayFunction(PeerDiscoveryControllerTest::shortDelayFunction);

    // Mock the creation of the PING packet, so that we can control the hash,
    // which gets validated when receiving the PONG.
    final PingPacketData mockPing =
        packetPackage
            .pingPacketDataFactory()
            .create(
                Optional.ofNullable(localPeer.getEndpoint()),
                peers.get(0).getEndpoint(),
                UInt64.ONE);
    final Packet mockPacket =
        packetPackage.packetFactory().create(PacketType.PING, mockPing, nodeKeys.get(0));
    mockPingPacketCreation(mockPacket);

    controller.start();

    final int timeouts = 4;
    for (int i = 0; i < timeouts; i++) {
      timer.runTimerHandlers();
    }
    final int expectedTimerEvents = (timeouts + 1) * peerCount;
    verify(timer, atLeast(expectedTimerEvents)).setTimer(anyLong(), any());

    // Within this time period, 4 timers should be placed with these timeouts.
    final long[] expectedTimeouts = {100, 200, 400, 800};
    for (final long timeout : expectedTimeouts) {
      verify(timer, times(peerCount)).setTimer(eq(timeout), any());
    }

    // Check that 5 PING packets were sent for each peer (the initial + 4 attempts following
    // timeouts).
    peers.forEach(
        p ->
            verify(outboundMessageHandler, times(timeouts + 1))
                .send(eq(p), matchPacketOfType(PacketType.PING)));

    controller
        .streamDiscoveredPeers()
        .forEach(p -> assertThat(p.getStatus()).isEqualTo(PeerDiscoveryStatus.BONDING));
  }

  private void mockPingPacketCreation(final Packet mockPacket) {
    mockPingPacketCreation(Optional.empty(), mockPacket);
  }

  private void mockPingPacketCreation(final DiscoveryPeerV4 peer, final Packet mockPacket) {
    mockPingPacketCreation(Optional.of(peer), mockPacket);
  }

  private void mockPingPacketCreation(
      final Optional<DiscoveryPeerV4> peer, final Packet mockPacket) {
    doAnswer(
            invocation -> {
              final Consumer<Packet> handler = invocation.getArgument(2);
              handler.accept(mockPacket);
              return null;
            })
        .when(controller)
        .createPacket(
            eq(PacketType.PING),
            peer.isPresent() ? matchPingDataForPeer(peer.get()) : any(),
            any());
  }

  @Test
  public void bootstrapPeersRetriesStoppedUponResponse() {
    // Create peers.
    final List<NodeKey> nodeKeys = PeerDiscoveryTestHelper.generateNodeKeys(3);
    final List<DiscoveryPeerV4> peers = helper.createDiscoveryPeers(nodeKeys);

    final MockTimerUtil timer = new MockTimerUtil();
    final OutboundMessageHandler outboundMessageHandler = mock(OutboundMessageHandler.class);
    controller =
        getControllerBuilder()
            .peers(peers)
            .timerUtil(timer)
            .outboundMessageHandler(outboundMessageHandler)
            .build();

    // Mock the creation of the PING packet, so that we can control the hash,
    // which gets validated when receiving the PONG.
    final PingPacketData mockPing =
        packetPackage
            .pingPacketDataFactory()
            .create(
                Optional.ofNullable(localPeer.getEndpoint()),
                peers.get(0).getEndpoint(),
                UInt64.ONE);
    final Packet mockPacket =
        packetPackage.packetFactory().create(PacketType.PING, mockPing, nodeKeys.get(0));
    mockPingPacketCreation(mockPacket);

    controller.start();

    // Invoke timers several times so that ping to peers should be resent
    for (int i = 0; i < 3; i++) {
      timer.runTimerHandlers();
    }

    // Assert PING packet was sent for peer[0] 4 times.
    for (final DiscoveryPeerV4 peer : peers) {
      verify(outboundMessageHandler, times(4)).send(eq(peer), matchPacketOfType(PacketType.PING));
    }

    // Simulate a PONG message from peer 0.
    final PongPacketData packetData =
        packetPackage
            .pongPacketDataFactory()
            .create(localPeer.getEndpoint(), mockPacket.getHash(), UInt64.ONE);
    final Packet packet =
        packetPackage.packetFactory().create(PacketType.PONG, packetData, nodeKeys.get(0));
    controller.onMessage(packet, peers.get(0));

    // Invoke timers again
    for (int i = 0; i < 2; i++) {
      timer.runTimerHandlers();
    }

    // Ensure we receive no more PING packets for peer[0].
    // Assert PING packet was sent for peer[0] 4 times.
    for (final DiscoveryPeerV4 peer : peers) {
      final int expectedCount = peer.equals(peers.get(0)) ? 4 : 6;
      verify(outboundMessageHandler, times(expectedCount))
          .send(eq(peer), matchPacketOfType(PacketType.PING));
    }
  }

  @Test
  public void shouldStopRetryingInteractionWhenLimitIsReached() {
    // Create peers.
    final List<NodeKey> nodeKeys = PeerDiscoveryTestHelper.generateNodeKeys(3);
    final List<DiscoveryPeerV4> peers = helper.createDiscoveryPeers(nodeKeys);

    final MockTimerUtil timer = new MockTimerUtil();
    final OutboundMessageHandler outboundMessageHandler = mock(OutboundMessageHandler.class);
    controller =
        getControllerBuilder()
            .peers(peers)
            .timerUtil(timer)
            .outboundMessageHandler(outboundMessageHandler)
            .build();

    // Mock the creation of the PING packet, so that we can control the hash,
    // which gets validated when receiving the PONG.
    final PingPacketData mockPing =
        packetPackage
            .pingPacketDataFactory()
            .create(
                Optional.ofNullable(localPeer.getEndpoint()),
                peers.get(0).getEndpoint(),
                UInt64.ONE);
    final Packet mockPacket =
        packetPackage.packetFactory().create(PacketType.PING, mockPing, nodeKeys.get(0));
    mockPingPacketCreation(mockPacket);

    controller.start();

    // Invoke timers several times so that ping to peers should be resent
    for (int i = 0; i < 10; i++) {
      timer.runTimerHandlers();
    }

    // Assert PING packet was sent only 6 times (initial attempt plus 5 retries)
    for (final DiscoveryPeerV4 peer : peers) {
      verify(outboundMessageHandler, times(6)).send(eq(peer), matchPacketOfType(PacketType.PING));
    }
  }

  @Test
  public void shouldRespondToPingRequest() {
    final List<DiscoveryPeerV4> peers = createPeersInLastBucket(localPeer, 1);

    final DiscoveryPeerV4 discoPeer = peers.get(0);

    final OutboundMessageHandler outboundMessageHandler = mock(OutboundMessageHandler.class);
    controller =
        getControllerBuilder()
            .peers(discoPeer)
            .outboundMessageHandler(outboundMessageHandler)
            .build();

    final Endpoint localEndpoint = localPeer.getEndpoint();

    // Setup ping to be sent to discoPeer
    final List<NodeKey> nodeKeys = PeerDiscoveryTestHelper.generateNodeKeys(1);
    final PingPacketData pingPacketData =
        packetPackage
            .pingPacketDataFactory()
            .create(Optional.ofNullable(localEndpoint), discoPeer.getEndpoint(), UInt64.ONE);
    final Packet discoPeerPing =
        packetPackage.packetFactory().create(PacketType.PING, pingPacketData, nodeKeys.get(0));
    mockPingPacketCreation(discoPeer, discoPeerPing);

    controller.onMessage(discoPeerPing, discoPeer);

    verify(outboundMessageHandler, times(1))
        .send(eq(discoPeer), matchPacketOfType(PacketType.PONG));
  }

  @Test
  public void shouldNotRespondToExpiredPingRequest() throws InterruptedException {
    final List<DiscoveryPeerV4> peers = createPeersInLastBucket(localPeer, 1);

    final DiscoveryPeerV4 discoPeer = peers.get(0);

    final OutboundMessageHandler outboundMessageHandler = mock(OutboundMessageHandler.class);
    controller =
        getControllerBuilder()
            .peers(discoPeer)
            .outboundMessageHandler(outboundMessageHandler)
            .build();

    final Endpoint localEndpoint = localPeer.getEndpoint();

    // Setup ping to be sent to discoPeer
    final List<NodeKey> nodeKeys = PeerDiscoveryTestHelper.generateNodeKeys(1);
    Clock fixedClock = Clock.fixed(Instant.ofEpochSecond(123), ZoneId.of("UTC"));
    PingPacketDataFactory pingPacketDataFactory =
        new PingPacketDataFactory(
            new EndpointValidator(), new ExpiryValidator(fixedClock), fixedClock);
    final PingPacketData pingPacketData =
        pingPacketDataFactory.create(
            Optional.ofNullable(localEndpoint),
            discoPeer.getEndpoint(),
            fixedClock.instant().getEpochSecond() + 1,
            UInt64.ONE);
    final Packet discoPeerPing =
        packetPackage.packetFactory().create(PacketType.PING, pingPacketData, nodeKeys.getFirst());
    mockPingPacketCreation(discoPeer, discoPeerPing);

    controller.onMessage(discoPeerPing, discoPeer);

    verify(outboundMessageHandler, times(0))
        .send(eq(discoPeer), matchPacketOfType(PacketType.PONG));
  }

  @Test
  public void bootstrapPeersPongReceived_HashMatched() {
    // Create peers.
    final List<NodeKey> nodeKeys = PeerDiscoveryTestHelper.generateNodeKeys(3);
    final List<DiscoveryPeerV4> peers = helper.createDiscoveryPeers(nodeKeys);

    final MockTimerUtil timer = new MockTimerUtil();
    final OutboundMessageHandler outboundMessageHandler = mock(OutboundMessageHandler.class);
    controller =
        getControllerBuilder()
            .peers(peers)
            .timerUtil(timer)
            .outboundMessageHandler(outboundMessageHandler)
            .build();

    // Mock the creation of the PING packet, so that we can control the hash, which gets validated
    // when receiving the PONG.
    final PingPacketData mockPing =
        packetPackage
            .pingPacketDataFactory()
            .create(
                Optional.ofNullable(localPeer.getEndpoint()),
                peers.get(0).getEndpoint(),
                UInt64.ONE);
    final Packet mockPacket =
        packetPackage.packetFactory().create(PacketType.PING, mockPing, nodeKeys.get(0));
    mockPingPacketCreation(mockPacket);

    controller.start();

    assertThat(
            controller
                .streamDiscoveredPeers()
                .filter(p -> p.getStatus() == PeerDiscoveryStatus.BONDING))
        .hasSize(3);

    // Simulate PONG messages from all peers
    for (int i = 0; i < 3; i++) {
      final PongPacketData packetData =
          packetPackage
              .pongPacketDataFactory()
              .create(localPeer.getEndpoint(), mockPacket.getHash(), UInt64.ONE);
      final Packet packet0 =
          packetPackage.packetFactory().create(PacketType.PONG, packetData, nodeKeys.get(i));
      controller.onMessage(packet0, peers.get(i));
    }

    // Ensure that the peer controller is now sending FIND_NEIGHBORS messages for this peer.
    for (int i = 0; i < 3; i++) {
      verify(outboundMessageHandler, times(1))
          .send(eq(peers.get(i)), matchPacketOfType(PacketType.FIND_NEIGHBORS));
    }

    // Invoke timeouts and check that we resent our neighbors request
    timer.runTimerHandlers();
    for (int i = 0; i < 3; i++) {
      verify(outboundMessageHandler, times(2))
          .send(eq(peers.get(i)), matchPacketOfType(PacketType.FIND_NEIGHBORS));
    }

    assertThat(
            controller
                .streamDiscoveredPeers()
                .filter(p -> p.getStatus() == PeerDiscoveryStatus.BONDING))
        .hasSize(0);
    assertThat(
            controller
                .streamDiscoveredPeers()
                .filter(p -> p.getStatus() == PeerDiscoveryStatus.BONDED))
        .hasSize(3);
  }

  @Test
  public void bootstrapPeersPongReceived_HashUnmatched() {
    // Create peers.
    final List<NodeKey> nodeKeys = PeerDiscoveryTestHelper.generateNodeKeys(3);
    final List<DiscoveryPeerV4> peers = helper.createDiscoveryPeers(nodeKeys);

    final OutboundMessageHandler outboundMessageHandler = mock(OutboundMessageHandler.class);
    controller =
        getControllerBuilder().peers(peers).outboundMessageHandler(outboundMessageHandler).build();
    controller.setRetryDelayFunction(PeerDiscoveryControllerTest::longDelayFunction);

    // Mock the creation of the PING packet, so that we can control the hash, which gets validated
    // when
    // processing the PONG.
    final PingPacketData mockPing =
        packetPackage
            .pingPacketDataFactory()
            .create(
                Optional.ofNullable(localPeer.getEndpoint()),
                peers.get(0).getEndpoint(),
                UInt64.ONE);
    final Packet mockPacket =
        packetPackage.packetFactory().create(PacketType.PING, mockPing, nodeKeys.get(0));
    mockPingPacketCreation(mockPacket);

    controller.start();

    assertThat(
            controller
                .streamDiscoveredPeers()
                .filter(p -> p.getStatus() == PeerDiscoveryStatus.BONDING))
        .hasSize(3);

    // Send a PONG packet from peer 1, with an incorrect hash.
    final PongPacketData packetData =
        packetPackage
            .pongPacketDataFactory()
            .create(localPeer.getEndpoint(), Bytes.fromHexString("1212"), UInt64.ONE);
    final Packet packet =
        packetPackage.packetFactory().create(PacketType.PONG, packetData, nodeKeys.get(1));
    controller.onMessage(packet, peers.get(1));

    // No FIND_NEIGHBORS packet was sent for peer 1.
    verify(outboundMessageHandler, never())
        .send(eq(peers.get(1)), matchPacketOfType(PacketType.FIND_NEIGHBORS));

    assertThat(
            controller
                .streamDiscoveredPeers()
                .filter(p -> p.getStatus() == PeerDiscoveryStatus.BONDING))
        .hasSize(3);
  }

  @Test
  public void findNeighborsSentAfterBondingFinished() {
    // Create three peers, out of which the first two are bootstrap peers.
    final List<NodeKey> nodeKeys = PeerDiscoveryTestHelper.generateNodeKeys(1);
    final List<DiscoveryPeerV4> peers = helper.createDiscoveryPeers(nodeKeys);

    // Initialize the peer controller, setting a high controller refresh interval and a high timeout
    // threshold,
    // to avoid retries getting in the way of this test.
    final OutboundMessageHandler outboundMessageHandler = mock(OutboundMessageHandler.class);
    controller =
        getControllerBuilder()
            .peers(peers.get(0))
            .outboundMessageHandler(outboundMessageHandler)
            .build();

    // Mock the creation of the PING packet, so that we can control the hash, which gets validated
    // when processing the PONG.
    final PingPacketData mockPing =
        packetPackage
            .pingPacketDataFactory()
            .create(
                Optional.ofNullable(localPeer.getEndpoint()),
                peers.get(0).getEndpoint(),
                UInt64.ONE);
    final Packet mockPacket =
        packetPackage.packetFactory().create(PacketType.PING, mockPing, nodeKeys.get(0));
    mockPingPacketCreation(mockPacket);
    controller.setRetryDelayFunction(PeerDiscoveryControllerTest::longDelayFunction);
    controller.start();

    // Verify that the PING was sent.
    verify(outboundMessageHandler, times(1))
        .send(eq(peers.get(0)), matchPacketOfType(PacketType.PING));

    // Simulate a PONG message from peer[0].
    respondWithPong(peers.get(0), nodeKeys.get(0), mockPacket.getHash());

    // Verify that the FIND_NEIGHBORS packet was sent with target == localPeer.
    final ArgumentCaptor<Packet> captor = ArgumentCaptor.forClass(Packet.class);
    verify(outboundMessageHandler, atLeast(1)).send(eq(peers.get(0)), captor.capture());
    final List<Packet> neighborsPackets =
        captor.getAllValues().stream()
            .filter(p -> p.getType().equals(PacketType.FIND_NEIGHBORS))
            .collect(Collectors.toList());
    assertThat(neighborsPackets.size()).isEqualTo(1);
    final Packet nieghborsPacket = neighborsPackets.get(0);
    final Optional<FindNeighborsPacketData> maybeData =
        nieghborsPacket.getPacketData(FindNeighborsPacketData.class);
    Assertions.assertThat(maybeData).isPresent();
    final FindNeighborsPacketData data = maybeData.get();
    assertThat(data.getTarget()).isEqualTo(localPeer.getId());

    assertThat(controller.streamDiscoveredPeers()).hasSize(1);
    assertThat(controller.streamDiscoveredPeers().findFirst().isPresent()).isTrue();
    assertThat(controller.streamDiscoveredPeers().findFirst().get().getStatus())
        .isEqualTo(PeerDiscoveryStatus.BONDED);
  }

  @Test
  public void addedToInvalidIpsWhenConnectTimedOut() {
    // Create a peer
    final List<NodeKey> nodeKeys = PeerDiscoveryTestHelper.generateNodeKeys(1);
    final NodeKey nodeKey = nodeKeys.getFirst();
    final DiscoveryPeerV4 peerThatTimesOut = helper.createDiscoveryPeers(nodeKeys).getFirst();

    // Initialize the peer controller, using a rlpx agent that times out when asked to connect.
    // Set a high controller refresh interval and a high timeout threshold, to avoid retries
    // getting in the way of this test.
    final OutboundMessageHandler outboundMessageHandler = mock(OutboundMessageHandler.class);
    RlpxAgent rlpxAgentMock = mock(RlpxAgent.class);
    when(rlpxAgentMock.connect(any(), any(ConnectSource.class)))
        .thenReturn(CompletableFuture.failedFuture(new Exception(new TimeoutException())));
    controller =
        getControllerBuilder()
            .outboundMessageHandler(outboundMessageHandler)
            .rlpxAgent(rlpxAgentMock)
            .build();

    // Mock the creation of the PING packet, so that we can control the hash, which gets validated
    // when processing the PONG.
    final PingPacketData mockPing =
        packetPackage
            .pingPacketDataFactory()
            .create(
                Optional.ofNullable(localPeer.getEndpoint()),
                peerThatTimesOut.getEndpoint(),
                UInt64.ONE);
    final Packet mockPacket =
        packetPackage.packetFactory().create(PacketType.PING, mockPing, nodeKey);
    mockPingPacketCreation(mockPacket);
    controller.setRetryDelayFunction(PeerDiscoveryControllerTest::longDelayFunction);
    controller.start();

    controller.handleBondingRequest(peerThatTimesOut);

    // Verify that the PING was sent.
    verify(outboundMessageHandler, times(1))
        .send(eq(peerThatTimesOut), matchPacketOfType(PacketType.PING));

    // Simulate a PONG message from the peer.
    respondWithPong(peerThatTimesOut, nodeKey, mockPacket.getHash());

    final List<DiscoveryPeerV4> peersInTable = controller.streamDiscoveredPeers().toList();
    assertThat(peersInTable).hasSize(0);
    assertThat(peersInTable).doesNotContain(peerThatTimesOut);

    // Try bonding again, and check that the peer is not sent the PING packet again
    controller.handleBondingRequest(peerThatTimesOut);

    // verify that the ping was not sent, no additional interaction
    verify(outboundMessageHandler, times(1))
        .send(eq(peerThatTimesOut), matchPacketOfType(PacketType.PING));
  }

  private ControllerBuilder getControllerBuilder() {
    final RlpxAgent rlpxAgent = mock(RlpxAgent.class);
    when(rlpxAgent.connect(any(), any(ConnectSource.class)))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException()));
    return ControllerBuilder.create()
        .nodeKey(localNodeKey)
        .localPeer(localPeer)
        .peerTable(peerTable)
        .rlpxAgent(rlpxAgent);
  }

  private void respondWithPong(
      final DiscoveryPeerV4 discoveryPeerV4, final NodeKey nodeKey, final Bytes hash) {
    final PongPacketData packetData0 =
        packetPackage.pongPacketDataFactory().create(localPeer.getEndpoint(), hash, UInt64.ONE);
    final Packet pongPacket0 =
        packetPackage.packetFactory().create(PacketType.PONG, packetData0, nodeKey);
    controller.onMessage(pongPacket0, discoveryPeerV4);
  }

  @Test
  public void peerSeenTwice() {
    // Create three peers, out of which the first two are bootstrap peers.
    final List<NodeKey> nodeKeys = PeerDiscoveryTestHelper.generateNodeKeys(3);
    final List<DiscoveryPeerV4> peers = helper.createDiscoveryPeers(nodeKeys);

    // Initialize the peer controller
    final OutboundMessageHandler outboundMessageHandler = mock(OutboundMessageHandler.class);
    controller =
        getControllerBuilder()
            .peers(peers.get(0), peers.get(1))
            .outboundMessageHandler(outboundMessageHandler)
            .build();

    // Mock the creation of the PING packet, so that we can control the hash, which gets validated
    // when processing the PONG.
    final PingPacketData pingPacketData =
        packetPackage
            .pingPacketDataFactory()
            .create(
                Optional.ofNullable(localPeer.getEndpoint()),
                peers.get(0).getEndpoint(),
                UInt64.ONE);
    final Packet pingPacket =
        packetPackage.packetFactory().create(PacketType.PING, pingPacketData, nodeKeys.get(0));

    mockPingPacketCreation(pingPacket);

    controller.setRetryDelayFunction(PeerDiscoveryControllerTest::longDelayFunction);
    controller.start();

    verify(outboundMessageHandler, times(1))
        .send(eq(peers.get(0)), matchPacketOfType(PacketType.PING));
    verify(outboundMessageHandler, times(1))
        .send(eq(peers.get(1)), matchPacketOfType(PacketType.PING));

    // Simulate a PONG message from peer[0].
    respondWithPong(peers.get(0), nodeKeys.get(0), pingPacket.getHash());

    // Assert that we're bonding with the third peer.
    assertThat(controller.streamDiscoveredPeers()).hasSize(2);
    assertThat(controller.streamDiscoveredPeers())
        .filteredOn(p -> p.getStatus() == PeerDiscoveryStatus.BONDING)
        .hasSize(1);
    assertThat(controller.streamDiscoveredPeers())
        .filteredOn(p -> p.getStatus() == PeerDiscoveryStatus.BONDED)
        .hasSize(1);

    final PongPacketData pongPacketData =
        packetPackage
            .pongPacketDataFactory()
            .create(localPeer.getEndpoint(), pingPacket.getHash(), UInt64.ONE);
    final Packet pongPacket =
        packetPackage.packetFactory().create(PacketType.PONG, pongPacketData, nodeKeys.get(1));
    controller.onMessage(pongPacket, peers.get(1));

    // Now after we got that pong we should have sent a find neighbours message...
    verify(outboundMessageHandler, times(1))
        .send(eq(peers.get(0)), matchPacketOfType(PacketType.FIND_NEIGHBORS));

    // Simulate a NEIGHBORS message from peer[0] listing peer[2].
    final NeighborsPacketData neighbors0 =
        packetPackage.neighborsPacketDataFactory().create(Collections.singletonList(peers.get(2)));
    final Packet neighborsPacket0 =
        packetPackage.packetFactory().create(PacketType.NEIGHBORS, neighbors0, nodeKeys.get(0));
    controller.onMessage(neighborsPacket0, peers.get(0));

    // Assert that we're bonded with the third peer.
    assertThat(controller.streamDiscoveredPeers()).hasSize(2);
    assertThat(controller.streamDiscoveredPeers())
        .filteredOn(p -> p.getStatus() == PeerDiscoveryStatus.BONDED)
        .hasSize(2);

    // Simulate bonding and neighbors packet from the second bootstrap peer, with peer[2] reported
    // in the peer list.
    final NeighborsPacketData neighbors1 =
        packetPackage.neighborsPacketDataFactory().create(Collections.singletonList(peers.get(2)));
    final Packet neighborsPacket1 =
        packetPackage.packetFactory().create(PacketType.NEIGHBORS, neighbors1, nodeKeys.get(1));
    controller.onMessage(neighborsPacket1, peers.get(1));

    verify(outboundMessageHandler, times(1))
        .send(eq(peers.get(2)), matchPacketOfType(PacketType.PING));

    // Send a PONG packet from peer[2], to transition it to the BONDED state.
    final PongPacketData packetData2 =
        packetPackage
            .pongPacketDataFactory()
            .create(localPeer.getEndpoint(), pingPacket.getHash(), UInt64.ONE);
    final Packet pongPacket2 =
        packetPackage.packetFactory().create(PacketType.PONG, packetData2, nodeKeys.get(2));
    controller.onMessage(pongPacket2, peers.get(2));

    // Assert we're now bonded with peer[2].
    assertThat(controller.streamDiscoveredPeers())
        .filteredOn(p -> p.equals(peers.get(2)) && p.getStatus() == PeerDiscoveryStatus.BONDED)
        .hasSize(1);

    verify(outboundMessageHandler, times(1))
        .send(eq(peers.get(2)), matchPacketOfType(PacketType.PING));
  }

  @Test
  public void startTwice() {
    startPeerDiscoveryController();
    assertThatThrownBy(() -> controller.start()).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void stopTwice() {
    startPeerDiscoveryController();
    controller.stop();
    controller.stop();
    // no exception
  }

  @Test
  public void shouldBondWithNewPeerWhenReceivedPing() {
    final List<DiscoveryPeerV4> peers = createPeersInLastBucket(localPeer, 1);
    startPeerDiscoveryController();

    final Packet pingPacket = mockPingPacket(peers.get(0), localPeer);
    controller.onMessage(pingPacket, peers.get(0));
    verify(controller, times(1)).bond(peers.get(0));
  }

  @Test
  public void shouldNotAddSelfWhenReceivedPingFromSelf() {
    startPeerDiscoveryController();
    final DiscoveryPeerV4 localPeer = DiscoveryPeerV4.fromEnode(this.localPeer.getEnodeURL());

    final Packet pingPacket = mockPingPacket(this.localPeer, this.localPeer);
    controller.onMessage(pingPacket, localPeer);

    assertThat(controller.streamDiscoveredPeers()).doesNotContain(localPeer);
  }

  @Test
  public void shouldNotRemoveExistingPeerWhenReceivedPing() {
    final List<DiscoveryPeerV4> peers = createPeersInLastBucket(localPeer, 1);
    startPeerDiscoveryController();

    peerTable.tryAdd(peers.get(0));
    assertThat(controller.streamDiscoveredPeers()).contains(peers.get(0));

    final Packet pingPacket = mockPingPacket(peers.get(0), localPeer);
    controller.onMessage(pingPacket, peers.get(0));

    assertThat(controller.streamDiscoveredPeers()).contains(peers.get(0));
  }

  @Test
  public void shouldNotAddNewPeerWhenReceivedPongFromDenylistedPeer() {
    final List<DiscoveryPeerV4> peers = createPeersInLastBucket(localPeer, 3);

    final DiscoveryPeerV4 discoPeer = peers.get(0);
    final DiscoveryPeerV4 otherPeer = peers.get(1);
    final DiscoveryPeerV4 otherPeer2 = peers.get(2);

    final PeerPermissionsDenylist denylist = PeerPermissionsDenylist.create();
    final OutboundMessageHandler outboundMessageHandler = mock(OutboundMessageHandler.class);
    controller =
        getControllerBuilder()
            .peers(discoPeer)
            .peerPermissions(denylist)
            .outboundMessageHandler(outboundMessageHandler)
            .build();

    final Endpoint localEndpoint = localPeer.getEndpoint();

    // Setup ping to be sent to discoPeer
    List<NodeKey> nodeKeys = PeerDiscoveryTestHelper.generateNodeKeys(1);
    PingPacketData pingPacketData =
        packetPackage
            .pingPacketDataFactory()
            .create(Optional.ofNullable(localEndpoint), discoPeer.getEndpoint(), UInt64.ONE);
    final Packet discoPeerPing =
        packetPackage.packetFactory().create(PacketType.PING, pingPacketData, nodeKeys.get(0));
    mockPingPacketCreation(discoPeer, discoPeerPing);

    controller.start();
    verify(outboundMessageHandler, times(1))
        .send(eq(peers.get(0)), matchPacketOfType(PacketType.PING));

    final Packet pongFromDiscoPeer =
        MockPacketDataFactory.mockPongPacket(discoPeer, discoPeerPing.getHash());
    controller.onMessage(pongFromDiscoPeer, discoPeer);

    verify(outboundMessageHandler, times(1))
        .send(eq(discoPeer), matchPacketOfType(PacketType.FIND_NEIGHBORS));

    // Setup ping to be sent to otherPeer after neighbors packet is received
    nodeKeys = PeerDiscoveryTestHelper.generateNodeKeys(1);
    pingPacketData =
        packetPackage
            .pingPacketDataFactory()
            .create(Optional.ofNullable(localEndpoint), otherPeer.getEndpoint(), UInt64.ONE);
    final Packet pingPacket =
        packetPackage.packetFactory().create(PacketType.PING, pingPacketData, nodeKeys.get(0));
    mockPingPacketCreation(otherPeer, pingPacket);

    // Setup ping to be sent to otherPeer2 after neighbors packet is received
    nodeKeys = PeerDiscoveryTestHelper.generateNodeKeys(1);
    pingPacketData =
        packetPackage
            .pingPacketDataFactory()
            .create(Optional.ofNullable(localEndpoint), otherPeer2.getEndpoint(), UInt64.ONE);
    final Packet pingPacket2 =
        packetPackage.packetFactory().create(PacketType.PING, pingPacketData, nodeKeys.get(0));
    mockPingPacketCreation(otherPeer2, pingPacket2);

    final Packet neighborsPacket =
        MockPacketDataFactory.mockNeighborsPacket(discoPeer, otherPeer, otherPeer2);
    controller.onMessage(neighborsPacket, discoPeer);

    verify(outboundMessageHandler, times(peers.size()))
        .send(any(), matchPacketOfType(PacketType.PING));

    final Packet pongPacket = MockPacketDataFactory.mockPongPacket(otherPeer, pingPacket.getHash());
    controller.onMessage(pongPacket, otherPeer);

    // Denylist otherPeer2 before sending return pong
    denylist.add(otherPeer2);
    final Packet pongPacket2 =
        MockPacketDataFactory.mockPongPacket(otherPeer2, pingPacket2.getHash());
    controller.onMessage(pongPacket2, otherPeer2);

    assertThat(controller.streamDiscoveredPeers()).hasSize(2);
    assertThat(controller.streamDiscoveredPeers()).contains(discoPeer);
    assertThat(controller.streamDiscoveredPeers()).contains(otherPeer);
    assertThat(controller.streamDiscoveredPeers()).doesNotContain(otherPeer2);
  }

  private PacketData matchPingDataForPeer(final DiscoveryPeerV4 peer) {
    return argThat(
        (PacketData data) ->
            ((PingPacketData) data).getTo().map(peer.getEndpoint()::equals).orElse(false));
  }

  private Packet matchPacketOfType(final PacketType type) {
    return argThat((Packet packet) -> packet.getType().equals(type));
  }

  @Test
  public void shouldNotBondWithDenylistedPeer() {
    final List<DiscoveryPeerV4> peers = createPeersInLastBucket(localPeer, 3);

    final DiscoveryPeerV4 discoPeer = peers.get(0);
    final DiscoveryPeerV4 otherPeer = peers.get(1);
    final DiscoveryPeerV4 otherPeer2 = peers.get(2);

    final PeerPermissionsDenylist denylist = PeerPermissionsDenylist.create();
    final OutboundMessageHandler outboundMessageHandler = mock(OutboundMessageHandler.class);
    controller =
        getControllerBuilder()
            .peers(discoPeer)
            .peerPermissions(denylist)
            .outboundMessageHandler(outboundMessageHandler)
            .build();

    final Endpoint localEndpoint = localPeer.getEndpoint();

    // Setup ping to be sent to discoPeer
    List<NodeKey> nodeKeys = PeerDiscoveryTestHelper.generateNodeKeys(1);
    PingPacketData pingPacketData =
        packetPackage
            .pingPacketDataFactory()
            .create(Optional.ofNullable(localEndpoint), discoPeer.getEndpoint(), UInt64.ONE);
    final Packet discoPeerPing =
        packetPackage.packetFactory().create(PacketType.PING, pingPacketData, nodeKeys.get(0));
    mockPingPacketCreation(discoPeer, discoPeerPing);

    controller.start();
    verify(outboundMessageHandler, times(1)).send(any(), matchPacketOfType(PacketType.PING));

    final Packet pongFromDiscoPeer =
        MockPacketDataFactory.mockPongPacket(discoPeer, discoPeerPing.getHash());
    controller.onMessage(pongFromDiscoPeer, discoPeer);

    verify(outboundMessageHandler, times(1))
        .send(eq(discoPeer), matchPacketOfType(PacketType.FIND_NEIGHBORS));

    // Setup ping to be sent to otherPeer after neighbors packet is received
    nodeKeys = PeerDiscoveryTestHelper.generateNodeKeys(1);
    pingPacketData =
        packetPackage
            .pingPacketDataFactory()
            .create(Optional.ofNullable(localEndpoint), otherPeer.getEndpoint(), UInt64.ONE);
    final Packet pingPacket =
        packetPackage.packetFactory().create(PacketType.PING, pingPacketData, nodeKeys.get(0));
    mockPingPacketCreation(otherPeer, pingPacket);

    // Setup ping to be sent to otherPeer2 after neighbors packet is received
    nodeKeys = PeerDiscoveryTestHelper.generateNodeKeys(1);
    pingPacketData =
        packetPackage
            .pingPacketDataFactory()
            .create(Optional.ofNullable(localEndpoint), otherPeer2.getEndpoint(), UInt64.ONE);
    final Packet pingPacket2 =
        packetPackage.packetFactory().create(PacketType.PING, pingPacketData, nodeKeys.get(0));
    mockPingPacketCreation(otherPeer2, pingPacket2);

    // Denylist peer
    denylist.add(otherPeer);

    final Packet neighborsPacket =
        MockPacketDataFactory.mockNeighborsPacket(discoPeer, otherPeer, otherPeer2);
    controller.onMessage(neighborsPacket, discoPeer);

    verify(controller, times(0)).bond(otherPeer);
    verify(controller, times(1)).bond(otherPeer2);
  }

  @Test
  public void shouldRespondToNeighborsRequestFromKnownPeer() {
    final List<DiscoveryPeerV4> peers = createPeersInLastBucket(localPeer, 1);

    final DiscoveryPeerV4 discoPeer = peers.get(0);

    final OutboundMessageHandler outboundMessageHandler = mock(OutboundMessageHandler.class);
    controller =
        getControllerBuilder()
            .peers(discoPeer)
            .outboundMessageHandler(outboundMessageHandler)
            .build();

    final Endpoint localEndpoint = localPeer.getEndpoint();

    // Setup ping to be sent to discoPeer
    final List<NodeKey> nodeKeys = PeerDiscoveryTestHelper.generateNodeKeys(1);
    final PingPacketData pingPacketData =
        packetPackage
            .pingPacketDataFactory()
            .create(Optional.ofNullable(localEndpoint), discoPeer.getEndpoint(), UInt64.ONE);
    final Packet discoPeerPing =
        packetPackage.packetFactory().create(PacketType.PING, pingPacketData, nodeKeys.get(0));
    mockPingPacketCreation(discoPeer, discoPeerPing);

    controller.start();
    verify(outboundMessageHandler, times(1)).send(any(), matchPacketOfType(PacketType.PING));

    final Packet pongFromDiscoPeer =
        MockPacketDataFactory.mockPongPacket(discoPeer, discoPeerPing.getHash());
    controller.onMessage(pongFromDiscoPeer, discoPeer);

    verify(outboundMessageHandler, times(1))
        .send(eq(discoPeer), matchPacketOfType(PacketType.FIND_NEIGHBORS));

    final Packet findNeighborsPacket = MockPacketDataFactory.mockFindNeighborsPacket(discoPeer);
    controller.onMessage(findNeighborsPacket, discoPeer);

    verify(outboundMessageHandler, times(1))
        .send(eq(discoPeer), matchPacketOfType(PacketType.NEIGHBORS));
  }

  @Test
  public void shouldNotRespondToNeighborsRequestFromUnknownPeer() {
    final List<DiscoveryPeerV4> peers = createPeersInLastBucket(localPeer, 2);

    final DiscoveryPeerV4 discoPeer = peers.get(0);
    final DiscoveryPeerV4 otherPeer = peers.get(1);

    final OutboundMessageHandler outboundMessageHandler = mock(OutboundMessageHandler.class);
    controller =
        getControllerBuilder()
            .peers(discoPeer)
            .outboundMessageHandler(outboundMessageHandler)
            .build();

    final Endpoint localEndpoint = localPeer.getEndpoint();

    // Setup ping to be sent to discoPeer
    final List<NodeKey> nodeKeys = PeerDiscoveryTestHelper.generateNodeKeys(1);
    final PingPacketData pingPacketData =
        packetPackage
            .pingPacketDataFactory()
            .create(Optional.ofNullable(localEndpoint), discoPeer.getEndpoint(), UInt64.ONE);
    final Packet discoPeerPing =
        packetPackage.packetFactory().create(PacketType.PING, pingPacketData, nodeKeys.get(0));
    mockPingPacketCreation(discoPeer, discoPeerPing);

    controller.start();
    verify(outboundMessageHandler, times(1)).send(any(), matchPacketOfType(PacketType.PING));

    final Packet pongFromDiscoPeer =
        MockPacketDataFactory.mockPongPacket(discoPeer, discoPeerPing.getHash());
    controller.onMessage(pongFromDiscoPeer, discoPeer);

    verify(outboundMessageHandler, times(1))
        .send(eq(discoPeer), matchPacketOfType(PacketType.FIND_NEIGHBORS));

    final Packet findNeighborsPacket = MockPacketDataFactory.mockFindNeighborsPacket(discoPeer);
    controller.onMessage(findNeighborsPacket, otherPeer);

    verify(outboundMessageHandler, times(0))
        .send(eq(otherPeer), matchPacketOfType(PacketType.NEIGHBORS));
  }

  @Test
  public void shouldNotRespondToExpiredNeighborsRequest() throws InterruptedException {
    final List<DiscoveryPeerV4> peers = createPeersInLastBucket(localPeer, 1);

    final DiscoveryPeerV4 discoPeer = peers.get(0);

    final OutboundMessageHandler outboundMessageHandler = mock(OutboundMessageHandler.class);
    controller =
        getControllerBuilder()
            .peers(discoPeer)
            .outboundMessageHandler(outboundMessageHandler)
            .build();

    final Endpoint localEndpoint = localPeer.getEndpoint();

    // Setup ping to be sent to discoPeer
    final List<NodeKey> nodeKeys = PeerDiscoveryTestHelper.generateNodeKeys(1);
    final PingPacketData pingPacketData =
        packetPackage
            .pingPacketDataFactory()
            .create(Optional.ofNullable(localEndpoint), discoPeer.getEndpoint(), UInt64.ONE);
    final Packet discoPeerPing =
        packetPackage.packetFactory().create(PacketType.PING, pingPacketData, nodeKeys.get(0));
    mockPingPacketCreation(discoPeer, discoPeerPing);

    controller.start();
    verify(outboundMessageHandler, times(1)).send(any(), matchPacketOfType(PacketType.PING));

    final Packet pongFromDiscoPeer =
        MockPacketDataFactory.mockPongPacket(discoPeer, discoPeerPing.getHash());
    controller.onMessage(pongFromDiscoPeer, discoPeer);

    verify(outboundMessageHandler, times(1))
        .send(eq(discoPeer), matchPacketOfType(PacketType.FIND_NEIGHBORS));

    final Packet findNeighborsPacket =
        MockPacketDataFactory.mockFindNeighborsPacket(discoPeer, 123);
    controller.onMessage(findNeighborsPacket, discoPeer);

    verify(outboundMessageHandler, times(0))
        .send(eq(discoPeer), matchPacketOfType(PacketType.NEIGHBORS));
  }

  @Test
  public void shouldNotRespondToNeighborsRequestFromDenylistedPeer() {
    final List<DiscoveryPeerV4> peers = createPeersInLastBucket(localPeer, 1);

    final DiscoveryPeerV4 discoPeer = peers.get(0);

    final PeerPermissionsDenylist denylist = PeerPermissionsDenylist.create();
    final OutboundMessageHandler outboundMessageHandler = mock(OutboundMessageHandler.class);
    controller =
        getControllerBuilder()
            .peers(discoPeer)
            .peerPermissions(denylist)
            .outboundMessageHandler(outboundMessageHandler)
            .build();

    final Endpoint localEndpoint = localPeer.getEndpoint();

    // Setup ping to be sent to discoPeer
    final List<NodeKey> nodeKeys = PeerDiscoveryTestHelper.generateNodeKeys(1);
    final PingPacketData pingPacketData =
        packetPackage
            .pingPacketDataFactory()
            .create(Optional.ofNullable(localEndpoint), discoPeer.getEndpoint(), UInt64.ONE);
    final Packet discoPeerPing =
        packetPackage.packetFactory().create(PacketType.PING, pingPacketData, nodeKeys.get(0));
    mockPingPacketCreation(discoPeer, discoPeerPing);

    controller.start();
    verify(outboundMessageHandler, times(1)).send(any(), matchPacketOfType(PacketType.PING));

    final Packet pongFromDiscoPeer =
        MockPacketDataFactory.mockPongPacket(discoPeer, discoPeerPing.getHash());
    controller.onMessage(pongFromDiscoPeer, discoPeer);

    verify(outboundMessageHandler, times(1))
        .send(eq(discoPeer), matchPacketOfType(PacketType.FIND_NEIGHBORS));

    denylist.add(discoPeer);
    final Packet findNeighborsPacket = MockPacketDataFactory.mockFindNeighborsPacket(discoPeer);
    controller.onMessage(findNeighborsPacket, discoPeer);

    verify(outboundMessageHandler, times(0))
        .send(eq(discoPeer), matchPacketOfType(PacketType.NEIGHBORS));
  }

  @Test
  public void shouldAddNewPeerWhenReceivedPongAndPeerTableBucketIsNotFull() {
    final List<DiscoveryPeerV4> peers = createPeersInLastBucket(localPeer, 1);

    // Mock the creation of the PING packet to control hash for PONG.
    final List<NodeKey> nodeKeys = PeerDiscoveryTestHelper.generateNodeKeys(1);
    final PingPacketData pingPacketData =
        packetPackage
            .pingPacketDataFactory()
            .create(
                Optional.ofNullable(localPeer.getEndpoint()),
                peers.get(0).getEndpoint(),
                UInt64.ONE);
    final Packet pingPacket =
        packetPackage.packetFactory().create(PacketType.PING, pingPacketData, nodeKeys.get(0));

    final OutboundMessageHandler outboundMessageHandler = mock(OutboundMessageHandler.class);
    controller =
        getControllerBuilder()
            .peers(peers.get(0))
            .outboundMessageHandler(outboundMessageHandler)
            .build();
    mockPingPacketCreation(pingPacket);

    controller.setRetryDelayFunction(PeerDiscoveryControllerTest::longDelayFunction);
    controller.start();

    verify(outboundMessageHandler, times(1)).send(any(), matchPacketOfType(PacketType.PING));

    final Packet pongPacket =
        MockPacketDataFactory.mockPongPacket(peers.get(0), pingPacket.getHash());
    controller.onMessage(pongPacket, peers.get(0));

    assertThat(controller.streamDiscoveredPeers()).contains(peers.get(0));
  }

  @Test
  public void shouldAddNewPeerWhenReceivedPongAndPeerTableBucketIsFull() {
    final List<DiscoveryPeerV4> peers = createPeersInLastBucket(localPeer, 17);

    final List<DiscoveryPeerV4> bootstrapPeers = peers.subList(0, 16);
    final OutboundMessageHandler outboundMessageHandler = mock(OutboundMessageHandler.class);
    controller =
        getControllerBuilder()
            .peers(bootstrapPeers)
            .outboundMessageHandler(outboundMessageHandler)
            .build();
    controller.setRetryDelayFunction(PeerDiscoveryControllerTest::longDelayFunction);

    // Mock the creation of PING packets to control hash PONG packets.
    final List<NodeKey> nodeKeys = PeerDiscoveryTestHelper.generateNodeKeys(1);
    final PingPacketData pingPacketData =
        packetPackage
            .pingPacketDataFactory()
            .create(
                Optional.ofNullable(localPeer.getEndpoint()),
                peers.get(0).getEndpoint(),
                UInt64.ONE);
    final Packet pingPacket =
        packetPackage.packetFactory().create(PacketType.PING, pingPacketData, nodeKeys.get(0));
    mockPingPacketCreation(pingPacket);

    controller.start();

    verify(outboundMessageHandler, times(16)).send(any(), matchPacketOfType(PacketType.PING));

    for (int i = 0; i <= 14; i++) {
      final Packet pongPacket =
          MockPacketDataFactory.mockPongPacket(peers.get(i), pingPacket.getHash());
      controller.onMessage(pongPacket, peers.get(i));
    }

    verify(outboundMessageHandler, times(0))
        .send(any(), matchPacketOfType(PacketType.FIND_NEIGHBORS));

    final Packet pongPacket15 =
        MockPacketDataFactory.mockPongPacket(peers.get(15), pingPacket.getHash());
    controller.onMessage(pongPacket15, peers.get(15));

    verify(outboundMessageHandler, times(3))
        .send(any(), matchPacketOfType(PacketType.FIND_NEIGHBORS));

    for (int i = 0; i <= 15; i++) {
      final Packet neighborsPacket =
          MockPacketDataFactory.mockNeighborsPacket(peers.get(i), peers.get(16));
      controller.onMessage(neighborsPacket, peers.get(i));
    }

    verify(outboundMessageHandler, times(1))
        .send(eq(peers.get(16)), matchPacketOfType(PacketType.PING));

    final Packet pongPacket16 =
        MockPacketDataFactory.mockPongPacket(peers.get(16), pingPacket.getHash());
    controller.onMessage(pongPacket16, peers.get(16));

    assertThat(controller.streamDiscoveredPeers()).contains(peers.get(16));
    assertThat(controller.streamDiscoveredPeers().collect(Collectors.toList())).hasSize(16);
    assertThat(evictedPeerFromBucket(bootstrapPeers, controller)).isTrue();
  }

  private boolean evictedPeerFromBucket(
      final List<DiscoveryPeerV4> peers, final PeerDiscoveryController controller) {
    for (final DiscoveryPeerV4 peer : peers) {
      if (controller.streamDiscoveredPeers().noneMatch(candidate -> candidate.equals(peer))) {
        return true;
      }
    }
    return false;
  }

  @Test
  public void shouldNotAddPeerInNeighborsPacketWithoutBonding() {
    final List<DiscoveryPeerV4> peers = createPeersInLastBucket(localPeer, 2);

    // Mock the creation of the PING packet to control hash for PONG.
    final List<NodeKey> nodeKeys = PeerDiscoveryTestHelper.generateNodeKeys(1);
    final PingPacketData pingPacketData =
        packetPackage
            .pingPacketDataFactory()
            .create(
                Optional.ofNullable(localPeer.getEndpoint()),
                peers.get(0).getEndpoint(),
                UInt64.ONE);
    final Packet pingPacket =
        packetPackage.packetFactory().create(PacketType.PING, pingPacketData, nodeKeys.get(0));

    final OutboundMessageHandler outboundMessageHandler = mock(OutboundMessageHandler.class);
    controller =
        getControllerBuilder()
            .peers(peers.get(0))
            .outboundMessageHandler(outboundMessageHandler)
            .build();
    mockPingPacketCreation(pingPacket);
    controller.start();

    verify(outboundMessageHandler, times(1))
        .send(eq(peers.get(0)), matchPacketOfType(PacketType.PING));

    final Packet pongPacket =
        MockPacketDataFactory.mockPongPacket(peers.get(0), pingPacket.getHash());
    controller.onMessage(pongPacket, peers.get(0));

    verify(outboundMessageHandler, times(1))
        .send(eq(peers.get(0)), matchPacketOfType(PacketType.FIND_NEIGHBORS));

    assertThat(controller.streamDiscoveredPeers()).doesNotContain(peers.get(1));
  }

  @Test
  public void streamDiscoveredPeers() {
    final List<DiscoveryPeerV4> peers = createPeersInLastBucket(localPeer, 3);
    final OutboundMessageHandler outboundMessageHandler = mock(OutboundMessageHandler.class);
    final PeerPermissions peerPermissions = mock(PeerPermissions.class);
    doReturn(true).when(peerPermissions).isPermitted(any(), any(), any());

    final DiscoveryPeerV4 localNode =
        DiscoveryPeerV4.fromEnode(
            EnodeURLImpl.builder()
                .ipAddress("127.0.0.1")
                .nodeId(Peer.randomId())
                .discoveryAndListeningPorts(30303)
                .build());
    localNode.setNodeRecord(
        NodeRecord.fromValues(IdentitySchemaInterpreter.V4, UInt64.ONE, Collections.emptyList()));

    controller =
        getControllerBuilder()
            .localPeer(localNode)
            .peers(peers)
            .outboundMessageHandler(outboundMessageHandler)
            .peerPermissions(peerPermissions)
            .build();
    controller.start();

    assertThat(controller.streamDiscoveredPeers().collect(Collectors.toList()))
        .containsExactlyInAnyOrderElementsOf(peers);

    // Disallow peer - it should be filtered from list
    final Peer disallowed = peers.get(0);
    doReturn(false)
        .when(peerPermissions)
        .isPermitted(eq(localNode), eq(disallowed), eq(Action.DISCOVERY_ALLOW_IN_PEER_TABLE));

    // Peer stream should filter disallowed
    assertThat(controller.streamDiscoveredPeers().collect(Collectors.toList()))
        .containsExactlyInAnyOrder(peers.get(1), peers.get(2));
  }

  @Test
  public void updatePermissions_restrictWithList() {
    final List<DiscoveryPeerV4> peers = createPeersInLastBucket(localPeer, 3);
    final OutboundMessageHandler outboundMessageHandler = mock(OutboundMessageHandler.class);
    final TestPeerPermissions peerPermissions = spy(new TestPeerPermissions());
    doReturn(true).when(peerPermissions).isPermitted(any(), any(), any());

    final DiscoveryPeerV4 localNode =
        DiscoveryPeerV4.fromEnode(
            EnodeURLImpl.builder()
                .ipAddress("127.0.0.1")
                .nodeId(Peer.randomId())
                .discoveryAndListeningPorts(30303)
                .build());
    localNode.setNodeRecord(
        NodeRecord.fromValues(IdentitySchemaInterpreter.V4, UInt64.ONE, Collections.emptyList()));

    controller =
        getControllerBuilder()
            .localPeer(localNode)
            .peers(peers)
            .outboundMessageHandler(outboundMessageHandler)
            .peerPermissions(peerPermissions)
            .build();
    controller.start();

    assertThat(controller.streamDiscoveredPeers().collect(Collectors.toList()))
        .containsExactlyInAnyOrderElementsOf(peers);

    // Disallow peer - it should be filtered from list
    final Peer disallowed = peers.get(0);
    doReturn(false)
        .when(peerPermissions)
        .isPermitted(eq(localNode), eq(disallowed), eq(Action.DISCOVERY_ALLOW_IN_PEER_TABLE));
    peerPermissions.testDispatchUpdate(true, Optional.of(Collections.singletonList(disallowed)));

    // Peer stream should filter disallowed
    assertThat(controller.streamDiscoveredPeers().collect(Collectors.toList()))
        .containsExactlyInAnyOrder(peers.get(1), peers.get(2));

    // Peer should be dropped
    verify(controller, times(1)).dropPeer(any());
    verify(controller, times(1)).dropPeer(disallowed);
  }

  @Test
  public void updatePermissions_restrictWithNoList() {
    final List<DiscoveryPeerV4> peers = createPeersInLastBucket(localPeer, 3);
    final OutboundMessageHandler outboundMessageHandler = mock(OutboundMessageHandler.class);
    final TestPeerPermissions peerPermissions = spy(new TestPeerPermissions());
    final MockTimerUtil timerUtil = new MockTimerUtil();
    doReturn(true).when(peerPermissions).isPermitted(any(), any(), any());

    final DiscoveryPeerV4 localNode =
        DiscoveryPeerV4.fromEnode(
            EnodeURLImpl.builder()
                .ipAddress("127.0.0.1")
                .nodeId(Peer.randomId())
                .discoveryAndListeningPorts(30303)
                .build());
    localNode.setNodeRecord(
        NodeRecord.fromValues(IdentitySchemaInterpreter.V4, UInt64.ONE, Collections.emptyList()));

    controller =
        getControllerBuilder()
            .localPeer(localNode)
            .peers(peers)
            .outboundMessageHandler(outboundMessageHandler)
            .peerPermissions(peerPermissions)
            .timerUtil(timerUtil)
            .build();
    controller.start();

    assertThat(controller.streamDiscoveredPeers().collect(Collectors.toList()))
        .containsExactlyInAnyOrderElementsOf(peers);

    // Disallow peer - it should be filtered from list
    final Peer disallowed = peers.get(0);
    doReturn(false)
        .when(peerPermissions)
        .isPermitted(eq(localNode), eq(disallowed), eq(Action.DISCOVERY_ALLOW_IN_PEER_TABLE));
    peerPermissions.testDispatchUpdate(true, Optional.empty());
    timerUtil.runHandlers();

    // Peer stream should filter disallowed
    assertThat(controller.streamDiscoveredPeers().collect(Collectors.toList()))
        .containsExactlyInAnyOrder(peers.get(1), peers.get(2));

    // Peer should be dropped
    verify(controller, times(1)).dropPeer(any());
    verify(controller, times(1)).dropPeer(disallowed);
  }

  @Test
  public void updatePermissions_relaxPermissionsWithList() {
    final List<DiscoveryPeerV4> peers = createPeersInLastBucket(localPeer, 3);
    final OutboundMessageHandler outboundMessageHandler = mock(OutboundMessageHandler.class);
    final TestPeerPermissions peerPermissions = spy(new TestPeerPermissions());
    final MockTimerUtil timerUtil = new MockTimerUtil();
    doReturn(true).when(peerPermissions).isPermitted(any(), any(), any());

    final DiscoveryPeerV4 localNode =
        DiscoveryPeerV4.fromEnode(
            EnodeURLImpl.builder()
                .ipAddress("127.0.0.1")
                .nodeId(Peer.randomId())
                .discoveryAndListeningPorts(30303)
                .build());
    localNode.setNodeRecord(
        NodeRecord.fromValues(IdentitySchemaInterpreter.V4, UInt64.ONE, Collections.emptyList()));

    controller =
        getControllerBuilder()
            .localPeer(localNode)
            .peers(peers)
            .outboundMessageHandler(outboundMessageHandler)
            .peerPermissions(peerPermissions)
            .timerUtil(timerUtil)
            .build();
    controller.start();

    assertThat(controller.streamDiscoveredPeers().collect(Collectors.toList()))
        .containsExactlyInAnyOrderElementsOf(peers);

    final Peer firstPeer = peers.get(0);
    peerPermissions.testDispatchUpdate(false, Optional.of(Collections.singletonList(firstPeer)));
    timerUtil.runHandlers();

    assertThat(controller.streamDiscoveredPeers().collect(Collectors.toList()))
        .containsExactlyInAnyOrderElementsOf(peers);
    verify(controller, never()).dropPeer(any());
  }

  @Test
  public void updatePermissions_relaxPermissionsWithNoList() {
    final List<DiscoveryPeerV4> peers = createPeersInLastBucket(localPeer, 3);
    final OutboundMessageHandler outboundMessageHandler = mock(OutboundMessageHandler.class);
    final TestPeerPermissions peerPermissions = spy(new TestPeerPermissions());
    final MockTimerUtil timerUtil = new MockTimerUtil();
    doReturn(true).when(peerPermissions).isPermitted(any(), any(), any());

    final DiscoveryPeerV4 localNode =
        DiscoveryPeerV4.fromEnode(
            EnodeURLImpl.builder()
                .ipAddress("127.0.0.1")
                .nodeId(Peer.randomId())
                .discoveryAndListeningPorts(30303)
                .build());
    localNode.setNodeRecord(
        NodeRecord.fromValues(IdentitySchemaInterpreter.V4, UInt64.ONE, Collections.emptyList()));

    controller =
        getControllerBuilder()
            .localPeer(localNode)
            .peers(peers)
            .outboundMessageHandler(outboundMessageHandler)
            .peerPermissions(peerPermissions)
            .timerUtil(timerUtil)
            .build();
    controller.start();

    assertThat(controller.streamDiscoveredPeers().collect(Collectors.toList()))
        .containsExactlyInAnyOrderElementsOf(peers);

    peerPermissions.testDispatchUpdate(false, Optional.empty());
    timerUtil.runHandlers();

    assertThat(controller.streamDiscoveredPeers().collect(Collectors.toList()))
        .containsExactlyInAnyOrderElementsOf(peers);
    verify(controller, never()).dropPeer(any());
  }

  @Test
  public void shouldRespondToENRRequest() {
    final List<DiscoveryPeerV4> peers = createPeersInLastBucket(localPeer, 1);

    // Mock the creation of the PING packet to control hash for PONG.
    final List<NodeKey> nodeKeys = PeerDiscoveryTestHelper.generateNodeKeys(1);
    final PingPacketData pingPacketData =
        packetPackage
            .pingPacketDataFactory()
            .create(
                Optional.ofNullable(localPeer.getEndpoint()),
                peers.get(0).getEndpoint(),
                UInt64.ONE);
    final Packet pingPacket =
        packetPackage.packetFactory().create(PacketType.PING, pingPacketData, nodeKeys.get(0));

    final OutboundMessageHandler outboundMessageHandler = mock(OutboundMessageHandler.class);
    controller =
        getControllerBuilder()
            .peers(peers.get(0))
            .outboundMessageHandler(outboundMessageHandler)
            .build();
    mockPingPacketCreation(pingPacket);
    controller.start();
    verify(outboundMessageHandler, times(1)).send(any(), matchPacketOfType(PacketType.PING));

    final Packet pongPacket =
        MockPacketDataFactory.mockPongPacket(peers.get(0), pingPacket.getHash());
    controller.onMessage(pongPacket, peers.get(0));
    assertThat(controller.streamDiscoveredPeers())
        .filteredOn(p -> p.getStatus() == PeerDiscoveryStatus.BONDED)
        .contains(peers.get(0));

    final EnrRequestPacketData enrRequestPacketData =
        packetPackage.enrRequestPacketDataFactory().create();
    final Packet enrRequestPacket =
        packetPackage
            .packetFactory()
            .create(PacketType.ENR_REQUEST, enrRequestPacketData, nodeKeys.get(0));
    controller.onMessage(enrRequestPacket, peers.get(0));
    verify(outboundMessageHandler, times(1))
        .send(any(), matchPacketOfType(PacketType.FIND_NEIGHBORS));
    verify(outboundMessageHandler, times(1))
        .send(any(), matchPacketOfType(PacketType.ENR_RESPONSE));
  }

  @Test
  public void shouldNotRespondToENRRequestForNonBondedPeer() {
    final List<NodeKey> nodeKeys = PeerDiscoveryTestHelper.generateNodeKeys(1);
    final List<DiscoveryPeerV4> peers = helper.createDiscoveryPeers(nodeKeys);
    final OutboundMessageHandler outboundMessageHandler = mock(OutboundMessageHandler.class);
    controller =
        getControllerBuilder()
            .peers(peers.get(0))
            .outboundMessageHandler(outboundMessageHandler)
            .build();

    final EnrRequestPacketData enrRequestPacketData =
        packetPackage.enrRequestPacketDataFactory().create();
    final Packet packet =
        packetPackage
            .packetFactory()
            .create(PacketType.ENR_REQUEST, enrRequestPacketData, nodeKeys.get(0));

    controller.onMessage(packet, peers.get(0));

    assertThat(controller.streamDiscoveredPeers())
        .filteredOn(p -> p.getStatus() == PeerDiscoveryStatus.BONDED)
        .hasSize(0);
    verify(outboundMessageHandler, times(0))
        .send(eq(peers.get(0)), matchPacketOfType(PacketType.ENR_REQUEST));
  }

  @Test
  public void shouldRespondToRecentENRRequestAfterBonding() {
    final List<NodeKey> nodeKeys = PeerDiscoveryTestHelper.generateNodeKeys(1);
    final List<DiscoveryPeerV4> peers = helper.createDiscoveryPeers(nodeKeys);
    final OutboundMessageHandler outboundMessageHandler = mock(OutboundMessageHandler.class);
    controller =
        getControllerBuilder()
            .peers(peers.get(0))
            .outboundMessageHandler(outboundMessageHandler)
            .build();

    // Mock the creation of the PING packet, so that we can control the hash, which gets validated
    // when receiving the PONG.
    final PingPacketData mockPing =
        packetPackage
            .pingPacketDataFactory()
            .create(
                Optional.ofNullable(localPeer.getEndpoint()),
                peers.get(0).getEndpoint(),
                UInt64.ONE);
    final Packet mockPacket =
        packetPackage.packetFactory().create(PacketType.PING, mockPing, nodeKeys.get(0));
    mockPingPacketCreation(mockPacket);

    controller.start();

    final PongPacketData pongRequestPacketData =
        packetPackage
            .pongPacketDataFactory()
            .create(localPeer.getEndpoint(), mockPacket.getHash(), UInt64.ONE);

    final EnrRequestPacketData enrRequestPacketData =
        packetPackage.enrRequestPacketDataFactory().create();

    final Packet enrPacket =
        packetPackage
            .packetFactory()
            .create(PacketType.ENR_REQUEST, enrRequestPacketData, nodeKeys.get(0));
    final Packet pongPacket =
        packetPackage
            .packetFactory()
            .create(PacketType.PONG, pongRequestPacketData, nodeKeys.get(0));

    controller.onMessage(enrPacket, peers.get(0));
    verify(outboundMessageHandler, never()).send(any(), matchPacketOfType(PacketType.ENR_RESPONSE));
    controller.onMessage(pongPacket, peers.get(0));

    verify(outboundMessageHandler, times(1))
        .send(any(), matchPacketOfType(PacketType.ENR_RESPONSE));
  }

  @Test
  public void shouldNotRespondENRPriorToPong() {
    final List<NodeKey> nodeKeys = PeerDiscoveryTestHelper.generateNodeKeys(1);
    final List<DiscoveryPeerV4> peers = helper.createDiscoveryPeers(nodeKeys);
    final OutboundMessageHandler outboundMessageHandler = mock(OutboundMessageHandler.class);
    final Cache<Bytes, Packet> enrs =
        CacheBuilder.newBuilder()
            .maximumSize(50)
            .expireAfterWrite(1, TimeUnit.NANOSECONDS)
            .ticker(
                new Ticker() {
                  int tickCount = 1;

                  @Override
                  public long read() {
                    return tickCount += 10;
                  }
                })
            .build();
    controller =
        getControllerBuilder()
            .peers(peers.get(0))
            .outboundMessageHandler(outboundMessageHandler)
            .enrCache(enrs)
            .build();

    // Mock the creation of the PING packet, so that we can control the hash, which gets validated
    // when receiving the PONG.
    final PingPacketData mockPing =
        packetPackage
            .pingPacketDataFactory()
            .create(
                Optional.ofNullable(localPeer.getEndpoint()),
                peers.get(0).getEndpoint(),
                UInt64.ONE);
    final Packet mockPacket =
        packetPackage.packetFactory().create(PacketType.PING, mockPing, nodeKeys.get(0));
    mockPingPacketCreation(mockPacket);

    controller.start();

    final PongPacketData pongRequestPacketData =
        packetPackage
            .pongPacketDataFactory()
            .create(localPeer.getEndpoint(), mockPacket.getHash(), UInt64.ONE);

    final EnrRequestPacketData enrRequestPacketData =
        packetPackage.enrRequestPacketDataFactory().create();

    final Packet enrPacket =
        packetPackage
            .packetFactory()
            .create(PacketType.ENR_REQUEST, enrRequestPacketData, nodeKeys.get(0));
    final Packet pongPacket =
        packetPackage
            .packetFactory()
            .create(PacketType.PONG, pongRequestPacketData, nodeKeys.get(0));

    controller.onMessage(enrPacket, peers.get(0));
    enrs.cleanUp();
    controller.onMessage(pongPacket, peers.get(0));

    verify(outboundMessageHandler, never())
        .send(
            argThat((DiscoveryPeerV4 peer) -> peer.equals(peers.get(0))),
            matchPacketOfType(PacketType.ENR_RESPONSE));
  }

  @Test
  public void forkIdShouldBeAvailableIfEnrPacketContainsForkId() {
    final List<NodeKey> nodeKeys = PeerDiscoveryTestHelper.generateNodeKeys(1);
    final List<DiscoveryPeerV4> peers = helper.createDiscoveryPeers(nodeKeys);
    final DiscoveryPeerV4 sender = peers.get(0);
    final Packet enrPacket = prepareForForkIdCheck(nodeKeys, sender, true);

    controller.onMessage(enrPacket, sender);

    final Optional<DiscoveryPeerV4> maybePeer =
        controller
            .streamDiscoveredPeers()
            .filter(p -> p.getId().equals(sender.getId()))
            .findFirst();

    assertThat(maybePeer.isPresent()).isTrue();
    assertThat(maybePeer.get().getForkId().isPresent()).isTrue();
    verify(controller, times(1)).connectOnRlpxLayer(eq(maybePeer.get()));
  }

  @Test
  public void shouldStillCallConnectIfNoForkIdSent() {
    final List<NodeKey> nodeKeys = PeerDiscoveryTestHelper.generateNodeKeys(1);
    final List<DiscoveryPeerV4> peers = helper.createDiscoveryPeers(nodeKeys);
    final DiscoveryPeerV4 sender = peers.get(0);
    final Packet enrPacket = prepareForForkIdCheck(nodeKeys, sender, false);

    controller.onMessage(enrPacket, sender);

    final Optional<DiscoveryPeerV4> maybePeer =
        controller
            .streamDiscoveredPeers()
            .filter(p -> p.getId().equals(sender.getId()))
            .findFirst();

    assertThat(maybePeer.isPresent()).isTrue();
    assertThat(maybePeer.get().getForkId().isPresent()).isFalse();
    verify(controller, times(1)).connectOnRlpxLayer(eq(maybePeer.get()));
  }

  @NotNull
  private Packet prepareForForkIdCheck(
      final List<NodeKey> nodeKeys, final DiscoveryPeerV4 sender, final boolean sendForkId) {
    final HashMap<PacketType, Bytes> packetTypeBytesHashMap = new HashMap<>();
    final OutboundMessageHandler outboundMessageHandler =
        (dp, pa) -> packetTypeBytesHashMap.put(pa.getType(), pa.getHash());
    final Cache<Bytes, Packet> enrs =
        CacheBuilder.newBuilder()
            .maximumSize(50)
            .expireAfterWrite(1, TimeUnit.NANOSECONDS)
            .ticker(
                new Ticker() {
                  int tickCount = 1;

                  @Override
                  public long read() {
                    return tickCount += 10;
                  }
                })
            .build();
    controller =
        getControllerBuilder()
            .peers(sender)
            .outboundMessageHandler(outboundMessageHandler)
            .enrCache(enrs)
            .filterOnForkId(true)
            .build();

    // Mock the creation of the PING packet, so that we can control the hash, which gets validated
    // when receiving the PONG.
    final PingPacketData mockPing =
        packetPackage
            .pingPacketDataFactory()
            .create(Optional.ofNullable(localPeer.getEndpoint()), sender.getEndpoint(), UInt64.ONE);
    final Packet mockPacket =
        packetPackage.packetFactory().create(PacketType.PING, mockPing, nodeKeys.get(0));
    mockPingPacketCreation(mockPacket);

    controller.start();

    final PongPacketData pongRequestPacketData =
        packetPackage
            .pongPacketDataFactory()
            .create(localPeer.getEndpoint(), mockPacket.getHash(), UInt64.ONE);

    final Packet pongPacket =
        packetPackage
            .packetFactory()
            .create(PacketType.PONG, pongRequestPacketData, nodeKeys.get(0));

    controller.onMessage(pongPacket, sender);

    final NodeRecord nodeRecord = createNodeRecord(nodeKeys.get(0), sendForkId);

    final EnrResponsePacketData enrResponsePacketData =
        packetPackage
            .enrResponsePacketDataFactory()
            .create(packetTypeBytesHashMap.get(PacketType.ENR_REQUEST), nodeRecord);
    final Packet enrPacket =
        packetPackage
            .packetFactory()
            .create(PacketType.ENR_RESPONSE, enrResponsePacketData, nodeKeys.get(0));
    return enrPacket;
  }

  private NodeRecord createNodeRecord(final NodeKey nodeKey, final boolean sendForkId) {
    final UInt64 sequenceNumber = UInt64.ZERO.add(1);
    final NodeRecordFactory nodeRecordFactory = NodeRecordFactory.DEFAULT;
    final SignatureAlgorithm signatureAlgorithm = SignatureAlgorithmFactory.getInstance();
    final Bytes addressBytes = Bytes.of(InetAddresses.forString("127.0.0.1").getAddress());

    final NodeRecord nodeRecord;
    if (sendForkId) {
      final Bytes forkIdHash = Bytes.fromHexString("0xfc64ec04");
      final Bytes forkIdNext = Bytes.fromHexString("118c30");
      final ArrayList<Bytes> forkIdBytesList = new ArrayList<>();
      forkIdBytesList.add(0, forkIdHash);
      forkIdBytesList.add(1, forkIdNext);
      nodeRecord =
          nodeRecordFactory.createFromValues(
              sequenceNumber,
              new EnrField(EnrField.ID, IdentitySchema.V4),
              new EnrField(
                  EnrField.PKEY_SECP256K1,
                  signatureAlgorithm.compressPublicKey(nodeKey.getPublicKey())),
              new EnrField(EnrField.IP_V4, addressBytes),
              new EnrField(EnrField.TCP, 7890),
              new EnrField(EnrField.UDP, 4871),
              new EnrField("eth", Collections.singletonList(forkIdBytesList)));
    } else {
      nodeRecord =
          nodeRecordFactory.createFromValues(
              sequenceNumber,
              new EnrField(EnrField.ID, IdentitySchema.V4),
              new EnrField(
                  EnrField.PKEY_SECP256K1,
                  signatureAlgorithm.compressPublicKey(nodeKey.getPublicKey())),
              new EnrField(EnrField.IP_V4, addressBytes),
              new EnrField(EnrField.TCP, 7890),
              new EnrField(EnrField.UDP, 4871));
    }
    nodeRecord.setSignature(
        nodeKey
            .sign(Hash.keccak256(nodeRecord.serializeNoSignature()))
            .encodedBytes()
            .slice(0, 64));
    return nodeRecord;
  }

  private Packet mockPingPacket(final DiscoveryPeerV4 from, final DiscoveryPeerV4 to) {
    final Packet packet = mock(Packet.class);

    final PingPacketData pingPacketData =
        packetPackage
            .pingPacketDataFactory()
            .create(Optional.ofNullable(from.getEndpoint()), to.getEndpoint(), UInt64.ONE);
    when(packet.getPacketData(any())).thenReturn(Optional.of(pingPacketData));
    final Bytes id = from.getId();
    when(packet.getNodeId()).thenReturn(id);
    when(packet.getType()).thenReturn(PacketType.PING);
    when(packet.getHash()).thenReturn(Bytes32.ZERO);

    return packet;
  }

  private List<DiscoveryPeerV4> createPeersInLastBucket(final Peer host, final int n) {
    final List<DiscoveryPeerV4> newPeers = new ArrayList<>(n);

    // Flipping the most significant bit of the keccak256 will place the peer
    // in the last bucket for the corresponding host peer.
    final Bytes32 keccak256 = host.keccak256();
    final MutableBytes template = MutableBytes.create(keccak256.size());
    byte msb = keccak256.get(0);
    msb ^= MOST_SIGNIFICANT_BIT_MASK;
    template.set(0, msb);

    for (int i = 0; i < n; i++) {
      template.setInt(template.size() - 4, i);
      final Bytes32 keccak = Bytes32.leftPad(template);
      final MutableBytes id = MutableBytes.create(64);
      UInt256.valueOf(i).copyTo(id, id.size() - Bytes32.SIZE);
      final DiscoveryPeerV4 peer =
          spy(
              DiscoveryPeerV4.fromEnode(
                  EnodeURLImpl.builder()
                      .nodeId(id)
                      .ipAddress("127.0.0.1")
                      .discoveryAndListeningPorts(100 + counter.incrementAndGet())
                      .build()));

      doReturn(keccak).when(peer).keccak256();
      newPeers.add(peer);
    }

    return newPeers;
  }

  private void startPeerDiscoveryController(final DiscoveryPeerV4... bootstrapPeers) {
    startPeerDiscoveryController(PeerDiscoveryControllerTest::longDelayFunction, bootstrapPeers);
  }

  private void startPeerDiscoveryController(
      final RetryDelayFunction retryDelayFunction, final DiscoveryPeerV4... bootstrapPeers) {
    // Create the controller.
    controller = getControllerBuilder().peers(bootstrapPeers).build();
    controller.setRetryDelayFunction(retryDelayFunction);
    controller.start();
  }

  static class ControllerBuilder {
    private Collection<DiscoveryPeerV4> discoPeers = Collections.emptyList();
    private MockTimerUtil timerUtil = new MockTimerUtil();
    private NodeKey nodeKey;
    private DiscoveryPeerV4 localPeer;
    private PeerTable peerTable;
    private OutboundMessageHandler outboundMessageHandler = OutboundMessageHandler.NOOP;
    private static final PeerDiscoveryTestHelper helper = new PeerDiscoveryTestHelper();
    private PeerPermissions peerPermissions = PeerPermissions.noop();

    private Cache<Bytes, Packet> enrs =
        CacheBuilder.newBuilder().maximumSize(50).expireAfterWrite(10, TimeUnit.SECONDS).build();
    private boolean filterOnForkId = false;
    private RlpxAgent rlpxAgent;

    public static ControllerBuilder create() {
      return new ControllerBuilder();
    }

    ControllerBuilder enrCache(final Cache<Bytes, Packet> cacheToUse) {
      this.enrs = cacheToUse;
      return this;
    }

    ControllerBuilder peers(final Collection<DiscoveryPeerV4> discoPeers) {
      this.discoPeers = discoPeers;
      return this;
    }

    ControllerBuilder peers(final DiscoveryPeerV4... discoPeers) {
      this.discoPeers = Arrays.asList(discoPeers);
      return this;
    }

    ControllerBuilder peerPermissions(final PeerPermissions peerPermissions) {
      this.peerPermissions = peerPermissions;
      return this;
    }

    ControllerBuilder timerUtil(final MockTimerUtil timerUtil) {
      this.timerUtil = timerUtil;
      return this;
    }

    ControllerBuilder nodeKey(final NodeKey nodeKey) {
      this.nodeKey = nodeKey;
      return this;
    }

    ControllerBuilder localPeer(final DiscoveryPeerV4 localPeer) {
      this.localPeer = localPeer;
      return this;
    }

    ControllerBuilder peerTable(final PeerTable peerTable) {
      this.peerTable = peerTable;
      return this;
    }

    ControllerBuilder outboundMessageHandler(final OutboundMessageHandler outboundMessageHandler) {
      this.outboundMessageHandler = outboundMessageHandler;
      return this;
    }

    public ControllerBuilder filterOnForkId(final boolean filterOnForkId) {
      this.filterOnForkId = filterOnForkId;
      return this;
    }

    public ControllerBuilder rlpxAgent(final RlpxAgent rlpxAgent) {
      this.rlpxAgent = rlpxAgent;
      return this;
    }

    PeerDiscoveryController build() {
      checkNotNull(nodeKey);
      if (localPeer == null) {
        localPeer = helper.createDiscoveryPeer(nodeKey);
      }
      if (peerTable == null) {
        peerTable = new PeerTable(localPeer.getId());
      }

      return spy(
          PeerDiscoveryController.builder()
              .nodeKey(nodeKey)
              .localPeer(localPeer)
              .peerTable(peerTable)
              .bootstrapNodes(discoPeers)
              .outboundMessageHandler(outboundMessageHandler)
              .timerUtil(timerUtil)
              .workerExecutor(new BlockingAsyncExecutor())
              .tableRefreshIntervalMs(TABLE_REFRESH_INTERVAL_MS)
              .peerRequirement(PEER_REQUIREMENT)
              .peerPermissions(peerPermissions)
              .metricsSystem(new NoOpMetricsSystem())
              .cacheForEnrRequests(enrs)
              .filterOnEnrForkId(filterOnForkId)
              .rlpxAgent(rlpxAgent)
              .build());
    }
  }

  private static class TestPeerPermissions extends PeerPermissions {

    @Override
    public boolean isPermitted(final Peer localNode, final Peer remotePeer, final Action action) {
      return true;
    }

    void testDispatchUpdate(
        final boolean permissionsRestricted, final Optional<List<Peer>> affectedPeers) {
      this.dispatchUpdate(permissionsRestricted, affectedPeers);
    }
  }
}
