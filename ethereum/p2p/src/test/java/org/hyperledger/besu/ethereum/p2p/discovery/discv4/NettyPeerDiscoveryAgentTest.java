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
package org.hyperledger.besu.ethereum.p2p.discovery.discv4;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.forkid.ForkId;
import org.hyperledger.besu.ethereum.forkid.ForkIdManager;
import org.hyperledger.besu.ethereum.p2p.config.DiscoveryConfiguration;
import org.hyperledger.besu.ethereum.p2p.discovery.NodeRecordManager;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.DiscoveryPeerV4;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.PacketType;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.DaggerPacketPackage;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.Packet;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.PacketPackage;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.ping.PingPacketData;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissions;
import org.hyperledger.besu.ethereum.p2p.rlpx.ConnectSource;
import org.hyperledger.besu.ethereum.p2p.rlpx.RlpxAgent;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.nat.NatService;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.channels.UnsupportedAddressTypeException;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt64;
import org.ethereum.beacon.discovery.schema.NodeRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class NettyPeerDiscoveryAgentTest {

  private NettyPeerDiscoveryAgent agent;
  private TrackingTransport transport;
  private DiscoveryPeerV4 peer;
  private Packet packet;

  @BeforeEach
  void setUp() {
    transport = new TrackingTransport();

    final NodeKey nodeKey = NodeKeyUtils.generate();
    final DiscoveryConfiguration config = new DiscoveryConfiguration();
    config.setBindHost("127.0.0.1");
    config.setAdvertisedHost("127.0.0.1");
    config.setBindPort(0);
    config.setEnodeBootnodes(Collections.emptyList());

    final ForkIdManager forkIdManager = mock(ForkIdManager.class);
    final ForkId forkId = new ForkId(Bytes.EMPTY, Bytes.EMPTY);
    lenient().when(forkIdManager.getForkIdForChainHead()).thenReturn(forkId);

    final RlpxAgent rlpxAgent = mock(RlpxAgent.class);
    lenient()
        .when(
            rlpxAgent.connect(
                org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any()))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException()));

    final NatService natService = new NatService(Optional.empty());
    final NodeRecordManager nodeRecordManager =
        new NodeRecordManager(
            new InMemoryKeyValueStorageProvider(), nodeKey, forkIdManager, natService);

    agent =
        NettyPeerDiscoveryAgent.createWithTransport(
            nodeKey,
            config,
            PeerPermissions.noop(),
            new NoOpMetricsSystem(),
            nodeRecordManager,
            forkIdManager,
            rlpxAgent,
            transport);

    peer =
        DiscoveryPeerV4.fromEnode(
            EnodeURLImpl.builder()
                .nodeId(Peer.randomId())
                .ipAddress("10.0.0.1")
                .listeningPort(30303)
                .discoveryPort(30303)
                .build());

    // Build a real signed packet so packetSerializer.encode() in log lambdas succeeds
    final PacketPackage packetPackage = DaggerPacketPackage.create();
    final Endpoint from = new Endpoint("127.0.0.1", 30303, Optional.empty());
    final Endpoint to = new Endpoint("10.0.0.1", 30303, Optional.empty());
    final PingPacketData pingData =
        packetPackage.pingPacketDataFactory().create(Optional.of(from), to, UInt64.ONE);
    packet = packetPackage.packetFactory().create(PacketType.PING, pingData, nodeKey);
  }

  @Test
  void sendOutgoingPacket_completesWithoutCallingTransport_afterStop() {
    agent.stop().join();

    final CompletableFuture<Void> result = agent.sendOutgoingPacket(peer, packet);

    assertThat(result).isCompleted();
    assertThat(transport.sendCallCount.get()).isZero();
  }

  @Test
  void sendOutgoingPacket_callsTransport_beforeStop() {
    final CompletableFuture<Void> result = agent.sendOutgoingPacket(peer, packet);

    assertThat(result).isCompleted();
    assertThat(transport.sendCallCount.get()).isOne();
  }

  @Test
  void handleOutgoingPacketError_doesNotThrow_afterStop() {
    agent.stop().join();
    assertThatCode(
            () -> agent.handleOutgoingPacketError(new RuntimeException("test error"), peer, packet))
        .doesNotThrowAnyException();
  }

  static Stream<Throwable> knownOutgoingErrors() {
    return Stream.of(
        new SocketException("Network is unreachable"),
        new SocketException("Operation not permitted"),
        new UnsupportedAddressTypeException(),
        new RuntimeException("unexpected error"));
  }

  @ParameterizedTest(name = "{index} - error type: {0}")
  @MethodSource("knownOutgoingErrors")
  void handleOutgoingPacketError_knownErrorTypes_doesNotThrow(final Throwable err) {
    assertThatCode(() -> agent.handleOutgoingPacketError(err, peer, packet))
        .doesNotThrowAnyException();
  }

  @Test
  void handleRawIncoming_dropsLateDecodeCompletion_afterStopGateSetWhileQueued() throws Exception {
    // Spy before start(): prepareHandlers() captures a `this::handleRawIncoming` reference bound
    // to whichever instance start() is called on, and handleRawIncoming's internal call to the
    // (protected, overridable) handleIncomingPacket(...) is virtually dispatched through the spy.
    agent = spy(agent);
    agent.start(30303).join();

    final Executor dispatchExecutor = agent.createDispatchExecutor();

    // Occupy the single dispatch thread so the real decode-completion continuation (triggered
    // below) queues behind this blocker instead of running immediately.
    final CountDownLatch releaseDispatchThread = new CountDownLatch(1);
    final CountDownLatch blockerRunning = new CountDownLatch(1);
    dispatchExecutor.execute(
        () -> {
          blockerRunning.countDown();
          try {
            releaseDispatchThread.await(5, TimeUnit.SECONDS);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        });
    assertThat(blockerRunning.await(5, TimeUnit.SECONDS)).isTrue();

    // Feed a real, validly-encoded PING packet through the transport's captured inbound handler,
    // simulating an incoming UDP datagram. stopGate is still false, so it passes the entry guard
    // and its decode-completion continuation queues behind the blocker above.
    final InetSocketAddress sender = new InetSocketAddress(InetAddress.getLoopbackAddress(), 30303);
    transport.inboundHandler.onPacket(sender, agent.packetSerializer.encode(packet));

    // Deterministically wait for the real decode task to finish: since decodeExecutorService
    // is single-threaded and FIFO, a no-op submitted after the real decode only completes once
    // the real decode (and its synchronous submission to the dispatch executor) has already
    // happened — avoiding a fixed sleep that could be too short under CI load.
    agent.createDecodeExecutor().execute(() -> null).get(5, TimeUnit.SECONDS);
    agent.stopGate.set(true);
    releaseDispatchThread.countDown();

    // Drain the dispatch thread: since it's a single FIFO executor, waiting for this sentinel
    // guarantees the real continuation (queued earlier) has already run.
    final CountDownLatch sentinelRan = new CountDownLatch(1);
    dispatchExecutor.execute(sentinelRan::countDown);
    assertThat(sentinelRan.await(5, TimeUnit.SECONDS)).isTrue();

    verify(agent, never()).handleIncomingPacket(any(), any());
  }

  @Test
  void start_withEphemeralIpv6BindPort_advertisesTheTransportsResolvedPortNotZero()
      throws Exception {
    final NodeKey nodeKey = NodeKeyUtils.generate();
    final DiscoveryConfiguration config = new DiscoveryConfiguration();
    config.setBindHost("127.0.0.1");
    config.setAdvertisedHost("127.0.0.1");
    config.setBindPort(0);
    config.setEnodeBootnodes(Collections.emptyList());
    config.setAdvertisedHostIpv6(Optional.of("2001:db8::1"));
    // The bug scenario: an ephemeral (OS-assigned) IPv6 discovery port, requested via 0.
    config.setBindPortIpv6(0);

    final ForkIdManager forkIdManager = mock(ForkIdManager.class);
    final ForkId forkId = new ForkId(Bytes.EMPTY, Bytes.EMPTY);
    lenient().when(forkIdManager.getForkIdForChainHead()).thenReturn(forkId);

    final RlpxAgent rlpxAgent = mock(RlpxAgent.class);
    lenient()
        .when(rlpxAgent.connect(any(), any(ConnectSource.class)))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException()));
    lenient().when(rlpxAgent.getIpv6ListeningPort()).thenReturn(Optional.of(30304));

    final NatService natService = new NatService(Optional.empty());
    final NodeRecordManager nodeRecordManager =
        new NodeRecordManager(
            new InMemoryKeyValueStorageProvider(), nodeKey, forkIdManager, natService);

    // The port an OS would actually assign for an ephemeral bind - deliberately different from
    // the configured 0, so the test fails if the fix regresses to using the configured value.
    final int resolvedIpv6Port = 54321;
    final TrackingTransport ipv6Transport = new TrackingTransport();
    ipv6Transport.ipv6BoundAddress =
        Optional.of(new InetSocketAddress(InetAddress.getByName("2001:db8::1"), resolvedIpv6Port));

    final NettyPeerDiscoveryAgent ipv6Agent =
        NettyPeerDiscoveryAgent.createWithTransport(
            nodeKey,
            config,
            PeerPermissions.noop(),
            new NoOpMetricsSystem(),
            nodeRecordManager,
            forkIdManager,
            rlpxAgent,
            ipv6Transport);

    try {
      ipv6Agent.start(30303).join();

      final NodeRecord nodeRecord =
          nodeRecordManager.getLocalNode().orElseThrow().getNodeRecord().orElseThrow();
      assertThat(nodeRecord.getUdp6Address()).isPresent();
      assertThat(nodeRecord.getUdp6Address().get().getPort()).isEqualTo(resolvedIpv6Port);
    } finally {
      ipv6Agent.stop().join();
    }
  }

  private static class TrackingTransport implements Transport {
    final AtomicInteger sendCallCount = new AtomicInteger(0);
    volatile InboundV4Handler inboundHandler;
    volatile Optional<InetSocketAddress> ipv6BoundAddress = Optional.empty();

    @Override
    public CompletableFuture<InetSocketAddress> start() {
      return CompletableFuture.completedFuture(
          new InetSocketAddress(InetAddress.getLoopbackAddress(), 30303));
    }

    @Override
    public CompletableFuture<Void> stop() {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> send(final InetSocketAddress recipient, final Bytes data) {
      sendCallCount.incrementAndGet();
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public void setInboundHandler(final InboundV4Handler handler) {
      this.inboundHandler = handler;
    }

    @Override
    public Optional<InetSocketAddress> getIpv6BoundAddress() {
      return ipv6BoundAddress;
    }
  }
}
