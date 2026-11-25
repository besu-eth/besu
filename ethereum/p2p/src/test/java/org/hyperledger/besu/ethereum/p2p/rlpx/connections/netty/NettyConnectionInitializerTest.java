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
package org.hyperledger.besu.ethereum.p2p.rlpx.connections.netty;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.ethereum.p2p.config.RlpxConfiguration;
import org.hyperledger.besu.ethereum.p2p.peers.DefaultPeer;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.p2p.peers.LocalNode;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.ConnectionInitializer.ListeningAddresses;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnectionEventDispatcher;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerLookup;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MockSubProtocol;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.util.NetworkUtility;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.net.InetAddresses;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class NettyConnectionInitializerTest {

  private NettyConnectionInitializer initializer;

  @AfterEach
  public void tearDown() throws Exception {
    if (initializer != null) {
      initializer.stop().get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void startBindsIpv4Address() throws Exception {
    initializer = createInitializer(ipv4OnlyConfig());

    final ListeningAddresses addrs = initializer.start().get(10, TimeUnit.SECONDS);

    assertThat(addrs.ipv4Address().getPort()).isGreaterThan(0);
    assertThat(addrs.ipv6Address()).isEmpty();

    // Verify the IPv4 socket actually accepts TCP connections
    try (var conn = new Socket()) {
      conn.connect(addrs.ipv4Address(), 5_000);
      assertThat(conn.isConnected()).isTrue();
    }
  }

  @Test
  public void startDualStackBindsBothAddresses() throws Exception {
    assumeTrue(NetworkUtility.isIPv6Available(), "IPv6 not available on this host");

    initializer = createInitializer(dualStackConfig());

    final ListeningAddresses addrs = initializer.start().get(10, TimeUnit.SECONDS);

    assertThat(addrs.ipv4Address().getPort()).isGreaterThan(0);
    assumeTrue(
        addrs.ipv6Address().isPresent(), "IPv6 bind failed; initializer degraded to IPv4-only");
    assertThat(addrs.ipv6Address().get().getPort()).isGreaterThan(0);

    // Verify the IPv4 socket actually accepts TCP connections
    try (var conn = new Socket()) {
      conn.connect(addrs.ipv4Address(), 5_000);
      assertThat(conn.isConnected()).isTrue();
    }

    // Verify the IPv6 socket actually accepts TCP connections
    final InetSocketAddress ipv6Addr = addrs.ipv6Address().get();
    try (var conn = new Socket()) {
      conn.connect(ipv6Addr, 5_000);
      assertThat(conn.isConnected()).isTrue();
    }
  }

  @Test
  public void startDualStackDegradesToIpv4WhenIpv6BindFails() throws Exception {
    assumeTrue(NetworkUtility.isIPv6Available(), "IPv6 not available on this host");

    // Pre-bind a port on ::1 to force the IPv6 bind to fail.
    try (var blocker = new ServerSocket()) {
      blocker.setReuseAddress(false);
      final InetAddress ipv6Loopback = InetAddresses.forString("::1");
      blocker.bind(new InetSocketAddress(ipv6Loopback, 0));
      final int blockedPort = blocker.getLocalPort();

      initializer =
          createInitializer(
              RlpxConfiguration.create()
                  .setBindHost("127.0.0.1")
                  .setBindPort(0)
                  .setBindHostIpv6(Optional.of("::1"))
                  .setBindPortIpv6(Optional.of(blockedPort)));

      final ListeningAddresses addrs = initializer.start().get(10, TimeUnit.SECONDS);

      // IPv4 must still be bound successfully
      assertThat(addrs.ipv4Address().getPort()).isGreaterThan(0);
      // IPv6 must be absent — degraded gracefully
      assertThat(addrs.ipv6Address()).isEmpty();

      // IPv4 socket must still accept connections
      try (var conn = new Socket()) {
        conn.connect(addrs.ipv4Address(), 5_000);
        assertThat(conn.isConnected()).isTrue();
      }
    }
  }

  @Test
  public void startDualStackIpv4AndIpv6PortsAreIndependent() throws Exception {
    assumeTrue(NetworkUtility.isIPv6Available(), "IPv6 not available on this host");

    initializer = createInitializer(dualStackConfig());

    final ListeningAddresses addrs = initializer.start().get(10, TimeUnit.SECONDS);

    assumeTrue(
        addrs.ipv6Address().isPresent(), "IPv6 bind failed; initializer degraded to IPv4-only");

    // Both sockets must receive a real ephemeral port (port 0 expands independently per socket).
    // The two ports may coincidentally be equal (different IPs share the ephemeral range), so we
    // only assert each is non-zero rather than testing inequality.
    assertThat(addrs.ipv4Address().getPort()).isGreaterThan(0);
    assertThat(addrs.ipv6Address().get().getPort()).isGreaterThan(0);
  }

  @Test
  public void startMergedDualStackBindsSingleSocketForBothFamilies() throws Exception {
    // Same port + both wildcard hosts is the config that reliably BindExceptions with two
    // independent per-family sockets on Linux (see NetworkUtility.isMergeableDualStackBind) - so
    // this must resolve to exactly one bound socket shared by both families.
    assumeTrue(NetworkUtility.isIPv6Available(), "IPv6 not available on this host");

    initializer = createInitializer(mergedDualStackConfig());

    final ListeningAddresses addrs = initializer.start().get(10, TimeUnit.SECONDS);

    assertThat(addrs.ipv6Address()).isPresent();
    assertThat(addrs.ipv4Address()).isEqualTo(addrs.ipv6Address().get());
    final int port = addrs.ipv4Address().getPort();
    assertThat(port).isGreaterThan(0);

    // Verify the single socket accepts both IPv4 and IPv6 TCP connections.
    try (var conn = new Socket()) {
      conn.connect(new InetSocketAddress(InetAddresses.forString("127.0.0.1"), port), 5_000);
      assertThat(conn.isConnected()).isTrue();
    }
    try (var conn = new Socket()) {
      conn.connect(new InetSocketAddress(InetAddresses.forString("::1"), port), 5_000);
      assertThat(conn.isConnected()).isTrue();
    }
  }

  @Test
  public void refusesInboundConnectionsBeyondThePendingLimit() throws Exception {
    initializer = createInitializer(ipv4OnlyConfig(), 2);
    final InetSocketAddress address = initializer.start().get(10, TimeUnit.SECONDS).ipv4Address();

    try (var first = new Socket();
        var second = new Socket();
        var third = new Socket();
        var fourth = new Socket()) {
      first.connect(address, 5_000);
      second.connect(address, 5_000);
      Awaitility.await()
          .atMost(5, TimeUnit.SECONDS)
          .until(() -> initializer.pendingInboundConnectionCount() == 2);

      // Both slots are taken, so a third connection is closed by the server without a handshake.
      third.connect(address, 5_000);
      assertThat(isClosedByServer(third)).isTrue();
      assertThat(initializer.pendingInboundConnectionCount()).isEqualTo(2);

      // A slot is freed as soon as the remote end closes, not only when the handshake times out.
      first.close();
      Awaitility.await()
          .atMost(5, TimeUnit.SECONDS)
          .until(() -> initializer.pendingInboundConnectionCount() == 1);

      fourth.connect(address, 5_000);
      Awaitility.await()
          .atMost(5, TimeUnit.SECONDS)
          .until(() -> initializer.pendingInboundConnectionCount() == 2);
    }
  }

  @Test
  public void releasesThePendingSlotOnceAnInboundHandshakeCompletes() throws Exception {
    final NodeKey listenerKey = NodeKeyUtils.generate();
    initializer = createHandshakingInitializer(listenerKey, 50);
    final InetSocketAddress address = initializer.start().get(10, TimeUnit.SECONDS).ipv4Address();
    final NettyConnectionInitializer dialer =
        createHandshakingInitializer(NodeKeyUtils.generate(), 50);
    dialer.start().get(10, TimeUnit.SECONDS);

    try {
      final var listener =
          DefaultPeer.fromEnodeURL(
              EnodeURLImpl.builder()
                  .ipAddress("127.0.0.1")
                  .listeningPort(address.getPort())
                  .nodeId(listenerKey.getPublicKey().getEncodedBytes())
                  .disableDiscovery()
                  .build());

      // A real ECIES handshake and HELLO exchange; the listener side is the inbound connection.
      final PeerConnection connection = dialer.connect(listener).get(10, TimeUnit.SECONDS);

      // The established connection must not keep a slot. The channel is still open, so it is the
      // completed handshake that released it, not the channel closing or the handshake timeout.
      Awaitility.await()
          .atMost(5, TimeUnit.SECONDS)
          .until(() -> initializer.pendingInboundConnectionCount() == 0);
      assertThat(connection.isDisconnected()).isFalse();
    } finally {
      dialer.stop().get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void stopImmediatelyAfterStartCompletesWithoutHanging() throws Exception {
    initializer = createInitializer(ipv4OnlyConfig());

    // Call start() but do NOT wait for it to complete — immediately stop.
    final var startFuture = initializer.start();
    final var stopFuture = initializer.stop();

    // Both futures must resolve within the timeout. Before the fix, stop() could hang
    // indefinitely when the bind had not yet completed.
    stopFuture.get(10, TimeUnit.SECONDS);
    // start() should also complete (exceptionally is fine — it was interrupted by stop).
    try {
      startFuture.get(10, TimeUnit.SECONDS);
    } catch (final ExecutionException ignored) {
      // Expected — stop() may have completed start()'s future exceptionally.
    }

    // Prevent double-stop in @AfterEach.
    initializer = null;
  }

  @Test
  public void stopImmediatelyAfterStartDualStackCompletesWithoutHanging() throws Exception {
    assumeTrue(NetworkUtility.isIPv6Available(), "IPv6 not available on this host");

    initializer = createInitializer(dualStackConfig());

    final var startFuture = initializer.start();
    final var stopFuture = initializer.stop();

    stopFuture.get(10, TimeUnit.SECONDS);
    try {
      startFuture.get(10, TimeUnit.SECONDS);
    } catch (final ExecutionException ignored) {
      // Expected — stop() may have completed start()'s future exceptionally.
    }

    initializer = null;
  }

  private static RlpxConfiguration ipv4OnlyConfig() {
    return RlpxConfiguration.create().setBindHost("127.0.0.1").setBindPort(0);
  }

  private static RlpxConfiguration dualStackConfig() {
    return RlpxConfiguration.create()
        .setBindHost("127.0.0.1")
        .setBindPort(0)
        .setBindHostIpv6(Optional.of("::1"))
        .setBindPortIpv6(Optional.of(0));
  }

  private static RlpxConfiguration mergedDualStackConfig() {
    return RlpxConfiguration.create()
        .setBindHost("0.0.0.0")
        .setBindPort(0)
        .setBindHostIpv6(Optional.of("::"))
        .setBindPortIpv6(Optional.of(0));
  }

  private static NettyConnectionInitializer createInitializer(final RlpxConfiguration config) {
    return createInitializer(config, 50);
  }

  private static NettyConnectionInitializer createInitializer(
      final RlpxConfiguration config, final int maxPendingInboundConnections) {
    return new NettyConnectionInitializer(
        NodeKeyUtils.generate(),
        config,
        mock(LocalNode.class),
        mock(PeerConnectionEventDispatcher.class),
        new NoOpMetricsSystem(),
        new PeerLookup(),
        maxPendingInboundConnections);
  }

  private static NettyConnectionInitializer createHandshakingInitializer(
      final NodeKey nodeKey, final int maxPendingInboundConnections) {
    final LocalNode localNode =
        LocalNode.create(
            "test",
            5,
            List.of(Capability.create("eth", 68)),
            EnodeURLImpl.builder()
                .ipAddress("127.0.0.1")
                .discoveryAndListeningPorts(30303)
                .nodeId(nodeKey.getPublicKey().getEncodedBytes())
                .build());
    return new NettyConnectionInitializer(
        nodeKey,
        ipv4OnlyConfig().setSupportedProtocols(MockSubProtocol.create("eth")),
        localNode,
        mock(PeerConnectionEventDispatcher.class),
        new NoOpMetricsSystem(),
        new PeerLookup(),
        maxPendingInboundConnections);
  }

  private static boolean isClosedByServer(final Socket socket) throws IOException {
    socket.setSoTimeout(5_000);
    try {
      return socket.getInputStream().read() == -1;
    } catch (final SocketException e) {
      // The server closed the connection with a TCP reset.
      return true;
    }
  }
}
