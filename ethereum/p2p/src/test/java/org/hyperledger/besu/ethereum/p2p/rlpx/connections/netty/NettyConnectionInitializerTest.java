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

import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.ethereum.p2p.config.RlpxConfiguration;
import org.hyperledger.besu.ethereum.p2p.peers.LocalNode;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.ConnectionInitializer.ListeningAddresses;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnectionEventDispatcher;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerLookup;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.util.NetworkUtility;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.net.InetAddresses;
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

  private static NettyConnectionInitializer createInitializer(final RlpxConfiguration config) {
    return new NettyConnectionInitializer(
        NodeKeyUtils.generate(),
        config,
        mock(LocalNode.class),
        mock(PeerConnectionEventDispatcher.class),
        new NoOpMetricsSystem(),
        new PeerLookup());
  }
}
