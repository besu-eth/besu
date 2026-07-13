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
package org.hyperledger.besu.ethereum.p2p.discovery.discv4.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.hyperledger.besu.ethereum.p2p.discovery.discv4.PeerDiscoveryAgentV4;
import org.hyperledger.besu.util.NetworkUtility;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.tuweni.bytes.Bytes;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class NettyTransportTest {

  private NettyTransport transport1;
  private NettyTransport transport2;

  @AfterEach
  public void tearDown() throws Exception {
    if (transport1 != null) {
      transport1.stop().get(5, TimeUnit.SECONDS);
    }
    if (transport2 != null) {
      transport2.stop().get(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void twoTransports_exchangePackets() throws Exception {
    final InetSocketAddress ephemeral = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);

    transport1 = NettyTransport.create(ephemeral);
    transport2 = NettyTransport.create(ephemeral);

    final AtomicReference<Bytes> receivedByTransport2 = new AtomicReference<>();
    transport2.setInboundHandler((sender, data) -> receivedByTransport2.set(data));

    final InetSocketAddress addr1 = transport1.start().get(5, TimeUnit.SECONDS);
    final InetSocketAddress addr2 = transport2.start().get(5, TimeUnit.SECONDS);

    assertThat(addr1.getPort()).isGreaterThan(0);
    assertThat(addr2.getPort()).isGreaterThan(0);

    final Bytes payload = Bytes.fromHexString("0xdeadbeef01020304");
    transport1.send(addr2, payload).get(5, TimeUnit.SECONDS);

    Awaitility.await()
        .atMost(2, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(receivedByTransport2.get()).isEqualTo(payload));
  }

  @Test
  public void sendCompletionFuture_resolvesOnSuccess() throws Exception {
    final InetSocketAddress ephemeral = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);

    transport1 = NettyTransport.create(ephemeral);
    transport2 = NettyTransport.create(ephemeral);

    transport2.setInboundHandler((sender, data) -> {});

    final InetSocketAddress addr2 = transport2.start().get(5, TimeUnit.SECONDS);
    transport1.start().get(5, TimeUnit.SECONDS);

    final CompletableFuture<Void> sendResult =
        transport1.send(addr2, Bytes.fromHexString("0xaabbcc"));

    assertThat(sendResult).succeedsWithin(5, TimeUnit.SECONDS);
  }

  @Test
  public void start_returnsActualBoundAddress() throws Exception {
    final InetSocketAddress ephemeral = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    transport1 = NettyTransport.create(ephemeral);

    final InetSocketAddress bound = transport1.start().get(5, TimeUnit.SECONDS);

    assertThat(bound.getPort()).isGreaterThan(0);
    assertThat(bound.getAddress()).isEqualTo(InetAddress.getLoopbackAddress());
  }

  @Test
  public void stop_completesCleanly() throws Exception {
    final InetSocketAddress ephemeral = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    transport1 = NettyTransport.create(ephemeral);
    transport1.start().get(5, TimeUnit.SECONDS);

    final CompletableFuture<Void> stopFuture = transport1.stop();
    transport1 = null; // tearDown won't try to stop it again

    assertThat(stopFuture).succeedsWithin(5, TimeUnit.SECONDS);
  }

  @Test
  public void start_bindsSuccessfully_onIpv6WildcardAddress() throws Exception {
    assumeTrue(NetworkUtility.isIPv6Available(), "IPv6 not available on this host");

    final InetSocketAddress ipv6Wildcard = new InetSocketAddress(InetAddress.getByName("::"), 0);
    transport1 = NettyTransport.create(ipv6Wildcard);

    final InetSocketAddress bound = transport1.start().get(5, TimeUnit.SECONDS);

    assertThat(bound.getPort()).isGreaterThan(0);
  }

  @Test
  public void start_afterStop_failsInsteadOfLeakingEventLoop() throws Exception {
    final InetSocketAddress ephemeral = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    transport1 = NettyTransport.create(ephemeral);

    transport1.stop().get(5, TimeUnit.SECONDS);
    final CompletableFuture<InetSocketAddress> restart = transport1.start();

    assertThat(restart).failsWithin(5, TimeUnit.SECONDS);
  }

  @Test
  public void start_afterBindFailure_canRetry() throws Exception {
    final InetSocketAddress ephemeral = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    transport1 = NettyTransport.create(ephemeral);
    final InetSocketAddress boundAddress = transport1.start().get(5, TimeUnit.SECONDS);

    // transport2 tries to bind to the port transport1 already holds, so this start() fails.
    transport2 = NettyTransport.create(boundAddress);
    assertThat(transport2.start()).failsWithin(5, TimeUnit.SECONDS);

    // Freeing the port and retrying should succeed, proving the failed start() didn't leave
    // transport2 permanently stuck in the "already started" state.
    transport1.stop().get(5, TimeUnit.SECONDS);
    transport1 = null;

    final InetSocketAddress retryBound = transport2.start().get(5, TimeUnit.SECONDS);
    assertThat(retryBound.getPort()).isEqualTo(boundAddress.getPort());
  }

  @Test
  public void oversizedDatagram_isDroppedNotDelivered() throws Exception {
    final InetSocketAddress ephemeral = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    transport1 = NettyTransport.create(ephemeral);
    transport2 = NettyTransport.create(ephemeral);

    final List<Bytes> receivedByTransport2 = new CopyOnWriteArrayList<>();
    transport2.setInboundHandler((sender, data) -> receivedByTransport2.add(data));

    final InetSocketAddress addr1 = transport1.start().get(5, TimeUnit.SECONDS);
    final InetSocketAddress addr2 = transport2.start().get(5, TimeUnit.SECONDS);
    assertThat(addr1.getPort()).isGreaterThan(0);

    final Bytes oversized = Bytes.wrap(new byte[PeerDiscoveryAgentV4.MAX_PACKET_SIZE_BYTES + 1]);
    transport1.send(addr2, oversized).get(5, TimeUnit.SECONDS);

    // A legitimately-sized packet sent afterward confirms transport2 is still alive and that
    // the oversized packet was dropped rather than merely delayed.
    final Bytes payload = Bytes.fromHexString("0xdeadbeef");
    transport1.send(addr2, payload).get(5, TimeUnit.SECONDS);

    Awaitility.await()
        .atMost(2, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(receivedByTransport2).contains(payload));

    assertThat(receivedByTransport2).containsExactly(payload);
  }
}
