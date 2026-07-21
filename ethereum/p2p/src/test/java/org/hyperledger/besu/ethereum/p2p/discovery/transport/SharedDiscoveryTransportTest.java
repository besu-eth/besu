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
package org.hyperledger.besu.ethereum.p2p.discovery.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.hyperledger.besu.util.NetworkUtility;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import com.google.common.net.InetAddresses;
import org.apache.tuweni.bytes.Bytes;
import org.awaitility.Awaitility;
import org.ethereum.beacon.discovery.pipeline.Envelope;
import org.ethereum.beacon.discovery.pipeline.Field;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

public class SharedDiscoveryTransportTest {

  private static final byte[] MASKING_KEY = new byte[16];
  private static final byte[] DISCV5_MAGIC = {0x64, 0x69, 0x73, 0x63, 0x76, 0x35, 0x00, 0x01};

  static {
    new Random().nextBytes(MASKING_KEY);
  }

  private SharedDiscoveryTransport transport1;
  private SharedDiscoveryTransport transport2;

  @AfterEach
  public void tearDown() throws Exception {
    if (transport1 != null) {
      transport1.stop().get(5, TimeUnit.SECONDS);
    }
    if (transport2 != null) {
      transport2.stop().get(5, TimeUnit.SECONDS);
    }
  }

  private static InetSocketAddress ephemeral() {
    return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
  }

  @Test
  public void start_bindsIpv4OnlyByDefault() throws Exception {
    transport1 =
        SharedDiscoveryTransport.builder()
            .ipv4BindAddress(ephemeral())
            .maskingKey(MASKING_KEY)
            .v4Enabled(true)
            .v5Enabled(true)
            .build();

    transport1.start().get(5, TimeUnit.SECONDS);

    assertThat(transport1.getBoundAddress(StandardProtocolFamily.INET).getPort()).isGreaterThan(0);
    assertThat(transport1.getChannel(StandardProtocolFamily.INET6)).isEmpty();
    assertThat(transport1.getBoundAddress(StandardProtocolFamily.INET6)).isNull();
  }

  @Test
  public void start_bindsBothFamiliesWhenIpv6Configured() throws Exception {
    assumeTrue(NetworkUtility.isIPv6Available(), "IPv6 not available on this host");

    transport1 =
        SharedDiscoveryTransport.builder()
            .ipv4BindAddress(ephemeral())
            .ipv6BindAddress(Optional.of(new InetSocketAddress(InetAddress.getByName("::"), 0)))
            .maskingKey(MASKING_KEY)
            .v4Enabled(true)
            .v5Enabled(true)
            .build();

    transport1.start().get(5, TimeUnit.SECONDS);

    assertThat(transport1.getBoundAddress(StandardProtocolFamily.INET).getPort()).isGreaterThan(0);
    assertThat(transport1.getBoundAddress(StandardProtocolFamily.INET6).getPort()).isGreaterThan(0);
  }

  @Test
  public void mergedDualStack_bindsSingleSocketForBothFamilies() throws Exception {
    // Same port + both wildcard hosts is the config that reliably BindExceptions with two
    // independent per-family sockets on Linux (see NetworkUtility.isMergeableDualStackBind) - so
    // this must resolve to exactly one bound channel shared by both families.
    assumeTrue(NetworkUtility.isIPv6Available(), "IPv6 not available on this host");

    transport1 =
        SharedDiscoveryTransport.builder()
            .ipv4BindAddress(new InetSocketAddress("0.0.0.0", 0))
            .ipv6BindAddress(Optional.of(new InetSocketAddress("::", 0)))
            .maskingKey(MASKING_KEY)
            .v4Enabled(true)
            .v5Enabled(true)
            .build();

    assertThat(transport1.isDualStackMergedIntoSingleSocket()).isTrue();

    transport1.start().get(5, TimeUnit.SECONDS);

    assertThat(transport1.getChannel(StandardProtocolFamily.INET))
        .isPresent()
        .isEqualTo(transport1.getChannel(StandardProtocolFamily.INET6));
    final InetSocketAddress boundV4 = transport1.getBoundAddress(StandardProtocolFamily.INET);
    final InetSocketAddress boundV6 = transport1.getBoundAddress(StandardProtocolFamily.INET6);
    assertThat(boundV4.getPort()).isGreaterThan(0).isEqualTo(boundV6.getPort());
  }

  @Test
  public void mergedDualStack_boundAddressIsFamilyTypedPerRequestedFamily() throws Exception {
    // Regression test: the discv5 library keys its own outbound-dispatch channel map off each
    // registered listener's getBoundAddress() family (see NettyDiscoveryClientImpl.send()
    // upstream). If the merged channel's real (IPv6-typed) local address were returned for both
    // families, the library would never learn a channel exists for IPv4 destinations and would
    // silently drop every outbound packet addressed to one.
    assumeTrue(NetworkUtility.isIPv6Available(), "IPv6 not available on this host");

    transport1 =
        SharedDiscoveryTransport.builder()
            .ipv4BindAddress(new InetSocketAddress("0.0.0.0", 0))
            .ipv6BindAddress(Optional.of(new InetSocketAddress("::", 0)))
            .maskingKey(MASKING_KEY)
            .v4Enabled(true)
            .v5Enabled(true)
            .build();

    assertThat(transport1.isDualStackMergedIntoSingleSocket()).isTrue();
    transport1.start().get(5, TimeUnit.SECONDS);

    final InetSocketAddress boundV4 = transport1.getBoundAddress(StandardProtocolFamily.INET);
    final InetSocketAddress boundV6 = transport1.getBoundAddress(StandardProtocolFamily.INET6);
    assertThat(boundV4.getAddress()).isInstanceOf(Inet4Address.class);
    assertThat(boundV6.getAddress()).isInstanceOf(Inet6Address.class);
    assertThat(boundV4.getPort()).isEqualTo(boundV6.getPort());
  }

  @Test
  public void mergedDualStack_deliversBothIpv4AndIpv6TrafficWithCorrectSenderTyping()
      throws Exception {
    assumeTrue(NetworkUtility.isIPv6Available(), "IPv6 not available on this host");

    transport1 =
        SharedDiscoveryTransport.builder()
            .ipv4BindAddress(new InetSocketAddress("0.0.0.0", 0))
            .ipv6BindAddress(Optional.of(new InetSocketAddress("::", 0)))
            .maskingKey(MASKING_KEY)
            .v4Enabled(true)
            .v5Enabled(true)
            .build();

    final List<InetSocketAddress> senders = new CopyOnWriteArrayList<>();
    transport1.setV4Handler((sender, data) -> senders.add(sender));
    transport1.start().get(5, TimeUnit.SECONDS);
    final int port = transport1.getBoundAddress(StandardProtocolFamily.INET).getPort();

    final byte[] v4Payload = new byte[98];
    new Random().nextBytes(v4Payload);
    sendRawUdp(v4Payload, new InetSocketAddress(InetAddresses.forString("127.0.0.1"), port));

    final byte[] v6Payload = new byte[98];
    new Random().nextBytes(v6Payload);
    sendRawUdp(v6Payload, new InetSocketAddress(InetAddresses.forString("::1"), port));

    Awaitility.await()
        .atMost(2, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(senders).hasSize(2));
    assertThat(senders).anyMatch(sender -> sender.getAddress() instanceof Inet4Address);
    assertThat(senders).anyMatch(sender -> sender.getAddress() instanceof Inet6Address);
  }

  @Test
  public void maskingKey_mustBeExactly16Bytes() {
    final SharedDiscoveryTransport.Builder builder =
        SharedDiscoveryTransport.builder()
            .ipv4BindAddress(ephemeral())
            .maskingKey(new byte[8])
            .v4Enabled(true)
            .v5Enabled(true);

    assertThat(catchThrowable(builder::build)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void v4Enabled_withOnlyIpv6BindAddress_startsSuccessfully() throws Exception {
    // IPv6-only is a legitimate DiscV4 config, matching upstream's owned-channel NettyTransport -
    // it just can't reach an IPv4 recipient (see NettyTransportTest), not a refusal to start.
    assumeTrue(NetworkUtility.isIPv6Available(), "IPv6 not available on this host");

    transport1 =
        SharedDiscoveryTransport.builder()
            .ipv4BindAddress(Optional.empty())
            .ipv6BindAddress(Optional.of(new InetSocketAddress(InetAddress.getByName("::"), 0)))
            .maskingKey(MASKING_KEY)
            .v4Enabled(true)
            .v5Enabled(false)
            .build();

    transport1.start().get(5, TimeUnit.SECONDS);

    assertThat(transport1.getChannel(StandardProtocolFamily.INET)).isEmpty();
    assertThat(transport1.getBoundAddress(StandardProtocolFamily.INET6).getPort()).isGreaterThan(0);
  }

  @Test
  public void atLeastOneBindAddressIsRequired() {
    final SharedDiscoveryTransport.Builder builder =
        SharedDiscoveryTransport.builder()
            .ipv4BindAddress(Optional.empty())
            .maskingKey(MASKING_KEY)
            .v4Enabled(false)
            .v5Enabled(true);

    assertThat(catchThrowable(builder::build)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void v5Only_bindsIpv6WithNoIpv4BindAddressConfigured() throws Exception {
    assumeTrue(NetworkUtility.isIPv6Available(), "IPv6 not available on this host");

    transport1 =
        SharedDiscoveryTransport.builder()
            .ipv4BindAddress(Optional.empty())
            .ipv6BindAddress(Optional.of(new InetSocketAddress(InetAddress.getByName("::"), 0)))
            .maskingKey(MASKING_KEY)
            .v4Enabled(false)
            .v5Enabled(true)
            .build();

    transport1.start().get(5, TimeUnit.SECONDS);

    assertThat(transport1.getChannel(StandardProtocolFamily.INET)).isEmpty();
    assertThat(transport1.getBoundAddress(StandardProtocolFamily.INET6).getPort()).isGreaterThan(0);
  }

  @Test
  public void v4Packet_isDeliveredToRegisteredHandler() throws Exception {
    transport1 =
        SharedDiscoveryTransport.builder()
            .ipv4BindAddress(ephemeral())
            .maskingKey(MASKING_KEY)
            .v4Enabled(true)
            .v5Enabled(true)
            .build();
    transport2 =
        SharedDiscoveryTransport.builder()
            .ipv4BindAddress(ephemeral())
            .maskingKey(MASKING_KEY)
            .v4Enabled(true)
            .v5Enabled(true)
            .build();

    final AtomicReference<Bytes> received = new AtomicReference<>();
    transport2.setV4Handler((sender, data) -> received.set(data));

    transport1.start().get(5, TimeUnit.SECONDS);
    transport2.start().get(5, TimeUnit.SECONDS);

    final byte[] payload = new byte[98];
    new Random().nextBytes(payload);
    sendRawUdp(payload, transport2.getBoundAddress(StandardProtocolFamily.INET));

    final Bytes expected = Bytes.wrap(payload);
    Awaitility.await()
        .atMost(2, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(received.get()).isEqualTo(expected));
  }

  @Test
  public void v5Packet_isPublishedOnIncomingPacketFlux() throws Exception {
    transport1 =
        SharedDiscoveryTransport.builder()
            .ipv4BindAddress(ephemeral())
            .maskingKey(MASKING_KEY)
            .v4Enabled(true)
            .v5Enabled(true)
            .build();

    transport1.start().get(5, TimeUnit.SECONDS);

    final AtomicReference<Envelope> received = new AtomicReference<>();
    Flux.from(transport1.getV5IncomingPackets(StandardProtocolFamily.INET))
        .subscribe(received::set);

    sendRawUdp(buildV5Packet(), transport1.getBoundAddress(StandardProtocolFamily.INET));

    Awaitility.await()
        .atMost(2, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(received.get()).isNotNull());
    final Object incoming = received.get().get(Field.INCOMING);
    assertThat(incoming).isInstanceOf(Bytes.class);
  }

  @Test
  public void v5PacketDispatch_runsOnDedicatedThread_notOnSharedEventLoop() throws Exception {
    transport1 =
        SharedDiscoveryTransport.builder()
            .ipv4BindAddress(ephemeral())
            .maskingKey(MASKING_KEY)
            .v4Enabled(true)
            .v5Enabled(true)
            .build();
    transport1.start().get(5, TimeUnit.SECONDS);

    final AtomicReference<String> dispatchThreadName = new AtomicReference<>();
    Flux.from(transport1.getV5IncomingPackets(StandardProtocolFamily.INET))
        .subscribe(env -> dispatchThreadName.set(Thread.currentThread().getName()));

    sendRawUdp(buildV5Packet(), transport1.getBoundAddress(StandardProtocolFamily.INET));

    Awaitility.await()
        .atMost(2, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(dispatchThreadName.get()).isNotNull());
    assertThat(dispatchThreadName.get()).isEqualTo("disc-v5-dispatch");
  }

  @Test
  public void slowV5Subscriber_doesNotBlockConcurrentV4Delivery() throws Exception {
    transport1 =
        SharedDiscoveryTransport.builder()
            .ipv4BindAddress(ephemeral())
            .maskingKey(MASKING_KEY)
            .v4Enabled(true)
            .v5Enabled(true)
            .build();

    final AtomicReference<Bytes> receivedV4 = new AtomicReference<>();
    transport1.setV4Handler((sender, data) -> receivedV4.set(data));
    transport1.start().get(5, TimeUnit.SECONDS);

    final CountDownLatch releaseV5Subscriber = new CountDownLatch(1);
    Flux.from(transport1.getV5IncomingPackets(StandardProtocolFamily.INET))
        .subscribe(
            env -> {
              try {
                releaseV5Subscriber.await(5, TimeUnit.SECONDS);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });

    final InetSocketAddress bound = transport1.getBoundAddress(StandardProtocolFamily.INET);
    // Blocks the dedicated V5 dispatch thread until the latch is released - would also block
    // the V4 packet sent next if V5 still shared the event-loop thread with it.
    sendRawUdp(buildV5Packet(), bound);

    final byte[] v4Payload = new byte[98];
    new Random().nextBytes(v4Payload);
    sendRawUdp(v4Payload, bound);

    try {
      final Bytes expected = Bytes.wrap(v4Payload);
      Awaitility.await()
          .atMost(2, TimeUnit.SECONDS)
          .untilAsserted(() -> assertThat(receivedV4.get()).isEqualTo(expected));
    } finally {
      releaseV5Subscriber.countDown();
    }
  }

  @Test
  public void v5PacketsArrivingBeforeSubscribe_areAllReplayed_notJustTheLast() throws Exception {
    transport1 =
        SharedDiscoveryTransport.builder()
            .ipv4BindAddress(ephemeral())
            .maskingKey(MASKING_KEY)
            .v4Enabled(true)
            .v5Enabled(true)
            .build();
    transport1.start().get(5, TimeUnit.SECONDS);

    final InetSocketAddress bound = transport1.getBoundAddress(StandardProtocolFamily.INET);
    final int packetCount = 5;
    for (int i = 0; i < packetCount; i++) {
      sendRawUdp(buildV5Packet(), bound);
    }
    // Give the dispatch executor time to push these into the sink before subscribing - otherwise
    // this could pass by accident even without a bounded replay buffer.
    Thread.sleep(200);

    final List<Envelope> received = new CopyOnWriteArrayList<>();
    Flux.from(transport1.getV5IncomingPackets(StandardProtocolFamily.INET))
        .subscribe(received::add);

    Awaitility.await()
        .atMost(2, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(received).hasSize(packetCount));
  }

  @Test
  public void stop_isIdempotent() throws Exception {
    transport1 =
        SharedDiscoveryTransport.builder()
            .ipv4BindAddress(ephemeral())
            .maskingKey(MASKING_KEY)
            .v4Enabled(true)
            .v5Enabled(false)
            .build();
    transport1.start().get(5, TimeUnit.SECONDS);

    transport1.stop().get(5, TimeUnit.SECONDS);
    assertThat(transport1.stop()).succeedsWithin(Duration.ofSeconds(5));
  }

  @Test
  public void stop_withoutStart_doesNotThrow() {
    transport1 =
        SharedDiscoveryTransport.builder()
            .ipv4BindAddress(ephemeral())
            .maskingKey(MASKING_KEY)
            .v4Enabled(true)
            .v5Enabled(false)
            .build();

    assertThat(transport1.stop()).succeedsWithin(Duration.ofSeconds(5));
  }

  @Test
  public void start_afterStop_doesNotLeakEventLoopGroup() throws Exception {
    transport1 =
        SharedDiscoveryTransport.builder()
            .ipv4BindAddress(ephemeral())
            .maskingKey(MASKING_KEY)
            .v4Enabled(true)
            .v5Enabled(false)
            .build();

    // stop() before start() is the no-op path (stop_withoutStart_doesNotThrow); start() must not
    // then proceed as if nothing happened - the same guard protects against stop() racing a
    // concurrent, in-flight start() in production.
    transport1.stop().get(5, TimeUnit.SECONDS);

    assertThat(transport1.start()).failsWithin(Duration.ofSeconds(5));
    assertThat(transport1.getChannel(StandardProtocolFamily.INET)).isEmpty();
    // Regression guard: start() creating an event loop group that nothing ever shuts down,
    // since stop() is a one-shot CAS and no-ops the second time.
    assertThat(transport1.eventLoopGroupForTesting().isShuttingDown()).isTrue();
  }

  @Test
  public void start_afterBindFailure_releasesEventLoopForRetry() throws Exception {
    transport1 =
        SharedDiscoveryTransport.builder()
            .ipv4BindAddress(ephemeral())
            .maskingKey(MASKING_KEY)
            .v4Enabled(true)
            .v5Enabled(false)
            .build();
    final InetSocketAddress bound =
        transport1
            .start()
            .thenApply(v -> transport1.getBoundAddress(StandardProtocolFamily.INET))
            .get(5, TimeUnit.SECONDS);

    // transport2 tries to bind the port transport1 already holds, so start() fails.
    transport2 =
        SharedDiscoveryTransport.builder()
            .ipv4BindAddress(bound)
            .maskingKey(MASKING_KEY)
            .v4Enabled(true)
            .v5Enabled(false)
            .build();
    assertThat(transport2.start()).failsWithin(Duration.ofSeconds(5));

    // Freeing the port and retrying with a fresh transport proves the failed bind didn't
    // leak transport2's event loop group or leave the port unusable.
    transport1.stop().get(5, TimeUnit.SECONDS);
    transport1 = null;

    final SharedDiscoveryTransport transport3 =
        SharedDiscoveryTransport.builder()
            .ipv4BindAddress(bound)
            .maskingKey(MASKING_KEY)
            .v4Enabled(true)
            .v5Enabled(false)
            .build();
    try {
      transport3.start().get(5, TimeUnit.SECONDS);
      assertThat(transport3.getBoundAddress(StandardProtocolFamily.INET).getPort())
          .isEqualTo(bound.getPort());
    } finally {
      transport3.stop().get(5, TimeUnit.SECONDS);
    }
  }

  private static void sendRawUdp(final byte[] payload, final InetSocketAddress destination)
      throws Exception {
    try (DatagramSocket socket = new DatagramSocket()) {
      socket.send(new DatagramPacket(payload, payload.length, destination));
    }
  }

  private static byte[] buildV5Packet() throws Exception {
    final byte[] iv = new byte[16];
    new Random().nextBytes(iv);

    final Cipher cipher = Cipher.getInstance("AES/CTR/NoPadding");
    cipher.init(
        Cipher.ENCRYPT_MODE, new SecretKeySpec(MASKING_KEY, "AES"), new IvParameterSpec(iv));
    final byte[] maskedHeader = cipher.doFinal(DISCV5_MAGIC);

    final byte[] packet = new byte[100];
    System.arraycopy(iv, 0, packet, 0, 16);
    System.arraycopy(maskedHeader, 0, packet, 16, 8);
    return packet;
  }
}
