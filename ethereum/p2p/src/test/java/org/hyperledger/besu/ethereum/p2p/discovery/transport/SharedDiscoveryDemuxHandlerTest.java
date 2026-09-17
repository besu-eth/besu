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

import org.hyperledger.besu.ethereum.p2p.discovery.discv4.PeerDiscoveryAgentV4;
import org.hyperledger.besu.plugin.services.metrics.Counter;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.socket.DatagramPacket;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

public class SharedDiscoveryDemuxHandlerTest {

  private static final byte[] MASKING_KEY = new byte[16];
  private static final byte[] DISCV5_MAGIC = {0x64, 0x69, 0x73, 0x63, 0x76, 0x35, 0x00, 0x01};
  private static final InetSocketAddress SENDER =
      new InetSocketAddress(InetAddress.getLoopbackAddress(), 12345);

  static {
    new Random().nextBytes(MASKING_KEY);
  }

  private final List<Bytes> v4Received = new CopyOnWriteArrayList<>();
  private final List<byte[]> v5Received = new CopyOnWriteArrayList<>();

  private final AtomicLong v4Count = new AtomicLong();
  private final AtomicLong v5Count = new AtomicLong();
  private final AtomicLong droppedCount = new AtomicLong();
  private final DemuxCounters counters =
      new DemuxCounters(
          countingCounter(v4Count), countingCounter(v5Count), countingCounter(droppedCount));

  private static Counter countingCounter(final AtomicLong counter) {
    return new Counter() {
      @Override
      public void inc() {
        counter.incrementAndGet();
      }

      @Override
      public void inc(final long amount) {
        counter.addAndGet(amount);
      }
    };
  }

  private EmbeddedChannel newChannel(final boolean v4Enabled, final boolean v5Enabled) {
    return new EmbeddedChannel(
        new SharedDiscoveryDemuxHandler(
            v4Enabled,
            v5Enabled,
            v5Enabled ? MASKING_KEY : null,
            v4Enabled ? (sender, data) -> v4Received.add(data) : null,
            v5Enabled ? pkt -> v5Received.add(readAllBytes(pkt)) : null,
            counters));
  }

  private static byte[] readAllBytes(final DatagramPacket pkt) {
    final byte[] bytes = new byte[pkt.content().readableBytes()];
    pkt.content().getBytes(pkt.content().readerIndex(), bytes);
    pkt.release();
    return bytes;
  }

  /**
   * Builds a packet whose masked header decrypts to the discv5 magic under {@link #MASKING_KEY}.
   */
  private static DatagramPacket buildV5Packet(final int totalSize) throws Exception {
    final byte[] iv = new byte[16];
    new Random().nextBytes(iv);

    final Cipher cipher = Cipher.getInstance("AES/CTR/NoPadding");
    cipher.init(
        Cipher.ENCRYPT_MODE, new SecretKeySpec(MASKING_KEY, "AES"), new IvParameterSpec(iv));
    final byte[] maskedHeader = cipher.doFinal(DISCV5_MAGIC);

    final ByteBuf buf = Unpooled.buffer(totalSize);
    buf.writeBytes(iv);
    buf.writeBytes(maskedHeader);
    while (buf.readableBytes() < totalSize) {
      buf.writeByte(0);
    }
    return new DatagramPacket(buf, null, SENDER);
  }

  private static DatagramPacket randomPacket(final int size) {
    final byte[] bytes = new byte[size];
    new Random().nextBytes(bytes);
    return new DatagramPacket(Unpooled.wrappedBuffer(bytes), null, SENDER);
  }

  @Test
  public void dropsPacketBelowMinimumSize() {
    final EmbeddedChannel channel = newChannel(true, true);
    channel.writeInbound(randomPacket(62));

    assertThat(v4Received).isEmpty();
    assertThat(v5Received).isEmpty();
  }

  @Test
  public void routesV5PacketToV5Sink() throws Exception {
    final EmbeddedChannel channel = newChannel(true, true);
    channel.writeInbound(buildV5Packet(100));

    assertThat(v5Received).hasSize(1);
    assertThat(v4Received).isEmpty();
  }

  @Test
  public void routesV4SizedPacketToV4Sink() {
    final EmbeddedChannel channel = newChannel(true, true);
    final byte[] payload = new byte[98];
    new Random().nextBytes(payload);
    channel.writeInbound(new DatagramPacket(Unpooled.wrappedBuffer(payload), null, SENDER));

    assertThat(v4Received).containsExactly(Bytes.wrap(payload));
    assertThat(v5Received).isEmpty();
  }

  @Test
  public void dropsPacketSmallerThanV4MinimumWhenNotV5Shaped() {
    final EmbeddedChannel channel = newChannel(true, true);
    // Between the 63-byte overall floor and the 98-byte DiscV4 floor.
    channel.writeInbound(randomPacket(80));

    assertThat(v4Received).isEmpty();
    assertThat(v5Received).isEmpty();
  }

  @Test
  public void dropsOversizedV4PacketBeforeDispatch() {
    final EmbeddedChannel channel = newChannel(true, true);
    channel.writeInbound(randomPacket(PeerDiscoveryAgentV4.MAX_PACKET_SIZE_BYTES + 1));

    assertThat(v4Received).isEmpty();
  }

  @Test
  public void v4Disabled_neverDispatchesV4SizedPacket() {
    final EmbeddedChannel channel = newChannel(false, true);
    channel.writeInbound(randomPacket(98));

    assertThat(v4Received).isEmpty();
  }

  @Test
  public void v5Disabled_v5ShapedPacketFallsThroughToV4() throws Exception {
    // v5Enabled=false means no cipher/key at all (mirrors SharedDiscoveryTransport, where
    // maskingKey is null unless v5Enabled), so a V5-shaped packet is just routed by size.
    final List<Bytes> v4Only = new CopyOnWriteArrayList<>();
    final EmbeddedChannel channel =
        new EmbeddedChannel(
            new SharedDiscoveryDemuxHandler(
                true, false, null, (sender, data) -> v4Only.add(data), null, counters));

    channel.writeInbound(buildV5Packet(100));

    assertThat(v4Only).hasSize(1);
  }

  @Test
  public void incrementsV5CounterOnV5Packet() throws Exception {
    final EmbeddedChannel channel = newChannel(true, true);
    channel.writeInbound(buildV5Packet(100));

    assertThat(v5Count.get()).isEqualTo(1);
    assertThat(v4Count.get()).isZero();
    assertThat(droppedCount.get()).isZero();
  }

  @Test
  public void incrementsV4CounterOnV4SizedPacket() {
    final EmbeddedChannel channel = newChannel(true, true);
    channel.writeInbound(randomPacket(98));

    assertThat(v4Count.get()).isEqualTo(1);
    assertThat(v5Count.get()).isZero();
    assertThat(droppedCount.get()).isZero();
  }

  @Test
  public void incrementsV4CounterEvenWhenOversized() {
    final EmbeddedChannel channel = newChannel(true, true);
    channel.writeInbound(randomPacket(PeerDiscoveryAgentV4.MAX_PACKET_SIZE_BYTES + 1));

    assertThat(v4Count.get()).isEqualTo(1);
    assertThat(v4Received).isEmpty();
  }

  @Test
  public void incrementsDroppedCounterOnUnrecognizedPacket() {
    final EmbeddedChannel channel = newChannel(true, true);
    channel.writeInbound(randomPacket(80));

    assertThat(droppedCount.get()).isEqualTo(1);
    assertThat(v4Count.get()).isZero();
    assertThat(v5Count.get()).isZero();
  }

  @Test
  public void incrementsDroppedCounterForTooSmallPacket() {
    final EmbeddedChannel channel = newChannel(true, true);
    channel.writeInbound(randomPacket(62));

    assertThat(v4Count.get()).isZero();
    assertThat(v5Count.get()).isZero();
    assertThat(droppedCount.get()).isEqualTo(1);
  }
}
