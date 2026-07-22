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

import org.hyperledger.besu.ethereum.p2p.discovery.discv4.PeerDiscoveryAgentV4;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Netty inbound handler that demultiplexes incoming UDP packets between DiscV4 and DiscV5 protocols
 * on a shared channel.
 *
 * <p>V5 detection: AES/CTR/NoPadding decrypt {@code packet[16:24]} with key = {@code
 * homeNodeId[:16]} and iv = {@code packet[:16]}. A match against the 8-byte magic {@code "discv5" +
 * 0x0001} identifies a V5 packet. Non-matching packets of at least 98 bytes are routed to V4.
 */
final class SharedDiscoveryDemuxHandler extends SimpleChannelInboundHandler<DatagramPacket> {

  private static final Logger LOG = LoggerFactory.getLogger(SharedDiscoveryDemuxHandler.class);

  // "discv5" (6 bytes) + version 0x0001 (2 bytes)
  private static final byte[] DISCV5_MAGIC = {0x64, 0x69, 0x73, 0x63, 0x76, 0x35, 0x00, 0x01};
  private static final int MASKING_IV_SIZE = 16;
  // V5 minimum per spec; V4 minimum is hash(32)+sig(64)+type(1)+data = 98
  private static final int MIN_PACKET_SIZE = 63;
  private static final int MIN_V4_PACKET_SIZE = 98;

  private final boolean v4Enabled;
  private final boolean v5Enabled;
  private final BiConsumer<InetSocketAddress, Bytes> v4Sink;
  private final Consumer<DatagramPacket> v5Sink;

  // Re-used AES/CTR cipher — handler is per-channel, Netty guarantees single-thread access.
  // The IV changes per packet but the key is constant, so build the SecretKeySpec once.
  private final Cipher cipher;
  private final SecretKeySpec secretKey;

  private final DemuxCounters counters;

  SharedDiscoveryDemuxHandler(
      final boolean v4Enabled,
      final boolean v5Enabled,
      final byte[] maskingKey,
      final BiConsumer<InetSocketAddress, Bytes> v4Sink,
      final Consumer<DatagramPacket> v5Sink,
      final DemuxCounters counters) {
    this.v4Enabled = v4Enabled;
    this.v5Enabled = v5Enabled;
    this.v4Sink = v4Sink;
    this.v5Sink = v5Sink;
    this.counters = counters;
    if (v5Enabled && maskingKey != null) {
      try {
        this.cipher = Cipher.getInstance("AES/CTR/NoPadding");
        this.secretKey = new SecretKeySpec(Arrays.copyOf(maskingKey, 16), "AES");
      } catch (final Exception e) {
        throw new IllegalStateException("Failed to create AES/CTR cipher for V5 detection", e);
      }
    } else {
      this.cipher = null;
      this.secretKey = null;
    }
  }

  @Override
  protected void channelRead0(final ChannelHandlerContext ctx, final DatagramPacket msg) {
    final int size = msg.content().readableBytes();
    if (size < MIN_PACKET_SIZE) {
      LOG.trace("Dropping too-small packet ({} bytes) from {}", size, msg.sender());
      return;
    }

    if (v5Enabled && isV5Packet(msg.content())) {
      counters.v5().inc();
      if (v5Sink != null) {
        msg.retain();
        v5Sink.accept(msg);
      }
    } else if (v4Enabled && size >= MIN_V4_PACKET_SIZE) {
      counters.v4().inc();
      if (size > PeerDiscoveryAgentV4.MAX_PACKET_SIZE_BYTES) {
        // Drop before allocating/copying - a UDP datagram can be up to ~64KiB, not worth
        // duplicating in memory just to be discarded by this same size check downstream.
        LOG.trace("Discarding over-sized V4 packet ({} bytes) from {}", size, msg.sender());
        return;
      }
      if (v4Sink != null) {
        final byte[] bytes = new byte[size];
        msg.content().getBytes(msg.content().readerIndex(), bytes);
        v4Sink.accept(msg.sender(), Bytes.wrap(bytes));
      }
    } else {
      counters.dropped().inc();
      LOG.trace("Dropping unrecognized packet ({} bytes) from {}", size, msg.sender());
    }
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
    LOG.debug("SharedDiscoveryDemuxHandler exception", cause);
  }

  /** Returns {@code true} if the packet is a V5 packet based on its masked header prefix. */
  private boolean isV5Packet(final ByteBuf buf) {
    if (buf.readableBytes() < MASKING_IV_SIZE + DISCV5_MAGIC.length) {
      return false;
    }
    try {
      final byte[] iv = new byte[MASKING_IV_SIZE];
      buf.getBytes(buf.readerIndex(), iv);
      final byte[] maskedHeader = new byte[DISCV5_MAGIC.length];
      buf.getBytes(buf.readerIndex() + MASKING_IV_SIZE, maskedHeader);

      cipher.init(Cipher.DECRYPT_MODE, secretKey, new IvParameterSpec(iv));
      final byte[] decrypted = cipher.doFinal(maskedHeader);
      return Arrays.equals(decrypted, DISCV5_MAGIC);
    } catch (final Exception e) {
      return false;
    }
  }
}
