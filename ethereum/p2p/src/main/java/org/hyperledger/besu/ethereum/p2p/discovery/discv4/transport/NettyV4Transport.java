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

import org.hyperledger.besu.ethereum.p2p.discovery.discv4.V4Transport;
import org.hyperledger.besu.ethereum.p2p.discovery.transport.SharedDiscoveryTransport;

import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.InternetProtocolFamily;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Netty-backed {@link V4Transport}. Two construction modes:
 *
 * <ul>
 *   <li><b>Shared-channel</b> ({@link #createShared}): borrows channels from a {@link
 *       SharedDiscoveryTransport}. The sole production path — DiscV4 always runs on the shared UDP
 *       socket, even when DiscV5 self-disables on a non-secp256k1 key.
 *   <li><b>Owned-channel</b> ({@link #createOwnedChannel}): binds and owns its own UDP socket.
 *       Retained for unit tests of the Netty transport in isolation.
 * </ul>
 */
public final class NettyV4Transport implements V4Transport {

  private static final Logger LOG = LoggerFactory.getLogger(NettyV4Transport.class);

  // Owned-channel mode fields (null in shared mode)
  private final InetSocketAddress bindAddress;
  private final NioEventLoopGroup eventLoopGroup;

  // Shared-channel mode field (null in owned mode)
  private final SharedDiscoveryTransport sharedTransport;

  private volatile NioDatagramChannel channel; // owned mode only
  private volatile InboundV4Handler inboundHandler;

  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicBoolean stopped = new AtomicBoolean(false);

  private NettyV4Transport(final InetSocketAddress bindAddress) {
    this.bindAddress = bindAddress;
    this.eventLoopGroup =
        new NioEventLoopGroup(1, (ThreadFactory) r -> new Thread(r, "discv4-eventloop"));
    this.sharedTransport = null;
  }

  private NettyV4Transport(final SharedDiscoveryTransport sharedTransport) {
    this.bindAddress = null;
    this.eventLoopGroup = null;
    this.sharedTransport = sharedTransport;
  }

  /**
   * Creates a transport that owns and manages its own UDP channel. Retained for tests; production
   * code uses {@link #createShared(SharedDiscoveryTransport)}.
   */
  @VisibleForTesting
  public static NettyV4Transport createOwnedChannel(final InetSocketAddress bindAddress) {
    return new NettyV4Transport(bindAddress);
  }

  /**
   * Creates a transport that borrows channels from a {@link SharedDiscoveryTransport} (BOTH mode).
   */
  public static NettyV4Transport createShared(final SharedDiscoveryTransport transport) {
    return new NettyV4Transport(transport);
  }

  @Override
  public void setInboundHandler(final InboundV4Handler handler) {
    this.inboundHandler = handler;
    if (sharedTransport != null) {
      // Register with shared transport so the demux routes V4 packets here
      sharedTransport.setV4Handler(handler::onPacket);
    }
  }

  @Override
  public CompletableFuture<InetSocketAddress> start() {
    if (!started.compareAndSet(false, true)) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("NettyV4Transport already started"));
    }
    if (sharedTransport != null) {
      // Shared mode: transport is already bound (started by CompositePeerDiscoveryAgent)
      final InetSocketAddress bound = sharedTransport.getBoundAddress(InternetProtocolFamily.IPv4);
      if (bound == null) {
        return CompletableFuture.failedFuture(
            new IllegalStateException(
                "SharedDiscoveryTransport IPv4 channel not bound — was transport.start() called?"));
      }
      LOG.debug("DiscV4 shared transport ready on {}", bound);
      return CompletableFuture.completedFuture(bound);
    }

    final CompletableFuture<InetSocketAddress> future = new CompletableFuture<>();
    final Bootstrap bootstrap = new Bootstrap();
    bootstrap
        .group(eventLoopGroup)
        // DiscV4 is IPv4-only; pin the family explicitly so 0.0.0.0 binds don't
        // open an IPv6 socket on Java 17+ (per Netty default).
        .channelFactory(
            (io.netty.channel.ChannelFactory<NioDatagramChannel>)
                () -> new NioDatagramChannel(InternetProtocolFamily.IPv4))
        .handler(
            new ChannelInitializer<NioDatagramChannel>() {
              @Override
              protected void initChannel(final NioDatagramChannel ch) {
                final ChannelPipeline pipeline = ch.pipeline();
                pipeline.addFirst(new LoggingHandler(LogLevel.TRACE));
                pipeline.addLast(new V4InboundHandler());
              }
            });

    final ChannelFuture bindFuture = bootstrap.bind(bindAddress);
    bindFuture.addListener(
        result -> {
          if (!result.isSuccess()) {
            eventLoopGroup.shutdownGracefully();
            future.completeExceptionally(result.cause());
            return;
          }
          this.channel = (NioDatagramChannel) bindFuture.channel();
          final InetSocketAddress bound = channel.localAddress();
          LOG.info(
              "DiscV4 UDP transport started, listening on {}:{}",
              bound.getHostString(),
              bound.getPort());
          future.complete(bound);
        });
    return future;
  }

  @Override
  public CompletableFuture<Void> send(final InetSocketAddress recipient, final Bytes data) {
    final NioDatagramChannel ch;
    if (sharedTransport != null) {
      final InternetProtocolFamily family =
          recipient.getAddress() instanceof Inet6Address
              ? InternetProtocolFamily.IPv6
              : InternetProtocolFamily.IPv4;
      ch = sharedTransport.getChannel(family).orElse(null);
    } else {
      ch = this.channel;
    }
    if (ch == null || !ch.isActive()) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("Transport is not started or already stopped"));
    }
    final CompletableFuture<Void> future = new CompletableFuture<>();
    ch.writeAndFlush(new DatagramPacket(Unpooled.wrappedBuffer(data.toArray()), recipient))
        .addListener(
            result -> {
              if (result.isSuccess()) {
                future.complete(null);
              } else {
                future.completeExceptionally(result.cause());
              }
            });
    return future;
  }

  @Override
  public CompletableFuture<Void> stop() {
    if (!stopped.compareAndSet(false, true)) {
      return CompletableFuture.completedFuture(null);
    }
    if (sharedTransport != null) {
      // Shared mode: lifecycle owned by SharedDiscoveryTransport / CompositePeerDiscoveryAgent
      return CompletableFuture.completedFuture(null);
    }
    final NioDatagramChannel ch = this.channel;
    if (ch == null || !ch.isOpen()) {
      return toFuture(eventLoopGroup.shutdownGracefully());
    }
    return toFuture(ch.close()).thenCompose(v -> toFuture(eventLoopGroup.shutdownGracefully()));
  }

  private static CompletableFuture<Void> toFuture(final io.netty.util.concurrent.Future<?> f) {
    final CompletableFuture<Void> cf = new CompletableFuture<>();
    f.addListener(
        result -> {
          if (result.isSuccess()) {
            cf.complete(null);
          } else {
            cf.completeExceptionally(result.cause());
          }
        });
    return cf;
  }

  private final class V4InboundHandler extends SimpleChannelInboundHandler<DatagramPacket> {

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final DatagramPacket msg) {
      final InboundV4Handler handler = inboundHandler;
      if (handler == null) {
        return;
      }
      final InetSocketAddress sender = msg.sender();
      // Copy before SimpleChannelInboundHandler releases the buffer
      final byte[] bytes = new byte[msg.content().readableBytes()];
      msg.content().readBytes(bytes);
      handler.onPacket(sender, Bytes.wrap(bytes));
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
      LOG.debug("DiscV4 inbound handler exception", cause);
    }
  }
}
