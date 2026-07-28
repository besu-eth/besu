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

import org.hyperledger.besu.ethereum.p2p.discovery.PeerDiscoveryServiceException;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.PeerDiscoveryAgentV4;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.Transport;
import org.hyperledger.besu.ethereum.p2p.discovery.transport.SharedDiscoveryTransport;

import java.io.IOException;
import java.net.BindException;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.StandardProtocolFamily;
import java.nio.channels.UnsupportedAddressTypeException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFactory;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.SocketProtocolFamily;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.Future;
import org.apache.tuweni.bytes.Bytes;
import org.ethereum.beacon.discovery.util.DecodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Netty-backed {@link Transport}. Two construction modes:
 *
 * <ul>
 *   <li><b>Shared-channel</b> ({@link #createShared}): borrows a channel from a {@link
 *       SharedDiscoveryTransport}, which is bound and owned externally — used whenever DiscV4 and
 *       DiscV5 demux off the same UDP socket.
 *   <li><b>Owned-channel</b> ({@link #create}): binds and owns its own UDP socket. Used by tests
 *       that exercise this transport in isolation.
 * </ul>
 */
public final class NettyTransport implements Transport {

  private static final Logger LOG = LoggerFactory.getLogger(NettyTransport.class);

  // Matches the logger LoggingHandler(LogLevel) itself logs through (Netty's InternalLogger for
  // LoggingHandler.class), so this check reflects exactly whether that handler would actually
  // produce output before paying its per-packet overhead to install/invoke it.
  private static final Logger NETTY_LOGGING_HANDLER_LOG =
      LoggerFactory.getLogger(LoggingHandler.class);

  // Netty's parameterless shutdownGracefully() defaults to a 2s quiet period (plus a 15s
  // timeout), unconditionally waited out even when the event loop is already idle. Nothing is
  // legitimately in flight by the time this transport shuts down its (single-thread, dedicated)
  // event loop group, so use a much shorter, explicit quiet period/timeout instead.
  private static final long SHUTDOWN_QUIET_PERIOD_MS = 0;
  private static final long SHUTDOWN_TIMEOUT_MS = 500;

  // Owned-channel mode field (null in shared mode).
  private final InetSocketAddress bindAddress;

  // Shared-channel mode field (null in owned mode).
  private final SharedDiscoveryTransport sharedTransport;

  // Lazily created in start() so an unstarted transport doesn't pay for an idle event-loop
  // thread. Owned mode only - shared mode has SharedDiscoveryTransport own the event loop group.
  private volatile EventLoopGroup eventLoopGroup;

  private volatile NioDatagramChannel channel; // owned mode only
  private volatile InboundV4Handler inboundHandler;

  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicBoolean stopped = new AtomicBoolean(false);

  private NettyTransport(final InetSocketAddress bindAddress) {
    this.bindAddress = bindAddress;
    this.sharedTransport = null;
  }

  private NettyTransport(final SharedDiscoveryTransport sharedTransport) {
    this.bindAddress = null;
    this.sharedTransport = sharedTransport;
  }

  public static NettyTransport create(final InetSocketAddress bindAddress) {
    return new NettyTransport(bindAddress);
  }

  /** Creates a transport that borrows its channel from a {@link SharedDiscoveryTransport}. */
  public static NettyTransport createShared(final SharedDiscoveryTransport sharedTransport) {
    return new NettyTransport(sharedTransport);
  }

  @Override
  public void setInboundHandler(final InboundV4Handler handler) {
    this.inboundHandler = handler;
    if (sharedTransport != null) {
      sharedTransport.setV4Handler(handler::onPacket);
    }
  }

  @Override
  public Optional<InetSocketAddress> getIpv6BoundAddress() {
    if (sharedTransport == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(sharedTransport.getBoundAddress(StandardProtocolFamily.INET6));
  }

  @Override
  public CompletableFuture<InetSocketAddress> start() {
    if (stopped.get()) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("NettyTransport already stopped"));
    }
    if (!started.compareAndSet(false, true)) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("NettyTransport already started"));
    }

    if (sharedTransport != null) {
      // Prefer IPv4, falling back to IPv6 for a single-stack IPv6-only bind (a legitimate
      // config that just can't reach an IPv4 recipient - that surfaces per-send, not here).
      InetSocketAddress bound = sharedTransport.getBoundAddress(StandardProtocolFamily.INET);
      if (bound == null) {
        bound = sharedTransport.getBoundAddress(StandardProtocolFamily.INET6);
      }
      if (bound == null) {
        return CompletableFuture.failedFuture(
            new IllegalStateException(
                "SharedDiscoveryTransport has no channel bound - was transport.start() called "
                    + "first?"));
      }
      LOG.debug("DiscV4 shared transport ready on {}", bound);
      return CompletableFuture.completedFuture(bound);
    }

    this.eventLoopGroup =
        new MultiThreadIoEventLoopGroup(
            1, (ThreadFactory) r -> new Thread(r, "discv4-eventloop"), NioIoHandler.newFactory());

    // Pin the channel's family to match the configured bind address explicitly, rather than
    // letting Netty infer it: on Java 17+, the default NioDatagramChannel opens an IPv6 socket
    // even for a 0.0.0.0 (IPv4) bind, which then rejects that bind.
    final SocketProtocolFamily family =
        bindAddress.getAddress() instanceof Inet6Address
            ? SocketProtocolFamily.INET6
            : SocketProtocolFamily.INET;

    final CompletableFuture<InetSocketAddress> future = new CompletableFuture<>();
    final Bootstrap bootstrap = new Bootstrap();
    bootstrap
        .group(eventLoopGroup)
        .channelFactory((ChannelFactory<NioDatagramChannel>) () -> new NioDatagramChannel(family))
        .handler(
            new ChannelInitializer<NioDatagramChannel>() {
              @Override
              protected void initChannel(final NioDatagramChannel ch) {
                final ChannelPipeline pipeline = ch.pipeline();
                if (NETTY_LOGGING_HANDLER_LOG.isTraceEnabled()) {
                  pipeline.addFirst(new LoggingHandler(LogLevel.TRACE));
                }
                pipeline.addLast(new V4InboundHandler());
              }
            });

    final ChannelFuture bindFuture = bootstrap.bind(bindAddress);
    bindFuture.addListener(
        result -> {
          if (!result.isSuccess()) {
            shutdownEventLoopGroup(eventLoopGroup);
            this.eventLoopGroup = null;
            started.set(false);
            Throwable cause = result.cause();
            if (cause instanceof BindException || cause instanceof SocketException) {
              cause =
                  new PeerDiscoveryServiceException(
                      String.format(
                          "Failed to bind Ethereum UDP discovery listener to %s:%d: %s",
                          bindAddress.getHostString(), bindAddress.getPort(), cause.getMessage()));
            }
            future.completeExceptionally(cause);
            return;
          }
          this.channel = (NioDatagramChannel) bindFuture.channel();
          if (stopped.get()) {
            // stop() ran while this bind was in flight and saw no channel to close, so it
            // already shut down the event loop group. Tear down the now-bound channel too,
            // instead of reporting a successful start after stop() already reported done.
            channel.close();
            shutdownEventLoopGroup(eventLoopGroup);
            future.completeExceptionally(
                new IllegalStateException("NettyTransport stopped during start()"));
            return;
          }
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
      final StandardProtocolFamily family =
          recipient.getAddress() instanceof Inet6Address
              ? StandardProtocolFamily.INET6
              : StandardProtocolFamily.INET;
      final Optional<NioDatagramChannel> maybeChannel = sharedTransport.getChannel(family);
      if (maybeChannel.isEmpty()) {
        // No channel bound for this family (e.g. an IPv6 peer on an IPv4-only shared transport)
        // is expected, not a failure - the caller quiet-traces UnsupportedAddressTypeException.
        return CompletableFuture.failedFuture(new UnsupportedAddressTypeException());
      }
      ch = maybeChannel.get();
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
    LOG.info("Stopping DiscV4 UDP transport");
    final EventLoopGroup group = this.eventLoopGroup;
    if (group == null) {
      // start() was never called (e.g. discovery disabled) - nothing to stop.
      LOG.info("DiscV4 UDP transport was never started; nothing to stop");
      return CompletableFuture.completedFuture(null);
    }
    final NioDatagramChannel ch = this.channel;
    if (ch == null || !ch.isOpen()) {
      return toFuture(shutdownEventLoopGroup(group))
          .whenComplete((v, ex) -> LOG.info("DiscV4 event loop group shut down"));
    }
    return closeChannelThenShutdownGroup(ch.close(), group);
  }

  /**
   * Shuts down {@code group} unconditionally once {@code closeFuture} completes, regardless of
   * whether the channel close itself succeeded — a failed close must not leak the event loop
   * thread.
   */
  @VisibleForTesting
  static CompletableFuture<Void> closeChannelThenShutdownGroup(
      final Future<?> closeFuture, final EventLoopGroup group) {
    return toFuture(closeFuture)
        .handle(
            (v, ex) -> {
              if (ex != null) {
                LOG.warn("Failed to close DiscV4 UDP channel", ex);
              } else {
                LOG.info("DiscV4 UDP channel closed");
              }
              return null;
            })
        .thenCompose(v -> toFuture(shutdownEventLoopGroup(group)))
        .whenComplete((v, ex) -> LOG.info("DiscV4 event loop group shut down"));
  }

  private static Future<?> shutdownEventLoopGroup(final EventLoopGroup group) {
    return group.shutdownGracefully(
        SHUTDOWN_QUIET_PERIOD_MS, SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  private static CompletableFuture<Void> toFuture(final Future<?> f) {
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
      final int size = msg.content().readableBytes();
      if (size > PeerDiscoveryAgentV4.MAX_PACKET_SIZE_BYTES) {
        // Drop before allocating/copying: a UDP datagram can be up to ~64KiB, so an oversized
        // packet would otherwise always be duplicated in memory even though it's immediately
        // discarded once the agent applies this same size check.
        LOG.trace("Discarding over-sized packet. Actual size (bytes): {}", size);
        return;
      }
      final InetSocketAddress sender = msg.sender();
      // Copy before SimpleChannelInboundHandler releases the buffer
      final byte[] bytes = new byte[size];
      msg.content().readBytes(bytes);
      handler.onPacket(sender, Bytes.wrap(bytes));
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
      if (cause instanceof IOException || cause instanceof DecodeException) {
        LOG.debug("DiscV4 inbound handler exception", cause);
      } else {
        LOG.error("DiscV4 inbound handler exception", cause);
      }
    }
  }
}
