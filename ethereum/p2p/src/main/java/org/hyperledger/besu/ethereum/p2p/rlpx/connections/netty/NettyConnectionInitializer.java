/*
 * Copyright ConsenSys AG.
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

import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.ethereum.p2p.config.RlpxConfiguration;
import org.hyperledger.besu.ethereum.p2p.peers.LocalNode;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.rlpx.ConnectCallback;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.ConnectionInitializer;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnectionEventDispatcher;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerLookup;
import org.hyperledger.besu.ethereum.p2p.rlpx.framing.Framer;
import org.hyperledger.besu.ethereum.p2p.rlpx.framing.FramerProvider;
import org.hyperledger.besu.ethereum.p2p.rlpx.handshake.HandshakeSecrets;
import org.hyperledger.besu.ethereum.p2p.rlpx.handshake.Handshaker;
import org.hyperledger.besu.ethereum.p2p.rlpx.handshake.HandshakerProvider;
import org.hyperledger.besu.ethereum.p2p.rlpx.handshake.ecies.ECIESHandshaker;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.data.EnodeURL;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.util.Subscribers;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntSupplier;
import java.util.stream.StreamSupport;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import jakarta.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyConnectionInitializer
    implements ConnectionInitializer, HandshakerProvider, FramerProvider {

  private static final Logger LOG = LoggerFactory.getLogger(NettyConnectionInitializer.class);
  private static final int TIMEOUT_SECONDS = 10;

  private final NodeKey nodeKey;
  private final RlpxConfiguration config;
  private final LocalNode localNode;
  private final PeerConnectionEventDispatcher eventDispatcher;
  private final MetricsSystem metricsSystem;
  private final Subscribers<ConnectCallback> connectSubscribers = Subscribers.create();
  private final PeerLookup peerLookup;

  private volatile ChannelFuture server;
  private volatile ChannelFuture serverIpv6;
  private volatile CompletableFuture<ListeningAddresses> listeningAddressesFuture;
  private final EventLoopGroup boss = new NioEventLoopGroup(1);
  private final EventLoopGroup workers = new NioEventLoopGroup(10);
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicBoolean stopped = new AtomicBoolean(false);

  public NettyConnectionInitializer(
      final NodeKey nodeKey,
      final RlpxConfiguration config,
      final LocalNode localNode,
      final PeerConnectionEventDispatcher eventDispatcher,
      final MetricsSystem metricsSystem,
      final PeerLookup peerLookup) {
    this.nodeKey = nodeKey;
    this.config = config;
    this.localNode = localNode;
    this.eventDispatcher = eventDispatcher;
    this.metricsSystem = metricsSystem;
    this.peerLookup = peerLookup;

    metricsSystem.createIntegerGauge(
        BesuMetricCategory.NETWORK,
        "netty_workers_pending_tasks",
        "The number of pending tasks in the Netty workers event loop",
        pendingTaskCounter(workers));

    metricsSystem.createIntegerGauge(
        BesuMetricCategory.NETWORK,
        "netty_boss_pending_tasks",
        "The number of pending tasks in the Netty boss event loop",
        pendingTaskCounter(boss));
  }

  @Override
  public CompletableFuture<ListeningAddresses> start() {
    final CompletableFuture<ListeningAddresses> startupFuture = new CompletableFuture<>();
    if (!started.compareAndSet(false, true)) {
      startupFuture.completeExceptionally(
          new IllegalStateException(
              "Attempt to start an already started " + this.getClass().getSimpleName()));
      return startupFuture;
    }
    this.listeningAddressesFuture = startupFuture;

    this.server =
        new ServerBootstrap()
            .group(boss, workers)
            .channel(NioServerSocketChannel.class)
            .childHandler(inboundChannelInitializer())
            .bind(config.getBindHost(), config.getBindPort());

    server.addListener(
        future -> {
          final InetSocketAddress socketAddress =
              (InetSocketAddress) server.channel().localAddress();
          if (!future.isSuccess() || socketAddress == null) {
            final String message =
                String.format(
                    "Unable to start listening on %s:%s. Check for port conflicts.",
                    config.getBindHost(), config.getBindPort());
            startupFuture.completeExceptionally(new IllegalStateException(message, future.cause()));
            return;
          }

          // Bind IPv6 socket when dual-stack is configured, using the same shared event loops.
          // The outer future must not complete until the IPv6 bind is also resolved so that
          // callers receive both bound addresses atomically.
          if (config.isDualStackEnabled()) {
            final String ipv6Host = config.getBindHostIpv6().orElseThrow();
            final int ipv6Port = config.getBindPortIpv6().orElseThrow();
            this.serverIpv6 =
                new ServerBootstrap()
                    .group(boss, workers)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(inboundChannelInitializer())
                    .bind(ipv6Host, ipv6Port);
            serverIpv6.addListener(
                ipv6Future -> {
                  if (ipv6Future.isSuccess()) {
                    final InetSocketAddress ipv6Address =
                        (InetSocketAddress) serverIpv6.channel().localAddress();
                    startupFuture.complete(
                        new ListeningAddresses(socketAddress, Optional.of(ipv6Address)));
                  } else {
                    LOG.warn(
                        "Failed to bind IPv6 RLPx socket on {}:{}, continuing with IPv4 only",
                        ipv6Host,
                        ipv6Port,
                        ipv6Future.cause());
                    serverIpv6 = null;
                    startupFuture.complete(new ListeningAddresses(socketAddress, Optional.empty()));
                  }
                });
          } else {
            startupFuture.complete(new ListeningAddresses(socketAddress, Optional.empty()));
          }
        });

    return startupFuture;
  }

  @Override
  public CompletableFuture<Void> stop() {
    final CompletableFuture<Void> stoppedFuture = new CompletableFuture<>();
    if (!started.get() || !stopped.compareAndSet(false, true)) {
      stoppedFuture.completeExceptionally(
          new IllegalStateException("Illegal attempt to stop " + this.getClass().getSimpleName()));
      return stoppedFuture;
    }

    // Unblock any caller waiting on start() that hasn't completed yet.
    if (listeningAddressesFuture != null) {
      listeningAddressesFuture.completeExceptionally(
          new IllegalStateException("Connection initializer was stopped before startup completed"));
    }

    // If the server channel has not been assigned yet (stop() called between CAS and bind()),
    // shut down event loops directly — there is nothing to close.
    if (server == null) {
      shutdownEventLoops(stoppedFuture);
      return stoppedFuture;
    }

    // Wait for the IPv4 bind to settle before closing. If already done, fires immediately.
    server.addListener(
        (ChannelFuture ipv4BindFuture) -> {
          if (!ipv4BindFuture.isSuccess()) {
            // Bind failed — channel was never fully registered, nothing to close.
            shutdownEventLoops(stoppedFuture);
            return;
          }

          final CompletableFuture<Void> ipv4Close = new CompletableFuture<>();
          ipv4BindFuture
              .channel()
              .close()
              .addListener(
                  closeFuture -> {
                    if (closeFuture.isSuccess()) {
                      ipv4Close.complete(null);
                    } else {
                      ipv4Close.completeExceptionally(closeFuture.cause());
                    }
                  });

          // Read serverIpv6 here — the start() listener has already run (listener
          // ordering), so this is the final state.
          final ChannelFuture ipv6 = serverIpv6;
          if (ipv6 != null) {
            final CompletableFuture<Void> ipv6Close = new CompletableFuture<>();
            ipv6.addListener(
                ipv6BindFuture -> {
                  if (!ipv6BindFuture.isSuccess()) {
                    ipv6Close.complete(null);
                    return;
                  }
                  ipv6.channel()
                      .close()
                      .addListener(
                          ipv6CloseFuture -> {
                            if (!ipv6CloseFuture.isSuccess()) {
                              LOG.warn(
                                  "Failed to close IPv6 RLPx socket cleanly",
                                  ipv6CloseFuture.cause());
                            }
                            ipv6Close.complete(null);
                          });
                });
            CompletableFuture.allOf(ipv4Close, ipv6Close)
                .whenComplete((v, err) -> shutdownEventLoops(stoppedFuture, err));
          } else {
            ipv4Close.whenComplete((v, err) -> shutdownEventLoops(stoppedFuture, err));
          }
        });

    return stoppedFuture;
  }

  private void shutdownEventLoops(final CompletableFuture<Void> stoppedFuture) {
    shutdownEventLoops(stoppedFuture, null);
  }

  private void shutdownEventLoops(
      final CompletableFuture<Void> stoppedFuture, final Throwable err) {
    workers.shutdownGracefully();
    boss.shutdownGracefully();
    if (err != null) {
      stoppedFuture.completeExceptionally(err);
    } else {
      stoppedFuture.complete(null);
    }
  }

  @Override
  public void subscribeIncomingConnect(final ConnectCallback callback) {
    connectSubscribers.subscribe(callback);
  }

  @Override
  public CompletableFuture<PeerConnection> connect(final Peer peer) {
    final CompletableFuture<PeerConnection> connectionFuture = new CompletableFuture<>();

    final EnodeURL enode = peer.getEnodeURL();
    new Bootstrap()
        .group(workers)
        .channel(NioSocketChannel.class)
        .remoteAddress(new InetSocketAddress(enode.getIp(), enode.getListeningPort().get()))
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, TIMEOUT_SECONDS * 1000)
        .handler(outboundChannelInitializer(peer, connectionFuture))
        .connect()
        .addListener(
            (f) -> {
              if (!f.isSuccess()) {
                connectionFuture.completeExceptionally(f.cause());
              }
            });

    return connectionFuture;
  }

  /**
   * @return a channel initializer for outbound connections
   */
  @NotNull
  private ChannelInitializer<SocketChannel> outboundChannelInitializer(
      final Peer peer, final CompletableFuture<PeerConnection> connectionFuture) {
    return new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(final SocketChannel ch) throws Exception {
        ch.pipeline()
            .addLast(
                timeoutHandler(
                    connectionFuture,
                    "Timed out waiting to establish connection with peer: " + peer.getId()));
        addAdditionalOutboundHandlers(ch, peer);
        ch.pipeline().addLast(outboundHandler(peer, connectionFuture));
      }
    };
  }

  /**
   * @return a channel initializer for inbound connections
   */
  private ChannelInitializer<SocketChannel> inboundChannelInitializer() {
    return new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(final SocketChannel ch) throws Exception {
        final CompletableFuture<PeerConnection> connectionFuture = new CompletableFuture<>();
        connectionFuture.thenAccept(
            connection -> connectSubscribers.forEach(c -> c.onConnect(connection)));
        ch.pipeline()
            .addLast(
                timeoutHandler(
                    connectionFuture, "Timed out waiting to fully establish incoming connection"));
        addAdditionalInboundHandlers(ch);
        ch.pipeline().addLast(inboundHandler(connectionFuture));
      }
    };
  }

  @NotNull
  private HandshakeHandlerInbound inboundHandler(
      final CompletableFuture<PeerConnection> connectionFuture) {
    return new HandshakeHandlerInbound(
        nodeKey,
        config.getSupportedProtocols(),
        localNode,
        connectionFuture,
        eventDispatcher,
        metricsSystem,
        this,
        this,
        peerLookup);
  }

  @NotNull
  private HandshakeHandlerOutbound outboundHandler(
      final Peer peer, final CompletableFuture<PeerConnection> connectionFuture) {
    return new HandshakeHandlerOutbound(
        nodeKey,
        peer,
        config.getSupportedProtocols(),
        localNode,
        connectionFuture,
        eventDispatcher,
        metricsSystem,
        this,
        this,
        peerLookup);
  }

  @NotNull
  private TimeoutHandler<Channel> timeoutHandler(
      final CompletableFuture<PeerConnection> connectionFuture, final String s) {
    return new TimeoutHandler<>(
        connectionFuture::isDone,
        TIMEOUT_SECONDS,
        () -> connectionFuture.completeExceptionally(new TimeoutException(s)));
  }

  void addAdditionalOutboundHandlers(final Channel ch, final Peer peer)
      throws GeneralSecurityException, IOException {}

  void addAdditionalInboundHandlers(final Channel ch)
      throws GeneralSecurityException, IOException {}

  private IntSupplier pendingTaskCounter(final EventLoopGroup eventLoopGroup) {
    return () ->
        StreamSupport.stream(eventLoopGroup.spliterator(), false)
            .filter(eventExecutor -> eventExecutor instanceof SingleThreadEventExecutor)
            .mapToInt(eventExecutor -> ((SingleThreadEventExecutor) eventExecutor).pendingTasks())
            .sum();
  }

  @Override
  public Handshaker buildInstance() {
    return new ECIESHandshaker();
  }

  @Override
  public Framer buildFramer(final HandshakeSecrets secrets) {
    return new Framer(secrets);
  }
}
