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

import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFactory;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.InternetProtocolFamily;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.tuweni.bytes.Bytes;
import org.ethereum.beacon.discovery.pipeline.Envelope;
import org.ethereum.beacon.discovery.pipeline.Field;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.ReplayProcessor;

/**
 * Shared UDP transport that owns one (or two, for dual-stack) {@link NioDatagramChannel} instances
 * and demultiplexes incoming packets between DiscV4 and DiscV5.
 *
 * <p>In BOTH mode, both agents write outbound packets via their respective agents but receive via
 * this shared transport's pipeline.
 */
public final class SharedDiscoveryTransport {

  private static final Logger LOG = LoggerFactory.getLogger(SharedDiscoveryTransport.class);

  private static final Counter NO_OP_COUNTER =
      new Counter() {
        @Override
        public void inc() {}

        @Override
        public void inc(final long amount) {}
      };

  private final InetSocketAddress ipv4BindAddress;
  private final Optional<InetSocketAddress> ipv6BindAddress;
  private final byte[] maskingKey;
  private final boolean v4Enabled;
  private final boolean v5Enabled;

  private final Counter demuxV4Counter;
  private final Counter demuxV5Counter;
  private final Counter demuxDroppedCounter;

  private final NioEventLoopGroup eventLoopGroup;

  private volatile NioDatagramChannel ipv4Channel;
  private volatile NioDatagramChannel ipv6Channel;

  // V5 envelope streams — created eagerly so getV5IncomingPackets() is safe before start()
  @SuppressWarnings("deprecation")
  private final ReplayProcessor<Envelope> ipv4V5Publisher = ReplayProcessor.cacheLast();

  @SuppressWarnings("deprecation")
  private final FluxSink<Envelope> ipv4V5Sink = ipv4V5Publisher.sink();

  @SuppressWarnings("deprecation")
  private final ReplayProcessor<Envelope> ipv6V5Publisher = ReplayProcessor.cacheLast();

  @SuppressWarnings("deprecation")
  private final FluxSink<Envelope> ipv6V5Sink = ipv6V5Publisher.sink();

  // V4 inbound handler — registered by NettyV4Transport before packets arrive
  private volatile BiConsumer<InetSocketAddress, Bytes> v4Handler;

  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicBoolean stopped = new AtomicBoolean(false);

  private SharedDiscoveryTransport(final Builder builder) {
    this.ipv4BindAddress = Objects.requireNonNull(builder.ipv4BindAddress, "ipv4BindAddress");
    this.ipv6BindAddress = builder.ipv6BindAddress;
    final byte[] key = Objects.requireNonNull(builder.maskingKey, "maskingKey");
    // Must be exactly 16 bytes — the discv5.1 spec defines the masking key as the first 16
    // bytes of the local node-id. A short key would be silently zero-padded by Arrays.copyOf
    // and cause V5 demux to misclassify every inbound packet.
    if (key.length != 16) {
      throw new IllegalArgumentException(
          "maskingKey must be exactly 16 bytes (got " + key.length + ")");
    }
    this.maskingKey = key.clone();
    this.v4Enabled = builder.v4Enabled;
    this.v5Enabled = builder.v5Enabled;
    this.eventLoopGroup =
        new NioEventLoopGroup(1, (ThreadFactory) r -> new Thread(r, "disc-shared-eventloop"));

    if (builder.metricsSystem != null) {
      final LabelledMetric<Counter> demuxCounter =
          builder.metricsSystem.createLabelledCounter(
              BesuMetricCategory.NETWORK,
              "discovery_demux_packets_total",
              "UDP packets demultiplexed by discovery protocol",
              "protocol");
      this.demuxV4Counter = demuxCounter.labels("v4");
      this.demuxV5Counter = demuxCounter.labels("v5");
      this.demuxDroppedCounter = demuxCounter.labels("dropped");
    } else {
      this.demuxV4Counter = NO_OP_COUNTER;
      this.demuxV5Counter = NO_OP_COUNTER;
      this.demuxDroppedCounter = NO_OP_COUNTER;
    }
  }

  /** Registers the V4 inbound handler. Called by {@code NettyV4Transport} before start. */
  public void setV4Handler(final BiConsumer<InetSocketAddress, Bytes> handler) {
    this.v4Handler = handler;
  }

  /** Binds all configured channels. Returns when all channels are ready. */
  public CompletableFuture<Void> start() {
    if (!started.compareAndSet(false, true)) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("SharedDiscoveryTransport already started"));
    }
    final List<CompletableFuture<Void>> futures = new ArrayList<>();
    futures.add(bindChannel(ipv4BindAddress, ipv4V5Sink, true));
    ipv6BindAddress.ifPresent(addr -> futures.add(bindChannel(addr, ipv6V5Sink, false)));
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
        .whenComplete(
            (v, ex) -> {
              if (ex != null) {
                // Release any partially-bound channel and shut down the event loop.
                // Log (but don't propagate) any cleanup failure — the original bind
                // exception is the one callers care about.
                stop()
                    .whenComplete(
                        (sv, sex) -> {
                          if (sex != null) {
                            LOG.warn("Cleanup after bind failure also failed", sex);
                          }
                        });
              }
            });
  }

  private CompletableFuture<Void> bindChannel(
      final InetSocketAddress bindAddr,
      final FluxSink<Envelope> v5EnvelopeSink,
      final boolean isIpv4) {

    final CompletableFuture<Void> future = new CompletableFuture<>();

    // V4 sink: dispatch to the registered handler (if any)
    final BiConsumer<InetSocketAddress, Bytes> v4Sink =
        (sender, data) -> {
          final BiConsumer<InetSocketAddress, Bytes> h = v4Handler;
          if (h != null) {
            h.accept(sender, data);
          }
        };

    // V5 sink: build Envelope and push to ReplayProcessor
    final Consumer<DatagramPacket> v5Sink =
        pkt -> {
          try {
            final Envelope env = new Envelope();
            final byte[] data = new byte[pkt.content().readableBytes()];
            pkt.content().readBytes(data);
            env.put(Field.INCOMING, Bytes.wrap(data));
            env.put(Field.REMOTE_SENDER, pkt.sender());
            try {
              v5EnvelopeSink.next(env);
            } catch (final IllegalStateException e) {
              // The sink can be cancelled/terminated during shutdown; drop the packet
              // quietly instead of routing it through Netty's exceptionCaught path.
              LOG.trace("V5 sink rejected envelope; likely shutdown race", e);
            }
          } finally {
            pkt.release();
          }
        };

    final InternetProtocolFamily family =
        isIpv4 ? InternetProtocolFamily.IPv4 : InternetProtocolFamily.IPv6;
    new Bootstrap()
        .group(eventLoopGroup)
        .channelFactory((ChannelFactory<NioDatagramChannel>) () -> new NioDatagramChannel(family))
        .handler(
            new ChannelInitializer<NioDatagramChannel>() {
              @Override
              protected void initChannel(final NioDatagramChannel ch) {
                ch.pipeline().addFirst(new LoggingHandler(LogLevel.TRACE));
                ch.pipeline()
                    .addLast(
                        new SharedDiscoveryDemuxHandler(
                            v4Enabled,
                            v5Enabled,
                            v5Enabled ? maskingKey : null,
                            v4Enabled ? v4Sink : null,
                            v5Enabled ? v5Sink : null,
                            demuxV4Counter,
                            demuxV5Counter,
                            demuxDroppedCounter));
              }
            })
        .bind(bindAddr)
        .addListener(
            (ChannelFuture result) -> {
              if (!result.isSuccess()) {
                future.completeExceptionally(result.cause());
                return;
              }
              final NioDatagramChannel ch = (NioDatagramChannel) result.channel();
              if (isIpv4) {
                ipv4Channel = ch;
                LOG.debug(
                    "Shared discovery transport bound on IPv4 {}:{}",
                    ch.localAddress().getHostString(),
                    ch.localAddress().getPort());
              } else {
                ipv6Channel = ch;
                LOG.debug(
                    "Shared discovery transport bound on IPv6 [{}]:{}",
                    ch.localAddress().getHostString(),
                    ch.localAddress().getPort());
              }
              future.complete(null);
            });
    return future;
  }

  /** Closes all channels and shuts down the event loop group. Idempotent. */
  public CompletableFuture<Void> stop() {
    if (!stopped.compareAndSet(false, true)) {
      return CompletableFuture.completedFuture(null);
    }
    final List<CompletableFuture<Void>> closeFutures = new ArrayList<>();
    final NioDatagramChannel v4 = ipv4Channel;
    final NioDatagramChannel v6 = ipv6Channel;
    if (v4 != null) {
      closeFutures.add(toFuture(v4.close()));
    }
    if (v6 != null) {
      closeFutures.add(toFuture(v6.close()));
    }
    return CompletableFuture.allOf(closeFutures.toArray(new CompletableFuture<?>[0]))
        .thenCompose(ignored -> toFuture(eventLoopGroup.shutdownGracefully()));
  }

  /** Returns the channel for the given address family, if bound. */
  public Optional<NioDatagramChannel> getChannel(final InternetProtocolFamily family) {
    return family == InternetProtocolFamily.IPv6
        ? Optional.ofNullable(ipv6Channel)
        : Optional.ofNullable(ipv4Channel);
  }

  /**
   * Returns the V5 incoming packet stream for the given address family. Safe to call before {@link
   * #start()} — subscriptions will receive packets once the channel is bound.
   */
  public Publisher<Envelope> getV5IncomingPackets(final InternetProtocolFamily family) {
    return family == InternetProtocolFamily.IPv6 ? ipv6V5Publisher : ipv4V5Publisher;
  }

  /**
   * Returns the bound local address for the given address family. Returns {@code null} if not
   * bound.
   */
  public InetSocketAddress getBoundAddress(final InternetProtocolFamily family) {
    return getChannel(family).map(NioDatagramChannel::localAddress).orElse(null);
  }

  public static Builder builder() {
    return new Builder();
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

  public static final class Builder {
    private InetSocketAddress ipv4BindAddress;
    private Optional<InetSocketAddress> ipv6BindAddress = Optional.empty();
    private byte[] maskingKey;
    private boolean v4Enabled = false;
    private boolean v5Enabled = false;
    private MetricsSystem metricsSystem;

    private Builder() {}

    public Builder ipv4BindAddress(final InetSocketAddress addr) {
      this.ipv4BindAddress = addr;
      return this;
    }

    public Builder ipv6BindAddress(final Optional<InetSocketAddress> addr) {
      this.ipv6BindAddress = addr;
      return this;
    }

    public Builder maskingKey(final byte[] key) {
      this.maskingKey = key;
      return this;
    }

    public Builder v4Enabled(final boolean enabled) {
      this.v4Enabled = enabled;
      return this;
    }

    public Builder v5Enabled(final boolean enabled) {
      this.v5Enabled = enabled;
      return this;
    }

    public Builder metricsSystem(final MetricsSystem metricsSystem) {
      this.metricsSystem = metricsSystem;
      return this;
    }

    public SharedDiscoveryTransport build() {
      return new SharedDiscoveryTransport(this);
    }
  }
}
