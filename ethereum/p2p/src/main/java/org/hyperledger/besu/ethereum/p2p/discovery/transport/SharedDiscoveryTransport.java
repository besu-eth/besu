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
import org.hyperledger.besu.util.NetworkUtility;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.InetAddresses;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFactory;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.SocketProtocolFamily;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.tuweni.bytes.Bytes;
import org.ethereum.beacon.discovery.pipeline.Envelope;
import org.ethereum.beacon.discovery.pipeline.Field;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Sinks;

/**
 * Shared UDP transport that owns one (or two, for dual-stack) {@link NioDatagramChannel} instances
 * and demultiplexes incoming packets between DiscV4 and DiscV5.
 *
 * <p>In BOTH mode, both agents write outbound packets via their respective agents but receive via
 * this shared transport's pipeline.
 */
public final class SharedDiscoveryTransport {

  private static final Logger LOG = LoggerFactory.getLogger(SharedDiscoveryTransport.class);

  // Stand-in host for the synthetic IPv4 address getBoundAddress() returns in the merged
  // dual-stack case - only its family (IPv4) and the real port are ever consumed, never this
  // value itself.
  private static final InetAddress IPV4_PLACEHOLDER = InetAddresses.forString("0.0.0.0");

  private final Optional<InetSocketAddress> ipv4BindAddress;
  private final Optional<InetSocketAddress> ipv6BindAddress;
  private final byte[] maskingKey;
  private final boolean v4Enabled;
  private final boolean v5Enabled;

  // True when both binds share a port and are their family's wildcard address. Two independent
  // per-family sockets on the same port BindException on Linux regardless of bind order (it
  // works on macOS/BSD, which is why this wasn't caught earlier) - so this case binds one
  // dual-stack IPv6 socket instead, serving both families.
  private final boolean mergedDualStack;

  private static final Sinks.EmitFailureHandler RETRY_ON_CONCURRENT =
      (signalType, result) -> result == Sinks.EmitResult.FAIL_NON_SERIALIZED;

  private final DemuxCounters demuxCounters;

  // Lazily created in start() - constructing it eagerly here would spawn a permanently-idle
  // thread whenever a transport is built but never started (e.g. discovery disabled).
  private volatile EventLoopGroup eventLoopGroup;

  // Lazily created in start() when V5 is enabled, so V5 packet processing (and the synchronous
  // discv5-library handshake work it triggers) can't head-of-line-block V4 I/O on the shared
  // Netty event-loop thread. Mirrors the decodeExecutor precedent in NettyPeerDiscoveryAgent.
  private volatile ExecutorService v5DispatchExecutor;

  private volatile NioDatagramChannel ipv4Channel;
  private volatile NioDatagramChannel ipv6Channel;

  // V5 envelope streams — created eagerly so getV5IncomingPackets() is safe before start().
  // Bounded (not .all(), fed by untrusted external UDP input) so packets arriving before the
  // discv5 library subscribes are replayed instead of dropped, without unbounded growth.
  private static final int V5_REPLAY_HISTORY_SIZE = 256;
  private final Sinks.Many<Envelope> ipv4V5Sink =
      Sinks.many().replay().limit(V5_REPLAY_HISTORY_SIZE);
  private final Sinks.Many<Envelope> ipv6V5Sink =
      Sinks.many().replay().limit(V5_REPLAY_HISTORY_SIZE);

  // V4 inbound handler — registered by NettyTransport before packets arrive
  private volatile BiConsumer<InetSocketAddress, Bytes> v4Handler;

  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicBoolean stopped = new AtomicBoolean(false);

  private SharedDiscoveryTransport(final Builder builder) {
    this.ipv4BindAddress = Objects.requireNonNull(builder.ipv4BindAddress, "ipv4BindAddress");
    this.ipv6BindAddress = builder.ipv6BindAddress;
    if (ipv4BindAddress.isEmpty() && ipv6BindAddress.isEmpty()) {
      throw new IllegalArgumentException(
          "At least one of ipv4BindAddress or ipv6BindAddress must be set");
    }
    final byte[] key = Objects.requireNonNull(builder.maskingKey, "maskingKey");
    // Must be exactly 16 bytes per the discv5.1 spec, or V5 demux would misclassify every packet.
    if (key.length != 16) {
      throw new IllegalArgumentException(
          "maskingKey must be exactly 16 bytes (got " + key.length + ")");
    }
    this.maskingKey = key.clone();
    this.v4Enabled = builder.v4Enabled;
    this.v5Enabled = builder.v5Enabled;
    // No IPv4-required guard here, matching upstream's owned-channel NettyTransport: IPv6-only
    // is a legitimate bind (e.g. a private IPv6-only network). Sends to an IPv4 recipient just
    // fail per-send (see NettyTransport.send()), not at startup.
    this.mergedDualStack =
        ipv4BindAddress.isPresent()
            && ipv6BindAddress.isPresent()
            && NetworkUtility.isMergeableDualStackBind(
                ipv4BindAddress.get().getHostString(),
                ipv4BindAddress.get().getPort(),
                ipv6BindAddress.get().getHostString(),
                ipv6BindAddress.get().getPort());
    this.demuxCounters = buildDemuxCounters(builder.metricsSystem);
  }

  private static DemuxCounters buildDemuxCounters(final MetricsSystem metricsSystem) {
    if (metricsSystem == null) {
      return DemuxCounters.NO_OP;
    }
    final LabelledMetric<Counter> demuxCounter =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.NETWORK,
            "discovery_demux_packets_total",
            "UDP packets demultiplexed by discovery protocol",
            "protocol");
    return new DemuxCounters(
        demuxCounter.labels(DemuxProtocol.V4.label()),
        demuxCounter.labels(DemuxProtocol.V5.label()),
        demuxCounter.labels(DemuxProtocol.DROPPED.label()));
  }

  /**
   * Returns whether both address families are served by a single dual-stack socket rather than two
   * independent per-family sockets. Callers that register one listener per family (e.g. discv5's
   * custom servers in {@code CompositePeerDiscoveryAgentFactory}) must still register one per
   * family even when this is {@code true} - the discv5 library keys its own outbound-dispatch
   * channel map off each registered listener's address family (see {@code
   * NettyDiscoveryClientImpl.send()} upstream), so omitting a family here would make it silently
   * drop every outbound packet addressed to that family. This does not risk double-processing
   * inbound packets: only one of the two per-family incoming-packet streams is ever fed when merged
   * (see {@link #bindChannel}), so the other is simply always empty.
   *
   * @return {@code true} if a single dual-stack socket serves both families
   */
  public boolean isDualStackMergedIntoSingleSocket() {
    return mergedDualStack;
  }

  /** Registers the V4 inbound handler. Called by {@code NettyTransport} before start. */
  public void setV4Handler(final BiConsumer<InetSocketAddress, Bytes> handler) {
    this.v4Handler = handler;
  }

  /** Binds all configured channels. Returns when all channels are ready. */
  public CompletableFuture<Void> start() {
    if (!started.compareAndSet(false, true)) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("SharedDiscoveryTransport already started"));
    }
    this.eventLoopGroup =
        new MultiThreadIoEventLoopGroup(
            1,
            (ThreadFactory) r -> new Thread(r, "disc-shared-eventloop"),
            NioIoHandler.newFactory());
    if (v5Enabled) {
      this.v5DispatchExecutor =
          Executors.newSingleThreadExecutor((ThreadFactory) r -> new Thread(r, "disc-v5-dispatch"));
    }
    if (stopped.get()) {
      // A concurrent stop() saw a null eventLoopGroup before we set it and won't run again -
      // shut down what we just created instead of leaking these threads.
      eventLoopGroup.shutdownGracefully();
      if (v5DispatchExecutor != null) {
        v5DispatchExecutor.shutdownNow();
      }
      return CompletableFuture.failedFuture(
          new IllegalStateException("SharedDiscoveryTransport already stopped"));
    }
    final List<CompletableFuture<Void>> futures = new ArrayList<>();
    if (mergedDualStack) {
      // Bind a single dual-stack IPv6 socket instead of two independent per-family sockets -
      // see the mergedDualStack field javadoc for why two sockets on the same port isn't viable.
      futures.add(
          bindChannel(ipv6BindAddress.get(), ipv6V5Sink, StandardProtocolFamily.INET6, true));
    } else {
      ipv4BindAddress.ifPresent(
          addr -> futures.add(bindChannel(addr, ipv4V5Sink, StandardProtocolFamily.INET, false)));
      ipv6BindAddress.ifPresent(
          addr -> futures.add(bindChannel(addr, ipv6V5Sink, StandardProtocolFamily.INET6, false)));
    }
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
        .whenComplete(
            (v, ex) -> {
              if (ex != null) {
                // Release any partially-bound channel; log but don't propagate cleanup
                // failure - the original bind exception is what callers care about.
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
      final Sinks.Many<Envelope> v5Sink,
      final StandardProtocolFamily family,
      final boolean merged) {

    final CompletableFuture<Void> future = new CompletableFuture<>();

    // V4 sink: dispatch to the registered handler (if any)
    final BiConsumer<InetSocketAddress, Bytes> v4Sink =
        (sender, data) -> {
          final BiConsumer<InetSocketAddress, Bytes> h = v4Handler;
          if (h != null) {
            h.accept(sender, data);
          }
        };

    // V5 sink: copy out of pkt on the Netty thread (before it's released), then dispatch onto
    // v5DispatchExecutor so downstream discv5-library processing doesn't block the shared
    // event-loop thread.
    final Consumer<DatagramPacket> v5PacketSink =
        pkt -> {
          final Envelope env;
          final InetSocketAddress sender = pkt.sender();
          try {
            env = new Envelope();
            final byte[] data = new byte[pkt.content().readableBytes()];
            pkt.content().readBytes(data);
            env.put(Field.INCOMING, Bytes.wrap(data));
            env.put(Field.REMOTE_SENDER, sender);
          } finally {
            pkt.release();
          }
          v5DispatchExecutor.execute(
              () -> {
                final Sinks.EmitResult result = v5Sink.tryEmitNext(env);
                if (result != Sinks.EmitResult.OK) {
                  // Most likely FAIL_OVERFLOW: the 256-entry replay buffer filled before the
                  // discv5 library subscribed. No retry - tryEmitNext has no retrying variant
                  // (RETRY_ON_CONCURRENT above is for emitComplete only) - just surface it.
                  LOG.trace("Dropped V5 packet from {}: {}", sender, result);
                }
              });
        };

    final boolean isIpv4 = family == StandardProtocolFamily.INET;
    new Bootstrap()
        .group(eventLoopGroup)
        .channelFactory(
            (ChannelFactory<NioDatagramChannel>)
                () -> new NioDatagramChannel(SocketProtocolFamily.of(family)))
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
                            v5Enabled ? v5PacketSink : null,
                            demuxCounters));
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
              if (stopped.get()) {
                // A concurrent stop() saw no channel to close and won't run again - tear this
                // one down instead of publishing a channel stop() already considers closed.
                ch.close();
                future.completeExceptionally(
                    new IllegalStateException("SharedDiscoveryTransport stopped during start()"));
                return;
              }
              if (merged) {
                // Single dual-stack socket serves both families - see mergedDualStack javadoc.
                ipv4Channel = ch;
                ipv6Channel = ch;
                LOG.debug(
                    "Shared discovery transport bound (dual-stack, single socket) on [{}]:{}",
                    ch.localAddress().getHostString(),
                    ch.localAddress().getPort());
              } else if (isIpv4) {
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
    final EventLoopGroup group = eventLoopGroup;
    if (v4 != null) {
      closeFutures.add(toFuture(v4.close()));
    }
    // When merged, v6 and v4 are the same socket - avoid closing it twice. Channel uses
    // identity equals(), so this is still an identity check.
    if (v6 != null && !v6.equals(v4)) {
      closeFutures.add(toFuture(v6.close()));
    }
    final ExecutorService dispatchExecutor = v5DispatchExecutor;
    return CompletableFuture.allOf(closeFutures.toArray(new CompletableFuture<?>[0]))
        .handle(
            (v, ex) -> {
              if (ex != null) {
                LOG.warn("Failed to close shared discovery channel(s)", ex);
              }
              return null;
            })
        .thenCompose(
            ignored ->
                // group is null if start() was never called (e.g. discovery disabled) - nothing
                // to shut down.
                group == null
                    ? CompletableFuture.completedFuture((Void) null)
                    : toFuture(group.shutdownGracefully()))
        .whenComplete(
            (v, ex) -> {
              // RETRY_ON_CONCURRENT retries if a last in-flight packet races this; accepts
              // FAIL_TERMINATED silently so double-stop is safe.
              ipv4V5Sink.emitComplete(RETRY_ON_CONCURRENT);
              ipv6V5Sink.emitComplete(RETRY_ON_CONCURRENT);
              // In-flight V5 packets aren't response-critical (discovery retries), so shutdownNow()
              // without waiting for queued dispatch tasks is fine.
              if (dispatchExecutor != null) {
                dispatchExecutor.shutdownNow();
              }
            });
  }

  /** Returns the channel for the given address family, if bound. */
  public Optional<NioDatagramChannel> getChannel(final StandardProtocolFamily family) {
    return family == StandardProtocolFamily.INET6
        ? Optional.ofNullable(ipv6Channel)
        : Optional.ofNullable(ipv4Channel);
  }

  @VisibleForTesting
  EventLoopGroup eventLoopGroupForTesting() {
    return eventLoopGroup;
  }

  @VisibleForTesting
  ExecutorService v5DispatchExecutorForTesting() {
    return v5DispatchExecutor;
  }

  /**
   * Returns the V5 incoming packet stream for the given address family. Safe to call before {@link
   * #start()} — subscriptions will receive packets once the channel is bound.
   */
  public Publisher<Envelope> getV5IncomingPackets(final StandardProtocolFamily family) {
    return family == StandardProtocolFamily.INET6 ? ipv6V5Sink.asFlux() : ipv4V5Sink.asFlux();
  }

  /**
   * Returns the bound local address for the given address family. Returns {@code null} if not
   * bound.
   *
   * <p>When a single dual-stack socket serves both families, requesting the IPv4 family returns a
   * synthetic IPv4-typed address (real port, placeholder host) instead of the channel's actual
   * (IPv6-typed) local address. Callers that key behavior off the returned address's family - the
   * discv5 library's own outbound dispatch keys its per-family channel map this way (see {@code
   * NettyDiscoveryClientImpl.send()} upstream) - would otherwise see this as IPv6-only and never
   * learn to route outbound IPv4-destined packets through the shared channel. Only the family and
   * port of the returned address are ever consumed downstream (see {@code
   * LocalNodeRecordStore.onBoundPortResolved()} upstream), so the placeholder host is never
   * observed.
   */
  public InetSocketAddress getBoundAddress(final StandardProtocolFamily family) {
    final NioDatagramChannel channel = getChannel(family).orElse(null);
    if (channel == null) {
      return null;
    }
    if (mergedDualStack && family == StandardProtocolFamily.INET) {
      return new InetSocketAddress(IPV4_PLACEHOLDER, channel.localAddress().getPort());
    }
    return channel.localAddress();
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
    private Optional<InetSocketAddress> ipv4BindAddress = Optional.empty();
    private Optional<InetSocketAddress> ipv6BindAddress = Optional.empty();
    private byte[] maskingKey;
    private boolean v4Enabled = false;
    private boolean v5Enabled = false;
    private MetricsSystem metricsSystem;

    private Builder() {}

    public Builder ipv4BindAddress(final InetSocketAddress addr) {
      this.ipv4BindAddress = Optional.of(addr);
      return this;
    }

    public Builder ipv4BindAddress(final Optional<InetSocketAddress> addr) {
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
