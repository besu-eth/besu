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

import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.util.concurrent.CompletableFuture;

import io.netty.channel.socket.nio.NioDatagramChannel;
import org.ethereum.beacon.discovery.network.NettyDiscoveryServer;
import org.ethereum.beacon.discovery.pipeline.Envelope;
import org.reactivestreams.Publisher;

/**
 * Adapts a {@link SharedDiscoveryTransport} channel to the discv5 library's {@link
 * NettyDiscoveryServer} contract.
 *
 * <p>{@link #start()} returns the already-bound channel immediately — it does not bind a new one.
 * {@link #stop()} is a no-op because the {@link SharedDiscoveryTransport} owns the channel
 * lifecycle.
 */
public final class BesuNettyDiscoveryServer implements NettyDiscoveryServer {

  private final SharedDiscoveryTransport transport;
  private final StandardProtocolFamily family;

  public BesuNettyDiscoveryServer(
      final SharedDiscoveryTransport transport, final StandardProtocolFamily family) {
    this.transport = transport;
    this.family = family;
  }

  /**
   * Returns the already-bound channel. The {@link SharedDiscoveryTransport} must be started before
   * the discv5 library calls this method.
   */
  @Override
  public CompletableFuture<NioDatagramChannel> start() {
    return transport
        .getChannel(family)
        .map(CompletableFuture::completedFuture)
        .orElseGet(
            () -> CompletableFuture.failedFuture(new IllegalStateException(noChannelMessage())));
  }

  /** No-op: the {@link SharedDiscoveryTransport} manages channel lifecycle. */
  @Override
  public void stop() {}

  @Override
  public InetSocketAddress getListenAddress() {
    final InetSocketAddress bound = transport.getBoundAddress(family);
    if (bound == null) {
      throw new IllegalStateException(noChannelMessage());
    }
    return bound;
  }

  @Override
  public Publisher<Envelope> getIncomingPackets() {
    return transport.getV5IncomingPackets(family);
  }

  private String noChannelMessage() {
    return "SharedDiscoveryTransport has no channel for family "
        + family
        + ". Was start() called first?";
  }
}
