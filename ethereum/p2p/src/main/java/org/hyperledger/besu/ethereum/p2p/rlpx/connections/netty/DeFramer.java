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

import org.hyperledger.besu.ethereum.p2p.network.exceptions.BreachOfProtocolException;
import org.hyperledger.besu.ethereum.p2p.network.exceptions.IncompatiblePeerException;
import org.hyperledger.besu.ethereum.p2p.network.exceptions.PeerChannelClosedException;
import org.hyperledger.besu.ethereum.p2p.network.exceptions.PeerDisconnectedException;
import org.hyperledger.besu.ethereum.p2p.network.exceptions.UnexpectedPeerConnectionException;
import org.hyperledger.besu.ethereum.p2p.peers.DefaultPeer;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.p2p.peers.LocalNode;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnectionEventDispatcher;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerLookup;
import org.hyperledger.besu.ethereum.p2p.rlpx.framing.Framer;
import org.hyperledger.besu.ethereum.p2p.rlpx.framing.FramingException;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.CapabilityMultiplexer;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.PeerInfo;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.SubProtocol;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.HelloMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.WireMessageCodes;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.timeout.IdleStateHandler;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class DeFramer extends ByteToMessageDecoder {

  private static final Logger LOG = LoggerFactory.getLogger(DeFramer.class);

  private final CompletableFuture<PeerConnection> connectFuture;

  private final PeerConnectionEventDispatcher connectionEventDispatcher;

  private final Framer framer;
  private final LocalNode localNode;
  // The peer we are expecting to connect to, if such a peer is known
  private final Optional<Peer> expectedPeer;
  private final List<SubProtocol> subProtocols;
  private final boolean inboundInitiated;
  private final PeerLookup peerLookup;
  private boolean hellosExchanged;
  private final LabelledMetric<Counter> outboundMessagesCounter;
  private final LabelledMetric<Counter> outboundBytesCounter;

  DeFramer(
      final Framer framer,
      final List<SubProtocol> subProtocols,
      final LocalNode localNode,
      final Optional<Peer> expectedPeer,
      final PeerConnectionEventDispatcher connectionEventDispatcher,
      final CompletableFuture<PeerConnection> connectFuture,
      final MetricsSystem metricsSystem,
      final boolean inboundInitiated,
      final PeerLookup peerLookup) {
    this.framer = framer;
    this.subProtocols = subProtocols;
    this.localNode = localNode;
    this.expectedPeer = expectedPeer;
    this.connectFuture = connectFuture;
    this.connectionEventDispatcher = connectionEventDispatcher;
    this.inboundInitiated = inboundInitiated;
    this.peerLookup = peerLookup;
    this.outboundMessagesCounter =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.NETWORK,
            "p2p_messages_outbound",
            "Count of each P2P message sent outbound.",
            "protocol",
            "name",
            "code");
    this.outboundBytesCounter =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.NETWORK,
            "p2p_bytes_outbound",
            "Count of bytes sent outbound.",
            "protocol",
            "name",
            "code");
  }

  @Override
  protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out) {
    MessageData message;
    while ((message = framer.deframe(in)) != null) {

      if (hellosExchanged) {

        out.add(message);

      } else if (message.getCode() == WireMessageCodes.HELLO) {

        hellosExchanged = true;
        // Decode first hello and use the payload to modify pipeline
        final PeerInfo peerInfo;
        try {
          peerInfo = HelloMessage.readFrom(message).getPeerInfo();
        } catch (final RLPException e) {
          LOG.debug("Received invalid HELLO message, set log level to TRACE for message body", e);
          connectFuture.completeExceptionally(e);
          ctx.close();
          return;
        }
        LOG.trace("Received HELLO message: {}", peerInfo);
        if (peerInfo.getVersion() >= 5) {
          LOG.trace("Enable compression for p2pVersion: {}", peerInfo.getVersion());
          framer.enableCompression();
        }

        final CapabilityMultiplexer capabilityMultiplexer =
            new CapabilityMultiplexer(
                subProtocols,
                localNode.getPeerInfo().getCapabilities(),
                peerInfo.getCapabilities());

        Optional<Peer> peer;
        if (expectedPeer.isPresent()) {
          peer = expectedPeer;
        } else {
          // This is an inbound "Hello" message. Create peer from information from the Hello message
          peer = createPeer(peerInfo, ctx);
          if (peer.isEmpty()) {
            LOG.debug("Failed to create connection for peer {}", peerInfo);
            connectFuture.completeExceptionally(new PeerChannelClosedException(peerInfo));
            ctx.close();
            return;
          }
          // If we can find the peer in the peerLookup we use it, because it could contains
          // additional information, like the fork id.
          final Optional<Peer> existingPeer = peerLookup.getPeer(peer.get());
          if (existingPeer.isPresent()) {
            peer = existingPeer;
          }
        }

        final PeerConnection connection =
            new NettyPeerConnection(
                ctx,
                peer.get(),
                peerInfo,
                capabilityMultiplexer,
                connectionEventDispatcher,
                outboundMessagesCounter,
                outboundBytesCounter,
                inboundInitiated);

        // Check peer is who we expected
        if (expectedPeer.isPresent()
            && !Objects.equals(expectedPeer.get().getId(), peerInfo.getNodeId())) {
          final String unexpectedMsg =
              String.format(
                  "Expected id %s, but got %s", expectedPeer.get().getId(), peerInfo.getNodeId());
          connectFuture.completeExceptionally(new UnexpectedPeerConnectionException(unexpectedMsg));
          LOG.debug("{}. Disconnecting.", unexpectedMsg);
          connection.disconnect(DisconnectMessage.DisconnectReason.UNEXPECTED_ID);
        }

        // Check that we have shared caps
        if (capabilityMultiplexer.getAgreedCapabilities().isEmpty()) {
          LOG.debug("Disconnecting because no capabilities are shared: {}", peerInfo);
          connectFuture.completeExceptionally(
              new IncompatiblePeerException("No shared capabilities"));
          connection.disconnect(
              DisconnectMessage.DisconnectReason.USELESS_PEER_NO_SHARED_CAPABILITIES);
        }

        // Setup next stage
        final AtomicBoolean waitingForPong = new AtomicBoolean(false);
        ctx.channel()
            .pipeline()
            .addLast(
                new IdleStateHandler(15, 0, 0),
                new WireKeepAlive(connection, waitingForPong),
                new ApiHandler(
                    capabilityMultiplexer, connection, connectionEventDispatcher, waitingForPong),
                new MessageFramer(capabilityMultiplexer, framer));
        connectFuture.complete(connection);

      } else if (message.getCode() == WireMessageCodes.DISCONNECT) {

        final DisconnectMessage disconnectMessage = DisconnectMessage.readFrom(message);
        LOG.debug(
            "Peer {} disconnected before sending HELLO.  Reason: {}",
            expectedPeer.map(Peer::getEnodeURLString).orElse("unknown"),
            disconnectMessage.getReason());
        ctx.close();
        connectFuture.completeExceptionally(
            new PeerDisconnectedException(disconnectMessage.getReason()));

      } else {
        // Unexpected message - disconnect

        LOG.debug(
            "Message received before HELLO's exchanged (BREACH_OF_PROTOCOL), disconnecting.  Peer: {}, Code: {}, Data: {}",
            expectedPeer.map(Peer::getEnodeURLString).orElse("unknown"),
            message.getCode(),
            message instanceof RawMessage raw && raw.getCompressedData() != null
                ? "snappy compressed data: " + Bytes.wrap(raw.getCompressedData())
                : message.getData().toString());
        ctx.writeAndFlush(
                new OutboundMessage(
                    null,
                    DisconnectMessage.create(
                        DisconnectMessage.DisconnectReason
                            .BREACH_OF_PROTOCOL_MESSAGE_RECEIVED_BEFORE_HELLO_EXCHANGE)))
            .addListener((f) -> ctx.close());
        connectFuture.completeExceptionally(
            new BreachOfProtocolException("Message received before HELLO's exchanged"));
      }
    }
  }

  private Optional<Peer> createPeer(final PeerInfo peerInfo, final ChannelHandlerContext ctx) {
    final InetSocketAddress remoteAddress = ((InetSocketAddress) ctx.channel().remoteAddress());
    if (remoteAddress == null) {
      return Optional.empty();
    }
    final int port = peerInfo.getPort();
    return Optional.of(
        DefaultPeer.fromEnodeURL(
            EnodeURLImpl.builder()
                .nodeId(peerInfo.getNodeId())
                .ipAddress(remoteAddress.getAddress())
                .listeningPort(port)
                // Discovery information is unknown, so disable it
                .disableDiscovery()
                .build()));
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable throwable)
      throws Exception {
    final Throwable cause =
        throwable instanceof DecoderException && throwable.getCause() != null
            ? throwable.getCause()
            : throwable;
    if (cause instanceof FramingException
        || cause instanceof RLPException
        || cause instanceof IllegalArgumentException) {
      LOG.debug("Invalid incoming message (BREACH_OF_PROTOCOL)", throwable);
      if (connectFuture.isDone() && !connectFuture.isCompletedExceptionally()) {
        connectFuture
            .get()
            .disconnect(
                DisconnectMessage.DisconnectReason
                    .BREACH_OF_PROTOCOL_INVALID_MESSAGE_RECEIVED_CAUGHT_EXCEPTION);
        return;
      }
    } else if (cause instanceof IOException) {
      // IO failures are routine when communicating with random peers across the network.
      LOG.debug("IO error while processing incoming message {}", throwable.getMessage());
    } else {
      LOG.error("Exception while processing incoming message", throwable);
    }
    if (connectFuture.isDone() && !connectFuture.isCompletedExceptionally()) {
      connectFuture
          .get()
          .terminateConnection(DisconnectMessage.DisconnectReason.TCP_SUBSYSTEM_ERROR, false);
    } else {
      connectFuture.completeExceptionally(throwable);
      ctx.close();
    }
  }
}
