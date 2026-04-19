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

import org.hyperledger.besu.ethereum.p2p.peers.LocalNode;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnectionEventDispatcher;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerLookup;
import org.hyperledger.besu.ethereum.p2p.rlpx.framing.Framer;
import org.hyperledger.besu.ethereum.p2p.rlpx.framing.FramerProvider;
import org.hyperledger.besu.ethereum.p2p.rlpx.handshake.Handshaker;
import org.hyperledger.besu.ethereum.p2p.rlpx.handshake.HandshakerProvider;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.SubProtocol;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.HelloMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.WireMessageCodes;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractHandshakeHandler extends SimpleChannelInboundHandler<ByteBuf> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractHandshakeHandler.class);

  protected final Handshaker handshaker;

  // The peer we are expecting to connect to, if such a peer is known
  private final Optional<Peer> expectedPeer;
  private final LocalNode localNode;

  private final PeerConnectionEventDispatcher connectionEventDispatcher;

  private final CompletableFuture<PeerConnection> connectionFuture;
  private final List<SubProtocol> subProtocols;

  private final MetricsSystem metricsSystem;

  private final FramerProvider framerProvider;
  private final boolean inboundInitiated;
  private final PeerLookup peerLookup;
  private final int maxMessageSize;

  AbstractHandshakeHandler(
      final List<SubProtocol> subProtocols,
      final LocalNode localNode,
      final Optional<Peer> expectedPeer,
      final CompletableFuture<PeerConnection> connectionFuture,
      final PeerConnectionEventDispatcher connectionEventDispatcher,
      final MetricsSystem metricsSystem,
      final HandshakerProvider handshakerProvider,
      final FramerProvider framerProvider,
      final boolean inboundInitiated,
      final PeerLookup peerLookup,
      final int maxMessageSize) {
    this.subProtocols = subProtocols;
    this.localNode = localNode;
    this.expectedPeer = expectedPeer;
    this.connectionFuture = connectionFuture;
    this.connectionEventDispatcher = connectionEventDispatcher;
    this.metricsSystem = metricsSystem;
    this.handshaker = handshakerProvider.buildInstance();
    this.framerProvider = framerProvider;
    this.inboundInitiated = inboundInitiated;
    this.peerLookup = peerLookup;
    this.maxMessageSize = maxMessageSize;
  }

  /**
   * Generates the next message in the handshake sequence.
   *
   * @param msg Incoming Message
   * @return Optional of the next Handshake message that needs to be returned to the peer
   */
  protected abstract Optional<ByteBuf> nextHandshakeMessage(ByteBuf msg);

  @Override
  protected final void channelRead0(final ChannelHandlerContext ctx, final ByteBuf msg) {
    final Optional<ByteBuf> nextMsg = nextHandshakeMessage(msg);
    if (nextMsg.isPresent()) {
      ctx.writeAndFlush(nextMsg.get());
    } else if (handshaker.getStatus() != Handshaker.HandshakeStatus.SUCCESS) {
      LOG.debug("waiting for more bytes");
    } else {

      final Bytes nodeId = handshaker.partyPubKey().getEncodedBytes();
      if (!localNode.isReady()) {
        // If we're handling a connection before the node is fully up, just disconnect
        LOG.debug("Rejecting connection because local node is not ready {}", nodeId);
        disconnect(ctx, DisconnectMessage.DisconnectReason.UNKNOWN);
        return;
      }

      LOG.trace("Sending framed hello");

      // Exchange keys done
      final Framer framer = this.framerProvider.buildFramer(handshaker.secrets());

      final ByteToMessageDecoder deFramer =
          new DeFramer(
              framer,
              subProtocols,
              localNode,
              expectedPeer,
              connectionEventDispatcher,
              connectionFuture,
              metricsSystem,
              inboundInitiated,
              peerLookup,
              maxMessageSize,
              nodeId);

      ctx.channel()
          .pipeline()
          .replace(this, "DeFramer", deFramer)
          .addBefore("DeFramer", "validate", new ValidateFirstOutboundMessage(framer));

      ctx.writeAndFlush(new OutboundMessage(null, HelloMessage.create(localNode.getPeerInfo())))
          .addListener(
              ff -> {
                if (ff.isSuccess()) {
                  LOG.trace("Successfully wrote hello message");
                }
              });
      msg.retain();
      ctx.fireChannelRead(msg);
    }
  }

  private void disconnect(
      final ChannelHandlerContext ctx, final DisconnectMessage.DisconnectReason reason) {
    ctx.writeAndFlush(new OutboundMessage(null, DisconnectMessage.create(reason)))
        .addListener(
            ff -> {
              ctx.close();
              connectionFuture.completeExceptionally(
                  new IllegalStateException("Disconnecting from peer: " + reason.name()));
            });
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable throwable) {
    LOG.trace("Handshake error:", throwable);
    connectionFuture.completeExceptionally(throwable);
    ctx.close();
  }

  /** Ensures that wire hello message is the first message written. */
  private static class ValidateFirstOutboundMessage extends MessageToByteEncoder<OutboundMessage> {
    private final Framer framer;

    private ValidateFirstOutboundMessage(final Framer framer) {
      this.framer = framer;
    }

    @Override
    protected void encode(
        final ChannelHandlerContext context,
        final OutboundMessage outboundMessage,
        final ByteBuf out) {
      if (outboundMessage.getCapability() != null
          || outboundMessage.getData().getCode() != WireMessageCodes.HELLO) {
        throw new IllegalStateException("First wire message sent wasn't a HELLO.");
      }
      framer.frame(outboundMessage.getData(), out);
      context.pipeline().remove(this);
    }
  }
}
