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

import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnectionEventDispatcher;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.CapabilityMultiplexer;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.PongMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.WireMessageCodes;
import org.hyperledger.besu.ethereum.rlp.RLPException;

import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

final class ApiHandler extends SimpleChannelInboundHandler<MessageData> {

  private static final Logger LOG = LoggerFactory.getLogger(ApiHandler.class);
  private static final Marker P2P_MESSAGE_MARKER = MarkerFactory.getMarker("P2PMSG");

  private final CapabilityMultiplexer multiplexer;
  private final AtomicBoolean waitingForPong;

  private final PeerConnectionEventDispatcher connectionEventDispatcher;

  private final PeerConnection connection;

  ApiHandler(
      final CapabilityMultiplexer multiplexer,
      final PeerConnection connection,
      final PeerConnectionEventDispatcher connectionEventDispatcher,
      final AtomicBoolean waitingForPong) {
    this.multiplexer = multiplexer;
    this.connectionEventDispatcher = connectionEventDispatcher;
    this.connection = connection;
    this.waitingForPong = waitingForPong;
  }

  @Override
  protected void channelRead0(final ChannelHandlerContext ctx, final MessageData originalMessage) {
    final CapabilityMultiplexer.ProtocolMessage demultiplexed =
        multiplexer.demultiplex(originalMessage);

    final MessageData message = demultiplexed.getMessage();

    // Handle Wire messages
    if (demultiplexed.getCapability() == null) {
      switch (message.getCode()) {
        case WireMessageCodes.PING:
          LOG.trace("Received Wire PING");
          try {
            connection.send(null, PongMessage.get());
          } catch (final PeerConnection.PeerNotConnected peerNotConnected) {
            // Nothing to do
          }
          break;
        case WireMessageCodes.PONG:
          LOG.trace("Received Wire PONG");
          waitingForPong.set(false);
          break;
        case WireMessageCodes.DISCONNECT:
          final DisconnectMessage disconnect = DisconnectMessage.readFrom(message);
          DisconnectMessage.DisconnectReason reason = DisconnectMessage.DisconnectReason.UNKNOWN;
          try {
            reason = disconnect.getReason();
            LOG.trace(
                "Received Wire DISCONNECT ({}) from peer: {}",
                reason.name(),
                connection.getPeerInfo());
          } catch (final RLPException e) {
            LOG.trace(
                "Received Wire DISCONNECT with invalid RLP. Peer: {}", connection.getPeerInfo());
          } catch (final Exception e) {
            LOG.error(
                "Received Wire DISCONNECT, but unable to parse reason. Peer: {}",
                connection.getPeerInfo(),
                e);
          }
          connection.terminateConnection(reason, true);
      }
      return;
    }
    if (LOG.isTraceEnabled()) {
      LOG.atTrace()
          .addMarker(P2P_MESSAGE_MARKER)
          .setMessage("Received {} from {} via protocol {}")
          .addArgument(message)
          .addArgument(connection.getPeerInfo())
          .addArgument(demultiplexed.getCapability())
          .addKeyValue("rawData", message.getData())
          .addKeyValue("decodedData", message::toStringDecoded)
          .log();
    }

    connectionEventDispatcher.dispatchMessage(demultiplexed.getCapability(), connection, message);
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable throwable) {
    LOG.error("Error:", throwable);
    connectionEventDispatcher.dispatchDisconnect(
        connection, DisconnectMessage.DisconnectReason.TCP_SUBSYSTEM_ERROR, false);
    ctx.close();
  }
}
