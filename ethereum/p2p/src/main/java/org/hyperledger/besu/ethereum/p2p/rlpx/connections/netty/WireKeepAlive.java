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
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.PingMessage;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class WireKeepAlive extends ChannelDuplexHandler {
  private static final Logger LOG = LoggerFactory.getLogger(WireKeepAlive.class);

  private final AtomicBoolean waitingForPong;

  private final PeerConnection connection;

  WireKeepAlive(final PeerConnection connection, final AtomicBoolean waitingForPong) {
    this.connection = connection;
    this.waitingForPong = waitingForPong;
  }

  @Override
  public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt)
      throws IOException {
    if (!(evt instanceof IdleStateEvent idleStateEvent
        && idleStateEvent.state() == IdleState.READER_IDLE)) {
      // We only care about idling of incoming data from our peer
      return;
    }

    if (waitingForPong.get()) {
      // We are still waiting for a response from our last pong, disconnect with timeout error
      LOG.debug("Wire PONG never received, disconnecting from peer.");
      connection.disconnect(DisconnectMessage.DisconnectReason.TIMEOUT);
      return;
    }

    try {
      LOG.trace(
          "Idle connection detected, sending Wire PING to peer {}",
          connection.getPeer().getLoggableId());
      connection.send(null, PingMessage.get());
      waitingForPong.set(true);
    } catch (final PeerConnection.PeerNotConnected ignored) {
      LOG.trace("PING not sent because peer is already disconnected");
    }
  }
}
