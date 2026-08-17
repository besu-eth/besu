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
package org.hyperledger.besu.ethereum.p2p.rlpx.connections.netty;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnectionEventDispatcher;
import org.hyperledger.besu.ethereum.p2p.rlpx.framing.FramingException;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.CapabilityMultiplexer;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.PeerInfo;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.WireMessageCodes;

import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ApiHandlerTest {

  private CapabilityMultiplexer multiplexer;
  private PeerConnection connection;
  private PeerConnectionEventDispatcher dispatcher;
  private ChannelHandlerContext ctx;
  private ApiHandler handler;

  @BeforeEach
  public void setUp() {
    multiplexer = mock(CapabilityMultiplexer.class);
    connection = mock(PeerConnection.class);
    dispatcher = mock(PeerConnectionEventDispatcher.class);
    ctx = mock(ChannelHandlerContext.class);

    when(connection.getPeerInfo()).thenReturn(mock(PeerInfo.class));

    handler = new ApiHandler(multiplexer, connection, dispatcher, new AtomicBoolean(false));
  }

  @Test
  public void disconnectWithFramingExceptionIsHandledQuietly() throws Exception {
    // A remote peer can send a DISCONNECT whose body fails Snappy decompression.
    // This should be treated as a malformed-but-benign disconnect, not an ERROR.
    final MessageData message = mock(MessageData.class);
    when(message.getCode()).thenReturn(WireMessageCodes.DISCONNECT);
    when(message.getData()).thenThrow(new FramingException("Snappy decompression failed"));

    final CapabilityMultiplexer.ProtocolMessage protocolMessage =
        mock(CapabilityMultiplexer.ProtocolMessage.class);
    when(protocolMessage.getCapability()).thenReturn(null);
    when(protocolMessage.getMessage()).thenReturn(message);
    when(multiplexer.demultiplex(any())).thenReturn(protocolMessage);

    // Should not throw; connection must be terminated regardless
    handler.channelRead0(ctx, mock(MessageData.class));

    verify(connection)
        .terminateConnection(eq(DisconnectMessage.DisconnectReason.UNKNOWN), eq(true));
  }

  @Test
  public void exceptionCaughtWithFramingExceptionDoesNotLogError() {
    // FramingException reaching exceptionCaught (e.g. from a non-DISCONNECT wire message path)
    // should be logged at DEBUG, not ERROR, consistent with DeFramer.exceptionCaught.
    final FramingException framingException = new FramingException("Snappy decompression failed");

    // Should not throw; dispatcher must be called to clean up the connection
    handler.exceptionCaught(ctx, framingException);

    verify(dispatcher)
        .dispatchDisconnect(
            eq(connection), eq(DisconnectMessage.DisconnectReason.TCP_SUBSYSTEM_ERROR), eq(false));
    verify(ctx).close();
  }

  @Test
  public void exceptionCaughtWithUnexpectedExceptionDispatches() {
    // Non-framing exceptions still go through the normal disconnect/close path
    handler.exceptionCaught(ctx, new RuntimeException("unexpected"));

    verify(dispatcher)
        .dispatchDisconnect(
            eq(connection), eq(DisconnectMessage.DisconnectReason.TCP_SUBSYSTEM_ERROR), eq(false));
    verify(ctx).close();
  }
}
