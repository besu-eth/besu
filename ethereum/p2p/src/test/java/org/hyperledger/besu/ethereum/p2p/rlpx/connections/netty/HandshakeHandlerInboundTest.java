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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.SECPPublicKey;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.ethereum.p2p.config.RlpxConfiguration;
import org.hyperledger.besu.ethereum.p2p.peers.LocalNode;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnectionEventDispatcher;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerLookup;
import org.hyperledger.besu.ethereum.p2p.rlpx.framing.Framer;
import org.hyperledger.besu.ethereum.p2p.rlpx.handshake.Handshaker;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MockSubProtocol;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.PeerInfo;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.HelloMessage;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class HandshakeHandlerInboundTest {

  private static final Capability ETH_68 = Capability.create("eth", 68);
  private static final Capability ETH_69 = Capability.create("eth", 69);

  private final Handshaker handshaker = mock(Handshaker.class);
  private final Framer framer = mock(Framer.class);
  private final LocalNode localNode = mock(LocalNode.class);
  private final CompletableFuture<PeerConnection> connection = new CompletableFuture<>();
  private final SECPPublicKey peerKey =
      SignatureAlgorithmFactory.getInstance().generateKeyPair().getPublicKey();

  @Test
  void agreesOnTheCapabilitiesOfTheHelloItSends() {
    when(localNode.isReady()).thenReturn(true);
    // eth/69 is added as the handshake completes, too late for this connection
    when(localNode.getPeerInfo()).thenReturn(localPeerInfo(ETH_68), localPeerInfo(ETH_68, ETH_69));
    when(handshaker.getStatus()).thenReturn(Handshaker.HandshakeStatus.SUCCESS);
    when(handshaker.partyPubKey()).thenReturn(peerKey);
    final EmbeddedChannel channel = channel();

    channel.writeInbound(Unpooled.buffer());
    final ArgumentCaptor<MessageData> sentHello = ArgumentCaptor.forClass(MessageData.class);
    verify(framer).frame(sentHello.capture(), any());
    receiveHello(channel, ETH_68, ETH_69);

    assertThat(HelloMessage.readFrom(sentHello.getValue()).getPeerInfo().getCapabilities())
        .containsExactly(ETH_68);
    assertThat(connection).isCompleted();
    assertThat(connection.join().getAgreedCapabilities()).containsExactly(ETH_68);
  }

  private EmbeddedChannel channel() {
    final HandshakeHandlerInbound handler =
        new HandshakeHandlerInbound(
            mock(NodeKey.class),
            List.of(MockSubProtocol.create("eth")),
            localNode,
            connection,
            mock(PeerConnectionEventDispatcher.class),
            new NoOpMetricsSystem(),
            () -> handshaker,
            secrets -> framer,
            new PeerLookup(),
            RlpxConfiguration.DEFAULT_MAX_MESSAGE_SIZE);
    // the connection made after the hello needs both ends as IP addresses
    return new EmbeddedChannel(handler) {
      @Override
      protected SocketAddress localAddress0() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 30303);
      }

      @Override
      protected SocketAddress remoteAddress0() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 30304);
      }
    };
  }

  private void receiveHello(final EmbeddedChannel channel, final Capability... capabilities) {
    final HelloMessage hello =
        HelloMessage.create(
            new PeerInfo(5, "peer", List.of(capabilities), 30303, peerKey.getEncodedBytes()));
    when(framer.deframe(any()))
        .thenReturn(new RawMessage(hello.getCode(), hello.getData()))
        .thenReturn(null);
    channel.writeInbound(Unpooled.wrappedBuffer(new byte[] {1}));
  }

  private static PeerInfo localPeerInfo(final Capability... capabilities) {
    return new PeerInfo(5, "local", List.of(capabilities), 30303, Peer.randomId());
  }
}
