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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.forkid.ForkId;
import org.hyperledger.besu.ethereum.p2p.EthProtocolHelper;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.DiscoveryPeerV4;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.PeerTable;
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
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnectionEvents;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerLookup;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.netty.testhelpers.NettyMocks;
import org.hyperledger.besu.ethereum.p2p.rlpx.framing.Framer;
import org.hyperledger.besu.ethereum.p2p.rlpx.framing.FramingException;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MockSubProtocol;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.PeerInfo;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.HelloMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.PingMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.WireMessageCodes;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.data.EnodeURL;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.DecoderException;
import io.netty.util.concurrent.ScheduledFuture;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class DeFramerTest {

  private final ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
  private final Channel channel = mock(Channel.class);
  private final ChannelId channelId = mock(ChannelId.class);
  private final ChannelPipeline pipeline = mock(ChannelPipeline.class);
  private final EventLoop eventLoop = mock(EventLoop.class);
  private final Framer framer = mock(Framer.class);
  private final PeerConnectionEvents connectionEventDispatcher = mock(PeerConnectionEvents.class);
  private final PeerConnection peerConnection = mock(PeerConnection.class);
  private final CompletableFuture<PeerConnection> connectFuture = new CompletableFuture<>();
  private final int remotePort = 12345;
  private final InetSocketAddress remoteAddress =
      new InetSocketAddress(InetAddress.getLoopbackAddress(), remotePort);

  private final int p2pVersion = 5;
  private final String clientId = "abc";
  private final int port = 30303;
  private final List<Capability> capabilities = Arrays.asList(EthProtocolHelper.LATEST);
  private final EnodeURLImpl localEnode =
      EnodeURLImpl.builder()
          .ipAddress("127.0.0.1")
          .discoveryAndListeningPorts(port)
          .nodeId(Peer.randomId())
          .build();
  private final LocalNode localNode =
      LocalNode.create(clientId, p2pVersion, capabilities, localEnode);

  private final DeFramer deFramer = createDeFramer(null, Optional.empty());

  @BeforeEach
  @SuppressWarnings("unchecked")
  public void setup() {
    when(ctx.channel()).thenReturn(channel);

    when(channel.remoteAddress()).thenReturn(remoteAddress);
    when(channel.pipeline()).thenReturn(pipeline);
    when(channel.id()).thenReturn(channelId);

    when(channelId.asLongText()).thenReturn("1");
    when(channelId.asShortText()).thenReturn("1");

    when(pipeline.addLast(any())).thenReturn(pipeline);
    when(pipeline.addFirst(any())).thenReturn(pipeline);

    when(channel.eventLoop()).thenReturn(eventLoop);
    when(eventLoop.schedule(any(Callable.class), anyLong(), any()))
        .thenReturn(mock(ScheduledFuture.class));
  }

  @Test
  public void exceptionCaught_shouldDisconnectForBreachOfProtocolWhenFramingExceptionThrown()
      throws Exception {
    connectFuture.complete(peerConnection);

    deFramer.exceptionCaught(ctx, new DecoderException(new FramingException("Test")));

    verify(peerConnection)
        .disconnect(DisconnectReason.BREACH_OF_PROTOCOL_INVALID_MESSAGE_RECEIVED_CAUGHT_EXCEPTION);
  }

  @Test
  public void exceptionCaught_shouldDisconnectForBreachOfProtocolWhenRlpExceptionThrown()
      throws Exception {
    connectFuture.complete(peerConnection);

    deFramer.exceptionCaught(ctx, new DecoderException(new RLPException("Test")));

    verify(peerConnection)
        .disconnect(DisconnectReason.BREACH_OF_PROTOCOL_INVALID_MESSAGE_RECEIVED_CAUGHT_EXCEPTION);
  }

  @Test
  public void exceptionCaught_shouldHandleFramingExceptionWhenFutureCompletedExceptionally()
      throws Exception {
    connectFuture.completeExceptionally(new Exception());

    deFramer.exceptionCaught(ctx, new DecoderException(new FramingException("Test")));

    verify(ctx).close();
  }

  @Test
  public void exceptionCaught_shouldHandleGenericExceptionWhenFutureCompletedExceptionally()
      throws Exception {
    connectFuture.completeExceptionally(new Exception());

    deFramer.exceptionCaught(ctx, new DecoderException(new RuntimeException("Test")));

    verify(ctx).close();
  }

  @Test
  public void decode_handlesHello() throws ExecutionException, InterruptedException {
    final ChannelFuture future = NettyMocks.channelFuture(false);
    when(channel.closeFuture()).thenReturn(future);

    final Peer peer = createRemotePeer();
    final PeerInfo remotePeerInfo = createPeerInfo(peer);

    final HelloMessage helloMessage = HelloMessage.create(remotePeerInfo);
    final ByteBuf data = Unpooled.wrappedBuffer(helloMessage.getData().toArray());
    when(framer.deframe(eq(data)))
        .thenReturn(new RawMessage(helloMessage.getCode(), helloMessage.getData()))
        .thenReturn(null);
    final List<Object> out = new ArrayList<>();
    deFramer.decode(ctx, data, out);

    assertThat(connectFuture).isDone();
    assertThat(connectFuture).isNotCompletedExceptionally();
    final PeerConnection peerConnection = connectFuture.get();
    assertThat(peerConnection.getPeerInfo()).isEqualTo(remotePeerInfo);

    final EnodeURL expectedEnode =
        EnodeURLImpl.builder()
            .configureFromEnode(peer.getEnodeURL())
            // Discovery information is not available from peer info
            .disableDiscovery()
            .build();
    assertThat(peerConnection.getPeer().getEnodeURL()).isEqualTo(expectedEnode);
    assertThat(out).isEmpty();

    // Next phase of pipeline should be setup
    verify(pipeline, times(1)).addLast(any(ChannelHandler[].class));

    // Next message should be pushed out
    final PingMessage nextMessage = PingMessage.get();
    final ByteBuf nextData = Unpooled.wrappedBuffer(nextMessage.getData().toArray());
    when(framer.deframe(eq(nextData)))
        .thenReturn(new RawMessage(nextMessage.getCode(), nextMessage.getData()))
        .thenReturn(null);
    verify(pipeline, times(1)).addLast(any(ChannelHandler[].class));
    deFramer.decode(ctx, nextData, out);
    assertThat(out.size()).isEqualTo(1);
  }

  @Test
  public void decode_handlesHelloFromPeerWithAdvertisedPortOf0()
      throws ExecutionException, InterruptedException {
    final ChannelFuture future = NettyMocks.channelFuture(false);
    when(channel.closeFuture()).thenReturn(future);

    final Peer peer = createRemotePeer();
    final PeerInfo remotePeerInfo =
        new PeerInfo(p2pVersion, clientId, capabilities, 0, peer.getId());
    final DeFramer deFramer = createDeFramer(null, Optional.empty());

    final HelloMessage helloMessage = HelloMessage.create(remotePeerInfo);
    final ByteBuf data = Unpooled.wrappedBuffer(helloMessage.getData().toArray());
    when(framer.deframe(eq(data)))
        .thenReturn(new RawMessage(helloMessage.getCode(), helloMessage.getData()))
        .thenReturn(null);
    final List<Object> out = new ArrayList<>();
    deFramer.decode(ctx, data, out);

    assertThat(connectFuture).isDone();
    assertThat(connectFuture).isNotCompletedExceptionally();
    final PeerConnection peerConnection = connectFuture.get();
    assertThat(peerConnection.getPeerInfo()).isEqualTo(remotePeerInfo);
    assertThat(out).isEmpty();

    final EnodeURL expectedEnode =
        EnodeURLImpl.builder()
            .ipAddress(remoteAddress.getAddress())
            .nodeId(peer.getId())
            // Listening port should be disabled
            .disableListening()
            // Discovery port is unknown
            .disableDiscovery()
            .build();
    assertThat(peerConnection.getPeer().getEnodeURL()).isEqualTo(expectedEnode);

    // Next phase of pipeline should be setup
    verify(pipeline, times(1)).addLast(any(ChannelHandler[].class));

    // Next message should be pushed out
    final PingMessage nextMessage = PingMessage.get();
    final ByteBuf nextData = Unpooled.wrappedBuffer(nextMessage.getData().toArray());
    when(framer.deframe(eq(nextData)))
        .thenReturn(new RawMessage(nextMessage.getCode(), nextMessage.getData()))
        .thenReturn(null);
    verify(pipeline, times(1)).addLast(any(ChannelHandler[].class));
    deFramer.decode(ctx, nextData, out);
    assertThat(out.size()).isEqualTo(1);
  }

  @Test
  public void decode_duringHandshakeFindsPeerInPeerTable()
      throws ExecutionException, InterruptedException {
    final ChannelFuture future = NettyMocks.channelFuture(false);
    when(channel.closeFuture()).thenReturn(future);

    final Peer peer = createRemotePeer();
    final PeerInfo remotePeerInfo =
        new PeerInfo(p2pVersion, clientId, capabilities, 0, peer.getId());

    final HelloMessage helloMessage = HelloMessage.create(remotePeerInfo);
    final Bytes nodeId = helloMessage.getPeerInfo().getNodeId();
    final String enodeURLString =
        "enode://" + nodeId.toString().substring(2) + "@" + "12.13.14.15:30303?discport=30301";
    final Optional<DiscoveryPeerV4> discoveryPeer =
        DiscoveryPeerV4.from(DefaultPeer.fromURI(enodeURLString));
    final ForkId forkId = new ForkId(Bytes.fromHexString("0x190a55ad"), 4L);
    discoveryPeer.orElseThrow().setForkId(forkId);
    final DeFramer deFramer = createDeFramer(null, discoveryPeer);
    final ByteBuf data = Unpooled.wrappedBuffer(helloMessage.getData().toArray());
    when(framer.deframe(eq(data)))
        .thenReturn(new RawMessage(helloMessage.getCode(), helloMessage.getData()))
        .thenReturn(null);
    final List<Object> out = new ArrayList<>();
    deFramer.decode(ctx, data, out);

    assertThat(connectFuture).isDone();
    assertThat(connectFuture).isNotCompletedExceptionally();
    final PeerConnection peerConnection = connectFuture.get();
    assertThat(peerConnection.getPeerInfo()).isEqualTo(remotePeerInfo);
    assertThat(peerConnection.getPeer().getForkId().orElseThrow()).isEqualTo(forkId);
  }

  @Test
  public void decode_handlesUnexpectedPeerId() {
    final ChannelFuture future = NettyMocks.channelFuture(false);
    when(channel.closeFuture()).thenReturn(future);

    final Peer peer = createRemotePeer();
    final Bytes mismatchedId = Peer.randomId();
    final PeerInfo remotePeerInfo =
        new PeerInfo(
            p2pVersion,
            clientId,
            capabilities,
            peer.getEnodeURL().getListeningPortOrZero(),
            mismatchedId);
    final DeFramer deFramer = createDeFramer(peer, Optional.empty());

    final HelloMessage helloMessage = HelloMessage.create(remotePeerInfo);
    final ByteBuf data = Unpooled.wrappedBuffer(helloMessage.getData().toArray());
    when(framer.deframe(eq(data)))
        .thenReturn(new RawMessage(helloMessage.getCode(), helloMessage.getData()))
        .thenReturn(null);
    final List<Object> out = new ArrayList<>();
    deFramer.decode(ctx, data, out);

    assertThat(connectFuture).isDone();
    assertThat(connectFuture).isCompletedExceptionally();
    assertThatThrownBy(connectFuture::get)
        .hasCauseInstanceOf(UnexpectedPeerConnectionException.class)
        .hasMessageContaining("Expected id " + peer.getId().toString());

    assertThat(out).isEmpty();

    // Next phase of pipeline should be setup
    verify(pipeline, times(1)).addLast(any(ChannelHandler[].class));
  }

  @Test
  public void decode_handlesNoSharedCaps() {
    final ChannelFuture future = NettyMocks.channelFuture(false);
    when(channel.closeFuture()).thenReturn(future);

    final PeerInfo remotePeerInfo =
        new PeerInfo(
            p2pVersion,
            "bla",
            Arrays.asList(Capability.create("eth", 254)),
            30303,
            Peer.randomId());
    final HelloMessage helloMessage = HelloMessage.create(remotePeerInfo);
    final ByteBuf data = Unpooled.wrappedBuffer(helloMessage.getData().toArray());
    when(framer.deframe(eq(data)))
        .thenReturn(new RawMessage(helloMessage.getCode(), helloMessage.getData()))
        .thenReturn(null);
    final List<Object> out = new ArrayList<>();
    deFramer.decode(ctx, data, out);

    assertThat(connectFuture).isDone();
    assertThat(connectFuture).isCompletedExceptionally();
    assertThatThrownBy(connectFuture::get).hasCauseInstanceOf(IncompatiblePeerException.class);
    assertThat(out).isEmpty();

    // Next phase of pipeline should be setup
    verify(pipeline, times(1)).addLast(any(ChannelHandler[].class));
  }

  @Test
  public void decode_shouldHandleImmediateDisconnectMessage() {
    final DisconnectMessage disconnectMessage =
        DisconnectMessage.create(DisconnectReason.TOO_MANY_PEERS);
    final ByteBuf disconnectData = Unpooled.wrappedBuffer(disconnectMessage.getData().toArray());
    when(framer.deframe(eq(disconnectData)))
        .thenReturn(new RawMessage(disconnectMessage.getCode(), disconnectMessage.getData()))
        .thenReturn(null);
    final List<Object> out = new ArrayList<>();
    deFramer.decode(ctx, disconnectData, out);

    assertThat(connectFuture).isDone();
    assertThatThrownBy(connectFuture::get)
        .hasCauseInstanceOf(PeerDisconnectedException.class)
        .hasMessageContaining(disconnectMessage.getReason().toString());
    verify(ctx).close();
    assertThat(out).isEmpty();
  }

  @Test
  public void decode_shouldHandleRemoteSocketAddressIsNull() {
    final Peer peer = createRemotePeer();
    final PeerInfo remotePeerInfo =
        new PeerInfo(p2pVersion, clientId, capabilities, 0, peer.getId());
    final HelloMessage helloMessage = HelloMessage.create(remotePeerInfo);
    final ByteBuf data = Unpooled.wrappedBuffer(helloMessage.getData().toArray());
    when(framer.deframe(any()))
        .thenReturn(new RawMessage(helloMessage.getCode(), helloMessage.getData()))
        .thenReturn(null);
    when(channel.remoteAddress()).thenReturn(null);
    final ChannelFuture future = NettyMocks.channelFuture(true);
    when(ctx.writeAndFlush(any())).thenReturn(future);
    final List<Object> out = new ArrayList<>();
    deFramer.decode(ctx, data, out);

    assertThat(connectFuture).isDone();
    assertThat(connectFuture).isCompletedExceptionally();
    assertThatThrownBy(connectFuture::get).hasCauseInstanceOf(PeerChannelClosedException.class);
    verify(ctx).close();
    assertThat(out).isEmpty();
  }

  @Test
  public void decode_shouldHandleInvalidMessage() {
    final MessageData messageData = PingMessage.get();
    final ByteBuf data = Unpooled.wrappedBuffer(messageData.getData().toArray());
    when(framer.deframe(eq(data)))
        .thenReturn(new RawMessage(messageData.getCode(), messageData.getData()))
        .thenReturn(null);
    final ChannelFuture future = NettyMocks.channelFuture(true);
    when(ctx.writeAndFlush(any())).thenReturn(future);
    final List<Object> out = new ArrayList<>();
    deFramer.decode(ctx, data, out);

    final ArgumentCaptor<Object> outboundMessageArgumentCaptor =
        ArgumentCaptor.forClass(OutboundMessage.class);
    verify(ctx, times(1)).writeAndFlush(outboundMessageArgumentCaptor.capture());
    final OutboundMessage outboundMessage =
        (OutboundMessage) outboundMessageArgumentCaptor.getValue();
    assertThat(outboundMessage.getCapability()).isNull();
    final MessageData outboundMessageData = outboundMessage.getData();
    assertThat(outboundMessageData.getCode()).isEqualTo(WireMessageCodes.DISCONNECT);
    assertThat(DisconnectMessage.readFrom(outboundMessageData).getReason())
        .isEqualTo(DisconnectReason.BREACH_OF_PROTOCOL);

    assertThat(connectFuture).isDone();
    assertThatThrownBy(connectFuture::get).hasCauseInstanceOf(BreachOfProtocolException.class);
    verify(ctx).close();
    assertThat(out).isEmpty();
  }

  private Peer createRemotePeer() {
    return DefaultPeer.fromEnodeURL(
        EnodeURLImpl.builder()
            .ipAddress(remoteAddress.getAddress())
            .discoveryAndListeningPorts(remotePort)
            .nodeId(Peer.randomId())
            .build());
  }

  private PeerInfo createPeerInfo(final Peer forPeer) {
    return new PeerInfo(
        p2pVersion,
        clientId,
        capabilities,
        forPeer.getEnodeURL().getListeningPortOrZero(),
        forPeer.getId());
  }

  private DeFramer createDeFramer(
      final Peer expectedPeer, final Optional<DiscoveryPeerV4> peerInPeerTable) {
    final PeerTable peerTable = new PeerTable(localNode.getPeerInfo().getNodeId());
    PeerLookup peerLookup = new PeerLookup();
    peerLookup.set(peer -> peerTable.get(peer).map(p -> p));
    peerInPeerTable.ifPresent(peerTable::tryAdd);
    return new DeFramer(
        framer,
        Arrays.asList(MockSubProtocol.create("eth")),
        localNode,
        Optional.ofNullable(expectedPeer),
        connectionEventDispatcher,
        connectFuture,
        new NoOpMetricsSystem(),
        true,
        peerLookup);
  }
}
