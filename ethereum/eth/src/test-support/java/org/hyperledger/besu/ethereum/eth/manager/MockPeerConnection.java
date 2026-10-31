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
package org.hyperledger.besu.ethereum.eth.manager;

import org.hyperledger.besu.ethereum.p2p.peers.DefaultPeer;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.PeerInfo;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;

public class MockPeerConnection implements PeerConnection {

  private static final PeerSendHandler NOOP_ON_SEND = (cap, msg, conn) -> {};
  private final PeerSendHandler onSend;
  private final Set<Capability> caps;
  private volatile boolean disconnected = false;
  private final Bytes nodeId;
  private final Peer peer;
  private final PeerInfo peerInfo;
  private Optional<DisconnectReason> disconnectReason = Optional.empty();
  private boolean statusSent;
  private boolean statusReceived;
  private boolean inboundInitiated;

  public MockPeerConnection(final Set<Capability> caps, final PeerSendHandler onSend) {
    this.caps = caps;
    this.onSend = onSend;
    this.nodeId = Peer.randomId();
    this.peer =
        DefaultPeer.fromEnodeURL(
            EnodeURLImpl.builder()
                .ipAddress("127.0.0.1")
                .nodeId(nodeId)
                .discoveryAndListeningPorts(30303)
                .build());
    this.peerInfo = new PeerInfo(5, "Mock", new ArrayList<>(caps), 30303, nodeId);
  }

  public MockPeerConnection(final Set<Capability> caps) {
    this(caps, NOOP_ON_SEND);
  }

  @Override
  public void send(final Capability capability, final MessageData message) throws PeerNotConnected {
    if (disconnected) {
      throw new PeerNotConnected("MockPeerConnection disconnected");
    }
    onSend.exec(capability, message, this);
  }

  @Override
  public Set<Capability> getAgreedCapabilities() {
    return caps;
  }

  @Override
  public Peer getPeer() {
    return peer;
  }

  @Override
  public PeerInfo getPeerInfo() {
    return peerInfo;
  }

  @Override
  public void terminateConnection(final DisconnectReason reason, final boolean peerInitiated) {
    disconnect(reason);
  }

  @Override
  public void disconnect(final DisconnectReason reason) {
    if (disconnected) {
      // Already disconnected
      return;
    }
    disconnectReason = Optional.of(reason);
    disconnected = true;
  }

  public Optional<DisconnectReason> getDisconnectReason() {
    return disconnectReason;
  }

  @Override
  public InetSocketAddress getLocalAddress() {
    throw new UnsupportedOperationException();
  }

  @Override
  public InetSocketAddress getRemoteAddress() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getInitiatedAt() {
    return 0;
  }

  @Override
  public boolean inboundInitiated() {
    return inboundInitiated;
  }

  public void setInboundInitiated(final boolean inboundInitiated) {
    this.inboundInitiated = inboundInitiated;
  }

  @Override
  public boolean isDisconnected() {
    return disconnected;
  }

  @Override
  public void setStatusSent() {
    this.statusSent = true;
  }

  @Override
  public void setStatusReceived() {
    this.statusReceived = true;
  }

  @Override
  public boolean getStatusExchanged() {
    return statusSent && statusReceived;
  }

  @FunctionalInterface
  public interface PeerSendHandler {
    void exec(Capability cap, MessageData msg, PeerConnection connection);
  }
}
