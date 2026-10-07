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
package org.hyperledger.besu.ethereum.p2p.rlpx.connections;

import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.CapabilityMultiplexer;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.PeerInfo;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.SubProtocol;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.WireMessageCodes;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

public abstract class AbstractPeerConnection implements PeerConnection {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractPeerConnection.class);
  private static final Marker P2P_MESSAGE_MARKER = MarkerFactory.getMarker("P2PMSG");
  private final Peer peer;
  private final PeerInfo peerInfo;
  private final InetSocketAddress localAddress;
  private final InetSocketAddress remoteAddress;
  private final String connectionId;
  private final CapabilityMultiplexer multiplexer;

  private final Set<Capability> agreedCapabilities;
  private final Map<String, Capability> protocolToCapability = new HashMap<>();
  private final AtomicBoolean disconnected = new AtomicBoolean(false);
  private final AtomicBoolean terminatedImmediately = new AtomicBoolean(false);
  protected final PeerConnectionEventDispatcher connectionEventDispatcher;
  private final LabelledMetric<Counter> outboundMessagesCounter;
  private final LabelledMetric<Counter> outboundBytesCounter;
  private final long initiatedAt;
  private final boolean inboundInitiated;
  private boolean statusSent;
  private boolean statusReceived;

  protected AbstractPeerConnection(
      final Peer peer,
      final PeerInfo peerInfo,
      final InetSocketAddress localAddress,
      final InetSocketAddress remoteAddress,
      final String connectionId,
      final CapabilityMultiplexer multiplexer,
      final PeerConnectionEventDispatcher connectionEventDispatcher,
      final LabelledMetric<Counter> outboundMessagesCounter,
      final LabelledMetric<Counter> outboundBytesCounter,
      final boolean inboundInitiated) {
    this.peer = peer;
    this.peerInfo = peerInfo;
    this.localAddress = localAddress;
    this.remoteAddress = remoteAddress;
    this.connectionId = connectionId;
    this.multiplexer = multiplexer;

    this.agreedCapabilities = multiplexer.getAgreedCapabilities();
    for (final Capability cap : agreedCapabilities) {
      protocolToCapability.put(cap.getName(), cap);
    }
    this.connectionEventDispatcher = connectionEventDispatcher;
    this.outboundMessagesCounter = outboundMessagesCounter;
    this.outboundBytesCounter = outboundBytesCounter;
    this.inboundInitiated = inboundInitiated;
    this.initiatedAt = System.currentTimeMillis();

    LOG.atDebug()
        .setMessage("New PeerConnection ({}) established with peer {}")
        .addArgument(this)
        .addArgument(peer.getLoggableId())
        .log();
  }

  @Override
  public void send(final Capability capability, final MessageData message) throws PeerNotConnected {
    if (isDisconnected()) {
      throw new PeerNotConnected("Attempt to send message to a closed peer connection");
    }
    doSend(capability, message);
  }

  private void doSend(final Capability capability, final MessageData message) {
    if (capability != null) {
      // Validate message is valid for this capability
      final SubProtocol subProtocol = multiplexer.subProtocol(capability);
      if (subProtocol == null
          || !subProtocol.isValidMessageCode(capability.getVersion(), message.getCode())) {
        throw new UnsupportedOperationException(
            "Attempt to send unsupported message ("
                + message.getCode()
                + ") via cap "
                + capability);
      }
      outboundMessagesCounter
          .labels(
              capability.toString(),
              subProtocol.messageName(capability.getVersion(), message.getCode()),
              Integer.toString(message.getCode()))
          .inc();
      outboundBytesCounter
          .labels(
              capability.toString(),
              subProtocol.messageName(capability.getVersion(), message.getCode()),
              Integer.toString(message.getCode()))
          .inc(message.getSize());

    } else {
      outboundMessagesCounter
          .labels(
              "Wire",
              WireMessageCodes.messageName(message.getCode()),
              Integer.toString(message.getCode()))
          .inc();
      outboundBytesCounter
          .labels(
              "Wire",
              WireMessageCodes.messageName(message.getCode()),
              Integer.toString(message.getCode()))
          .inc(message.getSize());
    }

    LOG.atTrace()
        .addMarker(P2P_MESSAGE_MARKER)
        .setMessage("Writing {} to {} via protocol {}")
        .addArgument(message)
        .addArgument(peerInfo)
        .addArgument(capability)
        .addKeyValue("rawData", message.getData())
        .addKeyValue("decodedData", message::toStringDecoded)
        .log();
    doSendMessage(capability, message);
  }

  protected abstract void doSendMessage(final Capability capability, final MessageData message);

  @Override
  public PeerInfo getPeerInfo() {
    return peerInfo;
  }

  @Override
  public Capability capability(final String protocol) {
    return protocolToCapability.get(protocol);
  }

  @Override
  public Peer getPeer() {
    return peer;
  }

  @Override
  public Set<Capability> getAgreedCapabilities() {
    return agreedCapabilities;
  }

  @Override
  public void terminateConnection(final DisconnectReason reason, final boolean peerInitiated) {
    if (terminatedImmediately.compareAndSet(false, true)) {
      if (disconnected.compareAndSet(false, true)) {
        connectionEventDispatcher.dispatchDisconnect(this, reason, peerInitiated);
      }
      // Always ensure the context gets closed immediately even if we previously sent a disconnect
      // message and are waiting to close.
      closeConnectionImmediately();
      LOG.atTrace()
          .setMessage("Terminating connection {}, reason {}")
          .addArgument(this)
          .addArgument(reason)
          .log();
    }
  }

  protected abstract void closeConnectionImmediately();

  protected abstract void closeConnection();

  @Override
  public void disconnect(final DisconnectReason reason) {
    if (disconnected.compareAndSet(false, true)) {
      try {
        // send the disconnect message first, in case the dispatchDisconnect throws an exception
        doSend(null, DisconnectMessage.create(reason));
        LOG.atDebug()
            .setMessage("Disconnecting connection {}, peer {} reason {}")
            .addArgument(this.hashCode())
            .addArgument(peer.getLoggableId())
            .addArgument(reason)
            .log();
        connectionEventDispatcher.dispatchDisconnect(this, reason, false);
      } finally {
        // always close the connection
        closeConnection();
      }
    }
  }

  @Override
  public boolean isDisconnected() {
    return disconnected.get();
  }

  @Override
  public InetSocketAddress getLocalAddress() {
    return localAddress;
  }

  @Override
  public InetSocketAddress getRemoteAddress() {
    return remoteAddress;
  }

  @Override
  public long getInitiatedAt() {
    return initiatedAt;
  }

  @Override
  public boolean inboundInitiated() {
    return inboundInitiated;
  }

  @Override
  public boolean equals(final Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof AbstractPeerConnection that)) {
      return false;
    }
    return Objects.equals(this.connectionId, that.connectionId)
        && Objects.equals(this.peer, that.peer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(connectionId, peer);
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
    return statusReceived && statusSent;
  }

  @Override
  public String toString() {
    return "[Connection with hashCode "
        + hashCode()
        + " inboundInitiated? "
        + inboundInitiated
        + " initAt "
        + initiatedAt
        + "]";
  }
}
