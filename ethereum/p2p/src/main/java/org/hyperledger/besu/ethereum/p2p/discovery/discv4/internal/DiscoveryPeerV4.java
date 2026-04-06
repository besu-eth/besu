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
package org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal;

import org.hyperledger.besu.ethereum.p2p.discovery.DiscoveryPeer;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.Endpoint;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;
import org.hyperledger.besu.plugin.data.EnodeURL;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.tuweni.bytes.Bytes;

/**
 * Represents an Ethereum node that we are interacting with through the discovery and wire
 * protocols.
 */
public class DiscoveryPeerV4 extends DiscoveryPeer {
  private PeerDiscoveryStatus status = PeerDiscoveryStatus.KNOWN;
  // Endpoint is a datastructure used in discovery messages
  private final Endpoint endpoint;

  // Timestamps.
  private final AtomicLong firstDiscovered = new AtomicLong(0L);

  private DiscoveryPeerV4(final EnodeURLImpl enode, final Endpoint endpoint) {
    super(enode);
    this.endpoint = endpoint;
  }

  public static DiscoveryPeerV4 fromEnode(final EnodeURLImpl enode) {
    return new DiscoveryPeerV4(enode, Endpoint.fromEnode(enode));
  }

  public static Optional<DiscoveryPeerV4> from(final Peer peer) {
    if (peer instanceof DiscoveryPeerV4) {
      return Optional.of((DiscoveryPeerV4) peer);
    }

    return Optional.of(peer)
        .map(Peer::getEnodeURL)
        .filter(EnodeURL::isRunningDiscovery)
        .map(DiscoveryPeerV4::fromEnode);
  }

  public static DiscoveryPeerV4 fromIdAndEndpoint(final Bytes id, final Endpoint endpoint) {
    return new DiscoveryPeerV4(endpoint.toEnode(id), endpoint);
  }

  public static DiscoveryPeerV4 readFrom(final RLPInput in) {
    final int size = in.enterList();

    // The last list item will be the id, pass size - 1 to Endpoint
    final Endpoint endpoint = Endpoint.decodeInline(in, size - 1);
    final Bytes id = in.readBytes();
    in.leaveList();

    return DiscoveryPeerV4.fromIdAndEndpoint(id, endpoint);
  }

  public void writeTo(final RLPOutput out) {
    out.startList();
    endpoint.encodeInline(out);
    out.writeBytes(getId());
    out.endList();
  }

  public PeerDiscoveryStatus getStatus() {
    return status;
  }

  public boolean isKnown() {
    return status == PeerDiscoveryStatus.KNOWN;
  }

  public boolean isBonded() {
    return status == PeerDiscoveryStatus.BONDED;
  }

  public void setBonded() {
    status = PeerDiscoveryStatus.BONDED;
  }

  public void setStatus(final PeerDiscoveryStatus status) {
    this.status = status;
  }

  public long getFirstDiscovered() {
    return firstDiscovered.get();
  }

  public void setFirstDiscovered(final long firstDiscovered) {
    this.firstDiscovered.compareAndExchange(0L, firstDiscovered);
  }

  @Override
  public boolean isReadyForConnections() {
    // On discv4, we consider a peer ready for connections if it's bonded and listening
    return isListening() && this.isBonded();
  }

  public Endpoint getEndpoint() {
    return endpoint;
  }

  public boolean discoveryEndpointMatches(final DiscoveryPeerV4 peer) {
    return peer.getEndpoint().getHost().equals(endpoint.getHost())
        && peer.getEndpoint().getUdpPort() == endpoint.getUdpPort();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("DiscoveryPeer{");
    sb.append("status=").append(status);
    sb.append(", enode=").append(this.getEnodeURL());
    sb.append(", firstDiscovered=").append(firstDiscovered);
    sb.append('}');
    return sb.toString();
  }
}
