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
package org.hyperledger.besu.ethereum.p2p.discovery.discv4;

import static org.hyperledger.besu.util.NetworkUtility.checkPort;

import org.hyperledger.besu.ethereum.p2p.discovery.PeerDiscoveryPacketDecodingException;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.DiscoveryPeerV4;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;
import org.hyperledger.besu.plugin.data.EnodeURL;
import org.hyperledger.besu.util.IllegalPortException;
import org.hyperledger.besu.util.Preconditions;

import java.net.InetAddress;
import java.util.Objects;
import java.util.Optional;

import com.google.common.net.InetAddresses;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates the network coordinates of a {@link DiscoveryPeerV4} as well as serialization logic
 * used in various Discovery messages.
 */
public class Endpoint {
  private static final Logger LOG = LoggerFactory.getLogger(Endpoint.class);
  private final Optional<String> host;
  private final int udpPort;
  private final Optional<Integer> tcpPort;

  public Endpoint(final String host, final int udpPort, final Optional<Integer> tcpPort) {
    checkPort(udpPort, "UDP");
    tcpPort.ifPresent(port -> checkPort(port, "TCP"));

    this.host = Optional.ofNullable(host);
    this.udpPort = udpPort;
    this.tcpPort = tcpPort;
  }

  public static Endpoint fromEnode(final EnodeURL enode) {
    Optional<Integer> discoveryPort = enode.getDiscoveryPort();

    if (discoveryPort.isEmpty()) {
      int defaultPort = EnodeURLImpl.DEFAULT_LISTENING_PORT;
      LOG.debug("Discovery disabled for enode {}. Using default port {}.", enode, defaultPort);
      return new Endpoint(enode.getIp().getHostAddress(), defaultPort, Optional.empty());
    }

    final Optional<Integer> listeningPort = enode.getListeningPort();
    return new Endpoint(enode.getIp().getHostAddress(), discoveryPort.get(), listeningPort);
  }

  public EnodeURLImpl toEnode(final Bytes nodeId) {
    return EnodeURLImpl.builder()
        .nodeId(nodeId)
        .ipAddress(host.orElse(""))
        .listeningPort(tcpPort.orElse(udpPort))
        .discoveryPort(udpPort)
        .build();
  }

  public String getHost() {
    return host.orElse("");
  }

  public int getUdpPort() {
    return udpPort;
  }

  public Optional<Integer> getTcpPort() {
    return tcpPort;
  }

  /**
   * If the tcp port is explicitly defined, return it. Otherwise, return the udp port assuming that
   * the tcp port should match the udp port.
   *
   * @return The tcp port to use for this endpoint.
   */
  public int getFunctionalTcpPort() {
    return tcpPort.orElse(udpPort);
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof Endpoint)) {
      return false;
    }
    final Endpoint other = (Endpoint) obj;
    return host.equals(other.host)
        && this.udpPort == other.udpPort
        && (this.tcpPort.equals(other.tcpPort));
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, udpPort, tcpPort);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("Endpoint{");
    sb.append("host='").append(host).append('\'');
    sb.append(", udpPort=").append(udpPort);
    tcpPort.ifPresent(p -> sb.append(", getTcpPort=").append(p));
    sb.append('}');
    return sb.toString();
  }

  /**
   * Encodes this endpoint into a standalone object.
   *
   * @param out The RLP output stream.
   */
  public void encodeStandalone(final RLPOutput out) {
    out.startList();
    encodeInline(out);
    out.endList();
  }

  /**
   * Encodes this endpoint to an RLP representation that is inlined into a containing object
   * (generally a Peer).
   *
   * @param out The RLP output stream.
   */
  public void encodeInline(final RLPOutput out) {
    if (host.isPresent()) {
      InetAddress hostAddress = InetAddresses.forString(host.get());
      if (hostAddress != null) {
        out.writeInetAddress(hostAddress);
      } else { // present, but not a parseable IP address
        out.writeNull();
      }
    } else {
      out.writeNull();
    }
    out.writeIntScalar(udpPort);
    tcpPort.ifPresentOrElse(out::writeIntScalar, out::writeNull);
  }

  /**
   * Decodes the input stream as an Endpoint instance appearing inline within another object
   * (generally a Peer).
   *
   * @param fieldCount The number of fields RLP list.
   * @param in The RLP input stream from which to read.
   * @return The decoded endpoint.
   */
  public static Endpoint decodeInline(final RLPInput in, final int fieldCount) {
    Preconditions.checkGuard(
        fieldCount == 2 || fieldCount == 3,
        PeerDiscoveryPacketDecodingException::new,
        "Invalid number of components in RLP representation of an endpoint: expected 2 or 3 elements but got %s",
        fieldCount);

    String hostAddress = null;
    if (!in.nextIsNull()) {
      InetAddress addr = in.readInetAddress();
      hostAddress = addr.getHostAddress();
    } else {
      in.skipNext();
    }
    final int udpPort = in.readIntScalar();

    // Some mainnet packets have been shown to either not have the TCP port field at all,
    // or to have an RLP NULL value for it.
    Optional<Integer> tcpPort = Optional.empty();
    if (fieldCount == 3) {
      if (in.nextIsNull()) {
        in.skipNext();
      } else {
        tcpPort = Optional.of(in.readIntScalar());
      }
    }
    return new Endpoint(hostAddress, udpPort, tcpPort);
  }

  /**
   * Decodes the RLP stream as a standalone Endpoint instance, which is not part of a Peer.
   *
   * @param in The RLP input stream from which to read.
   * @return The decoded endpoint.
   */
  public static Endpoint decodeStandalone(final RLPInput in) {
    final int size = in.enterList();
    final Endpoint endpoint = decodeInline(in, size);
    in.leaveList();
    return endpoint;
  }

  /**
   * Attempts to decodes the RLP stream as a standalone Endpoint instance, which is not part of a
   * Peer. If the from field is malformed, consumes the rest of the items in the list and returns
   * Optional.empty().
   *
   * @param in The RLP input stream from which to read.
   * @return Some decoded endpoint if it was possible to decode it, empty otherwise.
   */
  public static Optional<Endpoint> maybeDecodeStandalone(final RLPInput in) {
    try {
      return Optional.of(decodeStandalone(in));
    } catch (IllegalPortException __) {
      return Optional.empty();
    }
  }
}
