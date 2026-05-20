/*
 * Copyright contributors to Hyperledger Besu.
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

// Adapted from https://github.com/tmio/tuweni and licensed under Apache 2.0
package org.hyperledger.besu.ethereum.p2p.discovery.dns;

import org.hyperledger.besu.crypto.SECP256K1;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.ethereum.p2p.discovery.NodeIdentifier;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.google.common.base.MoreObjects;
import org.apache.tuweni.bytes.Bytes;
import org.ethereum.beacon.discovery.schema.EnrField;
import org.ethereum.beacon.discovery.schema.NodeRecord;
import org.ethereum.beacon.discovery.schema.NodeRecordFactory;

/**
 * A modified implementation of Ethereum Node Record (ENR) that is used by DNSResolver. See <a
 * href="https://eips.ethereum.org/EIPS/eip-778">EIP-778</a>
 */
public record EthereumNodeRecord(
    Bytes publicKey,
    Optional<InetAddress> ip,
    Optional<Integer> tcp,
    Optional<Integer> udp,
    Optional<InetAddress> ipV6,
    Optional<Integer> tcpV6,
    Optional<Integer> udpV6,
    NodeRecord nodeRecord)
    implements NodeIdentifier {

  // ENR identity scheme v4 always uses secp256k1, independent of the node's signature algorithm
  private static final SignatureAlgorithm SECP256K1 = new SECP256K1();

  @SuppressWarnings(
      "MethodInputParametersMustBeFinal") // needed since record constructors are not yet supported
  public EthereumNodeRecord {
    if (ip.isEmpty() && ipV6.isEmpty()) {
      throw new IllegalArgumentException(
          "Unable to create EthereumNodeRecord with no IPv4 or IPv6 address");
    }
  }

  /**
   * Creates an EthereumNodeRecord from an ENR string
   *
   * @param enr the ENR string
   * @return the EthereumNodeRecord
   */
  public static EthereumNodeRecord fromEnr(final String enr) {
    NodeRecord nodeRecord = NodeRecordFactory.DEFAULT.fromEnr(enr);
    return fromNodeRecord(nodeRecord);
  }

  /**
   * Creates an EthereumNodeRecord from a NodeRecord
   *
   * @param nodeRecord the NodeRecord
   * @return the EthereumNodeRecord
   */
  public static EthereumNodeRecord fromNodeRecord(final NodeRecord nodeRecord) {
    final Map<String, Object> fields = new HashMap<>();
    nodeRecord.forEachField(fields::put);
    return new EthereumNodeRecord(
        initPublicKeyBytes(fields),
        initIPAddr(fields),
        initTCP(fields),
        initUDP(fields),
        initIPv6Addr(fields),
        initTCPV6(fields),
        initUDPV6(fields),
        nodeRecord);
  }

  /**
   * Extracts the 64-byte uncompressed public key from a NodeRecord without requiring an IP address.
   *
   * @param nodeRecord the NodeRecord to extract the public key from
   * @return the 64-byte uncompressed public key (without the 0x04 prefix)
   * @throws IllegalArgumentException if the secp256k1 key is missing or malformed
   */
  public static Bytes uncompressedPublicKey(final NodeRecord nodeRecord) {
    final Object value = nodeRecord.get(EnrField.PKEY_SECP256K1);
    if (!(value instanceof Bytes keyBytes)) {
      throw new IllegalArgumentException("Missing secp256k1 entry in ENR");
    }
    var ecPoint = SECP256K1.getCurve().getCurve().decodePoint(keyBytes.toArrayUnsafe());
    var encodedPubKey = ecPoint.getEncoded(false);
    return Bytes.of(Arrays.copyOfRange(encodedPubKey, 1, encodedPubKey.length));
  }

  /**
   * Returns the public key of the ENR
   *
   * @return the public key of the ENR
   */
  static Bytes initPublicKeyBytes(final Map<String, Object> fields) {
    final Object value = fields.get("secp256k1");
    if (!(value instanceof Bytes keyBytes)) {
      throw new IllegalArgumentException("Missing secp256k1 entry in ENR");
    }
    // convert 33 bytes compressed public key to uncompressed using Bouncy Castle
    // ENR identity scheme v4 always uses secp256k1 for the public key
    var ecPoint = SECP256K1.getCurve().getCurve().decodePoint(keyBytes.toArrayUnsafe());
    // uncompressed public key is 65 bytes, first byte is 0x04.
    var encodedPubKey = ecPoint.getEncoded(false);
    return Bytes.of(Arrays.copyOfRange(encodedPubKey, 1, encodedPubKey.length));
  }

  /**
   * Returns the InetAddress of the ENR
   *
   * @return The IP address of the ENR
   */
  static Optional<InetAddress> initIPAddr(final Map<String, Object> fields) {
    final Object value = fields.get("ip");
    if (value instanceof Bytes ipBytes) {
      try {
        return Optional.of(InetAddress.getByAddress(ipBytes.toArrayUnsafe()));
      } catch (final Exception e) {
        return Optional.empty();
      }
    }
    return Optional.empty();
  }

  /**
   * The TCP port of the ENR
   *
   * @return the TCP port associated with this ENR
   */
  static Optional<Integer> initTCP(final Map<String, Object> fields) {
    return extractInt(fields, "tcp");
  }

  /**
   * The UDP port of the ENR. If the UDP port is not present, the TCP port is used.
   *
   * @return the UDP port associated with this ENR
   */
  static Optional<Integer> initUDP(final Map<String, Object> fields) {
    return extractInt(fields, "udp").or(() -> initTCP(fields));
  }

  static Optional<InetAddress> initIPv6Addr(final Map<String, Object> fields) {
    final Object value = fields.get("ip6");
    if (value instanceof Bytes ipBytes) {
      try {
        return Optional.of(InetAddress.getByAddress(ipBytes.toArrayUnsafe()));
      } catch (final Exception e) {
        return Optional.empty();
      }
    }
    return Optional.empty();
  }

  static Optional<Integer> initTCPV6(final Map<String, Object> fields) {
    return extractInt(fields, "tcp6");
  }

  static Optional<Integer> initUDPV6(final Map<String, Object> fields) {
    return extractInt(fields, "udp6").or(() -> initTCPV6(fields));
  }

  private static Optional<Integer> extractInt(final Map<String, Object> fields, final String key) {
    final Object value = fields.get(key);
    if (value instanceof Integer integer) {
      return Optional.of(integer);
    }
    return Optional.empty();
  }

  /**
   * @return the ENR as a URI
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("publicKey", publicKey)
        .add("ip", ip)
        .add("tcp", tcp)
        .add("udp", udp)
        .add("ipv6", ipV6)
        .add("tcpV6", tcpV6)
        .add("udpV6", udpV6)
        .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(publicKey, ip, tcp, udp, ipV6, tcpV6, udpV6, nodeRecord);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final EthereumNodeRecord other = (EthereumNodeRecord) obj;
    return Objects.equals(this.publicKey, other.publicKey)
        && Objects.equals(this.ip, other.ip)
        && Objects.equals(this.tcp, other.tcp)
        && Objects.equals(this.udp, other.udp)
        && Objects.equals(this.ipV6, other.ipV6)
        && Objects.equals(this.tcpV6, other.tcpV6)
        && Objects.equals(this.udpV6, other.udpV6)
        && Objects.equals(this.nodeRecord, other.nodeRecord);
  }

  @Override
  public Optional<InetAddress> getIpV4Address() {
    return ip;
  }

  @Override
  public Optional<Integer> getTcpListeningPort() {
    return tcp;
  }

  @Override
  public Optional<Integer> getUdpDiscoveryPort() {
    return udp;
  }

  @Override
  public Optional<InetAddress> getIpV6Address() {
    return ipV6;
  }

  @Override
  public Optional<Integer> getIpV6TcpListeningPort() {
    return tcpV6;
  }

  @Override
  public Optional<Integer> getIpV6UdpDiscoveryPort() {
    return udpV6;
  }
}
