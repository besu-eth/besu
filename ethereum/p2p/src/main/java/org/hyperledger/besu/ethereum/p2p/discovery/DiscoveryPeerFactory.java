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
package org.hyperledger.besu.ethereum.p2p.discovery;

import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.DiscoveryPeerV4;
import org.hyperledger.besu.ethereum.p2p.discovery.dns.EthereumNodeRecord;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;

import org.ethereum.beacon.discovery.schema.NodeRecord;

public class DiscoveryPeerFactory {

  private DiscoveryPeerFactory() {
    // utility class
  }

  public static DiscoveryPeer fromEnode(final EnodeURLImpl enode) {
    return DiscoveryPeerV4.fromEnode(enode);
  }

  public static DiscoveryPeer fromNodeRecord(
      final NodeRecord nodeRecord, final boolean preferIpv6Outbound) {
    EthereumNodeRecord enr = EthereumNodeRecord.fromNodeRecord(nodeRecord);
    return fromEthereumNodeRecord(enr, preferIpv6Outbound);
  }

  public static DiscoveryPeer fromEthereumNodeRecord(final EthereumNodeRecord enr) {
    return fromEthereumNodeRecord(enr, false);
  }

  public static DiscoveryPeer fromEthereumNodeRecord(
      final EthereumNodeRecord enr, final boolean preferIpv6Outbound) {
    DiscoveryPeer peer = fromEnode(buildEnodeUrl(enr, preferIpv6Outbound));
    peer.setNodeRecord(enr.nodeRecord());
    return peer;
  }

  private static EnodeURLImpl buildEnodeUrl(
      final EthereumNodeRecord enr, final boolean preferIpv6Outbound) {
    final boolean hasIpv4 = enr.ip().isPresent();
    final boolean hasIpv6 = enr.ipV6().isPresent();

    if (hasIpv6 && (!hasIpv4 || preferIpv6Outbound)) {
      return EnodeURLImpl.builder()
          .ipAddress(
              enr.ipV6()
                  .orElseThrow(
                      () ->
                          new IllegalStateException(
                              "IPv6 address not present in ENR despite shouldUseIpv6 returning true")))
          .nodeId(enr.publicKey())
          .discoveryPort(enr.udpV6().or(enr::udp))
          .listeningPort(enr.tcpV6().or(enr::tcp))
          .build();
    }
    return EnodeURLImpl.builder()
        .ipAddress(enr.ip().get())
        .nodeId(enr.publicKey())
        .discoveryPort(enr.udp())
        .listeningPort(enr.tcp())
        .build();
  }
}
