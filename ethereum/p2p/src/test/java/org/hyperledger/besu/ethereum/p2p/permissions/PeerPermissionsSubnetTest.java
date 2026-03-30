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
package org.hyperledger.besu.ethereum.p2p.permissions;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.p2p.peers.DefaultPeer;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissions.Action;

import java.net.InetSocketAddress;
import java.util.List;

import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import org.junit.jupiter.api.Test;

public class PeerPermissionsSubnetTest {

  private final Peer remoteNode = createPeer("127.0.0.1");

  @Test
  public void peerInSubnetRangeShouldBePermitted() {
    List<IPAddress> allowedSubnets = List.of(subnet("127.0.0.0/24"));
    PeerPermissionSubnet peerPermissionSubnet = new PeerPermissionSubnet(allowedSubnets);
    checkPermissions(peerPermissionSubnet, remoteNode, true);
  }

  @Test
  public void peerInAtLeastOneSubnetRangeShouldBePermitted() {
    List<IPAddress> allowedSubnets = List.of(subnet("127.0.0.0/24"), subnet("10.0.0.1/24"));
    PeerPermissionSubnet peerPermissionSubnet = new PeerPermissionSubnet(allowedSubnets);
    checkPermissions(peerPermissionSubnet, remoteNode, true);
  }

  @Test
  public void peerOutSubnetRangeShouldNotBePermitted() {
    List<IPAddress> allowedSubnets = List.of(subnet("10.0.0.0/24"));
    PeerPermissionSubnet peerPermissionSubnet = new PeerPermissionSubnet(allowedSubnets);
    checkPermissions(peerPermissionSubnet, remoteNode, false);
  }

  @Test
  public void peerShouldBePermittedIfNoSubnets() {
    PeerPermissionSubnet peerPermissionSubnet = new PeerPermissionSubnet(List.of());
    checkPermissions(peerPermissionSubnet, remoteNode, true);
  }

  @Test
  public void ipv6PeerInSubnetRangeShouldBePermitted() {
    Peer ipv6Peer = createPeer("fd00::1");
    List<IPAddress> allowedSubnets = List.of(subnet("fd00::/64"));
    PeerPermissionSubnet peerPermissionSubnet = new PeerPermissionSubnet(allowedSubnets);
    checkPermissions(peerPermissionSubnet, ipv6Peer, true);
  }

  @Test
  public void ipv6PeerOutSubnetRangeShouldNotBePermitted() {
    Peer ipv6Peer = createPeer("fe80::1");
    List<IPAddress> allowedSubnets = List.of(subnet("fd00::/64"));
    PeerPermissionSubnet peerPermissionSubnet = new PeerPermissionSubnet(allowedSubnets);
    checkPermissions(peerPermissionSubnet, ipv6Peer, false);
  }

  @Test
  public void inetSocketAddressInSubnetShouldBePermitted() {
    List<IPAddress> allowedSubnets = List.of(subnet("192.168.1.0/24"));
    PeerPermissionSubnet peerPermissionSubnet = new PeerPermissionSubnet(allowedSubnets);
    assertThat(peerPermissionSubnet.isPermitted(new InetSocketAddress("192.168.1.50", 30303)))
        .isTrue();
  }

  @Test
  public void inetSocketAddressOutsideSubnetShouldNotBePermitted() {
    List<IPAddress> allowedSubnets = List.of(subnet("192.168.1.0/24"));
    PeerPermissionSubnet peerPermissionSubnet = new PeerPermissionSubnet(allowedSubnets);
    assertThat(peerPermissionSubnet.isPermitted(new InetSocketAddress("10.0.0.1", 30303)))
        .isFalse();
  }

  @Test
  public void inetSocketAddressShouldBePermittedIfNoSubnets() {
    PeerPermissionSubnet peerPermissionSubnet = new PeerPermissionSubnet(List.of());
    assertThat(peerPermissionSubnet.isPermitted(new InetSocketAddress("10.0.0.1", 30303))).isTrue();
  }

  @Test
  public void ipv6InetSocketAddressInSubnetShouldBePermitted() {
    List<IPAddress> allowedSubnets = List.of(subnet("fd00::/64"));
    PeerPermissionSubnet peerPermissionSubnet = new PeerPermissionSubnet(allowedSubnets);
    assertThat(peerPermissionSubnet.isPermitted(new InetSocketAddress("fd00::1", 30303))).isTrue();
  }

  @Test
  public void ipv6InetSocketAddressOutsideSubnetShouldNotBePermitted() {
    List<IPAddress> allowedSubnets = List.of(subnet("fd00::/64"));
    PeerPermissionSubnet peerPermissionSubnet = new PeerPermissionSubnet(allowedSubnets);
    assertThat(peerPermissionSubnet.isPermitted(new InetSocketAddress("fe80::1", 30303))).isFalse();
  }

  private void checkPermissions(
      final PeerPermissions peerPermissions, final Peer remotePeer, final boolean expectedResult) {
    for (Action action : Action.values()) {
      assertThat(peerPermissions.isPermitted(createPeer("127.0.0.1"), remotePeer, action))
          .isEqualTo(expectedResult);
    }
  }

  private IPAddress subnet(final String cidr) {
    return new IPAddressString(cidr).getAddress().toPrefixBlock();
  }

  private Peer createPeer(final String ip) {
    return DefaultPeer.fromEnodeURL(
        EnodeURLImpl.builder()
            .nodeId(Peer.randomId())
            .ipAddress(ip)
            .discoveryAndListeningPorts(EnodeURLImpl.DEFAULT_LISTENING_PORT)
            .build());
  }
}
