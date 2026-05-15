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
package org.hyperledger.besu.ethereum.p2p.discovery.discv5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.ethereum.forkid.ForkIdManager;
import org.hyperledger.besu.ethereum.p2p.config.DiscoveryConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.ImmutableNetworkingConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.NetworkingConfiguration;
import org.hyperledger.besu.ethereum.p2p.discovery.NodeRecordManager;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.DiscoveryPeerV4;
import org.hyperledger.besu.ethereum.p2p.discovery.dns.EthereumNodeRecord;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissionSubnet;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissions;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissionsDenylist;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;

import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import org.apache.tuweni.bytes.Bytes;
import org.ethereum.beacon.discovery.AddressAccessPolicy;
import org.ethereum.beacon.discovery.schema.EnrField;
import org.ethereum.beacon.discovery.schema.NodeRecord;
import org.ethereum.beacon.discovery.schema.NodeRecordFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PeerDiscoveryAgentFactoryV5Test {

  // ENR with IP 15.204.180.57, TCP port 30303, UDP port 30303
  private static final String TEST_ENR =
      "enr:-KO4QK1ecw-CGrDDZ4YwFrhgqctD0tWMHKJhUVxsS4um3aUFe3yBHRtVL9uYKk16DurN1IdSKTOB1zNCvjBybjZ_KAq"
          + "GAYtJ5U8wg2V0aMfGhJsZKtCAgmlkgnY0gmlwhA_MtDmJc2VjcDI1NmsxoQNXD7fj3sscyOKBiHYy14igj1vJYWdKYZH7n3T8qRpIcYRzb"
          + "mFwwIN0Y3CCdl-DdWRwgnZf";

  @Mock private NodeKey nodeKey;
  @Mock private ForkIdManager forkIdManager;
  @Mock private NodeRecordManager nodeRecordManager;
  @Mock private DiscoveryPeerV4 localPeer;

  private NetworkingConfiguration config;
  private NodeRecord testNodeRecord;

  @BeforeEach
  void setUp() {
    config =
        ImmutableNetworkingConfiguration.builder()
            .discoveryConfiguration(
                DiscoveryConfiguration.create()
                    .setEnabled(true)
                    .setAdvertisedHost("127.0.0.1")
                    .setBindHost("0.0.0.0")
                    .setBindPort(0))
            .build();

    testNodeRecord = NodeRecordFactory.DEFAULT.fromEnr(TEST_ENR);
  }

  @Test
  void allowAllWhenNoRestrictionsConfigured() {
    final AddressAccessPolicy policy =
        createFactory(PeerPermissions.NOOP).createAddressAccessPolicy();

    assertThat(policy).isSameAs(AddressAccessPolicy.ALLOW_ALL);
  }

  @Test
  void allowNodeRecordWithinSubnet() {
    when(nodeRecordManager.getLocalNode()).thenReturn(Optional.of(localPeer));

    final PeerPermissions permissions = subnetPermissions("15.204.180.0/24");
    final AddressAccessPolicy policy = createFactory(permissions).createAddressAccessPolicy();

    // 15.204.180.57 is within 15.204.180.0/24
    assertThat(policy.allow(testNodeRecord)).isTrue();
  }

  @Test
  void rejectNodeRecordWhenPermissionsDeny() {
    when(nodeRecordManager.getLocalNode()).thenReturn(Optional.of(localPeer));

    final PeerPermissions rejectAll = rejectAllPermissions();
    final AddressAccessPolicy policy = createFactory(rejectAll).createAddressAccessPolicy();

    assertThat(policy.allow(testNodeRecord)).isFalse();
  }

  @Test
  void allowNodeRecordWhenPermissionsAllow() {
    when(nodeRecordManager.getLocalNode()).thenReturn(Optional.of(localPeer));

    final PeerPermissions allowAll = allowAllPermissions();
    final AddressAccessPolicy policy = createFactory(allowAll).createAddressAccessPolicy();

    assertThat(policy.allow(testNodeRecord)).isTrue();
  }

  @Test
  void rejectNodeRecordWhenLocalNodeNotYetInitialized() {
    when(nodeRecordManager.getLocalNode()).thenReturn(Optional.empty());

    final PeerPermissions rejectAll = rejectAllPermissions();
    final AddressAccessPolicy policy = createFactory(rejectAll).createAddressAccessPolicy();

    // Reject rather than bypass identity checks; the peer will be re-discovered
    assertThat(policy.allow(testNodeRecord)).isFalse();
  }

  @Test
  void rejectNodeRecordOutsideAllowedSubnet() {
    // 10.0.0.0/8 does not include 15.204.180.57
    final PeerPermissions permissions = subnetPermissions("10.0.0.0/8");
    final AddressAccessPolicy policy = createFactory(permissions).createAddressAccessPolicy();

    assertThat(policy.allow(testNodeRecord)).isFalse();
  }

  @Test
  void rejectNodeRecordWithNoAddressesAndMissingKey() {
    when(nodeRecordManager.getLocalNode()).thenReturn(Optional.of(localPeer));

    final PeerPermissions permissions = subnetPermissions("10.0.0.0/8");
    final AddressAccessPolicy policy = createFactory(permissions).createAddressAccessPolicy();

    // A mock NodeRecord with no addresses and no secp256k1 field — key extraction fails,
    // so the peer is rejected.
    final NodeRecord noAddressRecord = mock(NodeRecord.class);
    when(noAddressRecord.getUdpAddress()).thenReturn(Optional.empty());
    when(noAddressRecord.getUdp6Address()).thenReturn(Optional.empty());
    when(noAddressRecord.getTcpAddress()).thenReturn(Optional.empty());
    when(noAddressRecord.getTcp6Address()).thenReturn(Optional.empty());

    assertThat(policy.allow(noAddressRecord)).isFalse();
  }

  @Test
  void allowNoAddressRecordWhenNotOnDenylist() {
    when(nodeRecordManager.getLocalNode()).thenReturn(Optional.of(localPeer));

    final PeerPermissionsDenylist denylist = PeerPermissionsDenylist.create();
    final AddressAccessPolicy policy = createFactory(denylist).createAddressAccessPolicy();

    final Bytes compressedKey = (Bytes) testNodeRecord.get(EnrField.PKEY_SECP256K1);
    assertThat(policy.allow(noAddressRecordWithKeyFrom(compressedKey))).isTrue();
  }

  @Test
  void rejectNoAddressRecordWhenOnDenylist() {
    when(nodeRecordManager.getLocalNode()).thenReturn(Optional.of(localPeer));

    final Bytes compressedKey = (Bytes) testNodeRecord.get(EnrField.PKEY_SECP256K1);
    final PeerPermissionsDenylist denylist = PeerPermissionsDenylist.create();
    // The denylist stores the 64-byte uncompressed key; extract it the same way the allow path does
    denylist.add(EthereumNodeRecord.uncompressedPublicKey(testNodeRecord));
    final AddressAccessPolicy policy = createFactory(denylist).createAddressAccessPolicy();

    assertThat(policy.allow(noAddressRecordWithKeyFrom(compressedKey))).isFalse();
  }

  /**
   * Returns a mock NodeRecord that has no addresses but carries the given compressed secp256k1 key.
   */
  private static NodeRecord noAddressRecordWithKeyFrom(final Bytes compressedKey) {
    final NodeRecord record = mock(NodeRecord.class);
    when(record.getUdpAddress()).thenReturn(Optional.empty());
    when(record.getUdp6Address()).thenReturn(Optional.empty());
    when(record.getTcpAddress()).thenReturn(Optional.empty());
    when(record.getTcp6Address()).thenReturn(Optional.empty());
    when(record.get(EnrField.PKEY_SECP256K1)).thenReturn(compressedKey);
    return record;
  }

  @Test
  void allowInetSocketAddressInSubnet() {
    final PeerPermissions permissions = subnetPermissions("192.168.1.0/24");
    final AddressAccessPolicy policy = createFactory(permissions).createAddressAccessPolicy();

    assertThat(policy.allow(new InetSocketAddress("192.168.1.50", 30303))).isTrue();
    assertThat(policy.allow(new InetSocketAddress("10.0.0.1", 30303))).isFalse();
  }

  @Test
  void rejectNodeRecordBySubnetBeforeCheckingPeerPermissions() {
    // Subnet check should reject first — peer-level permissions must not be consulted.
    final PeerPermissions mockPermissions = mock(PeerPermissions.class);
    when(mockPermissions.isPermitted(any(InetSocketAddress.class))).thenReturn(false);

    final AddressAccessPolicy policy = createFactory(mockPermissions).createAddressAccessPolicy();

    // isPermitted(InetSocketAddress) returns false, so node record is rejected before
    // isPermitted(Peer, Peer, Action) is ever called
    assertThat(policy.allow(testNodeRecord)).isFalse();
    verify(mockPermissions, never()).isPermitted(any(Peer.class), any(), any());
  }

  @Test
  void rejectDualStackNodeRecordWhenIpv6OutsideSubnet() {
    // IPv4-only subnet: allows 15.204.180.0/24 but has no IPv6 allowance.
    // A dual-stack ENR with an IPv6 address should be rejected because the
    // IPv6 address is not in any allowed subnet.
    final PeerPermissions permissions = subnetPermissions("15.204.180.0/24");
    final AddressAccessPolicy policy = createFactory(permissions).createAddressAccessPolicy();

    final NodeRecord dualStackRecord = mock(NodeRecord.class);
    // IPv4 addresses within subnet
    lenient()
        .when(dualStackRecord.getUdpAddress())
        .thenReturn(Optional.of(new InetSocketAddress("15.204.180.57", 30303)));
    lenient()
        .when(dualStackRecord.getTcpAddress())
        .thenReturn(Optional.of(new InetSocketAddress("15.204.180.57", 30303)));
    // IPv6 addresses NOT in any allowed subnet
    lenient()
        .when(dualStackRecord.getUdp6Address())
        .thenReturn(Optional.of(new InetSocketAddress("fd00::1", 30303)));
    lenient()
        .when(dualStackRecord.getTcp6Address())
        .thenReturn(Optional.of(new InetSocketAddress("fd00::1", 30303)));

    assertThat(policy.allow(dualStackRecord)).isFalse();
  }

  @Test
  void allowDualStackNodeRecordChecksAllAddressFamilies() {
    // Verify that IP-level checks are invoked for both IPv4 and IPv6 addresses.
    // Use a mock PeerPermissions to isolate the IP-level check from the peer-level check,
    // since mocked NodeRecords cannot be converted to DiscoveryPeer.
    final PeerPermissions mockPermissions = mock(PeerPermissions.class);
    when(mockPermissions.isPermitted(any(InetSocketAddress.class))).thenReturn(true);

    final AddressAccessPolicy policy = createFactory(mockPermissions).createAddressAccessPolicy();

    final NodeRecord dualStackRecord = mock(NodeRecord.class);
    lenient()
        .when(dualStackRecord.getUdpAddress())
        .thenReturn(Optional.of(new InetSocketAddress("15.204.180.57", 30303)));
    lenient()
        .when(dualStackRecord.getTcpAddress())
        .thenReturn(Optional.of(new InetSocketAddress("15.204.180.57", 30303)));
    lenient()
        .when(dualStackRecord.getUdp6Address())
        .thenReturn(Optional.of(new InetSocketAddress("fd00::1", 30303)));
    lenient()
        .when(dualStackRecord.getTcp6Address())
        .thenReturn(Optional.of(new InetSocketAddress("fd00::1", 30303)));

    policy.allow(dualStackRecord);

    // Verify that isPermitted(InetSocketAddress) was called for both address families
    verify(mockPermissions, atLeastOnce())
        .isPermitted(new InetSocketAddress("15.204.180.57", 30303));
    verify(mockPermissions, atLeastOnce()).isPermitted(new InetSocketAddress("fd00::1", 30303));
  }

  @Test
  void allowNodeRecordWhenSubnetAndPermissionsBothPass() {
    when(nodeRecordManager.getLocalNode()).thenReturn(Optional.of(localPeer));

    // 15.204.0.0/16 includes 15.204.180.57
    final PeerPermissions permissions =
        PeerPermissions.combine(subnetPermissions("15.204.0.0/16"), allowAllPermissions());

    final AddressAccessPolicy policy = createFactory(permissions).createAddressAccessPolicy();

    assertThat(policy.allow(testNodeRecord)).isTrue();
  }

  private static IPAddress subnet(final String cidr) {
    return new IPAddressString(cidr).getAddress().toPrefixBlock();
  }

  private static PeerPermissions subnetPermissions(final String... cidrs) {
    final List<IPAddress> subnets =
        java.util.Arrays.stream(cidrs)
            .map(PeerDiscoveryAgentFactoryV5Test::subnet)
            .collect(java.util.stream.Collectors.toList());
    return new PeerPermissionSubnet(subnets);
  }

  private PeerDiscoveryAgentFactoryV5 createFactory(final PeerPermissions peerPermissions) {
    return new PeerDiscoveryAgentFactoryV5(
        config,
        nodeKey,
        peerPermissions,
        forkIdManager,
        new NoOpMetricsSystem(),
        nodeRecordManager);
  }

  private static PeerPermissions rejectAllPermissions() {
    return new PeerPermissions() {
      @Override
      public boolean isPermitted(final Peer localNode, final Peer remotePeer, final Action action) {
        return false;
      }
    };
  }

  private static PeerPermissions allowAllPermissions() {
    return new PeerPermissions() {
      @Override
      public boolean isPermitted(final Peer localNode, final Peer remotePeer, final Action action) {
        return true;
      }
    };
  }
}
