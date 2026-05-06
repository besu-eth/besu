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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.ethereum.chain.VariablesStorage;
import org.hyperledger.besu.ethereum.forkid.ForkId;
import org.hyperledger.besu.ethereum.forkid.ForkIdManager;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.nat.NatService;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.ethereum.beacon.discovery.schema.EnrField;
import org.ethereum.beacon.discovery.schema.NodeRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NodeRecordManagerTest {

  private NodeKey nodeKey;
  private StorageProvider storageProvider;
  private VariablesStorage variablesStorage;
  private VariablesStorage.Updater updater;
  private ForkIdManager forkIdManager;
  private NatService natService;
  private NodeRecordManager manager;

  @BeforeEach
  void setUp() {
    nodeKey = NodeKeyUtils.generate();
    storageProvider = mock(StorageProvider.class);
    variablesStorage = mock(VariablesStorage.class);
    updater = mock(VariablesStorage.Updater.class);
    forkIdManager = mock(ForkIdManager.class);
    natService = new NatService(Optional.empty());

    when(storageProvider.createVariablesStorage()).thenReturn(variablesStorage);
    when(variablesStorage.getLocalEnrSeqno()).thenReturn(Optional.empty());
    when(variablesStorage.updater()).thenReturn(updater);
    when(forkIdManager.getForkIdForChainHead()).thenReturn(new ForkId(Bytes.EMPTY, Bytes.EMPTY));

    manager = new NodeRecordManager(storageProvider, nodeKey, forkIdManager, natService);
  }

  @Test
  void ipv4OnlyEnrPopulatesIpv4FieldsOnly() {
    manager.initializeLocalNode(new HostEndpoint("127.0.0.1", 30303, 30303), Optional.empty());

    final NodeRecord record = getNodeRecord();

    // IPv4 fields must be present
    assertThat(record.get(EnrField.IP_V4)).isNotNull();
    assertThat(record.get(EnrField.UDP)).isEqualTo(30303);
    assertThat(record.get(EnrField.TCP)).isEqualTo(30303);

    // IPv6 fields must be absent
    assertThat(record.get(EnrField.IP_V6)).isNull();
    assertThat(record.get(EnrField.UDP_V6)).isNull();
    assertThat(record.get(EnrField.TCP_V6)).isNull();
  }

  @Test
  void ipv6OnlyEnrPopulatesIpv6FieldsOnly() {
    manager.initializeLocalNode(new HostEndpoint("::1", 30303, 30303), Optional.empty());

    final NodeRecord record = getNodeRecord();

    // IPv6 fields must be present (primary address is IPv6)
    assertThat(record.get(EnrField.IP_V6)).isNotNull();
    assertThat(record.get(EnrField.UDP_V6)).isEqualTo(30303);
    assertThat(record.get(EnrField.TCP_V6)).isEqualTo(30303);

    // IPv4 fields must be absent
    assertThat(record.get(EnrField.IP_V4)).isNull();
    assertThat(record.get(EnrField.UDP)).isNull();
    assertThat(record.get(EnrField.TCP)).isNull();
  }

  @Test
  void dualStackEnrPopulatesBothIpv4AndIpv6Fields() {
    manager.initializeLocalNode(
        new HostEndpoint("127.0.0.1", 30303, 30303),
        Optional.of(new HostEndpoint("::1", 30404, 30404)));

    final NodeRecord record = getNodeRecord();

    // IPv4 fields
    assertThat(record.get(EnrField.IP_V4)).isNotNull();
    assertThat(record.get(EnrField.UDP)).isEqualTo(30303);
    assertThat(record.get(EnrField.TCP)).isEqualTo(30303);

    // IPv6 fields
    assertThat(record.get(EnrField.IP_V6)).isNotNull();
    assertThat(record.get(EnrField.UDP_V6)).isEqualTo(30404);
    assertThat(record.get(EnrField.TCP_V6)).isEqualTo(30404);
  }

  @Test
  void dualStackEnrPopulatesCorrectIpAddressBytes() {
    manager.initializeLocalNode(
        new HostEndpoint("192.168.1.1", 30303, 30303),
        Optional.of(new HostEndpoint("fe80::1", 30404, 30404)));

    final NodeRecord record = getNodeRecord();

    final Bytes ipv4 = (Bytes) record.get(EnrField.IP_V4);
    assertThat(ipv4).isEqualTo(Bytes.of(192, 168, 1, 1));
    assertThat(ipv4.size()).isEqualTo(4);

    final Bytes ipv6 = (Bytes) record.get(EnrField.IP_V6);
    assertThat(ipv6.size()).isEqualTo(16);
  }

  @Test
  void ipv6OnlyEnrPopulatesCorrectIpv6AddressBytes() {
    manager.initializeLocalNode(new HostEndpoint("::1", 30303, 30303), Optional.empty());

    final NodeRecord record = getNodeRecord();

    final Bytes ipv6 = (Bytes) record.get(EnrField.IP_V6);
    assertThat(ipv6.size()).isEqualTo(16);
    // ::1 is 15 zero bytes followed by 0x01
    assertThat(ipv6.get(15)).isEqualTo((byte) 1);
  }

  @Test
  void unchangedFieldsReuseExistingEnr() {
    manager.initializeLocalNode(new HostEndpoint("127.0.0.1", 30303, 30303), Optional.empty());

    final NodeRecord first = getNodeRecord();

    // Call updateNodeRecord again with the same config
    manager.updateNodeRecord();

    final NodeRecord second = getNodeRecord();

    // Sequence number should not change since fields are identical
    assertThat(second.getSeq()).isEqualTo(first.getSeq());
  }

  @Test
  void dualStackDifferentPorts() {
    manager.initializeLocalNode(
        new HostEndpoint("10.0.0.1", 30303, 30304),
        Optional.of(new HostEndpoint("2001:db8::1", 30405, 30406)));

    final NodeRecord record = getNodeRecord();

    assertThat(record.get(EnrField.UDP)).isEqualTo(30303);
    assertThat(record.get(EnrField.TCP)).isEqualTo(30304);
    assertThat(record.get(EnrField.UDP_V6)).isEqualTo(30405);
    assertThat(record.get(EnrField.TCP_V6)).isEqualTo(30406);
  }

  // ── onDiscoveryPortResolved ──────────────────────────────────────────────────

  @Test
  void onDiscoveryPortResolved_ipv4Primary_ephemeralPort_updatesUdpField() {
    manager.initializeLocalNode(new HostEndpoint("127.0.0.1", 0, 30303), Optional.empty());

    manager.onDiscoveryPortResolved(Optional.of(54321), Optional.empty());

    final NodeRecord record = getNodeRecord();
    assertThat(record.get(EnrField.UDP)).isEqualTo(54321);
    assertThat(record.get(EnrField.TCP)).isEqualTo(30303);
    // No IPv6 fields for single-stack IPv4
    assertThat(record.get(EnrField.IP_V6)).isNull();
    assertThat(record.get(EnrField.UDP_V6)).isNull();
  }

  @Test
  void onDiscoveryPortResolved_ipv6Primary_ephemeralPort_updatesUdp6Field() {
    // Single-stack IPv6: primary is IPv6, no dual-stack secondary.
    // The resolved port arrives in resolvedUdp6Port; NodeRecordManager must route it to
    // primaryEndpoint (not to ipv6Endpoint, which is absent in single-stack mode).
    manager.initializeLocalNode(new HostEndpoint("::1", 0, 30303), Optional.empty());

    manager.onDiscoveryPortResolved(Optional.empty(), Optional.of(54321));

    final NodeRecord record = getNodeRecord();
    assertThat(record.get(EnrField.UDP_V6)).isEqualTo(54321);
    assertThat(record.get(EnrField.TCP_V6)).isEqualTo(30303);
    // No IPv4 fields for single-stack IPv6
    assertThat(record.get(EnrField.IP_V4)).isNull();
    assertThat(record.get(EnrField.UDP)).isNull();
  }

  @Test
  void onDiscoveryPortResolved_dualStack_deferEnrWriteUntilBothPortsResolved() {
    // Dual-stack, both ports ephemeral. The two UDP servers bind concurrently and each fires
    // one callback. The ENR must not be written after only one port resolves; it must wait
    // until both are non-zero so the seq counter increments exactly once.
    manager.initializeLocalNode(
        new HostEndpoint("127.0.0.1", 0, 30303), Optional.of(new HostEndpoint("::1", 0, 30404)));

    // After init: one ENR write (with udp=0, udp6=0)
    verify(updater, times(1)).commit();

    // IPv4 resolves first — IPv6 still pending → no new write
    manager.onDiscoveryPortResolved(Optional.of(12345), Optional.empty());
    verify(updater, times(1)).commit();

    // IPv6 resolves — both endpoints now non-zero → ENR written with correct ports
    manager.onDiscoveryPortResolved(Optional.empty(), Optional.of(56789));
    verify(updater, times(2)).commit();

    final NodeRecord record = getNodeRecord();
    assertThat(record.get(EnrField.UDP)).isEqualTo(12345);
    assertThat(record.get(EnrField.TCP)).isEqualTo(30303);
    assertThat(record.get(EnrField.UDP_V6)).isEqualTo(56789);
    assertThat(record.get(EnrField.TCP_V6)).isEqualTo(30404);
  }

  @Test
  void onDiscoveryPortResolved_dualStack_ipv4Fixed_ipv6Ephemeral_updatesUdp6Field() {
    // Dual-stack where IPv4 is a fixed port and IPv6 is ephemeral.
    // The IPv4 primary is already non-zero at startup; the ENR is written once at init
    // (with udp6=0) and then again when the IPv6 port resolves.
    manager.initializeLocalNode(
        new HostEndpoint("127.0.0.1", 30303, 30303),
        Optional.of(new HostEndpoint("::1", 0, 30404)));

    verify(updater, times(1)).commit();

    // IPv6 resolves — primary already non-zero → write immediately
    manager.onDiscoveryPortResolved(Optional.empty(), Optional.of(56789));
    verify(updater, times(2)).commit();

    final NodeRecord record = getNodeRecord();
    assertThat(record.get(EnrField.UDP)).isEqualTo(30303);
    assertThat(record.get(EnrField.UDP_V6)).isEqualTo(56789);
    assertThat(record.get(EnrField.TCP_V6)).isEqualTo(30404);
  }

  @Test
  void onDiscoveryPortResolved_duplicateCallback_doesNotOverwritePort() {
    // Once a port has been resolved, a second callback with a different port
    // must not re-apply it — the guard (discoveryPort == 0) prevents double-application.
    manager.initializeLocalNode(new HostEndpoint("127.0.0.1", 0, 30303), Optional.empty());

    manager.onDiscoveryPortResolved(Optional.of(54321), Optional.empty());

    // Second callback with a different port — must not overwrite the first value
    manager.onDiscoveryPortResolved(Optional.of(65432), Optional.empty());

    final NodeRecord record = getNodeRecord();
    assertThat(record.get(EnrField.UDP)).isEqualTo(54321);
  }

  @Test
  void onDiscoveryPortResolved_dualStack_ipv6ResolvesFirst_noOrderingDependency() {
    // Same as the IPv4-first dual-stack test, but IPv6 resolves before IPv4.
    // Verifies there is no ordering dependency between the two callbacks.
    manager.initializeLocalNode(
        new HostEndpoint("127.0.0.1", 0, 30303), Optional.of(new HostEndpoint("::1", 0, 30404)));

    verify(updater, times(1)).commit();

    // IPv6 resolves first — IPv4 still pending → no new write
    manager.onDiscoveryPortResolved(Optional.empty(), Optional.of(56789));
    verify(updater, times(1)).commit();

    // IPv4 resolves — both endpoints now non-zero → ENR written
    manager.onDiscoveryPortResolved(Optional.of(12345), Optional.empty());
    verify(updater, times(2)).commit();

    final NodeRecord record = getNodeRecord();
    assertThat(record.get(EnrField.UDP)).isEqualTo(12345);
    assertThat(record.get(EnrField.TCP)).isEqualTo(30303);
    assertThat(record.get(EnrField.UDP_V6)).isEqualTo(56789);
    assertThat(record.get(EnrField.TCP_V6)).isEqualTo(30404);
  }

  @Test
  public void testUpdateAdvertisedIpV6Host() {
    manager.initializeLocalNode(
        new HostEndpoint("127.0.0.1", 12345, 30303),
        Optional.of(new HostEndpoint("::1", 6789, 30404)));
    manager.updateAdvertisedIpv6Host("::2");

    NodeRecord record = getNodeRecord();

    final Bytes ipv6 = (Bytes) record.get(EnrField.IP_V6);
    assertThat(ipv6.size()).isEqualTo(16);
    // ::2 is 15 zero bytes followed by 0x10
    assertThat(ipv6.get(15)).isEqualTo((byte) 2);
    assertThat(record.get(EnrField.UDP_V6)).isEqualTo(6789);
    assertThat(record.get(EnrField.TCP_V6)).isEqualTo(30404);
  }

  private NodeRecord getNodeRecord() {
    return manager
        .getLocalNode()
        .flatMap(DiscoveryPeer::getNodeRecord)
        .orElseThrow(() -> new IllegalStateException("Node record not initialized"));
  }
}
