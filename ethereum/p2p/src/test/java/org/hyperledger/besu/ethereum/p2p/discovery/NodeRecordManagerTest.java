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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
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
import java.util.concurrent.atomic.AtomicReference;

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

    // Backed by a real reference so getLocalEnrSeqno() reflects the most recent
    // setLocalEnrSeqno() write, the same way real disk-backed storage would across a restart.
    final AtomicReference<Bytes> persistedEnr = new AtomicReference<>();
    when(storageProvider.createVariablesStorage()).thenReturn(variablesStorage);
    when(variablesStorage.getLocalEnrSeqno())
        .thenAnswer(invocation -> Optional.ofNullable(persistedEnr.get()));
    when(variablesStorage.updater()).thenReturn(updater);
    doAnswer(invocation -> persistedEnr.getAndSet(invocation.getArgument(0)))
        .when(updater)
        .setLocalEnrSeqno(any());
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
  void restartWithUnchangedFields_doesNotIncrementSeq() {
    // First "process": writes the initial ENR to the (shared, stateful) storage.
    manager.initializeLocalNode(new HostEndpoint("127.0.0.1", 30303, 30303), Optional.empty());
    final NodeRecord beforeRestart = getNodeRecord();

    // Second "process": a fresh NodeRecordManager instance reading the same persisted storage,
    // simulating a restart with identical configuration. This must reuse the persisted ENR
    // rather than incrementing seq, which is the exact regression this PR fixes (compressed
    // pubkey comparison and fork-id wrapper-list shape previously never matched, so seq
    // incremented on every restart even though nothing changed).
    manager = new NodeRecordManager(storageProvider, nodeKey, forkIdManager, natService);
    manager.initializeLocalNode(new HostEndpoint("127.0.0.1", 30303, 30303), Optional.empty());
    final NodeRecord afterRestart = getNodeRecord();

    assertThat(afterRestart.getSeq()).isEqualTo(beforeRestart.getSeq());
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
  void applyAutoDiscoveredIpv6Host_createsEndpointFromHintWhenNoneExists() {
    manager.initializeLocalNode(
        new HostEndpoint("127.0.0.1", 30303, 30303), Optional.empty(), Optional.of(30404));
    verify(updater, times(1)).commit();

    final NodeRecord before = getNodeRecord();
    assertThat(before.get(EnrField.IP_V6)).isNull();

    final Optional<NodeRecord> result = manager.applyAutoDiscoveredIpv6Host("2001:db8::1", 30303);

    assertThat(result).isPresent();
    // A second commit confirms the ENR was rewritten (with bumped seq) after auto-discovery.
    verify(updater, times(2)).commit();
    final NodeRecord record = getNodeRecord();
    final Bytes ipv6 = (Bytes) record.get(EnrField.IP_V6);
    assertThat(ipv6).isNotNull();
    assertThat(ipv6.size()).isEqualTo(16);
    assertThat(record.get(EnrField.UDP_V6)).isEqualTo(30303);
    assertThat(record.get(EnrField.TCP_V6)).isEqualTo(30404);
  }

  @Test
  void applyAutoDiscoveredIpv6Host_noOpWhenIpv6EndpointAlreadySet() {
    // Operator pinned --p2p-host-ipv6 — ipv6Endpoint present at init
    manager.initializeLocalNode(
        new HostEndpoint("127.0.0.1", 30303, 30303),
        Optional.of(new HostEndpoint("2001:db8::a", 30303, 30404)));
    verify(updater, times(1)).commit();
    final NodeRecord before = getNodeRecord();
    final Bytes ipv6Before = (Bytes) before.get(EnrField.IP_V6);

    final Optional<NodeRecord> result = manager.applyAutoDiscoveredIpv6Host("2001:db8::b", 30303);

    assertThat(result).isEmpty();
    // No additional commit: the no-op path must not write the ENR.
    verify(updater, times(1)).commit();
    final NodeRecord after = getNodeRecord();
    assertThat(after.get(EnrField.IP_V6)).isEqualTo(ipv6Before);
  }

  @Test
  void applyAutoDiscoveredIpv6Host_noOpWhenNoHint() {
    // Dual-stack bind absent — no IPv6 endpoint, no TCP port hint
    manager.initializeLocalNode(
        new HostEndpoint("127.0.0.1", 30303, 30303), Optional.empty(), Optional.empty());
    verify(updater, times(1)).commit();

    final Optional<NodeRecord> result = manager.applyAutoDiscoveredIpv6Host("2001:db8::1", 30303);

    assertThat(result).isEmpty();
    verify(updater, times(1)).commit();
    assertThat(getNodeRecord().get(EnrField.IP_V6)).isNull();
  }

  @Test
  void applyAutoDiscoveredIpv6Host_fireOnceThenLocks() {
    manager.initializeLocalNode(
        new HostEndpoint("127.0.0.1", 30303, 30303), Optional.empty(), Optional.of(30404));
    verify(updater, times(1)).commit();

    final Optional<NodeRecord> first = manager.applyAutoDiscoveredIpv6Host("2001:db8::1", 30303);
    assertThat(first).isPresent();
    verify(updater, times(2)).commit();
    final Bytes firstIpv6 = (Bytes) getNodeRecord().get(EnrField.IP_V6);

    // Subsequent attempt with a different host must be ignored (addresses don't change mid-flight).
    final Optional<NodeRecord> second = manager.applyAutoDiscoveredIpv6Host("2001:db8::2", 30305);

    assertThat(second).isEmpty();
    // Commit count must remain at 2 — no further write.
    verify(updater, times(2)).commit();
    final NodeRecord after = getNodeRecord();
    assertThat(after.get(EnrField.IP_V6)).isEqualTo(firstIpv6);
    assertThat(after.get(EnrField.UDP_V6)).isEqualTo(30303);
  }

  @Test
  void applyAutoDiscoveredIpv6Host_noOpWhenPeerObservedUdpPortIsZero() {
    // A peer report with udpPort=0 is invalid for advertisement; writing udp6=0 into the ENR
    // would produce an unusable record. The guard rejects the call before fire-once consumes it,
    // so a subsequent valid call can still succeed.
    manager.initializeLocalNode(
        new HostEndpoint("127.0.0.1", 30303, 30303), Optional.empty(), Optional.of(30404));
    verify(updater, times(1)).commit();

    final Optional<NodeRecord> rejected = manager.applyAutoDiscoveredIpv6Host("2001:db8::1", 0);

    assertThat(rejected).isEmpty();
    verify(updater, times(1)).commit();
    assertThat(getNodeRecord().get(EnrField.IP_V6)).isNull();

    // A follow-up call with a valid port should still succeed — the zero-port guard must not
    // consume the fire-once slot.
    final Optional<NodeRecord> accepted = manager.applyAutoDiscoveredIpv6Host("2001:db8::1", 30303);
    assertThat(accepted).isPresent();
    verify(updater, times(2)).commit();
    assertThat(getNodeRecord().get(EnrField.UDP_V6)).isEqualTo(30303);
  }

  @Test
  void applyAutoDiscoveredIpv6Host_orthogonalToNatServiceIpv4Resolution() {
    // NatService rewrites the primary IPv4 host at init; IPv6 auto-discovery must still write
    // ip6/udp6/tcp6 independently, and must not disturb the NAT-resolved IPv4 fields.
    final NatService natWithExternalIpv4 = mock(NatService.class);
    when(natWithExternalIpv4.queryExternalIPAddress("10.0.0.1")).thenReturn("198.51.100.10");
    final NodeRecordManager natManager =
        new NodeRecordManager(storageProvider, nodeKey, forkIdManager, natWithExternalIpv4);

    natManager.initializeLocalNode(
        new HostEndpoint("10.0.0.1", 30303, 30303), Optional.empty(), Optional.of(30404));

    final NodeRecord beforeAuto =
        natManager
            .getLocalNode()
            .flatMap(DiscoveryPeer::getNodeRecord)
            .orElseThrow(() -> new IllegalStateException("Node record not initialized"));
    final Bytes natResolvedIpv4 = (Bytes) beforeAuto.get(EnrField.IP_V4);
    assertThat(natResolvedIpv4).isEqualTo(Bytes.of(198, 51, 100, 10));

    final Optional<NodeRecord> result =
        natManager.applyAutoDiscoveredIpv6Host("2001:db8::1", 30303);

    assertThat(result).isPresent();
    final NodeRecord after =
        natManager
            .getLocalNode()
            .flatMap(DiscoveryPeer::getNodeRecord)
            .orElseThrow(() -> new IllegalStateException("Node record not initialized"));
    // NAT-resolved IPv4 fields untouched
    assertThat(after.get(EnrField.IP_V4)).isEqualTo(natResolvedIpv4);
    assertThat(after.get(EnrField.UDP)).isEqualTo(30303);
    assertThat(after.get(EnrField.TCP)).isEqualTo(30303);
    // IPv6 fields populated by auto-discovery
    assertThat(after.get(EnrField.IP_V6)).isNotNull();
    assertThat(after.get(EnrField.UDP_V6)).isEqualTo(30303);
    assertThat(after.get(EnrField.TCP_V6)).isEqualTo(30404);
  }

  @Test
  void applyAutoDiscoveredIpv6Host_usesPeerObservedUdpPortNotLocalUdpPortOfZero() {
    // Simulates --p2p-port-ipv6=0: the locally-bound UDP port is unknown at init time, and the
    // peer-observed port is what should land in udp6.
    manager.initializeLocalNode(
        new HostEndpoint("127.0.0.1", 30303, 30303), Optional.empty(), Optional.of(30404));

    manager.applyAutoDiscoveredIpv6Host("2001:db8::1", 54321);

    final NodeRecord record = getNodeRecord();
    assertThat(record.get(EnrField.UDP_V6)).isEqualTo(54321);
    assertThat(record.get(EnrField.TCP_V6)).isEqualTo(30404);
  }

  @Test
  void isPrimaryEndpointIpv6_reflectsPrimaryFamily() {
    assertThat(manager.isPrimaryEndpointIpv6()).isFalse();

    manager.initializeLocalNode(new HostEndpoint("127.0.0.1", 30303, 30303), Optional.empty());
    assertThat(manager.isPrimaryEndpointIpv6()).isFalse();

    final NodeRecordManager ipv6PrimaryManager =
        new NodeRecordManager(storageProvider, nodeKey, forkIdManager, natService);
    ipv6PrimaryManager.initializeLocalNode(
        new HostEndpoint("2001:db8::1", 30303, 30303), Optional.empty());
    assertThat(ipv6PrimaryManager.isPrimaryEndpointIpv6()).isTrue();
  }

  private NodeRecord getNodeRecord() {
    return manager
        .getLocalNode()
        .flatMap(DiscoveryPeer::getNodeRecord)
        .orElseThrow(() -> new IllegalStateException("Node record not initialized"));
  }
}
