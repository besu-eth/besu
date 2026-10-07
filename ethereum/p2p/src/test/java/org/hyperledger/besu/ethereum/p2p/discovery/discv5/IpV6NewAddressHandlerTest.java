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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.p2p.discovery.NodeRecordManager;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Optional;

import org.ethereum.beacon.discovery.schema.NodeRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IpV6NewAddressHandlerTest {
  @Mock private NodeRecordManager nodeRecordManager;
  @Mock private NodeRecord oldRecord;
  @Mock private NodeRecord newRecord;

  private static final int PEER_OBSERVED_UDP = 30303;
  // 2001:db8::/32 is RFC 3849 reserved-for-documentation; we use ::1 inside it as a stand-in for
  // any 2000::/3 global unicast address. The validator does not special-case 2001:db8::/32, so
  // tests reach the same code path real global unicast traffic would.
  //
  // InetAddress.getHostAddress() returns the JDK's canonical expanded form (e.g.
  // 2001:db8:0:0:0:0:0:1), not the RFC-5952 compressed form. The handler passes that string to
  // NodeRecordManager, so the Mockito stubs must use the same form. Computing it at runtime keeps
  // this robust across JDKs.
  private static final String GLOBAL_UNICAST_HOST_EXPANDED;
  private static final String ULA_HOST_EXPANDED;

  static {
    try {
      GLOBAL_UNICAST_HOST_EXPANDED = Inet6Address.getByName("2001:db8::1").getHostAddress();
      ULA_HOST_EXPANDED = Inet6Address.getByName("fd00:dead:beef::20").getHostAddress();
    } catch (final UnknownHostException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  @Test
  void ignoresIpv4PeerReports() throws UnknownHostException {
    final IpV6NewAddressHandler handler = new IpV6NewAddressHandler(nodeRecordManager);

    final Optional<NodeRecord> result =
        handler.newAddress(oldRecord, socketIpv4("203.0.113.5", PEER_OBSERVED_UDP));

    assertThat(result).isEmpty();
    verifyNoInteractions(nodeRecordManager);
  }

  @Test
  void shortCircuitsWhenPrimaryEndpointIsIpv6() throws UnknownHostException {
    when(nodeRecordManager.isPrimaryEndpointIpv6()).thenReturn(true);
    final IpV6NewAddressHandler handler = new IpV6NewAddressHandler(nodeRecordManager);

    final Optional<NodeRecord> result =
        handler.newAddress(oldRecord, socketIpv6("2001:db8::1", PEER_OBSERVED_UDP));

    assertThat(result).isEmpty();
    verify(nodeRecordManager, never())
        .applyAutoDiscoveredIpv6Host(eq(GLOBAL_UNICAST_HOST_EXPANDED), eq(PEER_OBSERVED_UDP));
  }

  @Test
  void rejectsLinkLocal() throws UnknownHostException {
    final IpV6NewAddressHandler handler = new IpV6NewAddressHandler(nodeRecordManager);

    final Optional<NodeRecord> result =
        handler.newAddress(oldRecord, socketIpv6("fe80::1", PEER_OBSERVED_UDP));

    assertThat(result).isEmpty();
    verify(nodeRecordManager, never()).applyAutoDiscoveredIpv6Host(any(), anyInt());
  }

  @Test
  void rejectsLoopback() throws UnknownHostException {
    final IpV6NewAddressHandler handler = new IpV6NewAddressHandler(nodeRecordManager);

    final Optional<NodeRecord> result =
        handler.newAddress(oldRecord, socketIpv6("::1", PEER_OBSERVED_UDP));

    assertThat(result).isEmpty();
    verify(nodeRecordManager, never()).applyAutoDiscoveredIpv6Host(any(), anyInt());
  }

  @Test
  void rejectsUnspecified() throws UnknownHostException {
    final IpV6NewAddressHandler handler = new IpV6NewAddressHandler(nodeRecordManager);

    final Optional<NodeRecord> result =
        handler.newAddress(oldRecord, socketIpv6("::", PEER_OBSERVED_UDP));

    assertThat(result).isEmpty();
    verify(nodeRecordManager, never()).applyAutoDiscoveredIpv6Host(any(), anyInt());
  }

  @Test
  void rejectsMulticast() throws UnknownHostException {
    final IpV6NewAddressHandler handler = new IpV6NewAddressHandler(nodeRecordManager);

    final Optional<NodeRecord> result =
        handler.newAddress(oldRecord, socketIpv6("ff02::1", PEER_OBSERVED_UDP));

    assertThat(result).isEmpty();
    verify(nodeRecordManager, never()).applyAutoDiscoveredIpv6Host(any(), anyInt());
  }

  @Test
  void rejectsIpv4MappedIpv6() throws UnknownHostException {
    // Java's InetAddress.getByName("::ffff:a.b.c.d") returns an Inet4Address, so we construct
    // the Inet6Address directly from raw bytes to simulate the case where the discovery library
    // or a malformed peer report supplies a real Inet6Address whose payload happens to fall in
    // the ::ffff:0:0/96 range. The validator must reject these — advertising them as `ip6`
    // would broadcast an IPv4 address under the wrong family.
    final IpV6NewAddressHandler handler = new IpV6NewAddressHandler(nodeRecordManager);

    final Inet6Address mapped = ipv4MappedIpv6((byte) 192, (byte) 168, (byte) 1, (byte) 1);

    final Optional<NodeRecord> result =
        handler.newAddress(oldRecord, new InetSocketAddress(mapped, PEER_OBSERVED_UDP));

    assertThat(result).isEmpty();
    verify(nodeRecordManager, never()).applyAutoDiscoveredIpv6Host(any(), anyInt());
  }

  @Test
  void acceptsGlobalUnicast() throws UnknownHostException {
    final IpV6NewAddressHandler handler = new IpV6NewAddressHandler(nodeRecordManager);
    when(nodeRecordManager.applyAutoDiscoveredIpv6Host(
            GLOBAL_UNICAST_HOST_EXPANDED, PEER_OBSERVED_UDP))
        .thenReturn(Optional.of(newRecord));

    final Optional<NodeRecord> result =
        handler.newAddress(oldRecord, socketIpv6("2001:db8::1", PEER_OBSERVED_UDP));

    assertThat(result).contains(newRecord);
    verify(nodeRecordManager, times(1))
        .applyAutoDiscoveredIpv6Host(GLOBAL_UNICAST_HOST_EXPANDED, PEER_OBSERVED_UDP);
  }

  @Test
  void acceptsUlaSinceJdkHasNoIpv6UlaPredicate() throws UnknownHostException {
    // ULAs (fc00::/7) are used in private deployments — including our Docker discv5 test harness
    // (fd00:dead:beef::/64). We deliberately accept them since the JDK does not expose an IPv6
    // ULA predicate and excluding them would break private/lab deployments.
    final IpV6NewAddressHandler handler = new IpV6NewAddressHandler(nodeRecordManager);
    when(nodeRecordManager.applyAutoDiscoveredIpv6Host(ULA_HOST_EXPANDED, PEER_OBSERVED_UDP))
        .thenReturn(Optional.of(newRecord));

    final Optional<NodeRecord> result =
        handler.newAddress(oldRecord, socketIpv6("fd00:dead:beef::20", PEER_OBSERVED_UDP));

    assertThat(result).contains(newRecord);
    verify(nodeRecordManager, times(1))
        .applyAutoDiscoveredIpv6Host(ULA_HOST_EXPANDED, PEER_OBSERVED_UDP);
  }

  @Test
  void propagatesEmptyWhenManagerRefuses() throws UnknownHostException {
    // E.g. when ipv6Endpoint already set (fire-once semantics) or no TCP port hint registered.
    final IpV6NewAddressHandler handler = new IpV6NewAddressHandler(nodeRecordManager);
    when(nodeRecordManager.applyAutoDiscoveredIpv6Host(
            GLOBAL_UNICAST_HOST_EXPANDED, PEER_OBSERVED_UDP))
        .thenReturn(Optional.empty());

    final Optional<NodeRecord> result =
        handler.newAddress(oldRecord, socketIpv6("2001:db8::1", PEER_OBSERVED_UDP));

    assertThat(result).isEmpty();
    verify(nodeRecordManager, times(1))
        .applyAutoDiscoveredIpv6Host(GLOBAL_UNICAST_HOST_EXPANDED, PEER_OBSERVED_UDP);
  }

  @Test
  void isAdvertisableIpv6Unicast_classifiesAddressesCorrectly() throws UnknownHostException {
    assertThat(
            IpV6NewAddressHandler.isAdvertisableIpv6Unicast(Inet6Address.getByName("2001:db8::1")))
        .isTrue();
    assertThat(IpV6NewAddressHandler.isAdvertisableIpv6Unicast(Inet6Address.getByName("fd00::1")))
        .isTrue();
    assertThat(IpV6NewAddressHandler.isAdvertisableIpv6Unicast(Inet6Address.getByName("fe80::1")))
        .isFalse();
    assertThat(IpV6NewAddressHandler.isAdvertisableIpv6Unicast(Inet6Address.getByName("::1")))
        .isFalse();
    assertThat(IpV6NewAddressHandler.isAdvertisableIpv6Unicast(Inet6Address.getByName("::")))
        .isFalse();
    assertThat(IpV6NewAddressHandler.isAdvertisableIpv6Unicast(Inet6Address.getByName("ff02::1")))
        .isFalse();
    assertThat(
            IpV6NewAddressHandler.isAdvertisableIpv6Unicast(Inet4Address.getByName("203.0.113.5")))
        .isFalse();
    // IPv4-mapped IPv6 (::ffff:0:0/96) — constructed from raw bytes to bypass JDK auto-unwrap.
    assertThat(
            IpV6NewAddressHandler.isAdvertisableIpv6Unicast(
                ipv4MappedIpv6((byte) 1, (byte) 2, (byte) 3, (byte) 4)))
        .isFalse();
    assertThat(
            IpV6NewAddressHandler.isAdvertisableIpv6Unicast(
                ipv4MappedIpv6((byte) 0, (byte) 0, (byte) 0, (byte) 0)))
        .isFalse();
  }

  private static InetSocketAddress socketIpv4(final String host, final int port)
      throws UnknownHostException {
    return new InetSocketAddress(Inet4Address.getByName(host), port);
  }

  private static InetSocketAddress socketIpv6(final String host, final int port)
      throws UnknownHostException {
    return new InetSocketAddress(Inet6Address.getByName(host), port);
  }

  private static Inet6Address ipv4MappedIpv6(final byte a, final byte b, final byte c, final byte d)
      throws UnknownHostException {
    final byte[] raw = new byte[16];
    raw[10] = (byte) 0xff;
    raw[11] = (byte) 0xff;
    raw[12] = a;
    raw[13] = b;
    raw[14] = c;
    raw[15] = d;
    return Inet6Address.getByAddress(null, raw, 0);
  }
}
