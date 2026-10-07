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

import org.hyperledger.besu.ethereum.p2p.discovery.NodeRecordManager;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Optional;

import inet.ipaddr.ipv6.IPv6Address;
import org.ethereum.beacon.discovery.schema.NodeRecord;
import org.ethereum.beacon.discovery.storage.NewAddressHandler;

/**
 * DiscV5 {@code newAddressHandler} that auto-discovers the local IPv6 advertised address from peer
 * consensus.
 *
 * <p>Invoked by the discovery library when {@code DefaultExternalAddressSelector} has accumulated
 * {@code MIN_CONFIRMATIONS} (&gt;=2) peer reports agreeing on an external socket address. IPv4
 * reports are ignored here because the existing {@code NatService} (UPnP, NAT-PMP, MANUAL, etc.)
 * covers IPv4 external address discovery via a one-shot resolve at startup.
 *
 * <p>Wired only when {@code --p2p-host-ipv6} is unpinned (see {@link
 * org.hyperledger.besu.ethereum.p2p.discovery.discv5.PeerDiscoveryAgentFactoryV5}); when pinned,
 * the discovery library is given a no-op handler instead. The handler additionally short-circuits
 * returning {@link Optional#empty()} when any of the following hold:
 *
 * <ul>
 *   <li>the peer-reported address is IPv4
 *   <li>the primary endpoint is already IPv6 (single-stack IPv6 deployment) — auto-discovery
 *       applies only to dual-stack with an IPv4 primary
 *   <li>the peer-reported address is not an advertisable IPv6 unicast: link-local ({@code
 *       fe80::/10}), loopback ({@code ::1}), unspecified ({@code ::}), multicast ({@code
 *       ff00::/8}), or IPv4-mapped ({@code ::ffff:0:0/96}) are rejected. Global unicast and ULAs
 *       ({@code fc00::/7}) are accepted.
 * </ul>
 */
public class IpV6NewAddressHandler implements NewAddressHandler {
  private final NodeRecordManager nodeRecordManager;

  /**
   * Constructs the DiscV5 IPv6 new-address handler.
   *
   * <p>The "primary endpoint is IPv6" gate is evaluated live via {@link
   * NodeRecordManager#isPrimaryEndpointIpv6()} on each invocation, so a later refactor that
   * permitted runtime endpoint changes would remain correct.
   *
   * @param nodeRecordManager the manager that owns the local ENR
   */
  public IpV6NewAddressHandler(final NodeRecordManager nodeRecordManager) {
    this.nodeRecordManager = nodeRecordManager;
  }

  @Override
  public Optional<NodeRecord> newAddress(
      final NodeRecord oldRecord, final InetSocketAddress newAddress) {
    final InetAddress addr = newAddress.getAddress();
    if (!(addr instanceof Inet6Address)) {
      return Optional.empty();
    }
    if (nodeRecordManager.isPrimaryEndpointIpv6()) {
      return Optional.empty();
    }
    if (!isAdvertisableIpv6Unicast(addr)) {
      return Optional.empty();
    }
    return nodeRecordManager.applyAutoDiscoveredIpv6Host(
        addr.getHostAddress(), newAddress.getPort());
  }

  /**
   * Returns whether the address is an IPv6 unicast address advertisable in an ENR (global unicast
   * or ULA).
   *
   * <p>Rejects link-local ({@code fe80::/10}), loopback ({@code ::1}), unspecified ({@code ::}),
   * multicast ({@code ff00::/8}), and IPv4-mapped ({@code ::ffff:0:0/96}) — the latter is an IPv4
   * address in IPv6 clothing and must not be advertised as a real IPv6 endpoint. Accepts global
   * unicast and ULAs ({@code fc00::/7}); the JDK does not provide an IPv6 ULA predicate, and we
   * deliberately do not exclude them despite their non-globally-routable status — private
   * deployments rely on ULAs.
   */
  static boolean isAdvertisableIpv6Unicast(final InetAddress addr) {
    // IPv4-mapped IPv6 (::ffff:0:0/96) is an IPv4 address in IPv6 clothing — JDK's
    // InetAddress.getByName() on the textual form auto-unwraps to Inet4Address, but raw
    // bytes from the discovery library or a malformed peer report can still arrive as
    // Inet6Address; we must not advertise them as a real IPv6 endpoint.
    return addr instanceof Inet6Address ipv6
        && !new IPv6Address(ipv6).isIPv4Mapped()
        && !ipv6.isLinkLocalAddress()
        && !ipv6.isLoopbackAddress()
        && !ipv6.isAnyLocalAddress()
        && !ipv6.isMulticastAddress();
  }
}
