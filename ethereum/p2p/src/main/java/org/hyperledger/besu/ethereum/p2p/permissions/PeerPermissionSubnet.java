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

import org.hyperledger.besu.ethereum.p2p.peers.Peer;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;

import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages peer permissions based on IP subnet restrictions.
 *
 * <p>This class extends {@link PeerPermissions} to implement access control based on IP subnets. It
 * allows for the configuration of permitted subnets and uses these configurations to determine
 * whether a peer should be allowed or denied access based on its IP address.
 *
 * <p>Supports both IPv4 and IPv6 subnets.
 *
 * <p>Note: If no subnets are specified, all peers are considered permitted by default.
 *
 * @see PeerPermissions
 */
public class PeerPermissionSubnet extends PeerPermissions {
  private static final Logger LOG = LoggerFactory.getLogger(PeerPermissionSubnet.class);

  private final List<IPAddress> allowedSubnets;

  /**
   * Constructs a new {@code PeerPermissionSubnet} instance with specified allowed subnets.
   *
   * @param allowedSubnets A list of {@link IPAddress} prefix blocks representing the subnets that
   *     are allowed to interact with the local node. Cannot be {@code null}.
   */
  public PeerPermissionSubnet(final List<IPAddress> allowedSubnets) {
    this.allowedSubnets = allowedSubnets;
  }

  /**
   * Determines if a peer is permitted based on the configured subnets.
   *
   * <p>This method checks if the remote peer's IP address falls within any of the configured
   * allowed subnets. If the peer's IP is within any of the allowed subnets, it is permitted.
   * Otherwise, it is denied.
   *
   * @param localNode This parameter is not used in the current implementation.
   * @param remotePeer The remote peer to check. Its IP address is used to determine permission.
   * @param action Ignored. If the peer is not allowed in the subnet, all actions are now allowed.
   * @return {@code true} if the peer is permitted based on its IP address; {@code false} otherwise.
   */
  @Override
  public boolean isPermitted(final Peer localNode, final Peer remotePeer, final Action action) {
    if (allowedSubnets == null || allowedSubnets.isEmpty()) {
      return true;
    }
    final String remotePeerHostAddress = remotePeer.getEnodeURL().getIpAsString();
    final IPAddress remoteAddress = new IPAddressString(remotePeerHostAddress).getAddress();
    return isAddressInSubnets(remoteAddress, remotePeerHostAddress);
  }

  @Override
  public boolean isPermitted(final InetSocketAddress address) {
    if (allowedSubnets == null || allowedSubnets.isEmpty()) {
      return true;
    }
    final InetAddress inetAddress = address.getAddress();
    final String hostAddress =
        inetAddress != null ? inetAddress.getHostAddress() : address.getHostString();
    final IPAddress remoteAddress = new IPAddressString(hostAddress).getAddress();
    return isAddressInSubnets(remoteAddress, hostAddress);
  }

  private boolean isAddressInSubnets(final IPAddress remoteAddress, final String addressString) {
    if (remoteAddress == null) {
      LOG.trace("Could not parse peer address: {}", addressString);
      return false;
    }
    for (final IPAddress subnet : allowedSubnets) {
      if (subnet.contains(remoteAddress)) {
        return true;
      }
    }
    LOG.trace("Peer {} is not allowed in any of the configured subnets.", addressString);
    return false;
  }
}
