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

import org.hyperledger.besu.ethereum.p2p.discovery.DiscoveryPeer;
import org.hyperledger.besu.ethereum.p2p.discovery.NodeRecordManager;

import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.util.Optional;

import org.ethereum.beacon.discovery.schema.NodeRecord;
import org.ethereum.beacon.discovery.storage.NewAddressHandler;

/**
 * A NewAddressHandler which ignores new IP V4 addresses and handles IP V6 addresses by updating the
 * NodeRecordManager's advertised IP V6 host, returning the updated NodeRecord
 */
public class IpV6NewAddressHandler implements NewAddressHandler {
  private final NodeRecordManager nodeRecordManager;
  private final boolean isAdvertisedIpv6HostSet;

  public IpV6NewAddressHandler(
      final NodeRecordManager nodeRecordManager, final boolean isAdvertisedIpv6HostSet) {
    this.nodeRecordManager = nodeRecordManager;
    this.isAdvertisedIpv6HostSet = isAdvertisedIpv6HostSet;
  }

  @Override
  public Optional<NodeRecord> newAddress(
      final NodeRecord oldRecord, final InetSocketAddress newAddress) {
    if (isIpv6(newAddress) && !isAdvertisedIpv6HostSet) {
      nodeRecordManager.updateAdvertisedIpv6Host(newAddress.getAddress().getHostAddress());
      return nodeRecordManager
          .getLocalNode()
          .flatMap(DiscoveryPeer::getNodeRecord)
          .or(() -> Optional.of(oldRecord));
    }
    return Optional.empty();
  }

  private boolean isIpv6(final InetSocketAddress newAddress) {
    return newAddress.getAddress() instanceof Inet6Address;
  }
}
