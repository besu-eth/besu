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

import java.net.InetAddress;
import java.util.Optional;

public interface NodeIdentifier {
  Optional<InetAddress> getIpV4Address();

  Optional<Integer> getTcpListeningPort();

  Optional<Integer> getUdpDiscoveryPort();

  Optional<InetAddress> getIpV6Address();

  Optional<Integer> getIpV6TcpListeningPort();

  Optional<Integer> getIpV6UdpDiscoveryPort();

  static boolean isSameListeningEndpoint(final NodeIdentifier node1, final NodeIdentifier node2) {
    return node1 != null
        && node2 != null
        && ((node1.getIpV4Address().isPresent()
                && node2.getIpV4Address().isPresent()
                && node1.getIpV4Address().equals(node2.getIpV4Address())
                && node1.getTcpListeningPort().equals(node2.getTcpListeningPort()))
            || (node1.getIpV6Address().isPresent()
                && node2.getIpV6Address().isPresent()
                && node1.getIpV6Address().equals(node2.getIpV6Address())
                && node1.getIpV6TcpListeningPort().equals(node2.getIpV6TcpListeningPort())));
  }
}
