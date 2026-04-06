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
package org.hyperledger.besu.ethereum.p2p.discovery;

import org.hyperledger.besu.util.number.Percentage;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import inet.ipaddr.IPAddress;
import org.apache.tuweni.bytes.Bytes;

public record P2PDiscoveryConfiguration(
    Boolean p2pEnabled,
    Boolean peerDiscoveryEnabled,
    String p2pHost,
    String p2pInterface,
    Integer p2pPort,
    Optional<String> p2pHostIpv6,
    Optional<String> p2pInterfaceIpv6,
    Integer p2pPortIpv6,
    Integer maxPeers,
    Boolean isLimitRemoteWireConnectionsEnabled,
    Percentage maxRemoteConnectionsPercentage,
    Boolean randomPeerPriority,
    Collection<Bytes> bannedNodeIds,
    List<IPAddress> allowedSubnets,
    Boolean poaDiscoveryRetryBootnodes,
    List<String> bootNodes,
    String discoveryDnsUrl,
    boolean preferIpv6Outbound) {}
