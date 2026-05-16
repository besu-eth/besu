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
package org.hyperledger.besu.ethereum.eth.manager;

import org.apache.tuweni.units.bigints.UInt256;

public record EthPeerImmutableAttributes(
    UInt256 estimatedTotalDifficulty,
    boolean hasEstimatedChainHeight,
    long estimatedChainHeight,
    int reputationScore,
    int outstandingRequests,
    long lastRequestTimestamp,
    long p50Latency,
    long p95Latency,
    long p99Latency,
    double successRate,
    long totalBytesTransferred,
    double averageBytesPerResponse,
    double rollingAverageBytesPerSecond,
    boolean isDisconnected,
    boolean isFullyValidated,
    boolean isServingSnap,
    boolean hasAvailableRequestCapacity,
    boolean isInboundInitiated,
    EthPeer ethPeer) {

  public static EthPeerImmutableAttributes from(final EthPeer peer) {
    return new EthPeerImmutableAttributes(
        peer.chainState().getEstimatedTotalDifficulty().toUInt256(),
        peer.chainState().hasEstimatedHeight(),
        peer.chainState().getEstimatedHeight(),
        peer.getReputation().getScore(),
        peer.outstandingRequests(),
        peer.getLastRequestTimestamp(),
        peer.getP50Latency(),
        peer.getP95Latency(),
        peer.getP99Latency(),
        peer.getSuccessRate(),
        peer.getTotalBytesTransferred(),
        peer.getAverageBytesPerResponse(),
        peer.getRollingAverageBytesPerSecond(),
        peer.isDisconnected(),
        peer.isFullyValidated(),
        peer.isServingSnap(),
        peer.hasAvailableRequestCapacity(),
        peer.getConnection().inboundInitiated(),
        peer);
  }
}
