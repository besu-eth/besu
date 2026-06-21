/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal;

import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.Packet;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiscoveryProtocolLogger {

  private static final Logger LOG = LoggerFactory.getLogger(DiscoveryProtocolLogger.class);
  private final LabelledMetric<Counter> outgoingMessageCounter;
  private final LabelledMetric<Counter> incomingMessageCounter;

  public DiscoveryProtocolLogger(final MetricsSystem metricsSystem) {
    outgoingMessageCounter =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.NETWORK,
            "discovery_messages_outbound",
            "Total number of P2P discovery messages sent",
            "name");
    incomingMessageCounter =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.NETWORK,
            "discovery_messages_inbound",
            "Total number of P2P discovery messages received",
            "name");
  }

  void logSendingPacket(final Peer peer, final Packet packet) {
    outgoingMessageCounter.labels(packet.getType().name()).inc();
    LOG.trace(
        "<<< Sending {} packet to peer {} ({}): {}",
        shortenPacketType(packet),
        peer.getLoggableId(),
        peer.getEnodeURL(),
        packet);
  }

  void logReceivedPacket(final Peer peer, final Packet packet) {
    incomingMessageCounter.labels(packet.getType().name()).inc();
    LOG.trace(
        ">>> Received {} packet from peer {} ({}): {}",
        shortenPacketType(packet),
        peer.getLoggableId(),
        peer.getEnodeURL(),
        packet);
  }

  private String shortenPacketType(final Packet packet) {
    return switch (packet.getType()) {
      case PING -> "PING ";
      case PONG -> "PONG ";
      case FIND_NEIGHBORS -> "FINDN";
      case NEIGHBORS -> "NEIGH";
      case ENR_REQUEST -> "ENRREQ";
      case ENR_RESPONSE -> "ENRRESP";
    };
  }
}
