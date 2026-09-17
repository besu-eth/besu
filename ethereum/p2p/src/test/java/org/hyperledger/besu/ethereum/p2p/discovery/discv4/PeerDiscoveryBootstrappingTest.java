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
package org.hyperledger.besu.ethereum.p2p.discovery.discv4;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.DiscoveryPeerV4;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.MockPeerDiscoveryAgent;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.MockPeerDiscoveryAgent.IncomingPacket;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.PacketType;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.Packet;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.ping.PingPacketData;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

public class PeerDiscoveryBootstrappingTest {

  private final PeerDiscoveryTestHelper helper = new PeerDiscoveryTestHelper();

  @Test
  public void bootstrappingPingsSentSingleBootstrapPeer() {
    // Start one test peer and use it as a bootstrap peer.
    final MockPeerDiscoveryAgent testAgent = helper.startDiscoveryAgent();

    // Start an agent.
    assertThat(testAgent.getAdvertisedPeer().isPresent()).isTrue();
    final PeerDiscoveryAgentV4 agent =
        helper.startDiscoveryAgent(testAgent.getAdvertisedPeer().get());

    final List<IncomingPacket> incomingPackets =
        testAgent.getIncomingPackets().stream()
            .filter(p -> p.packet.getType().equals(PacketType.PING))
            .collect(toList());
    assertThat(incomingPackets.size()).isEqualTo(1);
    final Packet pingPacket = incomingPackets.get(0).packet;
    assertThat(agent.getAdvertisedPeer().isPresent()).isTrue();
    assertThat(pingPacket.getNodeId()).isEqualTo(agent.getAdvertisedPeer().get().getId());

    assertThat(pingPacket.getPacketData(PingPacketData.class).isPresent()).isTrue();
    final PingPacketData pingData = pingPacket.getPacketData(PingPacketData.class).get();
    assertThat(pingData.getExpiration())
        .isGreaterThanOrEqualTo(System.currentTimeMillis() / 1000 - 10000);
    assertThat(pingData.getFrom()).contains(agent.getAdvertisedPeer().get().getEndpoint());
    assertThat(pingData.getTo()).contains(testAgent.getAdvertisedPeer().get().getEndpoint());
  }

  @Test
  public void bootstrappingPingsSentMultipleBootstrapPeers() {
    // Use these peers as bootstrap peers.
    final List<MockPeerDiscoveryAgent> bootstrapAgents = helper.startDiscoveryAgents(3);
    final List<DiscoveryPeerV4> bootstrapPeers =
        bootstrapAgents.stream()
            .map(PeerDiscoveryAgentV4::getAdvertisedPeer)
            .map(Optional::get)
            .collect(toList());

    // Start five agents.
    final List<MockPeerDiscoveryAgent> agents = helper.startDiscoveryAgents(5, bootstrapPeers);

    // Assert that all test peers received a Find Neighbors packet.
    for (final MockPeerDiscoveryAgent bootstrapAgent : bootstrapAgents) {
      // Five messages per peer (sent by each of the five agents).
      final List<Packet> packets =
          bootstrapAgent.getIncomingPackets().stream().map(p -> p.packet).collect(toList());

      // Assert that the node IDs we received belong to the test agents.
      final List<Bytes> senderIds =
          packets.stream().map(Packet::getNodeId).distinct().collect(toList());
      final List<Bytes> agentIds =
          agents.stream()
              .map(PeerDiscoveryAgentV4::getAdvertisedPeer)
              .map(Optional::get)
              .map(Peer::getId)
              .distinct()
              .collect(toList());

      assertThat(senderIds).containsExactlyInAnyOrderElementsOf(agentIds);

      // Traverse all received pings.
      final List<Packet> pingPackets =
          packets.stream().filter(p -> p.getType().equals(PacketType.PING)).collect(toList());
      for (final Packet packet : pingPackets) {
        // Assert that the packet was a Find Neighbors one.
        assertThat(packet.getType()).isEqualTo(PacketType.PING);

        // Assert on the content of the packet data.
        assertThat(packet.getPacketData(PingPacketData.class).isPresent()).isTrue();
        final PingPacketData ping = packet.getPacketData(PingPacketData.class).get();
        assertThat(ping.getExpiration())
            .isGreaterThanOrEqualTo(System.currentTimeMillis() / 1000 - 10000);
        assertThat(bootstrapAgent.getAdvertisedPeer().isPresent()).isTrue();
        assertThat(ping.getTo()).contains(bootstrapAgent.getAdvertisedPeer().get().getEndpoint());
      }
    }
  }

  @Test
  public void bootstrappingPeersListUpdated() {
    // Start an agent.
    final PeerDiscoveryAgentV4 bootstrapAgent = helper.startDiscoveryAgent(emptyList());

    // Start other five agents, pointing to the one above as a bootstrap peer.
    assertThat(bootstrapAgent.getAdvertisedPeer().isPresent()).isTrue();
    final List<MockPeerDiscoveryAgent> otherAgents =
        helper.startDiscoveryAgents(5, singletonList(bootstrapAgent.getAdvertisedPeer().get()));

    final Bytes[] otherPeersIds =
        otherAgents.stream().map(PeerDiscoveryAgentV4::getId).toArray(Bytes[]::new);

    assertThat(bootstrapAgent.streamDiscoveredPeers())
        .extracting(Peer::getId)
        .containsExactlyInAnyOrder(otherPeersIds);

    assertThat(bootstrapAgent.streamDiscoveredPeers()).allMatch(DiscoveryPeerV4::isBonded);

    // This agent will bootstrap off the bootstrap peer, will add all nodes returned by the latter,
    // and will bond with them, ultimately adding all 7 nodes in the network to its table.
    final PeerDiscoveryAgentV4 newAgent =
        helper.startDiscoveryAgent(bootstrapAgent.getAdvertisedPeer().get());
    assertThat(newAgent.streamDiscoveredPeers()).hasSize(6);
  }
}
