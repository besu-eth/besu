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
package org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet;

import static org.hyperledger.besu.util.Preconditions.checkGuard;

import org.hyperledger.besu.ethereum.p2p.discovery.PeerDiscoveryPacketDecodingException;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.PacketType;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.enrrequest.EnrRequestPacketDataRlpReader;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.enrresponse.EnrResponsePacketDataRlpReader;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.findneighbors.FindNeighborsPacketDataRlpReader;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.neighbors.NeighborsPacketDataRlpReader;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.ping.PingPacketDataRlpReader;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.pong.PongPacketDataRlpReader;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPException;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.tuweni.bytes.Bytes;

@Singleton
public class PacketDeserializer {

  private final PingPacketDataRlpReader pingPacketDataRlpReader;
  private final PongPacketDataRlpReader pongPacketDataRlpReader;
  private final FindNeighborsPacketDataRlpReader findNeighborsPacketDataRlpReader;
  private final NeighborsPacketDataRlpReader neighborsPacketDataRlpReader;
  private final EnrRequestPacketDataRlpReader enrRequestPacketDataRlpReader;
  private final EnrResponsePacketDataRlpReader enrResponsePacketDataRlpReader;
  private final PacketFactory packetFactory;

  public @Inject PacketDeserializer(
      final PingPacketDataRlpReader pingPacketDataRlpReader,
      final PongPacketDataRlpReader pongPacketDataRlpReader,
      final FindNeighborsPacketDataRlpReader findNeighborsPacketDataRlpReader,
      final NeighborsPacketDataRlpReader neighborsPacketDataRlpReader,
      final EnrRequestPacketDataRlpReader enrRequestPacketDataRlpReader,
      final EnrResponsePacketDataRlpReader enrResponsePacketDataRlpReader,
      final PacketFactory packetFactory) {
    this.pingPacketDataRlpReader = pingPacketDataRlpReader;
    this.pongPacketDataRlpReader = pongPacketDataRlpReader;
    this.findNeighborsPacketDataRlpReader = findNeighborsPacketDataRlpReader;
    this.neighborsPacketDataRlpReader = neighborsPacketDataRlpReader;
    this.enrRequestPacketDataRlpReader = enrRequestPacketDataRlpReader;
    this.enrResponsePacketDataRlpReader = enrResponsePacketDataRlpReader;
    this.packetFactory = packetFactory;
  }

  public Packet decode(final Bytes message) {
    checkGuard(
        message.size() >= Packet.PACKET_DATA_INDEX,
        PeerDiscoveryPacketDecodingException::new,
        "Packet too short: expected at least %s bytes, got %s",
        Packet.PACKET_DATA_INDEX,
        message.size());

    final byte type = message.get(Packet.PACKET_TYPE_INDEX);

    final PacketType packetType =
        PacketType.forByte(type)
            .orElseThrow(
                () ->
                    new PeerDiscoveryPacketDecodingException("Unrecognized packet type: " + type));

    final PacketDataDeserializer<? extends PacketData> deserializer =
        switch (packetType) {
          case PING -> pingPacketDataRlpReader;
          case PONG -> pongPacketDataRlpReader;
          case FIND_NEIGHBORS -> findNeighborsPacketDataRlpReader;
          case NEIGHBORS -> neighborsPacketDataRlpReader;
          case ENR_REQUEST -> enrRequestPacketDataRlpReader;
          case ENR_RESPONSE -> enrResponsePacketDataRlpReader;
        };
    final PacketData packetData;
    try {
      packetData = deserializer.readFrom(RLP.input(message.slice(Packet.PACKET_DATA_INDEX)));
    } catch (final RLPException e) {
      throw new PeerDiscoveryPacketDecodingException("Malformed packet of type: " + packetType, e);
    } catch (final IllegalArgumentException e) {
      throw new PeerDiscoveryPacketDecodingException(
          "Failed decoding packet of type: " + packetType, e);
    }
    return packetFactory.create(packetType, packetData, message);
  }
}
