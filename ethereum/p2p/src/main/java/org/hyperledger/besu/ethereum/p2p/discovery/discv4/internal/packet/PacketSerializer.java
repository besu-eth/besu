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

import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.enrrequest.EnrRequestPacketData;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.enrrequest.EnrRequestPacketDataRlpWriter;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.enrresponse.EnrResponsePacketData;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.enrresponse.EnrResponsePacketDataRlpWriter;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.findneighbors.FindNeighborsPacketData;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.findneighbors.FindNeighborsPacketDataRlpWriter;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.neighbors.NeighborsPacketData;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.neighbors.NeighborsPacketDataRlpWriter;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.ping.PingPacketData;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.ping.PingPacketDataRlpWriter;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.pong.PongPacketData;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.pong.PongPacketDataRlpWriter;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.MutableBytes;

@Singleton
public class PacketSerializer {
  private final PacketSignatureEncoder packetSignatureEncoder;
  private final PingPacketDataRlpWriter pingPacketDataRlpWriter;
  private final PongPacketDataRlpWriter pongPacketDataRlpWriter;
  private final FindNeighborsPacketDataRlpWriter findNeighborsPacketDataRlpWriter;
  private final NeighborsPacketDataRlpWriter neighborsPacketDataRlpWriter;
  private final EnrRequestPacketDataRlpWriter enrRequestPacketDataRlpWriter;
  private final EnrResponsePacketDataRlpWriter enrResponsePacketDataRlpWriter;

  public @Inject PacketSerializer(
      final PacketSignatureEncoder packetSignatureEncoder,
      final PingPacketDataRlpWriter pingPacketDataRlpWriter,
      final PongPacketDataRlpWriter pongPacketDataRlpWriter,
      final FindNeighborsPacketDataRlpWriter findNeighborsPacketDataRlpWriter,
      final NeighborsPacketDataRlpWriter neighborsPacketDataRlpWriter,
      final EnrRequestPacketDataRlpWriter enrRequestPacketDataRlpWriter,
      final EnrResponsePacketDataRlpWriter enrResponsePacketDataRlpWriter) {
    this.packetSignatureEncoder = packetSignatureEncoder;
    this.pingPacketDataRlpWriter = pingPacketDataRlpWriter;
    this.pongPacketDataRlpWriter = pongPacketDataRlpWriter;
    this.findNeighborsPacketDataRlpWriter = findNeighborsPacketDataRlpWriter;
    this.neighborsPacketDataRlpWriter = neighborsPacketDataRlpWriter;
    this.enrRequestPacketDataRlpWriter = enrRequestPacketDataRlpWriter;
    this.enrResponsePacketDataRlpWriter = enrResponsePacketDataRlpWriter;
  }

  public Bytes encode(final Packet packet) {
    final Bytes hash = packet.getHash();
    final Bytes encodedSignature = packetSignatureEncoder.encodeSignature(packet.getSignature());
    final BytesValueRLPOutput encodedData = new BytesValueRLPOutput();
    switch (packet.getType()) {
      case PING ->
          pingPacketDataRlpWriter.writeTo(
              packet.getPacketData(PingPacketData.class).orElseThrow(), encodedData);
      case PONG ->
          pongPacketDataRlpWriter.writeTo(
              packet.getPacketData(PongPacketData.class).orElseThrow(), encodedData);
      case FIND_NEIGHBORS ->
          findNeighborsPacketDataRlpWriter.writeTo(
              packet.getPacketData(FindNeighborsPacketData.class).orElseThrow(), encodedData);
      case NEIGHBORS ->
          neighborsPacketDataRlpWriter.writeTo(
              packet.getPacketData(NeighborsPacketData.class).orElseThrow(), encodedData);
      case ENR_REQUEST ->
          enrRequestPacketDataRlpWriter.writeTo(
              packet.getPacketData(EnrRequestPacketData.class).orElseThrow(), encodedData);
      case ENR_RESPONSE ->
          enrResponsePacketDataRlpWriter.writeTo(
              packet.getPacketData(EnrResponsePacketData.class).orElseThrow(), encodedData);
    }

    // Write directly into one pre-sized buffer rather than allocating encodedData's own buffer
    // via encoded() and then copying that into a second, concatenated buffer.
    final int headerSize = hash.size() + encodedSignature.size() + 1;
    final int rlpSize = encodedData.encodedSize();
    final MutableBytes result = MutableBytes.create(headerSize + rlpSize);
    hash.copyTo(result, 0);
    encodedSignature.copyTo(result, hash.size());
    result.set(hash.size() + encodedSignature.size(), packet.getType().getValue());
    encodedData.writeEncoded(result.mutableSlice(headerSize, rlpSize));
    return result;
  }
}
