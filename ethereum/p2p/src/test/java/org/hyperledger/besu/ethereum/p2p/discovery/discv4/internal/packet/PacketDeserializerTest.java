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

import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.Endpoint;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.PacketType;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.enrrequest.EnrRequestPacketData;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.enrrequest.EnrRequestPacketDataFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.enrrequest.EnrRequestPacketDataRlpReader;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.enrrequest.EnrRequestPacketDataRlpWriter;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.enrresponse.EnrResponsePacketData;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.enrresponse.EnrResponsePacketDataFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.enrresponse.EnrResponsePacketDataRlpReader;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.enrresponse.EnrResponsePacketDataRlpWriter;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.findneighbors.FindNeighborsPacketData;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.findneighbors.FindNeighborsPacketDataFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.findneighbors.FindNeighborsPacketDataRlpReader;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.findneighbors.FindNeighborsPacketDataRlpWriter;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.neighbors.NeighborsPacketData;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.neighbors.NeighborsPacketDataFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.neighbors.NeighborsPacketDataRlpReader;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.neighbors.NeighborsPacketDataRlpWriter;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.ping.PingPacketData;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.ping.PingPacketDataRlpReader;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.ping.PingPacketDataRlpWriter;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.pong.PongPacketData;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.pong.PongPacketDataFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.pong.PongPacketDataRlpReader;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.pong.PongPacketDataRlpWriter;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.validation.DiscoveryPeersValidator;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.validation.ExpiryValidator;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.validation.NodeRecordValidator;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.validation.RequestHashValidator;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.validation.TargetValidator;

import java.math.BigInteger;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt64;
import org.ethereum.beacon.discovery.schema.IdentitySchemaInterpreter;
import org.ethereum.beacon.discovery.schema.NodeRecordFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PacketDeserializerTest {
  private final Clock clock = Clock.fixed(Instant.ofEpochSecond(123), ZoneId.of("UTC"));
  private PacketDeserializer packetDeserializer;

  @BeforeEach
  public void beforeTest() {
    PingPacketDataRlpReader pingPacketDataRlpReader = new PingPacketDataRlpReader();
    PongPacketDataRlpReader pongPacketDataRlpReader =
        new PongPacketDataRlpReader(new PongPacketDataFactory(new ExpiryValidator(clock), clock));
    FindNeighborsPacketDataRlpReader findNeighborsPacketDataRlpReader =
        new FindNeighborsPacketDataRlpReader(
            new FindNeighborsPacketDataFactory(
                new TargetValidator(), new ExpiryValidator(clock), clock));
    NeighborsPacketDataRlpReader neighborsPacketDataRlpReader =
        new NeighborsPacketDataRlpReader(
            new NeighborsPacketDataFactory(
                new DiscoveryPeersValidator(), new ExpiryValidator(clock), clock));
    EnrRequestPacketDataRlpReader enrRequestPacketDataRlpReader =
        new EnrRequestPacketDataRlpReader(
            new EnrRequestPacketDataFactory(new ExpiryValidator(clock), clock));
    EnrResponsePacketDataRlpReader enrResponsePacketDataRlpReader =
        new EnrResponsePacketDataRlpReader(
            NodeRecordFactory.DEFAULT,
            new EnrResponsePacketDataFactory(
                new RequestHashValidator(), new NodeRecordValidator()));

    PacketFactory packetFactory =
        new PacketFactory(
            new PingPacketDataRlpWriter(),
            new PongPacketDataRlpWriter(),
            new FindNeighborsPacketDataRlpWriter(),
            new NeighborsPacketDataRlpWriter(),
            new EnrRequestPacketDataRlpWriter(),
            new EnrResponsePacketDataRlpWriter(),
            SignatureAlgorithmFactory.getInstance(),
            new PacketSignatureEncoder());
    packetDeserializer =
        new PacketDeserializer(
            pingPacketDataRlpReader,
            pongPacketDataRlpReader,
            findNeighborsPacketDataRlpReader,
            neighborsPacketDataRlpReader,
            enrRequestPacketDataRlpReader,
            enrResponsePacketDataRlpReader,
            packetFactory);
  }

  @Test
  public void testDecodeForPingPacket() {
    String packetHex =
        "0xc68a00f14b2e91d2592005ca4e77247c8e0627fc5a6e6d942bd9e1af41d2f5b9000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000020001d105c9840a00000182765f808201c8820315";
    Packet packet = packetDeserializer.decode(Bytes.fromHexString(packetHex));

    Assertions.assertNotNull(packet);
    Assertions.assertEquals(PacketType.PING, packet.getType());
    Assertions.assertEquals(
        "0xc68a00f14b2e91d2592005ca4e77247c8e0627fc5a6e6d942bd9e1af41d2f5b9",
        packet.getHash().toHexString());
    Assertions.assertEquals(
        SECPSignature.create(BigInteger.ONE, BigInteger.TWO, (byte) 0x00, BigInteger.valueOf(4)),
        packet.getSignature());

    PingPacketData actualPacketData = packet.getPacketData(PingPacketData.class).orElseThrow();
    Assertions.assertFalse(actualPacketData.getFrom().isPresent());
    Assertions.assertEquals(
        Optional.of(new Endpoint("10.0.0.1", 30303, Optional.empty())), actualPacketData.getTo());
    Assertions.assertEquals(456, actualPacketData.getExpiration());
    Assertions.assertEquals(Optional.of(UInt64.valueOf(789)), actualPacketData.getEnrSeq());
  }

  @Test
  public void testDecodeForPongPacket() {
    String packetHex =
        "0x21206a30fabe5d8fe7c4896bb25f51a9f2a22260da9164d21645fb482439736c000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000020002d3c9840a00000182765f808201238201c8820315";
    Packet packet = packetDeserializer.decode(Bytes.fromHexString(packetHex));

    Assertions.assertNotNull(packet);
    Assertions.assertEquals(PacketType.PONG, packet.getType());
    Assertions.assertEquals(
        "0x21206a30fabe5d8fe7c4896bb25f51a9f2a22260da9164d21645fb482439736c",
        packet.getHash().toHexString());
    Assertions.assertEquals(
        SECPSignature.create(BigInteger.ONE, BigInteger.TWO, (byte) 0x00, BigInteger.valueOf(4)),
        packet.getSignature());

    PongPacketData actualPacketData = packet.getPacketData(PongPacketData.class).orElseThrow();
    Assertions.assertEquals(
        new Endpoint("10.0.0.1", 30303, Optional.empty()), actualPacketData.getTo());
    Assertions.assertEquals(Bytes.fromHexString("0x0123"), actualPacketData.getPingHash());
    Assertions.assertEquals(456, actualPacketData.getExpiration());
    Assertions.assertEquals(Optional.of(UInt64.valueOf(789)), actualPacketData.getEnrSeq());
  }

  @Test
  public void testDecodeForFindNeighborsPacket() {
    String packetHex =
        "0xe51a3c97707983d13de6a7aa7c54cc67e8a8f964145bb86cd0af7ee6dd6a196b000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000020003f845b840cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd8201c8";
    Packet packet = packetDeserializer.decode(Bytes.fromHexString(packetHex));

    Assertions.assertNotNull(packet);
    Assertions.assertEquals(PacketType.FIND_NEIGHBORS, packet.getType());
    Assertions.assertEquals(
        "0xe51a3c97707983d13de6a7aa7c54cc67e8a8f964145bb86cd0af7ee6dd6a196b",
        packet.getHash().toHexString());
    Assertions.assertEquals(
        SECPSignature.create(BigInteger.ONE, BigInteger.TWO, (byte) 0x00, BigInteger.valueOf(4)),
        packet.getSignature());

    FindNeighborsPacketData actualPacketData =
        packet.getPacketData(FindNeighborsPacketData.class).orElseThrow();
    Assertions.assertEquals(Bytes.repeat((byte) 0xcd, 64), actualPacketData.getTarget());
    Assertions.assertEquals(456, actualPacketData.getExpiration());
  }

  @Test
  public void testDecodeForNeighborsPacket() {
    String packetHex =
        "0xc6dce7f54086fb7537a9c2793fd7938b057888442109304ea463017c6265e743000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000020004f852f84df84b840a00000182765f80b840989898989898989898989898989898989898989898989898989898989898989898989898989898989898989898989898989898989898989898989898989898988201c8";
    Packet packet = packetDeserializer.decode(Bytes.fromHexString(packetHex));

    Assertions.assertNotNull(packet);
    Assertions.assertEquals(PacketType.NEIGHBORS, packet.getType());
    Assertions.assertEquals(
        "0xc6dce7f54086fb7537a9c2793fd7938b057888442109304ea463017c6265e743",
        packet.getHash().toHexString());
    Assertions.assertEquals(
        SECPSignature.create(BigInteger.ONE, BigInteger.TWO, (byte) 0x00, BigInteger.valueOf(4)),
        packet.getSignature());

    NeighborsPacketData actualPacketData =
        packet.getPacketData(NeighborsPacketData.class).orElseThrow();
    Assertions.assertEquals(1, actualPacketData.getNodes().size());
    Assertions.assertEquals(
        Bytes.repeat((byte) 0x98, 64), actualPacketData.getNodes().getFirst().getId());
    Assertions.assertEquals(
        new Endpoint("10.0.0.1", 30303, Optional.empty()),
        actualPacketData.getNodes().getFirst().getEndpoint());
    Assertions.assertEquals(456, actualPacketData.getExpiration());
  }

  @Test
  public void testDecodeForEnrRequestPacket() {
    String packetHex =
        "0xfde48f75a7340fafea894eb45f7badecd9705227902c5b48928ad3c57b1963e7000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000020005c38201c8";
    Packet packet = packetDeserializer.decode(Bytes.fromHexString(packetHex));

    Assertions.assertNotNull(packet);
    Assertions.assertEquals(PacketType.ENR_REQUEST, packet.getType());
    Assertions.assertEquals(
        "0xfde48f75a7340fafea894eb45f7badecd9705227902c5b48928ad3c57b1963e7",
        packet.getHash().toHexString());
    Assertions.assertEquals(
        SECPSignature.create(BigInteger.ONE, BigInteger.TWO, (byte) 0x00, BigInteger.valueOf(4)),
        packet.getSignature());

    EnrRequestPacketData actualPacketData =
        packet.getPacketData(EnrRequestPacketData.class).orElseThrow();
    Assertions.assertEquals(456, actualPacketData.getExpiration());
  }

  @Test
  public void testDecodeForEnrResponsePacket() {
    String packetHex =
        "0x9d0b57ce920728f26211e7784f626bf9a881e7e942c7243160d535a3760e8848000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000020006f870821234f86bb860000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000820237826964827634";
    Packet packet = packetDeserializer.decode(Bytes.fromHexString(packetHex));

    Assertions.assertNotNull(packet);
    Assertions.assertEquals(PacketType.ENR_RESPONSE, packet.getType());
    Assertions.assertEquals(
        "0x9d0b57ce920728f26211e7784f626bf9a881e7e942c7243160d535a3760e8848",
        packet.getHash().toHexString());
    Assertions.assertEquals(
        SECPSignature.create(BigInteger.ONE, BigInteger.TWO, (byte) 0x00, BigInteger.valueOf(4)),
        packet.getSignature());

    EnrResponsePacketData actualPacketData =
        packet.getPacketData(EnrResponsePacketData.class).orElseThrow();
    Assertions.assertEquals(Bytes.fromHexString("0x1234"), actualPacketData.getRequestHash());
    Assertions.assertEquals(
        IdentitySchemaInterpreter.V4.getScheme(), actualPacketData.getEnr().getIdentityScheme());
    Assertions.assertEquals(UInt64.valueOf(567), actualPacketData.getEnr().getSeq());
  }
}
