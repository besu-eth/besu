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

import org.hyperledger.besu.crypto.SECPPublicKey;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.Endpoint;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.DiscoveryPeerV4;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.PacketType;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.enrrequest.EnrRequestPacketDataFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.enrrequest.EnrRequestPacketDataRlpWriter;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.enrresponse.EnrResponsePacketDataFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.enrresponse.EnrResponsePacketDataRlpWriter;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.findneighbors.FindNeighborsPacketDataFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.findneighbors.FindNeighborsPacketDataRlpWriter;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.neighbors.NeighborsPacketDataFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.neighbors.NeighborsPacketDataRlpWriter;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.ping.PingPacketDataFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.ping.PingPacketDataRlpWriter;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.pong.PongPacketDataFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.pong.PongPacketDataRlpWriter;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.validation.DiscoveryPeersValidator;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.validation.EndpointValidator;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.validation.ExpiryValidator;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.validation.NodeRecordValidator;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.validation.RequestHashValidator;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.validation.TargetValidator;

import java.math.BigInteger;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt64;
import org.ethereum.beacon.discovery.schema.EnrField;
import org.ethereum.beacon.discovery.schema.IdentitySchemaInterpreter;
import org.ethereum.beacon.discovery.schema.NodeRecordFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class PacketSerializerTest {
  final Clock clock = Clock.fixed(Instant.ofEpochSecond(123), ZoneId.of("UTC"));

  private PacketSerializer packetSerializer;

  @BeforeEach
  public void beforeTest() {
    PacketSignatureEncoder packetSignatureEncoder = new PacketSignatureEncoder();
    PingPacketDataRlpWriter pingPacketDataRlpWriter = new PingPacketDataRlpWriter();
    PongPacketDataRlpWriter pongPacketDataRlpWriter = new PongPacketDataRlpWriter();
    FindNeighborsPacketDataRlpWriter findNeighborsPacketDataRlpWriter =
        new FindNeighborsPacketDataRlpWriter();
    NeighborsPacketDataRlpWriter neighborsPacketDataRlpWriter = new NeighborsPacketDataRlpWriter();
    EnrRequestPacketDataRlpWriter enrRequestPacketDataRlpWriter =
        new EnrRequestPacketDataRlpWriter();
    EnrResponsePacketDataRlpWriter enrResponsePacketDataRlpWriter =
        new EnrResponsePacketDataRlpWriter();
    packetSerializer =
        new PacketSerializer(
            packetSignatureEncoder,
            pingPacketDataRlpWriter,
            pongPacketDataRlpWriter,
            findNeighborsPacketDataRlpWriter,
            neighborsPacketDataRlpWriter,
            enrRequestPacketDataRlpWriter,
            enrResponsePacketDataRlpWriter);
  }

  @Test
  public void testEncodeForPingPacket() {
    final PacketType type = PacketType.PING;
    final PingPacketDataFactory packetDataFactory =
        new PingPacketDataFactory(new EndpointValidator(), new ExpiryValidator(clock), clock);
    final PacketData data =
        packetDataFactory.create(
            Optional.empty(),
            new Endpoint("10.0.0.1", 30303, Optional.empty()),
            456,
            UInt64.valueOf(789));
    final Bytes hash =
        Bytes.fromHexString("0xc68a00f14b2e91d2592005ca4e77247c8e0627fc5a6e6d942bd9e1af41d2f5b9");
    final SECPSignature signature =
        SECPSignature.create(BigInteger.ONE, BigInteger.TWO, (byte) 0x00, BigInteger.valueOf(4));
    final SECPPublicKey publicKey = Mockito.mock(SECPPublicKey.class);
    Packet packet = new Packet(type, data, hash, signature, publicKey);

    Bytes result = packetSerializer.encode(packet);

    Mockito.verifyNoInteractions(publicKey);

    String expectedResult =
        "0xc68a00f14b2e91d2592005ca4e77247c8e0627fc5a6e6d942bd9e1af41d2f5b9000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000020001d105c9840a00000182765f808201c8820315";
    Assertions.assertEquals(expectedResult, result.toHexString());
  }

  @Test
  public void testEncodeForPongPacket() {
    final PacketType type = PacketType.PONG;
    final PongPacketDataFactory packetDataFactory =
        new PongPacketDataFactory(new ExpiryValidator(clock), clock);
    final PacketData data =
        packetDataFactory.create(
            new Endpoint("10.0.0.1", 30303, Optional.empty()),
            Bytes.fromHexString("0x0123"),
            456,
            UInt64.valueOf(789));
    final Bytes hash =
        Bytes.fromHexString("0x21206a30fabe5d8fe7c4896bb25f51a9f2a22260da9164d21645fb482439736c");
    final SECPSignature signature =
        SECPSignature.create(BigInteger.ONE, BigInteger.TWO, (byte) 0x00, BigInteger.valueOf(4));
    final SECPPublicKey publicKey = Mockito.mock(SECPPublicKey.class);
    Packet packet = new Packet(type, data, hash, signature, publicKey);

    Bytes result = packetSerializer.encode(packet);

    Mockito.verifyNoInteractions(publicKey);

    String expectedResult =
        "0x21206a30fabe5d8fe7c4896bb25f51a9f2a22260da9164d21645fb482439736c000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000020002d3c9840a00000182765f808201238201c8820315";
    Assertions.assertEquals(expectedResult, result.toHexString());
  }

  @Test
  public void testEncodeForFindNeighborsPacket() {
    final PacketType type = PacketType.FIND_NEIGHBORS;
    final FindNeighborsPacketDataFactory packetDataFactory =
        new FindNeighborsPacketDataFactory(
            new TargetValidator(), new ExpiryValidator(clock), clock);
    final PacketData data = packetDataFactory.create(Bytes.repeat((byte) 0xcd, 64), 456);
    final Bytes hash =
        Bytes.fromHexString("0xe51a3c97707983d13de6a7aa7c54cc67e8a8f964145bb86cd0af7ee6dd6a196b");
    final SECPSignature signature =
        SECPSignature.create(BigInteger.ONE, BigInteger.TWO, (byte) 0x00, BigInteger.valueOf(4));
    final SECPPublicKey publicKey = Mockito.mock(SECPPublicKey.class);
    Packet packet = new Packet(type, data, hash, signature, publicKey);

    Bytes result = packetSerializer.encode(packet);

    Mockito.verifyNoInteractions(publicKey);

    String expectedResult =
        "0xe51a3c97707983d13de6a7aa7c54cc67e8a8f964145bb86cd0af7ee6dd6a196b000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000020003f845b840cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd8201c8";
    Assertions.assertEquals(expectedResult, result.toHexString());
  }

  @Test
  public void testEncodeForNeighborsPacket() {
    final PacketType type = PacketType.NEIGHBORS;
    final NeighborsPacketDataFactory packetDataFactory =
        new NeighborsPacketDataFactory(
            new DiscoveryPeersValidator(), new ExpiryValidator(clock), clock);
    final PacketData data =
        packetDataFactory.create(
            List.of(
                DiscoveryPeerV4.fromIdAndEndpoint(
                    Bytes.repeat((byte) 0x98, 64),
                    new Endpoint("10.0.0.1", 30303, Optional.empty()))),
            456);
    final Bytes hash =
        Bytes.fromHexString("0xc6dce7f54086fb7537a9c2793fd7938b057888442109304ea463017c6265e743");
    final SECPSignature signature =
        SECPSignature.create(BigInteger.ONE, BigInteger.TWO, (byte) 0x00, BigInteger.valueOf(4));
    final SECPPublicKey publicKey = Mockito.mock(SECPPublicKey.class);
    Packet packet = new Packet(type, data, hash, signature, publicKey);

    Bytes result = packetSerializer.encode(packet);

    Mockito.verifyNoInteractions(publicKey);

    String expectedResult =
        "0xc6dce7f54086fb7537a9c2793fd7938b057888442109304ea463017c6265e743000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000020004f852f84df84b840a00000182765f80b840989898989898989898989898989898989898989898989898989898989898989898989898989898989898989898989898989898989898989898989898989898988201c8";
    Assertions.assertEquals(expectedResult, result.toHexString());
  }

  @Test
  public void testEncodeForEnrRequestPacket() {
    final PacketType type = PacketType.ENR_REQUEST;
    final EnrRequestPacketDataFactory packetDataFactory =
        new EnrRequestPacketDataFactory(new ExpiryValidator(clock), clock);
    final PacketData data = packetDataFactory.create(456);
    final Bytes hash =
        Bytes.fromHexString("0xfde48f75a7340fafea894eb45f7badecd9705227902c5b48928ad3c57b1963e7");
    final SECPSignature signature =
        SECPSignature.create(BigInteger.ONE, BigInteger.TWO, (byte) 0x00, BigInteger.valueOf(4));
    final SECPPublicKey publicKey = Mockito.mock(SECPPublicKey.class);
    Packet packet = new Packet(type, data, hash, signature, publicKey);

    Bytes result = packetSerializer.encode(packet);

    Mockito.verifyNoInteractions(publicKey);

    String expectedResult =
        "0xfde48f75a7340fafea894eb45f7badecd9705227902c5b48928ad3c57b1963e7000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000020005c38201c8";
    Assertions.assertEquals(expectedResult, result.toHexString());
  }

  @Test
  public void testEncodeForEnrResponsePacket() {
    final PacketType type = PacketType.ENR_RESPONSE;
    final EnrResponsePacketDataFactory packetDataFactory =
        new EnrResponsePacketDataFactory(new RequestHashValidator(), new NodeRecordValidator());
    final PacketData data =
        packetDataFactory.create(
            Bytes.fromHexString("0x1234"),
            NodeRecordFactory.DEFAULT.createFromValues(
                UInt64.valueOf(567),
                List.of(new EnrField("id", IdentitySchemaInterpreter.V4.getScheme()))));
    final Bytes hash =
        Bytes.fromHexString("0x9d0b57ce920728f26211e7784f626bf9a881e7e942c7243160d535a3760e8848");
    final SECPSignature signature =
        SECPSignature.create(BigInteger.ONE, BigInteger.TWO, (byte) 0x00, BigInteger.valueOf(4));
    final SECPPublicKey publicKey = Mockito.mock(SECPPublicKey.class);
    Packet packet = new Packet(type, data, hash, signature, publicKey);

    Bytes result = packetSerializer.encode(packet);

    Mockito.verifyNoInteractions(publicKey);

    String expectedResult =
        "0x9d0b57ce920728f26211e7784f626bf9a881e7e942c7243160d535a3760e8848000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000020006f870821234f86bb860000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000820237826964827634";
    Assertions.assertEquals(expectedResult, result.toHexString());
  }
}
