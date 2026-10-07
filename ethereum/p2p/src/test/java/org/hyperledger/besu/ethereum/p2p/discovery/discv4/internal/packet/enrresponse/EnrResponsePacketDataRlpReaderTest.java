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
package org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.enrresponse;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.ethereum.p2p.discovery.PeerDiscoveryPacketDecodingException;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;

import java.util.Map;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.rlp.EndOfRLPException;
import org.apache.tuweni.rlp.InvalidRLPEncodingException;
import org.apache.tuweni.units.bigints.UInt64;
import org.ethereum.beacon.discovery.schema.IdentitySchemaInterpreter;
import org.ethereum.beacon.discovery.schema.NodeRecord;
import org.ethereum.beacon.discovery.schema.NodeRecordFactory;
import org.ethereum.beacon.discovery.util.DecodeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EnrResponsePacketDataRlpReaderTest {

  private @Mock NodeRecordFactory nodeRecordFactory;
  private @Mock EnrResponsePacketDataFactory enrResponsePacketDataFactory;

  private EnrResponsePacketDataRlpReader reader;

  @BeforeEach
  public void beforeTest() {
    reader = new EnrResponsePacketDataRlpReader(nodeRecordFactory, enrResponsePacketDataFactory);
  }

  @Test
  public void testReadFrom() {
    final Bytes requestHash = Bytes.fromHexString("0xdeadbeef");
    final Bytes signature = Bytes.fromHexString("0xfeebdaed");
    final NodeRecord enr =
        NodeRecord.fromRawFields(
            IdentitySchemaInterpreter.V4,
            UInt64.valueOf(123),
            signature,
            Map.of("key", Bytes.fromHexString("0x4567")));
    final EnrResponsePacketData enrResponsePacketData = new EnrResponsePacketData(requestHash, enr);
    final BytesValueRLPInput in =
        new BytesValueRLPInput(
            Bytes.fromHexString("0xd384deadbeefcd84feebdaed7b836b6579824567"), false);

    Mockito.when(nodeRecordFactory.fromBytes(enr.serialize())).thenReturn(enr);
    Mockito.when(enrResponsePacketDataFactory.create(requestHash, enr))
        .thenReturn(enrResponsePacketData);

    final EnrResponsePacketData result = reader.readFrom(in);

    Mockito.verify(nodeRecordFactory).fromBytes(enr.serialize());
    Mockito.verify(enrResponsePacketDataFactory).create(requestHash, enr);

    Assertions.assertNotNull(result);
    Assertions.assertEquals(requestHash, result.getRequestHash());
    Assertions.assertEquals(enr, result.getEnr());
  }

  @Test
  public void testReadFromWithInvalidRlpEncodingThrowsDecodingException() {
    final BytesValueRLPInput in =
        new BytesValueRLPInput(
            Bytes.fromHexString("0xd384deadbeefcd84feebdaed7b836b6579824567"), false);

    Mockito.when(nodeRecordFactory.fromBytes(Mockito.any(Bytes.class)))
        .thenThrow(new InvalidRLPEncodingException("Insufficient bytes in RLP encoding"));

    assertThatThrownBy(() -> reader.readFrom(in))
        .isInstanceOf(PeerDiscoveryPacketDecodingException.class)
        .hasMessageContaining("Invalid ENR in ENR_RESPONSE packet")
        .hasCauseInstanceOf(InvalidRLPEncodingException.class);
  }

  @Test
  public void testReadFromWithEndOfRlpThrowsDecodingException() {
    final BytesValueRLPInput in =
        new BytesValueRLPInput(
            Bytes.fromHexString("0xd384deadbeefcd84feebdaed7b836b6579824567"), false);

    Mockito.when(nodeRecordFactory.fromBytes(Mockito.any(Bytes.class)))
        .thenThrow(new EndOfRLPException());

    assertThatThrownBy(() -> reader.readFrom(in))
        .isInstanceOf(PeerDiscoveryPacketDecodingException.class)
        .hasMessageContaining("Invalid ENR in ENR_RESPONSE packet")
        .hasCauseInstanceOf(EndOfRLPException.class);
  }

  @Test
  public void testReadFromWithDecodeExceptionThrowsDecodingException() {
    final BytesValueRLPInput in =
        new BytesValueRLPInput(
            Bytes.fromHexString("0xd384deadbeefcd84feebdaed7b836b6579824567"), false);

    Mockito.when(nodeRecordFactory.fromBytes(Mockito.any(Bytes.class)))
        .thenThrow(new DecodeException("decode failed"));

    assertThatThrownBy(() -> reader.readFrom(in))
        .isInstanceOf(PeerDiscoveryPacketDecodingException.class)
        .hasMessageContaining("Invalid ENR in ENR_RESPONSE packet")
        .hasCauseInstanceOf(DecodeException.class);
  }
}
