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
package org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.ping;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.hyperledger.besu.ethereum.p2p.discovery.discv4.Endpoint;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt64;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class PingPacketDataRlpReaderTest {
  private @Mock PingPacketDataFactory factory;

  private PingPacketDataRlpReader reader;

  @BeforeEach
  public void beforeTest() {
    reader = new PingPacketDataRlpReader(factory);
  }

  @Test
  public void testReadFrom() {
    String pingHexData = "0xdf05cb840a00000182765f8211d7cb840a00000282765f8222ce7b84075bcd15";

    Endpoint from = new Endpoint("10.0.0.1", 30303, Optional.of(4567));
    Endpoint to = new Endpoint("10.0.0.2", 30303, Optional.of(8910));
    long expiration = 123;
    UInt64 enrSeq = UInt64.valueOf(123456789);

    Mockito.when(factory.createForDecode(Optional.of(from), Optional.of(to), expiration, enrSeq))
        .thenReturn(new PingPacketData(Optional.of(from), to, expiration, enrSeq));

    PingPacketData result =
        reader.readFrom(new BytesValueRLPInput(Bytes.fromHexString(pingHexData), false));

    Assertions.assertNotNull(result);
    Assertions.assertTrue(result.getFrom().isPresent());
    Assertions.assertEquals(from, result.getFrom().get());
    Assertions.assertEquals(to, result.getTo());
    Assertions.assertEquals(expiration, result.getExpiration());
    Assertions.assertTrue(result.getEnrSeq().isPresent());
    Assertions.assertEquals(enrSeq, result.getEnrSeq().get());
  }

  @Test
  public void readsLegacyPingWithoutFromEndpoint() {
    final Endpoint to = new Endpoint("10.0.0.2", 30303, Optional.of(8910));
    final long expiration = 123;
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeIntScalar(PingPacketData.VERSION);
    to.encodeStandalone(out);
    out.writeLongScalar(expiration);
    out.endList();

    Mockito.when(factory.createForDecode(Optional.empty(), Optional.of(to), expiration, null))
        .thenReturn(new PingPacketData(Optional.empty(), to, expiration, null));

    final PingPacketData result = reader.readFrom(new BytesValueRLPInput(out.encoded(), false));

    Assertions.assertTrue(result.getFrom().isEmpty());
    Assertions.assertEquals(to, result.getTo());
  }

  @Test
  public void ignoresMalformedFromEndpoint() {
    final Endpoint to = new Endpoint("10.0.0.2", 30303, Optional.of(8910));
    final long expiration = 123;
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeIntScalar(PingPacketData.VERSION);
    writeMalformedEndpoint(out);
    to.encodeStandalone(out);
    out.writeLongScalar(expiration);
    out.endList();

    Mockito.when(factory.createForDecode(Optional.empty(), Optional.of(to), expiration, null))
        .thenReturn(new PingPacketData(Optional.empty(), to, expiration, null));

    final PingPacketData result = reader.readFrom(new BytesValueRLPInput(out.encoded(), false));

    Assertions.assertTrue(result.getFrom().isEmpty());
    Assertions.assertEquals(to, result.getTo());
  }

  @Test
  public void ignoresMalformedToEndpoint() {
    final Endpoint from = new Endpoint("10.0.0.1", 30303, Optional.of(4567));
    final long expiration = 123;
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeIntScalar(PingPacketData.VERSION);
    from.encodeStandalone(out);
    writeMalformedEndpoint(out);
    out.writeLongScalar(expiration);
    out.endList();

    Mockito.when(factory.createForDecode(Optional.of(from), Optional.empty(), expiration, null))
        .thenReturn(new PingPacketData(Optional.of(from), Optional.empty(), expiration, null));

    final PingPacketData result = reader.readFrom(new BytesValueRLPInput(out.encoded(), false));

    Assertions.assertEquals(Optional.of(from), result.getFrom());
    Assertions.assertTrue(result.getMaybeTo().isEmpty());
  }

  @Test
  public void ignoresInvalidEnrSequenceAndAdditionalElements() {
    final Endpoint from = new Endpoint("10.0.0.1", 30303, Optional.of(4567));
    final Endpoint to = new Endpoint("10.0.0.2", 30303, Optional.of(8910));
    final long expiration = 123;
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeIntScalar(PingPacketData.VERSION);
    from.encodeStandalone(out);
    to.encodeStandalone(out);
    out.writeLongScalar(expiration);
    out.writeBytes(malformedField());
    out.writeBytes(malformedField());
    out.endList();

    Mockito.when(factory.createForDecode(Optional.of(from), Optional.of(to), expiration, null))
        .thenReturn(new PingPacketData(Optional.of(from), to, expiration, null));

    final PingPacketData result = reader.readFrom(new BytesValueRLPInput(out.encoded(), false));

    Assertions.assertEquals(Optional.of(from), result.getFrom());
    Assertions.assertEquals(to, result.getTo());
    Assertions.assertTrue(result.getEnrSeq().isEmpty());
  }

  private static void writeMalformedEndpoint(final BytesValueRLPOutput out) {
    out.writeBytes(malformedField());
  }

  private static Bytes malformedField() {
    return Bytes.wrap(".,?%@)2:%-67-".getBytes(UTF_8));
  }
}
