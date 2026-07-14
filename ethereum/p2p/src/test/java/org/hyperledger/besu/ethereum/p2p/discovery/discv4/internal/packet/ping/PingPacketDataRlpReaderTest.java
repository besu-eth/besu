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

import org.hyperledger.besu.ethereum.p2p.discovery.discv4.Endpoint;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.validation.EndpointValidator;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.validation.ExpiryValidator;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import java.math.BigInteger;
import java.net.InetAddress;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt64;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PingPacketDataRlpReaderTest {

  private final Clock clock = Clock.fixed(Instant.ofEpochSecond(123), ZoneId.of("UTC"));
  private PingPacketDataRlpReader reader;

  @BeforeEach
  public void beforeTest() {
    reader =
        new PingPacketDataRlpReader(
            new PingPacketDataFactory(new EndpointValidator(), new ExpiryValidator(clock), clock));
  }

  @Test
  public void testReadFrom() {
    String pingHexData = "0xdf05cb840a00000182765f8211d7cb840a00000282765f8222ce7b84075bcd15";

    Endpoint from = new Endpoint("10.0.0.1", 30303, Optional.of(4567));
    Endpoint to = new Endpoint("10.0.0.2", 30303, Optional.of(8910));
    long expiration = 123;
    UInt64 enrSeq = UInt64.valueOf(123456789);

    PingPacketData result =
        reader.readFrom(new BytesValueRLPInput(Bytes.fromHexString(pingHexData), false));

    Assertions.assertNotNull(result);
    Assertions.assertTrue(result.getFrom().isPresent());
    Assertions.assertEquals(from, result.getFrom().get());
    Assertions.assertTrue(result.getTo().isPresent());
    Assertions.assertEquals(to, result.getTo().get());
    Assertions.assertEquals(expiration, result.getExpiration());
    Assertions.assertTrue(result.getEnrSeq().isPresent());
    Assertions.assertEquals(enrSeq, result.getEnrSeq().get());
  }

  @Test
  public void testReadFrom_malformedToEndpointIsIgnoredNotFatal() {
    // "to" endpoint list has 1 field instead of the required 2 or 3, so decoding it throws
    // PeerDiscoveryPacketDecodingException (not IllegalPortException). Per EIP-8, a malformed
    // "to" field must not prevent processing of the rest of the PING packet.
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeBigIntegerScalar(BigInteger.valueOf(4));
    out.startList();
    out.writeIntScalar(1);
    out.endList();
    out.writeLongScalar(123L);
    out.endList();

    final PingPacketData result = reader.readFrom(new BytesValueRLPInput(out.encoded(), false));

    Assertions.assertNotNull(result);
    Assertions.assertTrue(result.getFrom().isEmpty());
    Assertions.assertTrue(result.getTo().isEmpty());
    Assertions.assertEquals(123L, result.getExpiration());
  }

  @Test
  public void testReadFrom_malformedToEndpointWithInvalidPort_isIgnoredNotFatal() throws Exception {
    // "to" endpoint has a valid field count but an out-of-range port, so decoding it throws
    // IllegalPortException. Per EIP-8, a malformed "to" field must not prevent processing of the
    // rest of the PING packet.
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeBigIntegerScalar(BigInteger.valueOf(4));
    out.startList();
    out.writeInetAddress(InetAddress.getByName("10.0.0.2"));
    out.writeIntScalar(70000); // outside the valid 1-65535 range
    out.endList();
    out.writeLongScalar(123L);
    out.writeBigIntegerScalar(BigInteger.valueOf(123456789));
    out.endList();

    final PingPacketData result = reader.readFrom(new BytesValueRLPInput(out.encoded(), false));

    Assertions.assertNotNull(result);
    Assertions.assertTrue(result.getFrom().isEmpty());
    Assertions.assertTrue(result.getTo().isEmpty());
    Assertions.assertEquals(123L, result.getExpiration());
    Assertions.assertTrue(result.getEnrSeq().isPresent());
    Assertions.assertEquals(UInt64.valueOf(123456789), result.getEnrSeq().get());
  }

  @Test
  public void testReadFrom_malformedToEndpointWithInvalidAddressBytes_isIgnoredNotFatal() {
    // "to" endpoint's address field is 5 raw bytes - neither a 4-byte IPv4 nor 16-byte IPv6
    // address - so decoding it throws RLPException. Per EIP-8, a malformed "to" field must not
    // prevent processing of the rest of the PING packet.
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeBigIntegerScalar(BigInteger.valueOf(4));
    out.startList();
    out.writeBytes(Bytes.fromHexString("0x0102030405"));
    out.writeIntScalar(30303);
    out.endList();
    out.writeLongScalar(123L);
    out.writeBigIntegerScalar(BigInteger.valueOf(123456789));
    out.endList();

    final PingPacketData result = reader.readFrom(new BytesValueRLPInput(out.encoded(), false));

    Assertions.assertNotNull(result);
    Assertions.assertTrue(result.getFrom().isEmpty());
    Assertions.assertTrue(result.getTo().isEmpty());
    Assertions.assertEquals(123L, result.getExpiration());
    Assertions.assertTrue(result.getEnrSeq().isPresent());
    Assertions.assertEquals(UInt64.valueOf(123456789), result.getEnrSeq().get());
  }

  @Test
  public void testReadFrom_expiredPingIsRejected() {
    // Same payload as testReadFrom, but expiration (123) is before the fixed clock's "now" (200),
    // so this must be rejected rather than reaching the controller.
    final Clock expiredClock = Clock.fixed(Instant.ofEpochSecond(200), ZoneId.of("UTC"));
    reader =
        new PingPacketDataRlpReader(
            new PingPacketDataFactory(
                new EndpointValidator(), new ExpiryValidator(expiredClock), expiredClock));
    String pingHexData = "0xdf05cb840a00000182765f8211d7cb840a00000282765f8222ce7b84075bcd15";

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> reader.readFrom(new BytesValueRLPInput(Bytes.fromHexString(pingHexData), false)));
  }
}
