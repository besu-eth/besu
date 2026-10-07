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
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import java.util.Optional;

import org.apache.tuweni.units.bigints.UInt64;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PingPacketDataRlpWriterTest {

  private PingPacketDataRlpWriter writer;

  @BeforeEach
  public void beforeTest() {
    writer = new PingPacketDataRlpWriter();
  }

  @Test
  public void testWriteTo() {
    Endpoint from = new Endpoint("10.0.0.1", 30303, Optional.of(4567));
    Endpoint to = new Endpoint("10.0.0.2", 30303, Optional.of(8910));
    long expiration = 123;
    UInt64 enrSeq = UInt64.valueOf(123456789);
    final PingPacketData pingPacketData =
        new PingPacketData(Optional.of(from), Optional.of(to), expiration, enrSeq);
    final BytesValueRLPOutput out = new BytesValueRLPOutput();

    writer.writeTo(pingPacketData, out);

    Assertions.assertEquals(
        "0xdf05cb840a00000182765f8211d7cb840a00000282765f8222ce7b84075bcd15",
        out.encoded().toHexString());
  }
}
