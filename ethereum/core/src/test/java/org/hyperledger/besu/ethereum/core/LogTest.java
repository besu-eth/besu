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
package org.hyperledger.besu.ethereum.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.LogTopic;
import org.hyperledger.besu.ethereum.rlp.RLP;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

public class LogTest {
  final BlockDataGenerator gen = new BlockDataGenerator();

  @Test
  public void toFromRlp() {
    final Log log = gen.log(2);
    final Log copy = Log.readFrom(RLP.input(RLP.encode(log::writeTo)));
    assertThat(copy).isEqualTo(log);
  }

  @Test
  public void toFromRlpCompacted() {
    final Log log = gen.log(2);
    final Log copy = Log.readFrom(RLP.input(RLP.encode(rlpOut -> log.writeTo(rlpOut, true))), true);
    assertThat(copy).isEqualTo(log);
  }

  @Test
  public void toFromRlpCompactedWithLeadingZeros() {
    final Bytes logData = bytesWithLeadingZeros(10, 100);
    final List<LogTopic> logTopics =
        List.of(
            LogTopic.of(Bytes32.wrap(bytesWithLeadingZeros(20, 32))),
            LogTopic.of(Bytes32.wrap(bytesWithLeadingZeros(30, 32))));
    final Log log = new Log(gen.address(), logData, logTopics);
    final Log copy = Log.readFrom(RLP.input(RLP.encode(rlpOut -> log.writeTo(rlpOut, true))), true);
    assertThat(copy).isEqualTo(log);
  }

  @Test
  public void readFrom_compactedTrue_withWireFormatTopics() {
    // Simulates a log stored in wire (non-compacted) format being read back with compacted=true.
    // The autodetect path in Log.readFrom should recognise raw bytes32 topics (not lists) and
    // fall through to readBytes32() rather than attempting to read [leadingZeros, shortData].
    final Log log = gen.log(2);
    final Log copy =
        Log.readFrom(RLP.input(RLP.encode(rlpOut -> log.writeTo(rlpOut, false))), true);
    assertThat(copy).isEqualTo(log);
  }

  @Test
  public void readFrom_compactedTrue_withWireFormatTopicsContainingLeadingZeros() {
    // Log topic and data with leading zeros to exercise the full bytes32 path, with compacted=true.
    // The autodetect path in Log.readFrom should recognise raw bytes32 topics (not lists) and
    // fall through to readBytes32() rather than attempting to read [leadingZeros, shortData].
    final Bytes logData = bytesWithLeadingZeros(10, 100);
    final List<LogTopic> logTopics =
        List.of(
            LogTopic.of(Bytes32.wrap(bytesWithLeadingZeros(20, 32))),
            LogTopic.of(Bytes32.wrap(bytesWithLeadingZeros(30, 32))));
    final Log log = new Log(gen.address(), logData, logTopics);
    final Log copy =
        Log.readFrom(RLP.input(RLP.encode(rlpOut -> log.writeTo(rlpOut, false))), true);
    assertThat(copy).isEqualTo(log);
  }

  private Bytes bytesWithLeadingZeros(final int noLeadingZeros, final int totalSize) {
    return Bytes.concatenate(
        Bytes.repeat((byte) 0, noLeadingZeros), gen.bytesValue(totalSize - noLeadingZeros));
  }
}
