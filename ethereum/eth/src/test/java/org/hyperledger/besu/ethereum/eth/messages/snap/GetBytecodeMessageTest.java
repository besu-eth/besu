/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.eth.messages.snap;

import org.hyperledger.besu.ethereum.p2p.rlpx.wire.AbstractSnapMessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes32;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public final class GetBytecodeMessageTest {

  @Test
  public void roundTripTest() {

    final List<Bytes32> hashes = new ArrayList<>();
    final int hashCount = 20;
    for (int i = 0; i < hashCount; ++i) {
      hashes.add(Bytes32.random());
    }

    // Perform round-trip transformation
    final MessageData initialMessage = GetByteCodesMessage.create(hashes);
    final MessageData raw = new RawMessage(SnapV1.GET_BYTECODES, initialMessage.getData());

    final GetByteCodesMessage message = GetByteCodesMessage.readFrom(raw);

    // check match originals.
    Assertions.assertThat(message.codeHashes(false)).containsExactlyElementsOf(hashes);
    Assertions.assertThat(message.responseBytes(false))
        .isEqualTo(AbstractSnapMessageData.SIZE_REQUEST);
  }

  @Test
  public void wrapRoundTripTest() {
    final List<Bytes32> hashes = new ArrayList<>();
    for (int i = 0; i < 20; ++i) {
      hashes.add(Bytes32.random());
    }

    final GetByteCodesMessage initialMessage = GetByteCodesMessage.create(hashes);
    final MessageData wrapped = initialMessage.wrapMessageData(BigInteger.valueOf(42));
    final MessageData raw = new RawMessage(SnapV1.GET_BYTECODES, wrapped.getData());

    final GetByteCodesMessage message = GetByteCodesMessage.readFrom(raw);

    Assertions.assertThat(message.codeHashes(true)).containsExactlyElementsOf(hashes);
    Assertions.assertThat(message.responseBytes(true))
        .isEqualTo(AbstractSnapMessageData.SIZE_REQUEST);
  }

  @Test
  public void lazyIterationStaysIndependentAcrossCalls() {
    final List<Bytes32> hashes = new ArrayList<>();
    for (int i = 0; i < 5; ++i) {
      hashes.add(Bytes32.random());
    }

    final GetByteCodesMessage message = GetByteCodesMessage.create(hashes);

    // Each call to codeHashes() must yield a fresh, independent iterator over the same data.
    Assertions.assertThat(message.codeHashes(false)).containsExactlyElementsOf(hashes);
    Assertions.assertThat(message.codeHashes(false)).containsExactlyElementsOf(hashes);
    // responseBytes() must be readable regardless of whether the hashes were iterated.
    Assertions.assertThat(message.responseBytes(false))
        .isEqualTo(AbstractSnapMessageData.SIZE_REQUEST);
  }
}
