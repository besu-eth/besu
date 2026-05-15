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
    final GetByteCodesMessage.CodeHashes codeHashes = message.codeHashes(false);
    Assertions.assertThat(codeHashes.hashes()).isEqualTo(hashes);
    Assertions.assertThat(codeHashes.responseBytes())
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

    final GetByteCodesMessage.CodeHashes codeHashes = message.codeHashes(true);
    Assertions.assertThat(codeHashes.hashes()).isEqualTo(hashes);
    Assertions.assertThat(codeHashes.responseBytes())
        .isEqualTo(AbstractSnapMessageData.SIZE_REQUEST);
  }
}
