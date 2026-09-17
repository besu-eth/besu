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
package org.hyperledger.besu.ethereum.eth.messages;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public final class GetPaginatedReceiptsMessageTest {

  @Test
  public void roundTripTestWithZeroIndex() {
    roundTripTest(0);
  }

  @Test
  public void roundTripTestWithNonZeroIndex() {
    roundTripTest(5);
  }

  private void roundTripTest(final int firstBlockReceiptIndex) {
    // Generate some hashes
    final BlockDataGenerator gen = new BlockDataGenerator(1);
    final List<Hash> hashes = new ArrayList<>();
    final int hashCount = 20;
    for (int i = 0; i < hashCount; ++i) {
      hashes.add(gen.hash());
    }

    // Perform round-trip transformation
    // Create message, copy it to a generic message, then read back into a GetPaginatedReceipts
    // message
    final MessageData initialMessage =
        GetPaginatedReceiptsMessage.create(hashes, firstBlockReceiptIndex);
    final MessageData raw =
        new RawMessage(EthProtocolMessages.GET_RECEIPTS, initialMessage.getData());
    final GetPaginatedReceiptsMessage message = GetPaginatedReceiptsMessage.readFrom(raw);

    // Read data back out after round trip and check they match originals.
    Assertions.assertThat(message.firstBlockReceiptIndex()).isEqualTo(firstBlockReceiptIndex);
    final Iterator<Hash> readData = message.blockHashes().iterator();
    for (int i = 0; i < hashCount; ++i) {
      Assertions.assertThat(readData.next()).isEqualTo(hashes.get(i));
    }
    Assertions.assertThat(readData.hasNext()).isFalse();
  }

  @Test
  public void readFromReturnsSameInstanceIfAlreadyCorrectType() {
    final BlockDataGenerator gen = new BlockDataGenerator(1);
    final GetPaginatedReceiptsMessage original =
        GetPaginatedReceiptsMessage.create(List.of(gen.hash()), 0);

    final GetPaginatedReceiptsMessage result = GetPaginatedReceiptsMessage.readFrom(original);

    Assertions.assertThat(result).isSameAs(original);
  }

  @Test
  public void readFromThrowsOnWrongMessageCode() {
    final BlockDataGenerator gen = new BlockDataGenerator(1);
    final MessageData wrongCodeMessage =
        new RawMessage(EthProtocolMessages.GET_BLOCK_HEADERS, gen.hash().getBytes());

    Assertions.assertThatThrownBy(() -> GetPaginatedReceiptsMessage.readFrom(wrongCodeMessage))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void getCodeReturnsGetReceipts() {
    final GetPaginatedReceiptsMessage message = GetPaginatedReceiptsMessage.create(List.of(), 0);

    Assertions.assertThat(message.getCode()).isEqualTo(EthProtocolMessages.GET_RECEIPTS);
  }
}
