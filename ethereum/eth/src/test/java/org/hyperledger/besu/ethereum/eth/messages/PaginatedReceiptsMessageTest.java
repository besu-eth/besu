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

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncodingConfiguration;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public final class PaginatedReceiptsMessageTest {

  static List<Arguments> testDeserializeFromWireWithLastBlockIncomplete() {
    return List.of(Arguments.of(0, false), Arguments.of(1, true));
  }

  @ParameterizedTest
  @MethodSource("testDeserializeFromWireWithLastBlockIncomplete")
  public void testDeserializeFromWireWithLastBlockIncomplete(
      final int value, final boolean expected) {
    final BlockDataGenerator gen = new BlockDataGenerator(1);
    final BytesValueRLPOutput blockRlp = new BytesValueRLPOutput();
    blockRlp.startList();
    TransactionReceiptEncoder.writeTo(
        gen.receipt(),
        blockRlp,
        TransactionReceiptEncodingConfiguration.ETH69_RECEIPT_CONFIGURATION);
    blockRlp.endList();

    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.writeLongScalar(value);
    out.startList();
    out.writeRaw(blockRlp.encoded());
    out.endList();

    final PaginatedReceiptsMessage decoded =
        PaginatedReceiptsMessage.readFrom(
            new RawMessage(EthProtocolMessages.RECEIPTS, out.encoded()));
    assertThat(decoded.lastBlockIncomplete()).isEqualTo(expected);
  }

  @Test
  public void testCreateUnsafePreservesLastBlockIncomplete() {
    final BlockDataGenerator gen = new BlockDataGenerator(1);
    final List<List<TransactionReceipt>> receipts = List.of(List.of(gen.receipt()));

    final BytesValueRLPOutput blockReceipts = new BytesValueRLPOutput();
    blockReceipts.startList();
    receipts
        .getFirst()
        .forEach(
            r ->
                TransactionReceiptEncoder.writeTo(
                    r,
                    blockReceipts,
                    TransactionReceiptEncodingConfiguration.DEFAULT_NETWORK_CONFIGURATION));
    blockReceipts.endList();

    final BytesValueRLPOutput messageData = new BytesValueRLPOutput();
    messageData.writeLongScalar(0);
    messageData.startList();
    messageData.writeRaw(blockReceipts.encoded());
    messageData.endList();

    // createUnsafe stores the flag directly without re-parsing
    final PaginatedReceiptsMessage messageComplete =
        PaginatedReceiptsMessage.createUnsafe(messageData.encoded(), false);
    assertThat(messageComplete.lastBlockIncomplete()).isFalse();

    final PaginatedReceiptsMessage messageIncomplete =
        PaginatedReceiptsMessage.createUnsafe(messageData.encoded(), true);
    assertThat(messageIncomplete.lastBlockIncomplete()).isTrue();
  }

  @Test
  public void testMinimalEncoding() {
    // Test minimal encoding without actual receipts to isolate the flag logic
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.writeLongScalar(1); // Flag = true
    out.startList(); // Empty list of blocks
    out.endList();

    final PaginatedReceiptsMessage message =
        PaginatedReceiptsMessage.readFrom(
            new RawMessage(EthProtocolMessages.RECEIPTS, out.encoded()));
    assertThat(message.lastBlockIncomplete()).isTrue();
  }
}
