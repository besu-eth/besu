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
package org.hyperledger.besu.ethereum.eth.messages;

import static org.hyperledger.besu.ethereum.eth.core.Utils.serializeReceiptsList;

import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncodingConfiguration;
import org.hyperledger.besu.ethereum.eth.core.Utils;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public final class ReceiptsMessageTest {
  @Test
  public void testReceiptsMessageEth68() {
    roundTripTest(TransactionReceiptEncodingConfiguration.DEFAULT_NETWORK_CONFIGURATION);
  }

  @Test
  public void testReceiptsMessageEth69() {
    roundTripTest(TransactionReceiptEncodingConfiguration.ETH69_RECEIPT_CONFIGURATION);
  }

  public void roundTripTest(final TransactionReceiptEncodingConfiguration encodingConfiguration) {
    // Generate some data
    final BlockDataGenerator gen = new BlockDataGenerator(1);
    final List<List<TransactionReceipt>> receipts = new ArrayList<>();
    final int dataCount = 20;
    final int receiptsPerSet = 3;
    for (int i = 0; i < dataCount; ++i) {
      final List<TransactionReceipt> receiptSet = new ArrayList<>();
      for (int j = 0; j < receiptsPerSet; j++) {
        receiptSet.add(gen.receipt());
      }
      receipts.add(receiptSet);
    }

    // Perform round-trip transformation
    // Create specific message, copy it to a generic message, then read back into a specific format
    final MessageData initialMessage =
        ReceiptsMessage.createUnsafe(serializeReceiptsList(receipts, encodingConfiguration));
    final MessageData raw = new RawMessage(EthProtocolMessages.RECEIPTS, initialMessage.getData());
    final ReceiptsMessage message = ReceiptsMessage.readFrom(raw);

    // Read data back out after round trip and check they match originals.
    final Iterator<List<TransactionReceipt>> readData =
        message.syncReceipts().stream().map(Utils::syncReceiptsToReceipts).iterator();
    for (int i = 0; i < dataCount; ++i) {
      Assertions.assertThat(readData.next()).isEqualTo(receipts.get(i));
    }
    Assertions.assertThat(readData.hasNext()).isFalse();
  }
}
