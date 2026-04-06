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

import org.hyperledger.besu.ethereum.core.SyncTransactionReceipt;
import org.hyperledger.besu.ethereum.core.encoding.receipt.SyncTransactionReceiptDecoder;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.AbstractMessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;

public class ReceiptsMessage extends AbstractMessageData {
  /**
   * This default decoder instance is used for performance reasons to avoid creating a new decoder
   * for every ReceiptsMessage
   */
  private static final SyncTransactionReceiptDecoder DEFAULT_SYNC_TRANSACTION_RECEIPT_DECODER =
      new SyncTransactionReceiptDecoder();

  private List<List<SyncTransactionReceipt>> syncReceiptsByBlock;

  protected ReceiptsMessage(final Bytes data) {
    super(data);
  }

  public static ReceiptsMessage readFrom(final MessageData message) {
    if (message instanceof ReceiptsMessage) {
      return (ReceiptsMessage) message;
    }
    final int code = message.getCode();
    if (code != EthProtocolMessages.RECEIPTS) {
      throw new IllegalArgumentException(
          String.format("Message has code %d and thus is not a ReceiptsMessage.", code));
    }
    return new ReceiptsMessage(message.getData());
  }

  /**
   * Create a message with raw, already encoded data. No checks are performed to validate the
   * rlp-encoded data.
   *
   * @param data An rlp-encoded list of sets of receipts
   * @return A new ReceiptsMessage
   */
  public static ReceiptsMessage createUnsafe(final Bytes data) {
    return new ReceiptsMessage(data);
  }

  @Override
  public int getCode() {
    return EthProtocolMessages.RECEIPTS;
  }

  public List<List<SyncTransactionReceipt>> syncReceipts() {
    if (syncReceiptsByBlock == null) {
      deserialize();
    }
    return syncReceiptsByBlock;
  }

  protected void deserialize() {
    final RLPInput input = new BytesValueRLPInput(data, false);
    deserializeReceiptLists(input);
  }

  protected void deserializeReceiptLists(final RLPInput input) {
    final List<List<SyncTransactionReceipt>> receiptsByBlock = new ArrayList<>(input.enterList());
    while (input.nextIsList()) {
      final int setSize = input.enterList();
      final List<SyncTransactionReceipt> receiptSet = new ArrayList<>(setSize);
      for (int i = 0; i < setSize; i++) {
        receiptSet.add(
            DEFAULT_SYNC_TRANSACTION_RECEIPT_DECODER.decode(
                input.nextIsList() ? input.currentListAsBytes() : input.readBytes()));
      }
      input.leaveList();
      receiptsByBlock.add(receiptSet);
    }
    input.leaveList();
    syncReceiptsByBlock = receiptsByBlock;
  }
}
