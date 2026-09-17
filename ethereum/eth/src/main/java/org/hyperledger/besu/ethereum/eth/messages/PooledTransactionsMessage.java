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

import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionDecoder;
import org.hyperledger.besu.ethereum.core.encoding.TransactionEncoder;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.AbstractMessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;

public final class PooledTransactionsMessage extends AbstractMessageData {

  private static final int MESSAGE_CODE = EthProtocolMessages.POOLED_TRANSACTIONS;
  private List<Transaction> pooledTransactions;

  private PooledTransactionsMessage(final Bytes rlp) {
    super(rlp);
  }

  @Override
  public int getCode() {
    return MESSAGE_CODE;
  }

  public static PooledTransactionsMessage create(final List<Transaction> transactions) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.writeList(
        transactions,
        (transaction, rlpOutput) -> {
          TransactionEncoder.encodeRLP(transaction, rlpOutput, EncodingContext.POOLED_TRANSACTION);
        });
    return new PooledTransactionsMessage(out.encoded());
  }

  /**
   * Create a message with raw, already encoded data. No checks are performed to validate the
   * rlp-encoded data.
   *
   * @param data An rlp-encoded list of transactions
   * @return A new PooledTransactionsMessage
   */
  public static PooledTransactionsMessage createUnsafe(final Bytes data) {
    return new PooledTransactionsMessage(data);
  }

  public static PooledTransactionsMessage readFrom(final MessageData message) {
    if (message instanceof PooledTransactionsMessage pooledTransactionsMessage) {
      return pooledTransactionsMessage;
    }
    final int code = message.getCode();
    if (code != MESSAGE_CODE) {
      throw new IllegalArgumentException(
          String.format("Message has code %d and thus is not a PooledTransactionsMessage.", code));
    }

    return new PooledTransactionsMessage(message.getData());
  }

  public List<Transaction> transactions() {
    if (pooledTransactions == null) {
      final BytesValueRLPInput in = new BytesValueRLPInput(getData(), false);
      pooledTransactions =
          in.readList(
              input -> TransactionDecoder.decodeRLP(input, EncodingContext.POOLED_TRANSACTION));
    }
    return pooledTransactions;
  }
}
