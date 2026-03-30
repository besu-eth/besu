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
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.AbstractMessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;

import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.tuweni.bytes.Bytes;

public class TransactionsMessage extends AbstractMessageData {

  public static TransactionsMessage readFrom(final MessageData message) {
    if (message instanceof TransactionsMessage) {
      return (TransactionsMessage) message;
    }
    final int code = message.getCode();
    if (code != EthProtocolMessages.TRANSACTIONS) {
      throw new IllegalArgumentException(
          String.format("Message has code %d and thus is not a TransactionsMessage.", code));
    }
    return new TransactionsMessage(message.getData());
  }

  @VisibleForTesting
  public static TransactionsMessage create(final Iterable<Transaction> transactions) {
    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    tmp.startList();
    for (final Transaction transaction : transactions) {
      transaction.writeTo(tmp);
    }
    tmp.endList();
    return new TransactionsMessage(tmp.encoded());
  }

  private TransactionsMessage(final Bytes data) {
    super(data);
  }

  @Override
  public int getCode() {
    return EthProtocolMessages.TRANSACTIONS;
  }

  public List<Transaction> transactions() {
    return new BytesValueRLPInput(data, false).readList(Transaction::readFrom);
  }

  public static class SizeLimitedBuilder {
    private final int maxTransactionsMessageSize;
    private final BytesValueRLPOutput message = new BytesValueRLPOutput();
    private int estimatedMsgSize = RLP.MAX_PREFIX_SIZE;
    private boolean finalized = false;

    public SizeLimitedBuilder(final int maxTransactionsMessageSize) {
      this.maxTransactionsMessageSize = maxTransactionsMessageSize;
      message.startList();
    }

    public boolean add(final Transaction transaction) {
      final BytesValueRLPOutput encodedTransaction = new BytesValueRLPOutput();
      transaction.writeTo(encodedTransaction);
      final Bytes encodedBytes = encodedTransaction.encoded();
      if (estimatedMsgSize + encodedBytes.size() > maxTransactionsMessageSize) {
        return false;
      }
      message.writeRaw(encodedBytes);
      estimatedMsgSize += encodedBytes.size();
      return true;
    }

    public int getEstimatedMessageSize() {
      return estimatedMsgSize;
    }

    public TransactionsMessage build() {
      Preconditions.checkState(!finalized, "Cannot call build twice");
      finalized = true;
      message.endList();
      return new TransactionsMessage(message.encoded());
    }
  }
}
