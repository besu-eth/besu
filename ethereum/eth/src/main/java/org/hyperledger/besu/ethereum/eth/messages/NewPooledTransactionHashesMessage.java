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

import static org.hyperledger.besu.ethereum.eth.encoding.TransactionAnnouncementDecoder.getDecoder;
import static org.hyperledger.besu.ethereum.eth.encoding.TransactionAnnouncementEncoder.getEncoder;

import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionAnnouncement;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.AbstractMessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.RLP;

import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;

public class NewPooledTransactionHashesMessage extends AbstractMessageData {
  private static final int MESSAGE_CODE = EthProtocolMessages.NEW_POOLED_TRANSACTION_HASHES;
  private List<TransactionAnnouncement> pendingTransactions;
  private final Capability capability;

  @VisibleForTesting
  public NewPooledTransactionHashesMessage(final Bytes rlp, final Capability capability) {
    super(rlp);
    this.capability = capability;
  }

  @Override
  public int getCode() {
    return MESSAGE_CODE;
  }

  public static NewPooledTransactionHashesMessage create(
      final List<Transaction> pendingTransactions, final Capability capability) {
    return new NewPooledTransactionHashesMessage(
        getEncoder(capability).encode(pendingTransactions), capability);
  }

  public static NewPooledTransactionHashesMessage readFrom(
      final MessageData message, final Capability capability) {

    if (message instanceof NewPooledTransactionHashesMessage) {
      return (NewPooledTransactionHashesMessage) message;
    }
    final int code = message.getCode();
    if (code != MESSAGE_CODE) {
      throw new IllegalArgumentException(
          String.format(
              "Message has code %d and thus is not a NewPooledTransactionHashesMessage.", code));
    }
    return new NewPooledTransactionHashesMessage(message.getData(), capability);
  }

  public List<TransactionAnnouncement> pendingTransactionAnnouncements() {
    if (pendingTransactions == null) {
      pendingTransactions = getDecoder(capability).decode(RLP.input(data));
    }
    return pendingTransactions;
  }
}
