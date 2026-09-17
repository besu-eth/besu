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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionAnnouncement;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

public class NewPooledTransactionHashesMessageTest {

  @Test
  public void roundTripNewPooledTransactionHashesMessage() {
    final List<Transaction> transactions = List.of(new BlockDataGenerator().transaction());
    final NewPooledTransactionHashesMessage msg =
        NewPooledTransactionHashesMessage.create(transactions, EthProtocol.LATEST);
    assertThat(msg.getCode()).isEqualTo(EthProtocolMessages.NEW_POOLED_TRANSACTION_HASHES);
    final List<TransactionAnnouncement> pendingAnnouncements =
        msg.pendingTransactionAnnouncements();
    assertThat(pendingAnnouncements)
        .isEqualTo(transactions.stream().map(TransactionAnnouncement::new).toList());
  }

  @Test
  public void readFromMessageWithWrongCodeThrows() {
    final RawMessage rawMsg = new RawMessage(EthProtocolMessages.BLOCK_HEADERS, Bytes.of(0));

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> NewPooledTransactionHashesMessage.readFrom(rawMsg, EthProtocol.LATEST));
  }
}
