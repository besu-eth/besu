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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

public class PooledTransactionsMessageTest {

  @Test
  public void roundTripPooledTransactionsMessage() {
    List<Transaction> tx =
        Arrays.asList(
            Transaction.builder()
                .type(TransactionType.FRONTIER)
                .nonce(42)
                .gasLimit(654321)
                .gasPrice(Wei.of(2))
                .value(Wei.of(1337))
                .payload(Bytes.EMPTY)
                .signAndBuild(SignatureAlgorithmFactory.getInstance().generateKeyPair()));
    final PooledTransactionsMessage msg = PooledTransactionsMessage.create(tx);
    assertThat(msg.getCode()).isEqualTo(EthProtocolMessages.POOLED_TRANSACTIONS);
    assertThat(msg.transactions(tx.size())).isEqualTo(tx);
  }

  @Test
  public void readFromMessageWithWrongCodeThrows() {
    final RawMessage rawMsg = new RawMessage(EthProtocolMessages.BLOCK_HEADERS, Bytes.of(0));

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> PooledTransactionsMessage.readFrom(rawMsg));
  }

  @Test
  public void rejectsMoreThanMaxTransactionsBeforeDecodingAnyOfThem() {
    final int maxTransactions = 3;
    // The entries are not transactions, so decoding any of them would fail with a different error
    final PooledTransactionsMessage message = junkPooledTransactionsMessage(maxTransactions + 1);

    assertThatThrownBy(() -> message.transactions(maxTransactions))
        .isInstanceOf(RLPException.class)
        .hasMessageContaining("exceeds the maximum permitted size");
  }

  @Test
  public void decodesEntriesWhenCountIsWithinMax() {
    // Control for the test above: within the bound the same junk entries are decoded, and fail
    // with a decoding error rather than the count check
    final int maxTransactions = 3;
    final PooledTransactionsMessage message = junkPooledTransactionsMessage(maxTransactions);

    assertThatThrownBy(() -> message.transactions(maxTransactions))
        .isInstanceOf(RLPException.class)
        .hasMessageNotContaining("exceeds the maximum permitted size");
  }

  @Test
  public void decodesAllTransactionsWhenCountEqualsMax() {
    final List<Transaction> transactions =
        new ArrayList<>(new BlockDataGenerator().transactions(3));
    final PooledTransactionsMessage message = PooledTransactionsMessage.create(transactions);

    assertThat(message.transactions(transactions.size())).isEqualTo(transactions);
  }

  @Test
  public void appliesMaxOnEveryCallNotOnlyTheFirst() {
    final List<Transaction> transactions =
        new ArrayList<>(new BlockDataGenerator().transactions(3));
    final PooledTransactionsMessage message = PooledTransactionsMessage.create(transactions);

    assertThat(message.transactions(transactions.size())).isEqualTo(transactions);
    assertThatThrownBy(() -> message.transactions(transactions.size() - 1))
        .isInstanceOf(RLPException.class)
        .hasMessageContaining("exceeds the maximum permitted size");
  }

  private static PooledTransactionsMessage junkPooledTransactionsMessage(final int entryCount) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    for (int i = 0; i < entryCount; i++) {
      out.writeBytes(Bytes.of(0x01));
    }
    out.endList();
    return PooledTransactionsMessage.readFrom(
        new RawMessage(EthProtocolMessages.POOLED_TRANSACTIONS, out.encoded()));
  }
}
