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

import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

public class TransactionsMessageTest {
  private final BlockDataGenerator generator = new BlockDataGenerator();
  private final int maxTransactionsMessageSize =
      EthProtocolConfiguration.DEFAULT.getMaxTransactionsMessageSize();
  private final int safeTxPayloadSize = maxTransactionsMessageSize / 2 + 1;

  @Test
  public void transactionRoundTrip() {
    // Setup list of transactions
    final int txCount = 20;
    final BlockDataGenerator gen = new BlockDataGenerator(1);
    final List<Transaction> transactions = new ArrayList<>();
    for (int i = 0; i < txCount; ++i) {
      transactions.add(gen.transaction());
    }

    // Create TransactionsMessage
    final MessageData initialMessage = TransactionsMessage.create(transactions);
    // Read message into a generic RawMessage
    final MessageData raw =
        new RawMessage(EthProtocolMessages.TRANSACTIONS, initialMessage.getData());
    // Transform back to a TransactionsMessage from RawMessage
    final TransactionsMessage message = TransactionsMessage.readFrom(raw);

    // Check that transactions match original inputs after transformations
    assertThat(message.transactions()).isEqualTo(transactions);
  }

  @Test
  public void createLimited() {
    final Set<Transaction> transactions = generator.transactions(6000);
    final List<Transaction> remainingTransactions = new ArrayList<>(transactions);
    final Set<Transaction> includedTransactions = new HashSet<>();

    while (!remainingTransactions.isEmpty()) {
      final TransactionsMessage.SizeLimitedBuilder messageBuilder =
          new TransactionsMessage.SizeLimitedBuilder(maxTransactionsMessageSize);

      // Add transactions until the message is full
      remainingTransactions.removeIf(
          tx -> {
            if (messageBuilder.add(tx)) {
              includedTransactions.add(tx);
              return true;
            }
            return false;
          });

      assertThat(messageBuilder.build().getSize()).isLessThanOrEqualTo(maxTransactionsMessageSize);
    }

    assertThat(remainingTransactions.size()).isEqualTo(0);
    assertThat(includedTransactions)
        .containsExactlyInAnyOrder(transactions.toArray(new Transaction[] {}));
  }

  @Test
  public void createLimitedWithTransactionsJustUnderTheLimit() {
    final Set<Transaction> transactions =
        Stream.generate(() -> generator.transaction(Bytes.wrap(new byte[safeTxPayloadSize])))
            .limit(3)
            .collect(Collectors.toUnmodifiableSet());
    final List<Transaction> remainingTransactions = new ArrayList<>(transactions);
    final Set<Transaction> includedTransactions = new HashSet<>();

    while (!remainingTransactions.isEmpty()) {
      final TransactionsMessage.SizeLimitedBuilder messageBuilder =
          new TransactionsMessage.SizeLimitedBuilder(maxTransactionsMessageSize);

      // Add transactions until the message is full
      remainingTransactions.removeIf(
          tx -> {
            if (messageBuilder.add(tx)) {
              includedTransactions.add(tx);
              return true;
            }
            return false;
          });

      // Each transaction is just under half the limit, so only 1 should fit per message
      assertThat(
              (int)
                  transactions.stream()
                      .filter(
                          tx ->
                              includedTransactions.contains(tx)
                                  && !remainingTransactions.contains(tx))
                      .count())
          .isGreaterThanOrEqualTo(1);
    }
    assertThat(includedTransactions)
        .containsExactlyInAnyOrder(transactions.toArray(new Transaction[] {}));
  }

  @Test
  public void addTransactionReturnsFalseWhenMessageIsFull() {
    final Transaction largeTransaction =
        generator.transaction(Bytes.wrap(new byte[maxTransactionsMessageSize - 200]));
    final Transaction smallTransaction = generator.transaction();

    final TransactionsMessage.SizeLimitedBuilder messageBuilder =
        new TransactionsMessage.SizeLimitedBuilder(maxTransactionsMessageSize);

    assertThat(messageBuilder.add(largeTransaction)).isTrue();
    assertThat(messageBuilder.add(smallTransaction)).isFalse();
  }

  @Test
  public void build() {
    final Set<Transaction> transactions = generator.transactions(10);
    final TransactionsMessage.SizeLimitedBuilder messageBuilder =
        new TransactionsMessage.SizeLimitedBuilder(maxTransactionsMessageSize);

    transactions.forEach(messageBuilder::add);

    final TransactionsMessage transactionsMessage = messageBuilder.build();
    assertThat(transactionsMessage).isNotNull();
    assertThat(transactionsMessage.transactions()).hasSameSizeAs(transactions);
  }

  @Test
  public void estimatedMessageSizeIncreasesAsTransactionsAreAdded() {
    final List<Transaction> transactions = new ArrayList<>(generator.transactions(5));
    final TransactionsMessage.SizeLimitedBuilder messageBuilder =
        new TransactionsMessage.SizeLimitedBuilder(maxTransactionsMessageSize);

    int previousSize = messageBuilder.getEstimatedMessageSize();

    for (Transaction tx : transactions) {
      messageBuilder.add(tx);
      int currentSize = messageBuilder.getEstimatedMessageSize();
      assertThat(currentSize).isGreaterThan(previousSize);
      previousSize = currentSize;
    }
  }
}
