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
package org.hyperledger.besu.ethereum.eth.transactions;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofMinutes;
import static java.time.Instant.now;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.messages.TransactionsMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;
import org.hyperledger.besu.metrics.StubMetricsSystem;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TransactionsMessageProcessorTest {

  @Mock private TransactionPool transactionPool;
  @Mock private PeerTransactionTracker transactionTracker;
  @Mock private EthPeer peer1;

  private final BlockDataGenerator generator = new BlockDataGenerator();
  private final Transaction transaction1 = generator.transaction();
  private final Transaction transaction2 = generator.transaction();
  private final Transaction transaction3 = generator.transaction();

  private TransactionsMessageProcessor messageHandler;
  private StubMetricsSystem metricsSystem;

  @BeforeEach
  public void setup() {
    metricsSystem = new StubMetricsSystem();

    messageHandler =
        new TransactionsMessageProcessor(
            transactionTracker,
            transactionPool,
            new TransactionPoolMetrics(metricsSystem),
            EthProtocolConfiguration.DEFAULT_MAX_TRANSACTIONS_PER_MESSAGE);
  }

  @Test
  public void shouldMarkAllReceivedTransactionsAsSeen() {
    when(transactionTracker.receivedTransactions(
            peer1, asList(transaction1, transaction2, transaction3)))
        .thenReturn(asList(transaction1, transaction2, transaction3));

    messageHandler.processTransactionsMessage(
        peer1,
        TransactionsMessage.create(asList(transaction1, transaction2, transaction3)),
        now(),
        ofMinutes(1));

    verify(transactionTracker)
        .receivedTransactions(peer1, asList(transaction1, transaction2, transaction3));
  }

  @Test
  public void shouldAddReceivedTransactionsToTransactionPool() {
    when(transactionTracker.receivedTransactions(
            peer1, asList(transaction1, transaction2, transaction3)))
        .thenReturn(asList(transaction1, transaction2, transaction3));

    messageHandler.processTransactionsMessage(
        peer1,
        TransactionsMessage.create(asList(transaction1, transaction2, transaction3)),
        now(),
        ofMinutes(1));

    verify(transactionPool).addRemoteTransactions(asList(transaction1, transaction2, transaction3));
  }

  @Test
  public void shouldIgnoreExpiredMessage() {
    messageHandler.processTransactionsMessage(
        peer1,
        TransactionsMessage.create(asList(transaction1, transaction2, transaction3)),
        now().minus(ofMinutes(1)),
        ofMillis(1));
    verifyNoInteractions(transactionTracker);
    verifyNoInteractions(transactionPool);
    assertThat(
            metricsSystem.getCounterValue(
                TransactionPoolMetrics.EXPIRED_MESSAGES_COUNTER_NAME,
                TransactionsMessageProcessor.METRIC_LABEL))
        .isEqualTo(1);
  }

  @Test
  public void shouldAddOnlyFreshTransactionsToPool() {
    // Tracker deduplicates: only transaction1 is fresh; transaction2 and transaction3 already seen
    when(transactionTracker.receivedTransactions(
            peer1, asList(transaction1, transaction2, transaction3)))
        .thenReturn(singletonList(transaction1));

    messageHandler.processTransactionsMessage(
        peer1,
        TransactionsMessage.create(asList(transaction1, transaction2, transaction3)),
        now(),
        ofMinutes(1));

    verify(transactionPool).addRemoteTransactions(singletonList(transaction1));
    verifyNoMoreInteractions(transactionPool);
  }

  @Test
  public void shouldDisconnectPeerWhenTooManyTransactionsInMessage() {
    final int maxPerMessage = 2;
    final TransactionsMessageProcessor strictHandler =
        new TransactionsMessageProcessor(
            transactionTracker,
            transactionPool,
            new TransactionPoolMetrics(metricsSystem),
            maxPerMessage);

    final List<Transaction> tooMany = new ArrayList<>();
    for (int i = 0; i < maxPerMessage + 1; i++) {
      tooMany.add(generator.transaction());
    }

    strictHandler.processTransactionsMessage(
        peer1, TransactionsMessage.create(tooMany), now(), ofMinutes(1));

    verify(peer1).disconnect(DisconnectReason.BREACH_OF_PROTOCOL_MALFORMED_MESSAGE_RECEIVED);
    verifyNoInteractions(transactionPool);
    verifyNoInteractions(transactionTracker);
  }
}
