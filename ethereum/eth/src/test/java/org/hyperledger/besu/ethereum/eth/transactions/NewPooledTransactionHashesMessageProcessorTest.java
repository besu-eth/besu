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
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.messages.NewPooledTransactionHashesMessage;
import org.hyperledger.besu.ethereum.eth.transactions.NewPooledTransactionHashesMessageProcessor.FetcherCreatorTask;
import org.hyperledger.besu.metrics.StubMetricsSystem;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class NewPooledTransactionHashesMessageProcessorTest {

  @Mock private TransactionPool transactionPool;

  @Mock(answer = RETURNS_DEEP_STUBS)
  private TransactionPoolConfiguration transactionPoolConfiguration;

  @Mock private PeerTransactionTracker transactionTracker;
  @Mock private EthPeer peer1;
  @Mock private EthContext ethContext;
  @Mock private EthScheduler ethScheduler;

  private final BlockDataGenerator generator = new BlockDataGenerator();

  private final Transaction transaction1 = generator.transaction();
  private final Transaction transaction2 = generator.transaction();
  private final Transaction transaction3 = generator.transaction();
  private final List<Transaction> transactionList =
      List.of(transaction1, transaction2, transaction3);
  private NewPooledTransactionHashesMessageProcessor messageHandler;
  private StubMetricsSystem metricsSystem;

  @BeforeEach
  public void setup() {
    metricsSystem = new StubMetricsSystem();
    when(transactionPoolConfiguration.getUnstable().getEth65TrxAnnouncedBufferingPeriod())
        .thenReturn(Duration.ofMillis(500));
    messageHandler =
        new NewPooledTransactionHashesMessageProcessor(
            transactionTracker,
            transactionPool,
            transactionPoolConfiguration,
            ethContext,
            new TransactionPoolMetrics(metricsSystem),
            EthProtocolConfiguration.DEFAULT_MAX_TRANSACTIONS_MESSAGE_SIZE);
    when(ethContext.getScheduler()).thenReturn(ethScheduler);
  }

  @Test
  void shouldAddNewHashesToRequestQueue() {
    final List<TransactionAnnouncement> expectedAnnouncements =
        TransactionAnnouncement.create(transactionList);

    messageHandler.processNewPooledTransactionHashesMessage(
        peer1,
        NewPooledTransactionHashesMessage.create(transactionList, EthProtocol.LATEST),
        now(),
        ofMinutes(1));

    verify(transactionTracker).receivedAnnouncements(eq(peer1), eq(expectedAnnouncements));
  }

  @Test
  void shouldIgnoreExpiredMessage() {
    messageHandler.processNewPooledTransactionHashesMessage(
        peer1,
        NewPooledTransactionHashesMessage.create(transactionList, EthProtocol.LATEST),
        now().minus(ofMinutes(1)),
        ofMillis(1));
    verifyNoInteractions(transactionTracker);
    verifyNoInteractions(ethScheduler);
    assertThat(
            metricsSystem.getCounterValue(
                TransactionPoolMetrics.EXPIRED_MESSAGES_COUNTER_NAME,
                NewPooledTransactionHashesMessageProcessor.METRIC_LABEL))
        .isEqualTo(1);
  }

  @Test
  void shouldCountAlreadySeenAnnouncements() {
    // Simulate the tracker recognizing 2 of 3 announcements as already seen (returns 1 fresh)
    when(transactionTracker.receivedAnnouncements(eq(peer1), any()))
        .thenReturn(List.of(new TransactionAnnouncement(transaction1)));

    messageHandler.processNewPooledTransactionHashesMessage(
        peer1,
        NewPooledTransactionHashesMessage.create(transactionList, EthProtocol.LATEST),
        now(),
        ofMinutes(1));

    assertThat(
            metricsSystem.getCounterValue(
                "remote_transactions_already_seen_total",
                NewPooledTransactionHashesMessageProcessor.METRIC_LABEL))
        .isEqualTo(2);
  }

  @Test
  void shouldScheduleGetPooledTransactionsTaskWhenNewTransactionAddedFromPeerForTheFirstTime() {
    messageHandler.processNewPooledTransactionHashesMessage(
        peer1,
        NewPooledTransactionHashesMessage.create(
            List.of(transaction1, transaction2), EthProtocol.LATEST),
        now(),
        ofMinutes(1));

    verify(ethScheduler, times(1))
        .scheduleFutureTaskWithFixedDelay(
            any(FetcherCreatorTask.class), any(Duration.class), any(Duration.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldNotScheduleGetPooledTransactionsTaskTwice() {
    when(ethScheduler.scheduleFutureTaskWithFixedDelay(
            any(FetcherCreatorTask.class), any(Duration.class), any(Duration.class)))
        .thenReturn(mock(ScheduledFuture.class));

    messageHandler.processNewPooledTransactionHashesMessage(
        peer1,
        NewPooledTransactionHashesMessage.create(
            Collections.singletonList(transaction1), EthProtocol.LATEST),
        now(),
        ofMinutes(1));

    messageHandler.processNewPooledTransactionHashesMessage(
        peer1,
        NewPooledTransactionHashesMessage.create(
            Collections.singletonList(transaction2), EthProtocol.LATEST),
        now(),
        ofMinutes(1));

    verify(ethScheduler, times(1))
        .scheduleFutureTaskWithFixedDelay(
            any(FetcherCreatorTask.class), any(Duration.class), any(Duration.class));
  }
}
