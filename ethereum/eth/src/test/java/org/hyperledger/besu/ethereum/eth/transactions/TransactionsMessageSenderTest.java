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

import static com.google.common.collect.Sets.newHashSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.messages.EthProtocolMessages;
import org.hyperledger.besu.ethereum.eth.messages.TransactionsMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class TransactionsMessageSenderTest {
  private final EthPeers ethPeers = mock(EthPeers.class);
  private final EthScheduler ethScheduler = new DeterministicEthScheduler();

  private final EthPeer peer1 = mock(EthPeer.class);
  private final EthPeer peer2 = mock(EthPeer.class);

  private final BlockDataGenerator generator = new BlockDataGenerator();
  private final Transaction transaction1 = generator.transaction();
  private final Transaction transaction2 = generator.transaction();
  private final Transaction transaction3 = generator.transaction();

  private final PeerTransactionTracker transactionTracker =
      new PeerTransactionTracker(TransactionPoolConfiguration.DEFAULT, ethPeers, ethScheduler);
  private final TransactionsMessageSender messageSender =
      new TransactionsMessageSender(
          transactionTracker, EthProtocolConfiguration.DEFAULT.getMaxTransactionsMessageSize());

  @BeforeEach
  void setUp() {
    when(peer1.isDisconnected()).thenReturn(false);
    when(peer2.isDisconnected()).thenReturn(false);
    transactionTracker.onPeerConnected(peer1);
    transactionTracker.onPeerConnected(peer2);
  }

  @Test
  public void shouldSendTransactionsToEachPeer() throws Exception {
    transactionTracker.addToPeerSendQueue(peer1, List.of(transaction1));
    transactionTracker.addToPeerSendQueue(peer1, List.of(transaction2));
    transactionTracker.addToPeerSendQueue(peer2, List.of(transaction3));

    messageSender.sendTransactionsToPeer(peer1);
    messageSender.sendTransactionsToPeer(peer2);

    verify(peer1).send(transactionsMessageContaining(transaction1, transaction2));
    verify(peer2).send(transactionsMessageContaining(transaction3));
    verifyNoMoreInteractions(ignoreStubs(peer1, peer2));
  }

  @Test
  public void shouldSendTransactionsInBatchesWithLimit() throws Exception {
    final Set<Transaction> transactions = generator.transactions(6000);

    transactions.forEach(
        transaction -> transactionTracker.addToPeerSendQueue(peer1, List.of(transaction)));

    messageSender.sendTransactionsToPeer(peer1);

    final ArgumentCaptor<MessageData> messageDataArgumentCaptor =
        ArgumentCaptor.forClass(MessageData.class);
    verify(peer1, times(2)).send(messageDataArgumentCaptor.capture());

    final List<MessageData> sentMessages = messageDataArgumentCaptor.getAllValues();

    assertThat(sentMessages).hasSize(2);
    assertThat(sentMessages)
        .allMatch(message -> message.getCode() == EthProtocolMessages.TRANSACTIONS);
    final Set<Transaction> firstBatch = getTransactionsFromMessage(sentMessages.get(0));
    final Set<Transaction> secondBatch = getTransactionsFromMessage(sentMessages.get(1));

    assertThat(firstBatch).hasSizeGreaterThan(secondBatch.size());
    assertThat(Sets.union(firstBatch, secondBatch)).isEqualTo(transactions);
  }

  @Test
  public void shouldNotSendTransactionAlreadySeenByPeer() throws Exception {
    // Mark transaction2 as already seen by peer1 (e.g. received from that peer earlier)
    transactionTracker.markTransactionsAsSeen(peer1, List.of(transaction2.getHash()));

    transactionTracker.addToPeerSendQueue(peer1, List.of(transaction1, transaction2));
    transactionTracker.addToPeerSendQueue(peer2, List.of(transaction3));

    messageSender.sendTransactionsToPeer(peer1);
    messageSender.sendTransactionsToPeer(peer2);

    // peer1 receives only transaction1; transaction2 filtered out because already seen
    verify(peer1).send(transactionsMessageContaining(transaction1));
    verify(peer2).send(transactionsMessageContaining(transaction3));
    verifyNoMoreInteractions(ignoreStubs(peer1, peer2));
  }

  @Test
  public void shouldNotSendTransactionAlreadyAnnouncedToPeer() throws Exception {
    // Simulate transaction1 having been announced to peer1 already
    transactionTracker.markAnnouncementsAsSeenByTransaction(peer1, List.of(transaction1));

    // Queue transaction1 for full broadcast to the same peer
    transactionTracker.addToPeerSendQueue(peer1, List.of(transaction1));

    messageSender.sendTransactionsToPeer(peer1);

    // peer1 must not receive the full transaction — already announced
    verify(peer1, never()).send(any());
  }

  @Test
  public void shouldDropOversizedTransactionAndContinueSending() throws Exception {
    // A message size of 1 byte makes every transaction "too large to fit"
    final TransactionsMessageSender tinySender =
        new TransactionsMessageSender(transactionTracker, 1);

    transactionTracker.addToPeerSendQueue(peer1, List.of(transaction1, transaction2));
    transactionTracker.addToPeerSendQueue(peer2, List.of(transaction3));

    tinySender.sendTransactionsToPeer(peer1);
    tinySender.sendTransactionsToPeer(peer2);

    // Every transaction is too large — no message is ever sent to any peer
    verify(peer1, never()).send(any());
    verify(peer2, never()).send(any());
  }

  private MessageData transactionsMessageContaining(final Transaction... transactions) {
    return argThat(
        message -> {
          final Set<Transaction> actualSentTransactions = getTransactionsFromMessage(message);
          final Set<Transaction> expectedTransactions = newHashSet(transactions);
          return message.getCode() == EthProtocolMessages.TRANSACTIONS
              && actualSentTransactions.equals(expectedTransactions);
        });
  }

  private Set<Transaction> getTransactionsFromMessage(final MessageData message) {
    final TransactionsMessage transactionsMessage = TransactionsMessage.readFrom(message);
    return newHashSet(transactionsMessage.transactions());
  }
}
