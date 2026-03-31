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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.manager.MockPeerConnection;
import org.hyperledger.besu.ethereum.eth.messages.EthProtocolMessages;
import org.hyperledger.besu.ethereum.eth.messages.NewPooledTransactionHashesMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class NewPooledTransactionHashesMessageSenderTest {
  private final EthPeers ethPeers = mock(EthPeers.class);
  private final EthScheduler ethScheduler = new DeterministicEthScheduler();

  private final EthPeer peer1 = mock(EthPeer.class);
  private final EthPeer peer2 = mock(EthPeer.class);

  private final BlockDataGenerator generator = new BlockDataGenerator();
  private final Transaction transaction1 = generator.transaction();
  private final Transaction transaction2 = generator.transaction();
  private final Transaction transaction3 = generator.transaction();

  private PeerTransactionTracker transactionTracker;
  private NewPooledTransactionHashesMessageSender messageSender;

  @BeforeEach
  public void setUp() {
    when(ethPeers.getMaxPeers()).thenReturn(25);
    when(peer1.isDisconnected()).thenReturn(false);
    when(peer2.isDisconnected()).thenReturn(false);

    transactionTracker =
        new PeerTransactionTracker(TransactionPoolConfiguration.DEFAULT, ethPeers, ethScheduler);
    messageSender = new NewPooledTransactionHashesMessageSender(transactionTracker);

    transactionTracker.onPeerConnected(peer1);
    transactionTracker.onPeerConnected(peer2);

    when(peer1.getConnection())
        .thenReturn(new MockPeerConnection(Set.of(EthProtocol.ETH68), (cap, msg, conn) -> {}));
    when(peer2.getConnection())
        .thenReturn(new MockPeerConnection(Set.of(EthProtocol.ETH68), (cap, msg, conn) -> {}));
  }

  @Test
  public void shouldSendPendingTransactionsToEachPeer() throws Exception {

    transactionTracker.addToPeerAnnouncementsSendQueue(peer1, List.of(transaction1));
    transactionTracker.addToPeerAnnouncementsSendQueue(peer1, List.of(transaction2));
    transactionTracker.addToPeerAnnouncementsSendQueue(peer2, List.of(transaction3));

    List.of(peer1, peer2).forEach(messageSender::sendTransactionAnnouncementsToPeer);

    verify(peer1).send(transactionsMessageContaining(transaction1, transaction2));
    verify(peer2).send(transactionsMessageContaining(transaction3));
    verify(peer1).getConnection();
    verify(peer2).getConnection();
    verifyNoMoreInteractions(ignoreStubs(peer1, peer2));
  }

  @Test
  public void shouldSendTransactionsInBatchesWithLimit() throws Exception {
    final Set<Transaction> transactions = new HashSet<>(generator.transactions(6000));

    transactions.forEach(
        transaction ->
            transactionTracker.addToPeerAnnouncementsSendQueue(peer1, List.of(transaction)));

    messageSender.sendTransactionAnnouncementsToPeer(peer1);
    final ArgumentCaptor<MessageData> messageDataArgumentCaptor =
        ArgumentCaptor.forClass(MessageData.class);
    verify(peer1, times(2)).send(messageDataArgumentCaptor.capture());

    final List<MessageData> sentMessages = messageDataArgumentCaptor.getAllValues();

    assertThat(sentMessages)
        .hasSize(2)
        .allMatch(
            message -> message.getCode() == EthProtocolMessages.NEW_POOLED_TRANSACTION_HASHES);
    final Set<TransactionAnnouncement> firstBatch =
        getAnnouncementsFromMessage(sentMessages.get(0));
    final Set<TransactionAnnouncement> secondBatch =
        getAnnouncementsFromMessage(sentMessages.get(1));

    final int expectedFirstBatchSize = 4096, expectedSecondBatchSize = 1904;
    assertThat(firstBatch).hasSize(expectedFirstBatchSize);
    assertThat(secondBatch).hasSize(expectedSecondBatchSize);

    assertThat(Sets.union(firstBatch, secondBatch))
        .containsExactlyInAnyOrderElementsOf(
            transactions.stream().map(TransactionAnnouncement::new).toList());
  }

  @Test
  public void shouldNotSendAnnouncementAlreadySeenByPeer() throws Exception {
    // Mark transaction1 as already received by peer1 (e.g. via a full-tx send)
    transactionTracker.markTransactionsAsSeen(peer1, List.of(transaction1.getHash()));

    transactionTracker.addToPeerAnnouncementsSendQueue(peer1, List.of(transaction1, transaction2));

    messageSender.sendTransactionAnnouncementsToPeer(peer1);

    // transaction1 was already seen — only transaction2 is announced
    verify(peer1).send(transactionsMessageContaining(transaction2));
    verify(peer1).getConnection();
    verifyNoMoreInteractions(ignoreStubs(peer1));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldStopSendingOnPeerNotConnected() throws Exception {
    // Exactly MAX_TRANSACTIONS_HASHES + 1 transactions: first full batch triggers a send that
    // fails, causing the loop to break so the second tx is never sent.
    final int batchSize = 4096; // MAX_TRANSACTIONS_HASHES
    final Set<Transaction> transactions = new HashSet<>(generator.transactions(batchSize + 1));
    transactions.forEach(
        tx -> transactionTracker.addToPeerAnnouncementsSendQueue(peer1, List.of(tx)));

    // peer1.send() always throws PeerNotConnected
    doThrow(new PeerConnection.PeerNotConnected("peer disconnected")).when(peer1).send(any());

    messageSender.sendTransactionAnnouncementsToPeer(peer1);

    // The first full batch triggered exactly one send call, then the loop broke —
    // the single remaining tx was NOT sent.
    verify(peer1, times(1)).send(any());
    verify(peer1).getConnection();
    verifyNoMoreInteractions(ignoreStubs(peer1));
  }

  @Test
  public void shouldNotDropAnnouncementsAtExactBatchBoundary() throws Exception {
    // exactly MAX_TRANSACTIONS_HASHES + 1 must produce two messages (4096 + 1)
    final int batchSize = 4096; // MAX_TRANSACTIONS_HASHES
    final Set<Transaction> transactions = new HashSet<>(generator.transactions(batchSize + 1));
    transactions.forEach(
        tx -> transactionTracker.addToPeerAnnouncementsSendQueue(peer1, List.of(tx)));

    messageSender.sendTransactionAnnouncementsToPeer(peer1);

    final ArgumentCaptor<MessageData> captor = ArgumentCaptor.forClass(MessageData.class);
    verify(peer1, times(2)).send(captor.capture());
    verify(peer1).getConnection();
    verifyNoMoreInteractions(ignoreStubs(peer1));

    final List<MessageData> messages = captor.getAllValues();
    assertThat(getAnnouncementsFromMessage(messages.get(0))).hasSize(batchSize);
    assertThat(getAnnouncementsFromMessage(messages.get(1))).hasSize(1);
    assertThat(
            Sets.union(
                getAnnouncementsFromMessage(messages.get(0)),
                getAnnouncementsFromMessage(messages.get(1))))
        .hasSize(batchSize + 1);
  }

  private MessageData transactionsMessageContaining(final Transaction... transactions) {
    return argThat(
        message -> {
          final Set<TransactionAnnouncement> actualSentAnnouncements =
              getAnnouncementsFromMessage(message);
          final Set<TransactionAnnouncement> expectedAnnouncements =
              Arrays.stream(transactions)
                  .map(TransactionAnnouncement::new)
                  .collect(Collectors.toSet());
          return message.getCode() == EthProtocolMessages.NEW_POOLED_TRANSACTION_HASHES
              && actualSentAnnouncements.equals(expectedAnnouncements);
        });
  }

  private Set<TransactionAnnouncement> getAnnouncementsFromMessage(final MessageData message) {
    final NewPooledTransactionHashesMessage transactionsMessage =
        NewPooledTransactionHashesMessage.readFrom(message, EthProtocol.LATEST);
    return newHashSet(transactionsMessage.pendingTransactionAnnouncements());
  }
}
