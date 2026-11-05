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
import org.hyperledger.besu.ethereum.core.CellsOnlyBlobTransactionFixture;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.kzg.CellMask;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.EthProtocolVersion;
import org.hyperledger.besu.ethereum.eth.core.transactions.TransactionAnnouncements;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.manager.MockPeerConnection;
import org.hyperledger.besu.ethereum.eth.messages.EthProtocolMessages;
import org.hyperledger.besu.ethereum.eth.messages.NewPooledTransactionHashesMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class NewPooledTransactionHashesMessageSenderTest {
  private final EthPeers ethPeers = mock(EthPeers.class);
  private final EthScheduler ethScheduler = new DeterministicEthScheduler();

  private final EthPeer peer1 = mock(EthPeer.class);
  private final EthPeer peer2 = mock(EthPeer.class);

  private final BlockDataGenerator generator = new BlockDataGenerator();
  private final CellsOnlyBlobTransactionFixture cellsOnlyFixture =
      new CellsOnlyBlobTransactionFixture();
  private final Transaction transaction1 = generator.transaction();
  private final Transaction transaction2 = generator.transaction();
  private final Transaction transaction3 = generator.transaction();

  private final EthProtocolConfiguration ethProtocolConfiguration =
      EthProtocolConfiguration.DEFAULT;
  private final int maxAnnouncementsPerMessage =
      ethProtocolConfiguration.getMaxAnnouncementsPerMessage();
  private PeerTransactionTracker transactionTracker;
  private NewPooledTransactionHashesMessageSender messageSender;

  @BeforeEach
  public void setUp() {
    when(ethPeers.getMaxPeers()).thenReturn(25);
    when(peer1.isDisconnected()).thenReturn(false);
    when(peer2.isDisconnected()).thenReturn(false);

    transactionTracker =
        new PeerTransactionTracker(TransactionPoolConfiguration.DEFAULT, ethPeers, ethScheduler);
    messageSender =
        new NewPooledTransactionHashesMessageSender(
            transactionTracker, ethProtocolConfiguration.getMaxAnnouncementsPerMessage());

    transactionTracker.onPeerConnected(peer1);
    transactionTracker.onPeerConnected(peer2);

    when(peer1.getConnection())
        .thenReturn(
            new MockPeerConnection(
                Set.of(EthProtocolVersion.V68.getCapability()), (cap, msg, conn) -> {}));
    when(peer2.getConnection())
        .thenReturn(
            new MockPeerConnection(
                Set.of(EthProtocolVersion.V68.getCapability()), (cap, msg, conn) -> {}));
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
        .allMatch(message -> message.getCode() == EthProtocolMessages.NEW_POOLED_TRANSACTION_HASHES)
        // a full batch must be accepted by a peer using the same limits
        .allSatisfy(
            message ->
                assertThat(message.getSize())
                    .isLessThanOrEqualTo(
                        ethProtocolConfiguration.getMaxNewPooledTransactionHashesMessageSize()));
    final Set<TransactionAnnouncement> firstBatch =
        getAnnouncementsFromMessage(sentMessages.get(0));
    final Set<TransactionAnnouncement> secondBatch =
        getAnnouncementsFromMessage(sentMessages.get(1));

    final int expectedFirstBatchSize = 4096, expectedSecondBatchSize = 1904;
    assertThat(firstBatch).hasSize(expectedFirstBatchSize);
    assertThat(secondBatch).hasSize(expectedSecondBatchSize);

    assertThat(Sets.union(firstBatch, secondBatch))
        .containsExactlyInAnyOrderElementsOf(
            transactions.stream().map(TransactionAnnouncements::create).toList());
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

  @Test
  public void shouldNotAnnounceACellsOnlyTransactionToAPreEth72Peer() throws Exception {
    final Transaction cellsOnly = cellsOnlyFixture.create(1, CellMask.FULL);

    transactionTracker.addToPeerAnnouncementsSendQueue(peer1, List.of(cellsOnly, transaction1));

    messageSender.sendTransactionAnnouncementsToPeer(peer1);

    // peer1 speaks eth/68, where the announced size is that of the pooled form carrying the blob
    // payloads: a size we cannot compute, for a transaction we could not then serve.
    verify(peer1).send(transactionsMessageContaining(transaction1));
    verify(peer1).getConnection();
    verifyNoMoreInteractions(ignoreStubs(peer1));
  }

  @Test
  public void shouldAnnounceACellsOnlyTransactionToAnEth72Peer() throws Exception {
    when(peer2.getConnection())
        .thenReturn(
            new MockPeerConnection(
                Set.of(EthProtocolVersion.V72.getCapability()), (cap, msg, conn) -> {}));
    final Transaction cellsOnly = cellsOnlyFixture.create(1, CellMask.FULL);

    transactionTracker.addToPeerAnnouncementsSendQueue(peer2, List.of(cellsOnly));

    messageSender.sendTransactionAnnouncementsToPeer(peer2);

    // eth/72 elides the payloads, so holding only the cells is enough to serve it.
    final ArgumentCaptor<MessageData> captor = ArgumentCaptor.forClass(MessageData.class);
    verify(peer2).send(captor.capture());
    final List<TransactionAnnouncement> announcements =
        List.copyOf(getAnnouncementsFromMessage(captor.getValue()));
    assertThat(announcements).hasSize(1);
    assertThat(announcements.getFirst().hash()).isEqualTo(cellsOnly.getHash());
    assertThat(announcements.getFirst().cellMask()).isEqualTo(CellMask.FULL);
  }

  @Test
  public void shouldStartANewMessageWhenTheCellMaskChanges() throws Exception {
    when(peer2.getConnection())
        .thenReturn(
            new MockPeerConnection(
                Set.of(EthProtocolVersion.V72.getCapability()), (cap, msg, conn) -> {}));
    final CellMask partialMask =
        CellMask.fromBytes(Bytes.concatenate(Bytes.of((byte) 0x0f), Bytes.repeat((byte) 0, 15)));
    final Transaction fullyHeld = cellsOnlyFixture.create(1, CellMask.FULL);
    final Transaction partiallyHeld = cellsOnlyFixture.create(1, partialMask);

    transactionTracker.addToPeerAnnouncementsSendQueue(peer2, List.of(fullyHeld, partiallyHeld));

    messageSender.sendTransactionAnnouncementsToPeer(peer2);

    // An eth/72 message carries a single mask for every blob transaction in it, so two masks
    // cannot share a message.
    final ArgumentCaptor<MessageData> captor = ArgumentCaptor.forClass(MessageData.class);
    verify(peer2, times(2)).send(captor.capture());
    assertThat(captor.getAllValues())
        .map(this::getAnnouncementsFromMessage)
        .map(announcements -> announcements.iterator().next())
        .containsExactlyInAnyOrder(
            eth72AnnouncementOf(fullyHeld, CellMask.FULL),
            eth72AnnouncementOf(partiallyHeld, partialMask));
  }

  @Test
  public void shouldNotMixCellMasksWhenANonBlobTransactionComesBetweenThem() throws Exception {
    when(peer2.getConnection())
        .thenReturn(
            new MockPeerConnection(
                Set.of(EthProtocolVersion.V72.getCapability()), (cap, msg, conn) -> {}));
    final CellMask partialMask =
        CellMask.fromBytes(Bytes.concatenate(Bytes.of((byte) 0x0f), Bytes.repeat((byte) 0, 15)));
    final Transaction fullyHeld = cellsOnlyFixture.create(1, CellMask.FULL);
    final Transaction partiallyHeld = cellsOnlyFixture.create(1, partialMask);

    // An order the send queue can produce: it interleaves broadcasts, and being access ordered it
    // moves a re-queued transaction to the tail. A sender that tracked a single current mask would
    // lose the mask at the non-blob transaction and let the last one into the same message.
    transactionTracker.addToPeerAnnouncementsSendQueue(
        peer2, List.of(fullyHeld, transaction1, partiallyHeld));

    messageSender.sendTransactionAnnouncementsToPeer(peer2);

    // One message per availability, and nothing lost: handing the encoder two masks would have
    // thrown with all three already marked as seen by the peer, so they would never be announced.
    final ArgumentCaptor<MessageData> captor = ArgumentCaptor.forClass(MessageData.class);
    verify(peer2, times(3)).send(captor.capture());
    assertThat(captor.getAllValues())
        .map(this::getAnnouncementsFromMessage)
        .map(announcements -> announcements.iterator().next())
        .containsExactlyInAnyOrder(
            eth72AnnouncementOf(fullyHeld, CellMask.FULL),
            eth72AnnouncementOf(transaction1, null),
            eth72AnnouncementOf(partiallyHeld, partialMask));
  }

  private TransactionAnnouncement eth72AnnouncementOf(
      final Transaction transaction, final CellMask cellMask) {
    // Not TransactionAnnouncement(Transaction), which sizes the transaction by the pre eth/72
    // pooled form, the one a cells-only transaction has no encoding for.
    return new TransactionAnnouncement(
        transaction.getHash(),
        transaction.getType(),
        (long) transaction.getSizeForEth72Announcement(),
        cellMask);
  }

  private MessageData transactionsMessageContaining(final Transaction... transactions) {
    return argThat(
        message -> {
          final Set<TransactionAnnouncement> actualSentAnnouncements =
              getAnnouncementsFromMessage(message);
          final Set<TransactionAnnouncement> expectedAnnouncements =
              Arrays.stream(transactions)
                  .map(TransactionAnnouncements::create)
                  .collect(Collectors.toSet());
          return message.getCode() == EthProtocolMessages.NEW_POOLED_TRANSACTION_HASHES
              && actualSentAnnouncements.equals(expectedAnnouncements);
        });
  }

  private Set<TransactionAnnouncement> getAnnouncementsFromMessage(final MessageData message) {
    final NewPooledTransactionHashesMessage transactionsMessage =
        NewPooledTransactionHashesMessage.readFrom(message, EthProtocol.LATEST);
    return newHashSet(
        transactionsMessage.pendingTransactionAnnouncements(maxAnnouncementsPerMessage));
  }

  @Test
  public void shouldCapAnnouncementsPerMessageAtConfiguredMax() throws Exception {
    final int maxAnnouncements = 2;
    final NewPooledTransactionHashesMessageSender cappedSender =
        new NewPooledTransactionHashesMessageSender(transactionTracker, maxAnnouncements);
    final List<Transaction> transactions = new ArrayList<>(generator.transactions(5));
    transactions.forEach(
        transaction ->
            transactionTracker.addToPeerAnnouncementsSendQueue(peer1, List.of(transaction)));

    cappedSender.sendTransactionAnnouncementsToPeer(peer1);

    final ArgumentCaptor<MessageData> sent = ArgumentCaptor.forClass(MessageData.class);
    verify(peer1, times(3)).send(sent.capture());
    assertThat(sent.getAllValues())
        .extracting(message -> getAnnouncementsFromMessage(message).size())
        .containsExactly(2, 2, 1);
  }
}
