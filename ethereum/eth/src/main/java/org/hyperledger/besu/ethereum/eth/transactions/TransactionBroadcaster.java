/*
 * Copyright contributors to Hyperledger Besu.
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

import static org.hyperledger.besu.datatypes.TransactionType.BLOB;
import static org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction.toTransactionList;

import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool.TransactionBatchAddedListener;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionBroadcaster
    implements TransactionBatchAddedListener, PendingTransactionDroppedListener {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionBroadcaster.class);

  private static final EnumSet<TransactionType> ANNOUNCE_HASH_ONLY_TX_TYPES = EnumSet.of(BLOB);

  private static final Boolean HASH_ONLY_BROADCAST = Boolean.TRUE;
  private static final Boolean FULL_BROADCAST = Boolean.FALSE;

  private final PeerTransactionTracker transactionTracker;
  private final TransactionsMessageSender transactionsMessageSender;
  private final NewPooledTransactionHashesMessageSender newPooledTransactionHashesMessageSender;
  private final EthContext ethContext;
  private final Random random;

  public TransactionBroadcaster(
      final EthContext ethContext,
      final PeerTransactionTracker transactionTracker,
      final TransactionsMessageSender transactionsMessageSender,
      final NewPooledTransactionHashesMessageSender newPooledTransactionHashesMessageSender) {
    this(
        ethContext,
        transactionTracker,
        transactionsMessageSender,
        newPooledTransactionHashesMessageSender,
        null);
  }

  @VisibleForTesting
  protected TransactionBroadcaster(
      final EthContext ethContext,
      final PeerTransactionTracker transactionTracker,
      final TransactionsMessageSender transactionsMessageSender,
      final NewPooledTransactionHashesMessageSender newPooledTransactionHashesMessageSender,
      final Long seed) {
    this.transactionTracker = transactionTracker;
    this.transactionsMessageSender = transactionsMessageSender;
    this.newPooledTransactionHashesMessageSender = newPooledTransactionHashesMessageSender;
    this.ethContext = ethContext;
    this.random = seed != null ? new Random(seed) : new Random();
  }

  public void relayTransactionPoolTo(
      final EthPeer peer, final Collection<PendingTransaction> pendingTransactions) {
    if (!pendingTransactions.isEmpty()) {
      LOG.trace(
          "Send all txpool hashes, size={}, to newly connected peer={}",
          pendingTransactions.size(),
          peer);
      sendTransactionHashes(toTransactionList(pendingTransactions), List.of(peer));
    }
  }

  @Override
  public void onTransactionsAdded(final Collection<Transaction> transactions) {
    final int currPeerCount = ethContext.getEthPeers().peerCount();
    if (currPeerCount == 0) {
      return;
    }

    final int numPeersToSendFullTransactions = (int) Math.round(Math.sqrt(currPeerCount));

    final Map<Boolean, List<Transaction>> transactionByBroadcastMode =
        transactions.stream()
            .collect(
                Collectors.partitioningBy(
                    tx -> ANNOUNCE_HASH_ONLY_TX_TYPES.contains(tx.getType())));

    final List<EthPeer> peers =
        ethContext
            .getEthPeers()
            .streamAvailablePeers()
            .map(EthPeerImmutableAttributes::ethPeer)
            .collect(Collectors.toCollection(ArrayList::new));

    Collections.shuffle(peers, random);

    final List<EthPeer> sendFullTransactionsPeers =
        peers.subList(0, numPeersToSendFullTransactions);
    final List<EthPeer> sendOnlyHashesPeers =
        peers.subList(numPeersToSendFullTransactions, peers.size());

    LOG.atTrace()
        .setMessage("Sending full transactions to {} peers, transaction hashes only to {} peers")
        .addArgument(sendFullTransactionsPeers::size)
        .addArgument(sendOnlyHashesPeers::size)
        .log();

    sendToOnlyHashPeers(transactionByBroadcastMode, sendOnlyHashesPeers);
    sendToFullTransactionsPeers(transactionByBroadcastMode, sendFullTransactionsPeers);
  }

  private void sendToOnlyHashPeers(
      final Map<Boolean, List<Transaction>> txsByHashOnlyBroadcast,
      final List<EthPeer> hashOnlyPeers) {
    final List<Transaction> allTransactions =
        txsByHashOnlyBroadcast.values().stream().flatMap(List::stream).toList();

    sendTransactionHashes(allTransactions, hashOnlyPeers);
  }

  private void sendToFullTransactionsPeers(
      final Map<Boolean, List<Transaction>> txsByHashOnlyBroadcast,
      final List<EthPeer> fullTransactionsPeers) {
    sendFullTransactions(txsByHashOnlyBroadcast.get(FULL_BROADCAST), fullTransactionsPeers);
    sendTransactionHashes(txsByHashOnlyBroadcast.get(HASH_ONLY_BROADCAST), fullTransactionsPeers);
  }

  private void sendFullTransactions(
      final List<Transaction> transactions, final List<EthPeer> fullTransactionPeers) {
    if (!transactions.isEmpty()) {
      fullTransactionPeers.forEach(
          peer -> {
            transactionTracker.addToPeerSendQueue(peer, transactions);
            ethContext
                .getScheduler()
                .scheduleSyncWorkerTask(
                    () -> transactionsMessageSender.sendTransactionsToPeer(peer));
          });
    }
  }

  private void sendTransactionHashes(
      final List<Transaction> transactions, final List<EthPeer> transactionHashPeers) {
    if (!transactions.isEmpty()) {
      transactionHashPeers.stream()
          .forEach(
              peer -> {
                transactionTracker.addToPeerAnnouncementsSendQueue(peer, transactions);
                ethContext
                    .getScheduler()
                    .scheduleSyncWorkerTask(
                        () ->
                            newPooledTransactionHashesMessageSender
                                .sendTransactionAnnouncementsToPeer(peer));
              });
    }
  }

  @Override
  public void onTransactionDropped(final Transaction transaction, final RemovalReason reason) {
    transactionTracker.onTransactionDropped(transaction, reason);
  }
}
