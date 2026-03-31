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

import static org.hyperledger.besu.ethereum.core.Transaction.toHashList;

import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.messages.TransactionsMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection.PeerNotConnected;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TransactionsMessageSender {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionsMessageSender.class);

  private final PeerTransactionTracker transactionTracker;
  private final int maxTransactionsMessageSize;

  public TransactionsMessageSender(
      final PeerTransactionTracker transactionTracker, final int maxTransactionsMessageSize) {
    this.transactionTracker = transactionTracker;
    this.maxTransactionsMessageSize = maxTransactionsMessageSize;
  }

  void sendTransactionsToPeer(final EthPeer peer) {
    final List<Transaction> transactionsBatch = new ArrayList<>();

    TransactionsMessage.SizeLimitedBuilder limitedTransactionsMessageBuilder =
        new TransactionsMessage.SizeLimitedBuilder(maxTransactionsMessageSize);

    Transaction txToSend = transactionTracker.claimTransactionToSendToPeer(peer);
    while (txToSend != null) {

      if (!transactionTracker.hasPeerSeenTransactionOrAnnouncement(peer, txToSend.getHash())) {
        transactionTracker.markTransactionsAsSeen(peer, List.of(txToSend.getHash()));

        final var added = limitedTransactionsMessageBuilder.add(txToSend);

        if (added) {
          transactionsBatch.add(txToSend);
        } else {
          // message is full, then send it and prepare the next batch
          send(peer, transactionsBatch, limitedTransactionsMessageBuilder.build());

          // prepare the next batch
          transactionsBatch.clear();
          limitedTransactionsMessageBuilder =
              new TransactionsMessage.SizeLimitedBuilder(maxTransactionsMessageSize);
          if (limitedTransactionsMessageBuilder.add(txToSend)) {
            transactionsBatch.add(txToSend);
          } else {
            LOG.warn(
                "maxTransactionsMessageSize (%d bytes) is set too small, since transaction %s does not fit"
                    .formatted(maxTransactionsMessageSize, txToSend.getHash()));
          }
        }
      }
      txToSend = transactionTracker.claimTransactionToSendToPeer(peer);
    }

    if (!transactionsBatch.isEmpty()) {
      send(peer, transactionsBatch, limitedTransactionsMessageBuilder.build());
    }
  }

  private static void send(
      final EthPeer peer,
      final List<Transaction> includedTransactions,
      final TransactionsMessage transactionsMessage) {
    if (!includedTransactions.isEmpty()) {
      try {
        LOG.atTrace()
            .setMessage(
                "Sent transactions to peer={}, this message txs count={},"
                    + " this message hashes={}")
            .addArgument(peer)
            .addArgument(includedTransactions::size)
            .addArgument(() -> toHashList(includedTransactions))
            .log();

        peer.send(transactionsMessage);
      } catch (final PeerNotConnected e) {
        LOG.atTrace()
            .setMessage(
                "Peer no more connected while sending transactions: peer={}, message hashes={}")
            .addArgument(peer)
            .addArgument(() -> toHashList(includedTransactions))
            .log();
      } catch (final Exception e) {
        LOG.debug("Failed to send transactions to peer {}", peer, e);
      }
    }
  }
}
