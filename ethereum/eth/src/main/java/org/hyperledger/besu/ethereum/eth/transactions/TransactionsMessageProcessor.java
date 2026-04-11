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

import static java.time.Instant.now;
import static org.hyperledger.besu.ethereum.core.Transaction.toHashList;

import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.messages.TransactionsMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;
import org.hyperledger.besu.ethereum.rlp.RLPException;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TransactionsMessageProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionsMessageProcessor.class);
  static final String METRIC_LABEL = "transactions";
  private final PeerTransactionTracker transactionTracker;
  private final TransactionPool transactionPool;

  private final TransactionPoolMetrics metrics;

  public TransactionsMessageProcessor(
      final PeerTransactionTracker transactionTracker,
      final TransactionPool transactionPool,
      final TransactionPoolMetrics metrics) {
    this.transactionTracker = transactionTracker;
    this.transactionPool = transactionPool;
    this.metrics = metrics;
    metrics.initExpiredMessagesCounter(METRIC_LABEL);
  }

  void processTransactionsMessage(
      final EthPeer peer,
      final TransactionsMessage transactionsMessage,
      final Instant queueAt,
      final Duration keepAlive) {
    // Check if message is not expired.
    final var latency = Duration.between(queueAt, now());
    if (latency.compareTo(keepAlive) < 0) {
      this.processTransactionsMessage(peer, transactionsMessage);
    } else {
      LOG.atTrace()
          .setMessage(
              "Ignoring expired transactions message: peer={}, latency={}, queuedAt={}, keepAlive={}, hashes={}")
          .addArgument(peer)
          .addArgument(latency)
          .addArgument(queueAt)
          .addArgument(keepAlive)
          .addArgument(() -> toHashList(transactionsMessage.transactions()))
          .log();
      metrics.incrementExpiredMessages(METRIC_LABEL);
    }
  }

  private void processTransactionsMessage(
      final EthPeer peer, final TransactionsMessage transactionsMessage) {
    try {
      final List<Transaction> incomingTransactions = transactionsMessage.transactions();
      final Collection<Transaction> freshTransactions =
          transactionTracker.receivedTransactions(peer, incomingTransactions);

      metrics.incrementAlreadySeenTransactions(
          METRIC_LABEL, incomingTransactions.size() - freshTransactions.size());
      LOG.atTrace()
          .setMessage("Received transactions message: peer={} incoming hashes={}, fresh hashes={}")
          .addArgument(peer)
          .addArgument(() -> toHashList(incomingTransactions))
          .addArgument(() -> toHashList(freshTransactions))
          .log();

      transactionPool.addRemoteTransactions(freshTransactions);

    } catch (final RLPException ex) {
      if (peer != null) {
        LOG.debug(
            "Malformed transaction message received (BREACH_OF_PROTOCOL), disconnecting: {}",
            peer,
            ex);
        peer.disconnect(DisconnectReason.BREACH_OF_PROTOCOL_MALFORMED_MESSAGE_RECEIVED);
      }
    }
  }
}
