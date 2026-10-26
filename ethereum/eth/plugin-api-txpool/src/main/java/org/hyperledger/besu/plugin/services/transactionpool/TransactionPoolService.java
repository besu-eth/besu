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
package org.hyperledger.besu.plugin.services.transactionpool;

import org.hyperledger.besu.datatypes.PendingTransaction;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.plugin.Unstable;
import org.hyperledger.besu.plugin.services.BesuService;
import org.hyperledger.besu.plugin.services.Subscription;
import org.hyperledger.besu.plugin.services.transactionpool.spi.TransactionAddedListener;
import org.hyperledger.besu.plugin.services.transactionpool.spi.TransactionDroppedListener;

import java.util.Collection;
import java.util.Optional;

/**
 * Service to control and inspect the transaction pool: enable or disable the pool, query whether it
 * is currently enabled, and read the pending transactions.
 */
public interface TransactionPoolService extends BesuService {
  /** Disables the transaction pool. */
  void disableTransactionPool();

  /** Enables the transaction pool. */
  void enableTransactionPool();

  /**
   * Checks if the transaction pool is enabled.
   *
   * @return {@code true} if the transaction pool is enabled; otherwise {@code false}
   */
  boolean isTransactionPoolEnabled();

  /**
   * Returns the collection of pending transactions.
   *
   * @return a collection of pending transactions
   */
  Collection<? extends PendingTransaction> getPendingTransactions();

  /**
   * Subscribes to transactions added to the pool.
   *
   * <p>The listener is notified asynchronously, on a Besu thread different from the one that added
   * the transaction, so by the time it is invoked the transaction may no longer be in the pool.
   * Transaction added and dropped events are delivered one at a time, in the order the transaction
   * pool notifies them, which is not always the order in which the changes happened. When the
   * transaction pool is disabled, the events not yet delivered are discarded. A slow listener
   * delays the delivery of the following events to all the transaction listeners, so long-running
   * work should be moved to a separate thread. Exceptions thrown by the listener are logged and do
   * not prevent the delivery of the following events. Since delivery is asynchronous, events
   * already queued when the subscription is closed could still be delivered.
   *
   * @param listener the listener that receives each added transaction
   * @return the subscription; close it to stop receiving events
   */
  Subscription subscribeTransactionAdded(TransactionAddedListener listener);

  /**
   * Subscribes to transactions dropped from the pool.
   *
   * <p>The listener is notified asynchronously, on a Besu thread different from the one that
   * dropped the transaction. Transaction added and dropped events are delivered one at a time, in
   * the order the transaction pool notifies them, which is not always the order in which the
   * changes happened. When the transaction pool is disabled, the events not yet delivered are
   * discarded. A slow listener delays the delivery of the following events to all the transaction
   * listeners, so long-running work should be moved to a separate thread. Exceptions thrown by the
   * listener are logged and do not prevent the delivery of the following events. Since delivery is
   * asynchronous, events already queued when the subscription is closed could still be delivered.
   *
   * @param listener the listener that receives each dropped transaction and the reason
   * @return the subscription; close it to stop receiving events
   */
  Subscription subscribeTransactionDropped(TransactionDroppedListener listener);

  /**
   * Get the number of transactions currently in the pool. While the pool is disabled, for example
   * during initial sync, it holds no transactions and this returns 0.
   *
   * @return the number of pending transactions
   */
  @Unstable
  default int getPendingTransactionCount() {
    throw new UnsupportedOperationException();
  }

  /**
   * Submit a transaction to the pool as a local transaction, the way {@code eth_sendRawTransaction}
   * does: it is validated, added to the pool with the priority of local transactions, and broadcast
   * to peers if accepted.
   *
   * <p>The transaction must be an instance that Besu created, such as one returned by {@code
   * RlpConverterService.buildTransactionFromRlp}, a transaction of a block body returned by {@code
   * BlockchainService}, or the transaction of a pending transaction returned by {@link
   * #getPendingTransactions()}. Other implementations of {@link Transaction} are rejected.
   *
   * @param transaction the transaction to submit
   * @return empty if the transaction was accepted, otherwise the reason it was rejected, as the
   *     name of a Besu {@code TransactionInvalidReason} (for example {@code NONCE_TOO_LOW}, or
   *     {@code TX_POOL_DISABLED} while the pool is disabled). The set of reasons is informational
   *     and may grow.
   * @throws IllegalArgumentException if the transaction is not an instance that Besu created
   */
  @Unstable
  default Optional<String> addTransactionViaApi(final Transaction transaction) {
    throw new UnsupportedOperationException();
  }
}
