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
import org.hyperledger.besu.plugin.services.BesuService;
import org.hyperledger.besu.plugin.services.Subscription;
import org.hyperledger.besu.plugin.services.transactionpool.spi.TransactionAddedListener;
import org.hyperledger.besu.plugin.services.transactionpool.spi.TransactionDroppedListener;

import java.util.Collection;

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
}
