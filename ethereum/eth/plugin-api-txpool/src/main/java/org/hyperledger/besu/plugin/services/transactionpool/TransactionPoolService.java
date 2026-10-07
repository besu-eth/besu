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
   * @param listener the listener that receives each added transaction
   * @return the subscription; close it to stop receiving events
   */
  Subscription subscribeTransactionAdded(TransactionAddedListener listener);

  /**
   * Subscribes to transactions dropped from the pool.
   *
   * @param listener the listener that receives each dropped transaction and the reason
   * @return the subscription; close it to stop receiving events
   */
  Subscription subscribeTransactionDropped(TransactionDroppedListener listener);
}
