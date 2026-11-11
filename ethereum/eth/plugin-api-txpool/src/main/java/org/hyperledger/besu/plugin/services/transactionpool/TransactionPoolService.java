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
import org.hyperledger.besu.plugin.RunningService;

import java.util.Collection;

/**
 * Service to control and inspect the transaction pool: enable or disable the pool, query whether it
 * is currently enabled, and read the pending transactions.
 *
 * <p>Available once the node is running. On a node with no initial sync to wait for, the pool is
 * enabled only after every plugin's {@code start()} has returned, and otherwise once the initial
 * sync completes, so its state during {@code start()} is transient, and enabling it from there
 * would restore a saved pool before the remaining plugins have started. To be notified of
 * transactions added to or dropped from the pool, subscribe through {@link TxPoolEventService},
 * which is available from {@code start()} so that the subscription is in place before the pool is
 * enabled.
 */
public interface TransactionPoolService extends RunningService {
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
}
