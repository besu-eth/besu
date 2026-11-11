/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.eth.transactions.pluginadapter;

import static org.hyperledger.besu.ethereum.core.plugins.Subscriptions.unsubscribeOnClose;

import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.plugin.services.Subscription;
import org.hyperledger.besu.plugin.services.transactionpool.TxPoolEventService;
import org.hyperledger.besu.plugin.services.transactionpool.spi.TransactionAddedListener;
import org.hyperledger.besu.plugin.services.transactionpool.spi.TransactionDroppedListener;

/**
 * The transaction pool event service implementation. It offers only the subscriptions, so a plugin
 * holding it in {@code start()} cannot reach the running-tier {@link
 * org.hyperledger.besu.plugin.services.transactionpool.TransactionPoolService} methods.
 */
public class TxPoolEventServiceImpl implements TxPoolEventService {

  private final TransactionPool transactionPool;

  /**
   * Creates a new TxPoolEventServiceImpl.
   *
   * @param transactionPool the transaction pool whose events are offered
   */
  public TxPoolEventServiceImpl(final TransactionPool transactionPool) {
    this.transactionPool = transactionPool;
  }

  @Override
  public Subscription subscribeTransactionAdded(final TransactionAddedListener listener) {
    final long id =
        transactionPool.subscribePendingTransactionsAsync(
            listener::onTransactionAdded, listener.getClass().getName());
    return unsubscribeOnClose(() -> transactionPool.unsubscribePendingTransactions(id));
  }

  @Override
  public Subscription subscribeTransactionDropped(final TransactionDroppedListener listener) {
    final long id =
        transactionPool.subscribeDroppedTransactionsAsync(
            (transaction, reason) -> listener.onTransactionDropped(transaction, reason.label()),
            listener.getClass().getName());
    return unsubscribeOnClose(() -> transactionPool.unsubscribeDroppedTransactions(id));
  }
}
