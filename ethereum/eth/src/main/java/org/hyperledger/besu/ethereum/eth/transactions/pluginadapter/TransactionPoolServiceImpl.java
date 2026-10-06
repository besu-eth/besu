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
package org.hyperledger.besu.ethereum.eth.transactions.pluginadapter;

import static org.hyperledger.besu.ethereum.core.plugins.Subscriptions.unsubscribeOnClose;

import org.hyperledger.besu.datatypes.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.plugin.services.Subscription;
import org.hyperledger.besu.plugin.services.transactionpool.TransactionPoolService;
import org.hyperledger.besu.plugin.services.transactionpool.spi.TransactionAddedListener;
import org.hyperledger.besu.plugin.services.transactionpool.spi.TransactionDroppedListener;

import java.util.Collection;

/** The Transaction pool service implementation. */
public class TransactionPoolServiceImpl implements TransactionPoolService {

  private final TransactionPool transactionPool;

  /**
   * Creates a new TransactionPoolServiceImpl.
   *
   * @param transactionPool the transaction pool to control
   */
  public TransactionPoolServiceImpl(final TransactionPool transactionPool) {
    this.transactionPool = transactionPool;
  }

  @Override
  public void disableTransactionPool() {
    transactionPool.setDisabled();
  }

  @Override
  public void enableTransactionPool() {
    transactionPool.setEnabled();
  }

  @Override
  public boolean isTransactionPoolEnabled() {
    return transactionPool.isEnabled();
  }

  @Override
  public Collection<? extends PendingTransaction> getPendingTransactions() {
    return transactionPool.getPendingTransactions();
  }

  @Override
  public Subscription subscribeTransactionAdded(final TransactionAddedListener listener) {
    final long id = transactionPool.subscribePendingTransactions(listener::onTransactionAdded);
    return unsubscribeOnClose(() -> transactionPool.unsubscribePendingTransactions(id));
  }

  @Override
  public Subscription subscribeTransactionDropped(final TransactionDroppedListener listener) {
    final long id =
        transactionPool.subscribeDroppedTransactions(
            (transaction, reason) -> listener.onTransactionDropped(transaction, reason.label()));
    return unsubscribeOnClose(() -> transactionPool.unsubscribeDroppedTransactions(id));
  }
}
