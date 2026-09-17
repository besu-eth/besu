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
package org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.pending;

import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.Subscription;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.SubscriptionManager;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.request.SubscriptionType;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransactionAddedListener;

import java.util.List;

public class PendingTransactionSubscriptionService implements PendingTransactionAddedListener {

  private final SubscriptionManager subscriptionManager;

  public PendingTransactionSubscriptionService(final SubscriptionManager subscriptionManager) {
    this.subscriptionManager = subscriptionManager;
  }

  @Override
  public void onPendingTransactionAdded(final PendingTransaction pendingTransaction) {
    notifySubscribers(pendingTransaction.getTransaction());
  }

  private void notifySubscribers(final Transaction pendingTransaction) {
    final List<Subscription> subscriptions = pendingTransactionSubscriptions();

    final PendingTransactionResult hashResult =
        new PendingTransactionResult(pendingTransaction.getHash());
    final PendingTransactionDetailResult detailResult =
        new PendingTransactionDetailResult(pendingTransaction);
    for (final Subscription subscription : subscriptions) {
      if (Boolean.TRUE.equals(subscription.getIncludeTransaction())) {
        subscriptionManager.sendMessage(subscription.getSubscriptionId(), detailResult);
      } else {
        subscriptionManager.sendMessage(subscription.getSubscriptionId(), hashResult);
      }
    }
  }

  private List<Subscription> pendingTransactionSubscriptions() {
    return subscriptionManager.subscriptionsOfType(
        SubscriptionType.NEW_PENDING_TRANSACTIONS, Subscription.class);
  }
}
