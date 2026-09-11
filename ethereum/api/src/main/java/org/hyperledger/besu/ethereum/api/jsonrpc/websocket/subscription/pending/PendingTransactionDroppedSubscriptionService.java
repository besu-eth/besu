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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.Subscription;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.SubscriptionManager;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.request.SubscriptionType;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransactionDroppedListener;
import org.hyperledger.besu.ethereum.eth.transactions.RemovalReason;

import java.util.List;

public class PendingTransactionDroppedSubscriptionService
    implements PendingTransactionDroppedListener {

  private final SubscriptionManager subscriptionManager;

  public PendingTransactionDroppedSubscriptionService(
      final SubscriptionManager subscriptionManager) {
    this.subscriptionManager = subscriptionManager;
  }

  @Override
  public void onPendingTransactionDropped(
      final PendingTransaction pendingTransaction, final RemovalReason reason) {
    notifySubscribers(pendingTransaction.getHash());
  }

  private void notifySubscribers(final Hash pendingTransaction) {
    final List<Subscription> subscriptions = pendingDroppedTransactionSubscriptions();

    final PendingTransactionResult msg = new PendingTransactionResult(pendingTransaction);
    for (final Subscription subscription : subscriptions) {
      subscriptionManager.sendMessage(subscription.getSubscriptionId(), msg);
    }
  }

  private List<Subscription> pendingDroppedTransactionSubscriptions() {
    return subscriptionManager.subscriptionsOfType(
        SubscriptionType.DROPPED_PENDING_TRANSACTIONS, Subscription.class);
  }
}
