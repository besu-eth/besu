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
package org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription;

import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.blockheaders.NewBlockHeadersSubscription;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.logs.LogsSubscription;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.request.SubscribeRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.request.SubscriptionType;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.syncing.SyncingSubscription;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.transactionreceipts.TransactionReceiptsSubscription;

import java.util.function.Function;

public class SubscriptionBuilder {

  public Subscription build(
      final long subscriptionId, final String connectionId, final SubscribeRequest request) {
    final SubscriptionType subscriptionType = request.getSubscriptionType();
    switch (subscriptionType) {
      case NEW_BLOCK_HEADERS:
        {
          return new NewBlockHeadersSubscription(
              subscriptionId, connectionId, request.getIncludeTransaction());
        }
      case LOGS:
        {
          return logsSubscription(subscriptionId, connectionId, request);
        }
      case SYNCING:
        {
          return new SyncingSubscription(subscriptionId, connectionId, subscriptionType);
        }
      case TRANSACTION_RECEIPTS:
        {
          return new TransactionReceiptsSubscription(
              subscriptionId, connectionId, request.getTransactionHashes());
        }
      case NEW_PENDING_TRANSACTIONS:
      default:
        return new Subscription(
            subscriptionId, connectionId, subscriptionType, request.getIncludeTransaction());
    }
  }

  private Subscription logsSubscription(
      final long subscriptionId, final String connectionId, final SubscribeRequest request) {
    return new LogsSubscription(subscriptionId, connectionId, request.getFilterParameter());
  }

  @SuppressWarnings("unchecked")
  public <T> Function<Subscription, T> mapToSubscriptionClass(final Class<T> clazz) {
    return subscription -> {
      if (clazz.isAssignableFrom(subscription.getClass())) {
        return (T) subscription;
      } else {
        final String msg =
            String.format(
                "%s instance can't be mapped to type %s",
                subscription.getClass().getSimpleName(), clazz.getSimpleName());
        throw new IllegalArgumentException(msg);
      }
    };
  }
}
