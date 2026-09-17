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
package org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.transactionreceipts;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.Subscription;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.request.SubscriptionType;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TransactionReceiptsSubscription extends Subscription {

  private final Set<Hash> transactionHashesFilter;

  public TransactionReceiptsSubscription(
      final Long subscriptionId, final String connectionId, final List<Hash> transactionHashes) {
    super(subscriptionId, connectionId, SubscriptionType.TRANSACTION_RECEIPTS, Boolean.FALSE);
    this.transactionHashesFilter =
        transactionHashes == null || transactionHashes.isEmpty()
            ? Collections.emptySet()
            : new HashSet<>(transactionHashes);
  }

  public Set<Hash> getTransactionHashesFilter() {
    return transactionHashesFilter;
  }

  public boolean hasFilter() {
    return !transactionHashesFilter.isEmpty();
  }
}
