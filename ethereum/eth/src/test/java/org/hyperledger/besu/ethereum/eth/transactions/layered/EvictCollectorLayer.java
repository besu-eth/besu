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
package org.hyperledger.besu.ethereum.eth.transactions.layered;

import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionAddedResult;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionEventNotifier;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolMetrics;

import java.util.ArrayList;
import java.util.List;

public class EvictCollectorLayer extends EndLayer {
  static final String LAYER_NAME = "evict-collector";
  final List<PendingTransaction> evictedTxs = new ArrayList<>();

  public EvictCollectorLayer(
      final TransactionPoolMetrics metrics, final TransactionEventNotifier eventNotifier) {
    super(metrics, eventNotifier);
  }

  @Override
  public String name() {
    return LAYER_NAME;
  }

  @Override
  public TransactionAddedResult add(
      final PendingTransaction pendingTransaction, final int gap, final AddReason addReason) {
    final var res = super.add(pendingTransaction, gap, addReason);
    evictedTxs.add(pendingTransaction);
    return res;
  }

  public List<PendingTransaction> getEvictedTransactions() {
    return evictedTxs;
  }
}
