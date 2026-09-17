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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionPoolResult;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.SenderPendingTransactionsData;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SequencedMap;
import java.util.function.Function;
import java.util.stream.Collectors;

abstract class AbstractTxPoolContent<T> implements JsonRpcMethod {

  protected final TransactionPool transactionPool;

  protected AbstractTxPoolContent(final TransactionPool transactionPool) {
    this.transactionPool = transactionPool;
  }

  protected record PendingAndQueued<T>(
      SequencedMap<String, T> pendingByNonce, SequencedMap<String, T> queuedByNonce) {}

  protected PendingAndQueued<T> getPendingAndQueued(
      final SenderPendingTransactionsData pendingTransactionsData,
      final Function<PendingTransaction, T> pendingTransactionRender) {
    final List<PendingTransaction> pendingTransactions =
        pendingTransactionsData.pendingTransactions();
    long expectedNonce = pendingTransactionsData.nonce();
    int idx = 0;
    while (idx < pendingTransactions.size()
        && expectedNonce == pendingTransactions.get(idx).getNonce()) {
      ++expectedNonce;
      ++idx;
    }

    final SequencedMap<String, T> pendingByNonce =
        renderPendingTransactions(pendingTransactions.subList(0, idx), pendingTransactionRender);

    final SequencedMap<String, T> queuedByNonce =
        renderPendingTransactions(
            pendingTransactions.subList(idx, pendingTransactions.size()), pendingTransactionRender);

    return new PendingAndQueued<T>(pendingByNonce, queuedByNonce);
  }

  protected TransactionPoolResult<Map<String, SequencedMap<String, T>>> contentMap(
      final Function<PendingTransaction, T> render) {
    final Map<String, SequencedMap<String, T>> pending = new HashMap<>();
    final Map<String, SequencedMap<String, T>> queued = new HashMap<>();

    transactionPool
        .getPendingTransactionsBySender()
        .forEach(
            (sender, pendingTransactionsData) -> {
              final PendingAndQueued<T> pq = getPendingAndQueued(pendingTransactionsData, render);
              if (!pq.pendingByNonce().isEmpty()) {
                pending.put(sender.toString(), pq.pendingByNonce());
              }
              if (!pq.queuedByNonce().isEmpty()) {
                queued.put(sender.toString(), pq.queuedByNonce());
              }
            });
    return new TransactionPoolResult<>(pending, queued);
  }

  private SequencedMap<String, T> renderPendingTransactions(
      final List<PendingTransaction> pendingTransactions,
      final Function<PendingTransaction, T> pendingTransactionRender) {
    return pendingTransactions.stream()
        .collect(
            Collectors.toMap(
                ptx -> Long.toString(ptx.getNonce()),
                pendingTransactionRender,
                (a, b) -> a,
                LinkedHashMap::new));
  }
}
