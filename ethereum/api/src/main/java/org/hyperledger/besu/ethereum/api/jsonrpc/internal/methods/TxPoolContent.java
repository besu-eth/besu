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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionPendingResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionPoolContentResult;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.SenderPendingTransactionsData;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SequencedMap;
import java.util.stream.Collectors;

public class TxPoolContent implements JsonRpcMethod {

  private final TransactionPool transactionPool;

  public TxPoolContent(final TransactionPool transactionPool) {
    this.transactionPool = transactionPool;
  }

  @Override
  public String getName() {
    return RpcMethod.TX_POOL_CONTENT.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    return new JsonRpcSuccessResponse(requestContext.getRequest().getId(), content());
  }

  private TransactionPoolContentResult content() {
    final Map<Address, SenderPendingTransactionsData> pendingTransactionsBySender =
        transactionPool.getPendingTransactionsBySender();

    final Map<String, SequencedMap<String, TransactionPendingResult>> pending = new HashMap<>();
    final Map<String, SequencedMap<String, TransactionPendingResult>> queued = new HashMap<>();

    pendingTransactionsBySender.forEach(
        (sender, pendingTransactionsData) -> {
          final List<PendingTransaction> pendingTransactions =
              pendingTransactionsData.pendingTransactions();
          long expectedNonce = pendingTransactionsData.nonce();
          int idx = 0;
          while (idx < pendingTransactions.size()
              && expectedNonce == pendingTransactions.get(idx).getNonce()) {
            ++expectedNonce;
            ++idx;
          }

          final SequencedMap<String, TransactionPendingResult> pendingByNonce =
              pendingTransactions.subList(0, idx).stream()
                  .map(PendingTransaction::getTransaction)
                  .collect(
                      Collectors.toMap(
                          tx -> Long.toString(tx.getNonce()),
                          TransactionPendingResult::new,
                          (a, b) -> a,
                          LinkedHashMap::new));

          final SequencedMap<String, TransactionPendingResult> queuedByNonce =
              pendingTransactions.subList(idx, pendingTransactions.size()).stream()
                  .map(PendingTransaction::getTransaction)
                  .collect(
                      Collectors.toMap(
                          tx -> Long.toString(tx.getNonce()),
                          TransactionPendingResult::new,
                          (a, b) -> a,
                          LinkedHashMap::new));

          if (!pendingByNonce.isEmpty()) {
            pending.put(sender.toString(), pendingByNonce);
          }
          if (!queuedByNonce.isEmpty()) {
            queued.put(sender.toString(), queuedByNonce);
          }
        });
    return new TransactionPoolContentResult(pending, queued);
  }
}
