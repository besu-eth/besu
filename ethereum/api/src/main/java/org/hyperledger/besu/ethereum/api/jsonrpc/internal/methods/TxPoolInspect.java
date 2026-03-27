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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionPoolResult;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.SenderPendingTransactionsData;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;

import java.util.HashMap;
import java.util.Map;
import java.util.SequencedMap;

public class TxPoolInspect extends AbstractTxPoolContent<String> {

  public TxPoolInspect(final TransactionPool transactionPool) {
    super(transactionPool);
  }

  @Override
  public String getName() {
    return RpcMethod.TX_POOL_INSPECT.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    return new JsonRpcSuccessResponse(requestContext.getRequest().getId(), content());
  }

  private TransactionPoolResult<Map<String, SequencedMap<String, String>>> content() {
    final Map<Address, SenderPendingTransactionsData> pendingTransactionsBySender =
        transactionPool.getPendingTransactionsBySender();

    final Map<String, SequencedMap<String, String>> pending = new HashMap<>();
    final Map<String, SequencedMap<String, String>> queued = new HashMap<>();

    pendingTransactionsBySender.forEach(
        (sender, pendingTransactionsData) -> {
          final PendingAndQueued<String> pendingAndQueued =
              getPendingAndQueued(pendingTransactionsData, TxPoolInspect::humanReadableView);

          if (!pendingAndQueued.pendingByNonce().isEmpty()) {
            pending.put(sender.toString(), pendingAndQueued.pendingByNonce());
          }
          if (!pendingAndQueued.queuedByNonce().isEmpty()) {
            queued.put(sender.toString(), pendingAndQueued.queuedByNonce());
          }
        });
    return new TransactionPoolResult<>(pending, queued);
  }

  // Produces the spec-defined summary: "<to>: <value> wei + <gasLimit> gas × <gasPrice> wei"
  // For contract creation the destination is "contract creation".
  static String humanReadableView(final PendingTransaction pendingTransaction) {
    final Transaction tx = pendingTransaction.getTransaction();
    final String to = tx.getTo().map(Address::toString).orElse("contract creation");
    final String gasPrice =
        tx.getGasPrice()
            .map(gp -> gp.toBigInteger().toString())
            .orElseGet(() -> tx.getMaxFeePerGas().map(f -> f.toBigInteger().toString()).orElse("0"));
    return to
        + ": "
        + tx.getValue().toBigInteger()
        + " wei + "
        + tx.getGasLimit()
        + " gas \u00d7 "
        + gasPrice
        + " wei";
  }
}
