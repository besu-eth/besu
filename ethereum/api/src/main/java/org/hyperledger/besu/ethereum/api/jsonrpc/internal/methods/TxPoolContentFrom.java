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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionPendingResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionPoolResult;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;

import java.util.SequencedMap;

public class TxPoolContentFrom extends AbstractTxPoolContent<TransactionPendingResult> {

  public TxPoolContentFrom(final TransactionPool transactionPool) {
    super(transactionPool);
  }

  @Override
  public String getName() {
    return RpcMethod.TX_POOL_CONTENT_FROM.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    try {
      final Address sender = requestContext.getRequiredParameter(0, Address.class);

      return new JsonRpcSuccessResponse(requestContext.getRequest().getId(), contentFrom(sender));
    } catch (JsonRpcParameter.JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid address parameter (index 0)", RpcErrorType.INVALID_ADDRESS_PARAMS, e);
    }
  }

  private TransactionPoolResult<SequencedMap<String, TransactionPendingResult>> contentFrom(
      final Address sender) {
    final PendingAndQueued<TransactionPendingResult> pendingAndQueued =
        getPendingAndQueued(
            transactionPool.getPendingTransactionsFor(sender), TransactionPendingResult::new);

    return new TransactionPoolResult<>(
        pendingAndQueued.pendingByNonce(), pendingAndQueued.queuedByNonce());
  }
}
