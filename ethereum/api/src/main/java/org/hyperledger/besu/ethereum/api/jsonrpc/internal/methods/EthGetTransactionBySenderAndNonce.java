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
import org.hyperledger.besu.datatypes.parameters.UnsignedLongParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionPendingResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionWithMetadataResult;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;

import java.util.Optional;

public class EthGetTransactionBySenderAndNonce implements JsonRpcMethod {

  private final BlockchainQueries blockchainQueries;
  private final TransactionPool transactionPool;

  public EthGetTransactionBySenderAndNonce(
      final BlockchainQueries blockchainQueries, final TransactionPool transactionPool) {
    this.blockchainQueries = blockchainQueries;
    this.transactionPool = transactionPool;
  }

  @Override
  public String getName() {
    return RpcMethod.ETH_GET_TRANSACTION_BY_SENDER_AND_NONCE.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    if (requestContext.getRequest().getParamLength() != 2) {
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(), RpcErrorType.INVALID_PARAM_COUNT);
    }

    final Address sender;
    try {
      sender = requestContext.getRequiredParameter(0, Address.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid sender address parameter (index 0)", RpcErrorType.INVALID_ADDRESS_PARAMS, e);
    }

    final long nonce;
    try {
      nonce = requestContext.getRequiredParameter(1, UnsignedLongParameter.class).getValue();
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid nonce parameter (index 1)", RpcErrorType.INVALID_NONCE_PARAMS, e);
    }

    // Check the transaction pool first (pending transactions)
    final Optional<TransactionPendingResult> pendingResult =
        transactionPool.getPendingTransactionsFor(sender).pendingTransactions().stream()
            .filter(pt -> pt.getNonce() == nonce)
            .findFirst()
            .map(pt -> new TransactionPendingResult(pt.getTransaction()));
    if (pendingResult.isPresent()) {
      return new JsonRpcSuccessResponse(requestContext.getRequest().getId(), pendingResult.get());
    }

    // Fall back to the mined transaction index
    final Object result =
        blockchainQueries
            .transactionBySenderAndNonce(sender, nonce)
            .map(TransactionWithMetadataResult::new)
            .orElse(null);

    return new JsonRpcSuccessResponse(requestContext.getRequest().getId(), result);
  }
}
