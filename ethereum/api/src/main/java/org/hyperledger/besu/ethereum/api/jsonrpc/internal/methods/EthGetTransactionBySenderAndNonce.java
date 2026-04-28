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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionBaseResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionWithMetadataResult;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.TransactionWithMetadata;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;

import java.util.Optional;

public class EthGetTransactionBySenderAndNonce implements JsonRpcMethod {

  private final BlockchainQueries blockchain;
  private final TransactionPool transactionPool;

  public EthGetTransactionBySenderAndNonce(
      final BlockchainQueries blockchain, final TransactionPool transactionPool) {
    this.blockchain = blockchain;
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
          "Invalid address parameter (index 0)", RpcErrorType.INVALID_ADDRESS_PARAMS, e);
    }

    final long nonce;
    try {
      nonce = requestContext.getRequiredParameter(1, UnsignedLongParameter.class).getValue();
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid nonce parameter (index 1)", RpcErrorType.INVALID_NONCE_PARAMS, e);
    }

    return new JsonRpcSuccessResponse(
        requestContext.getRequest().getId(), getResult(sender, nonce));
  }

  private Object getResult(final Address sender, final long nonce) {
    final Optional<TransactionBaseResult> pendingTransactionResult =
        transactionPool.getPendingTransactionsFor(sender).pendingTransactions().stream()
            .map(PendingTransaction::getTransaction)
            .filter(tx -> Long.compareUnsigned(tx.getNonce(), nonce) == 0)
            .findFirst()
            .map(TransactionBaseResult::new);
    if (pendingTransactionResult.isPresent()) {
      return pendingTransactionResult.get();
    }

    return transactionBySenderAndNonceInBlockchain(sender, nonce)
        .map(TransactionWithMetadataResult::new)
        .orElse(null);
  }

  private Optional<TransactionWithMetadata> transactionBySenderAndNonceInBlockchain(
      final Address sender, final long nonce) {
    final long chainHeadBlockNumber = blockchain.getBlockchain().getChainHeadBlockNumber();
    final long senderTxCountAtHead = blockchain.getTransactionCount(sender, chainHeadBlockNumber);
    if (Long.compareUnsigned(senderTxCountAtHead, nonce) <= 0) {
      return Optional.empty();
    }

    long low = 0;
    long high = chainHeadBlockNumber;

    while (low < high) {
      final long mid = low + ((high - low) >>> 1);
      final long txCountAtMid = blockchain.getTransactionCount(sender, mid);
      if (Long.compareUnsigned(txCountAtMid, nonce) <= 0) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }

    return findTransactionInBlockBySenderAndNonce(low, sender, nonce);
  }

  private Optional<TransactionWithMetadata> findTransactionInBlockBySenderAndNonce(
      final long blockNumber, final Address sender, final long nonce) {
    final int txCountInBlock = blockchain.getTransactionCount(blockNumber).orElse(0);
    for (int i = 0; i < txCountInBlock; i++) {
      final Optional<TransactionWithMetadata> candidate =
          blockchain.transactionByBlockNumberAndIndex(blockNumber, i);
      if (candidate.isPresent()
          && candidate.get().getTransaction().getSender().equals(sender)
          && Long.compareUnsigned(candidate.get().getTransaction().getNonce(), nonce) == 0) {
        return candidate;
      }
    }

    return Optional.empty();
  }
}
