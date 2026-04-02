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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.parameters.UnsignedIntParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionWithMetadataResult;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.TransactionWithMetadata;

import java.util.Optional;

public class EthGetTransactionByBlockHashAndIndex implements JsonRpcMethod {

  private final BlockchainQueries blockchain;

  public EthGetTransactionByBlockHashAndIndex(final BlockchainQueries blockchain) {
    this.blockchain = blockchain;
  }

  @Override
  public String getName() {
    return RpcMethod.ETH_GET_TRANSACTION_BY_BLOCK_HASH_AND_INDEX.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    final Hash hash;
    try {
      hash = requestContext.getRequiredParameter(0, Hash.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid transaction hash parameter (index 0)",
          RpcErrorType.INVALID_TRANSACTION_HASH_PARAMS,
          e);
    }
    final int index;
    try {
      index = requestContext.getRequiredParameter(1, UnsignedIntParameter.class).getValue();
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid transaction id parameter (index 1)",
          RpcErrorType.INVALID_TRANSACTION_ID_PARAMS,
          e);
    }
    final Optional<TransactionWithMetadata> transactionWithMetadata =
        blockchain.transactionByBlockHashAndIndex(hash, index);
    final TransactionResult result =
        transactionWithMetadata.map(TransactionWithMetadataResult::new).orElse(null);
    return new JsonRpcSuccessResponse(requestContext.getRequest().getId(), result);
  }
}
