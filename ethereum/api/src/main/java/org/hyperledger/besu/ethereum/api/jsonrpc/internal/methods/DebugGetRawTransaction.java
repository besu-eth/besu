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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

public class DebugGetRawTransaction implements JsonRpcMethod {

  protected final BlockchainQueries blockchainQueries;

  public DebugGetRawTransaction(final BlockchainQueries blockchainQueries) {
    this.blockchainQueries = blockchainQueries;
  }

  @Override
  public String getName() {
    return RpcMethod.DEBUG_GET_RAW_TRANSACTION.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    final Hash txHash;
    try {
      txHash = requestContext.getRequiredParameter(0, Hash.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid transaction hash parameter (index 0)",
          RpcErrorType.INVALID_TRANSACTION_HASH_PARAMS,
          e);
    }

    return blockchainQueries
        .transactionByHash(txHash)
        .map(
            tx ->
                new JsonRpcSuccessResponse(
                    requestContext.getRequest().getId(), toRawString(tx.getTransaction())))
        .orElse(new JsonRpcSuccessResponse(requestContext.getRequest().getId(), null));
  }

  private String toRawString(final Transaction transaction) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    transaction.writeTo(out);
    return out.encoded().toHexString();
  }
}
