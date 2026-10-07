/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameterOrBlockHash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlockAccessListResult;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.BlockHeader;

public class EthGetBlockAccessList extends AbstractBlockParameterOrBlockHashMethod {

  public EthGetBlockAccessList(final BlockchainQueries blockchainQueries) {
    super(blockchainQueries);
  }

  @Override
  public String getName() {
    return RpcMethod.ETH_GET_BLOCK_ACCESS_LIST.getMethodName();
  }

  @Override
  protected BlockParameterOrBlockHash blockParameterOrBlockHash(
      final JsonRpcRequestContext request) {
    try {
      return request.getRequiredParameter(0, BlockParameterOrBlockHash.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block or block hash parameters (index 0)", RpcErrorType.INVALID_BLOCK_PARAMS, e);
    }
  }

  @Override
  protected Object pendingResult(final JsonRpcRequestContext request) {
    return null;
  }

  @Override
  protected Object resultByBlockHash(final JsonRpcRequestContext request, final Hash blockHash) {
    return getBlockAccessListResult(request, blockHash);
  }

  private Object getBlockAccessListResult(
      final JsonRpcRequestContext request, final Hash blockHash) {
    final var maybeHeader = getBlockchainQueries().getBlockHeaderByHash(blockHash);
    if (maybeHeader.isEmpty()) {
      return null;
    }

    final BlockHeader header = maybeHeader.get();
    if (!getBlockchainQueries().isBlockAccessListSupported(header)) {
      return new JsonRpcErrorResponse(
          request.getRequest().getId(), RpcErrorType.RESOURCE_NOT_FOUND);
    }

    final var maybeAccessList =
        getBlockchainQueries().getBlockchain().getBlockAccessList(header.getHash());

    if (maybeAccessList.isEmpty()) {
      return new JsonRpcErrorResponse(
          request.getRequest().getId(), RpcErrorType.PRUNED_HISTORY_UNAVAILABLE);
    }

    return BlockAccessListResult.fromBlockAccessList(maybeAccessList.get());
  }
}
