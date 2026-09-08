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

import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.cache.TransactionLogBloomCacher;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.Map;
import java.util.Optional;

public class AdminLogsRemoveCache implements JsonRpcMethod {
  private final BlockchainQueries blockchainQueries;

  public AdminLogsRemoveCache(final BlockchainQueries blockchainQueries) {
    this.blockchainQueries = blockchainQueries;
  }

  @Override
  public String getName() {
    return RpcMethod.ADMIN_LOGS_REMOVE_CACHE.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    final Optional<BlockParameter> startBlockParameter;
    try {
      startBlockParameter = requestContext.getOptionalParameter(0, BlockParameter.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid start block parameter (index 0)", RpcErrorType.INVALID_BLOCK_NUMBER_PARAMS, e);
    }
    final Optional<BlockParameter> stopBlockParameter;
    try {
      stopBlockParameter = requestContext.getOptionalParameter(1, BlockParameter.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid stop block parameter (index 1)", RpcErrorType.INVALID_BLOCK_NUMBER_PARAMS, e);
    }

    final long startBlock;
    if (startBlockParameter.isEmpty() || startBlockParameter.get().isEarliest()) {
      startBlock = BlockHeader.GENESIS_BLOCK_NUMBER;
    } else if (startBlockParameter.get().getNumber().isPresent()) {
      startBlock = startBlockParameter.get().getNumber().get();
      if (blockchainQueries.getBlockchain().getBlockByNumber(startBlock).isEmpty()) {
        throw new IllegalStateException("Block not found, " + startBlock);
      }
    } else {
      // latest, pending
      startBlock = blockchainQueries.headBlockNumber();
    }

    final long stopBlock;
    if (stopBlockParameter.isEmpty()) {
      if (startBlockParameter.isEmpty()) {
        stopBlock = blockchainQueries.headBlockNumber();
      } else {
        stopBlock = startBlock;
      }
    } else if (stopBlockParameter.get().isEarliest()) {
      stopBlock = BlockHeader.GENESIS_BLOCK_NUMBER;
    } else if (stopBlockParameter.get().getNumber().isPresent()) {
      stopBlock = stopBlockParameter.get().getNumber().get();
      if (blockchainQueries.getBlockchain().getBlockByNumber(stopBlock).isEmpty()) {
        throw new IllegalStateException("Block not found, " + stopBlock);
      }
    } else {
      // latest, pending
      stopBlock = blockchainQueries.headBlockNumber();
    }

    if (stopBlock < startBlock) {
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(), RpcErrorType.INVALID_BLOCK_NUMBER_PARAMS);
    }

    final TransactionLogBloomCacher transactionLogBloomCacher =
        blockchainQueries
            .getTransactionLogBloomCacher()
            .orElseThrow(
                () ->
                    new InternalError(
                        "Error attempting to get TransactionLogBloomCacher. Please ensure that the TransactionLogBloomCacher is enabled."));

    if (!transactionLogBloomCacher.removeSegments(startBlock, stopBlock)) {
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(), RpcErrorType.CACHE_REMOVAL_IN_PROGRESS);
    }

    return new JsonRpcSuccessResponse(
        requestContext.getRequest().getId(), Map.of("Status", "Cache Removed"));
  }
}
