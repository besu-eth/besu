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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.cache.TransactionLogBloomCacher;

import java.util.Map;
import java.util.Optional;

public class AdminLogsRepairCache implements JsonRpcMethod {
  private final BlockchainQueries blockchainQueries;

  public AdminLogsRepairCache(final BlockchainQueries blockchainQueries) {
    this.blockchainQueries = blockchainQueries;
  }

  @Override
  public String getName() {
    return RpcMethod.ADMIN_LOGS_REPAIR_CACHE.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    final Optional<Long> blockNumber;
    try {
      blockNumber = requestContext.getOptionalParameter(0, Long.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block number parameter (index 0)", RpcErrorType.INVALID_BLOCK_NUMBER_PARAMS, e);
    }

    if (blockNumber.isPresent()
        && blockchainQueries.getBlockchain().getBlockByNumber(blockNumber.get()).isEmpty()) {
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(), RpcErrorType.UNKNOWN_BLOCK);
    }

    final TransactionLogBloomCacher transactionLogBloomCacher =
        blockchainQueries
            .getTransactionLogBloomCacher()
            .orElseThrow(
                () ->
                    new InternalError(
                        "Error attempting to get TransactionLogBloomCacher. Please ensure that the TransactionLogBloomCacher is enabled."));

    transactionLogBloomCacher.ensurePreviousSegmentsArePresent(
        blockNumber.orElse(blockchainQueries.headBlockNumber()), true);

    final Map<String, String> response =
        Map.of(
            "Status",
            transactionLogBloomCacher.getCachingStatus().isCaching()
                ? "Already running"
                : "Started");

    return new JsonRpcSuccessResponse(requestContext.getRequest().getId(), response);
  }
}
