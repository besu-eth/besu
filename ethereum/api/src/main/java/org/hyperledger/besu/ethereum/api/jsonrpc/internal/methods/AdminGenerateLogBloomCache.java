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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;

import java.util.Optional;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;

public class AdminGenerateLogBloomCache implements JsonRpcMethod {

  private final BlockchainQueries blockchainQueries;

  public AdminGenerateLogBloomCache(final BlockchainQueries blockchainQueries) {
    this.blockchainQueries = blockchainQueries;
  }

  @Override
  public String getName() {
    return RpcMethod.ADMIN_GENERATE_LOG_BLOOM_CACHE.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    final Optional<BlockParameter> startBlockParam;
    try {
      startBlockParam = requestContext.getOptionalParameter(0, BlockParameter.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid start block parameter (index 0)", RpcErrorType.INVALID_BLOCK_PARAMS, e);
    }
    final Optional<BlockParameter> stopBlockParam;
    try {
      stopBlockParam = requestContext.getOptionalParameter(1, BlockParameter.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid stop block parameter (index 1)", RpcErrorType.INVALID_BLOCK_PARAMS, e);
    }

    // Both bounds are clamped to the chain head. Caching a segment beyond the head can never
    // produce anything useful, since there are no headers to read.
    // The stop bound is EXCLUSIVE -- generateLogBloomCache walks
    // `for (blockNum = start; blockNum < stop; blockNum += BLOCKS_PER_BLOOM_CACHE)` -- so the
    // ceiling is head + 1. Clamping to the head itself would drop the segment that starts at the
    // head whenever the head sits on a BLOCKS_PER_BLOOM_CACHE boundary.

    // Memoized so that a request whose bounds are already constant -- notably the no-argument form,
    // which resolves to a rejected (0, -1) -- does not query the chain head at all.
    final Supplier<Long> headBlock = Suppliers.memoize(blockchainQueries::headBlockNumber);

    final long startBlock;
    if (startBlockParam.isEmpty() || startBlockParam.get().isEarliest()) {
      startBlock = 0;
    } else if (startBlockParam.get().getNumber().isPresent()) {
      startBlock = Math.min(startBlockParam.get().getNumber().get(), headBlock.get());
    } else {
      // latest, pending
      startBlock = headBlock.get();
    }

    final long stopBlock;
    if (stopBlockParam.isEmpty()) {
      if (startBlockParam.isEmpty()) {
        // No arguments at all: leave the bounds crossed so requestCaching rejects the request,
        // which is the long-standing behaviour of this method.
        stopBlock = -1L;
      } else {
        stopBlock = headBlock.get() + 1;
      }
    } else if (stopBlockParam.get().isEarliest()) {
      stopBlock = 0;
    } else if (stopBlockParam.get().getNumber().isPresent()) {
      stopBlock = Math.min(stopBlockParam.get().getNumber().get(), headBlock.get() + 1);
    } else {
      // latest, pending
      stopBlock = headBlock.get() + 1;
    }

    return new JsonRpcSuccessResponse(
        requestContext.getRequest().getId(),
        blockchainQueries
            .getTransactionLogBloomCacher()
            .map(cacher -> cacher.requestCaching(startBlock, stopBlock))
            .orElse(null));
  }
}
