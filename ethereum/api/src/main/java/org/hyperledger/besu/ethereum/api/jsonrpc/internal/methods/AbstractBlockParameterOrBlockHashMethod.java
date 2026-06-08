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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameterOrBlockHash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;

public abstract class AbstractBlockParameterOrBlockHashMethod implements JsonRpcMethod {

  protected final Supplier<BlockchainQueries> blockchainQueries;

  protected AbstractBlockParameterOrBlockHashMethod(final BlockchainQueries blockchainQueries) {
    this(Suppliers.ofInstance(blockchainQueries));
  }

  protected AbstractBlockParameterOrBlockHashMethod(
      final Supplier<BlockchainQueries> blockchainQueries) {
    this.blockchainQueries = blockchainQueries;
  }

  protected abstract BlockParameterOrBlockHash blockParameterOrBlockHash(
      JsonRpcRequestContext request);

  /**
   * Reads the block parameter at the given index, defaulting to the latest block when it is omitted
   * (per execution-apis the Block parameter is optional, defaulting to 'latest'). Subclasses whose
   * block parameter is optional should delegate to this from {@link #blockParameterOrBlockHash}.
   *
   * @param request the request
   * @param index the position of the block parameter
   * @return the requested block selector, or latest when the parameter is omitted
   */
  protected BlockParameterOrBlockHash blockParameterOrBlockHashWithLatestDefault(
      final JsonRpcRequestContext request, final int index) {
    try {
      return request
          .getOptionalParameter(index, BlockParameterOrBlockHash.class)
          .orElse(BlockParameterOrBlockHash.LATEST);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block or block hash parameter (index " + index + ")",
          RpcErrorType.INVALID_BLOCK_PARAMS,
          e);
    }
  }

  protected abstract Object resultByBlockHash(JsonRpcRequestContext request, Hash blockHash);

  protected Object resultByBlockHeader(
      final JsonRpcRequestContext request, final BlockHeader blockHeader) {
    return resultByBlockHash(request, blockHeader.getBlockHash());
  }

  protected BlockchainQueries getBlockchainQueries() {
    return blockchainQueries.get();
  }

  protected JsonRpcResponse blockNotFoundResponse(final JsonRpcRequestContext requestContext) {
    return new JsonRpcSuccessResponse(requestContext.getRequest().getId(), null);
  }

  protected Object pendingResult(final JsonRpcRequestContext request) {
    // TODO: Update once we mine and better understand pending semantics.
    // For now act like we are not mining and just return latest.
    return latestResult(request);
  }

  protected Object latestResult(final JsonRpcRequestContext request) {
    return resultByBlockHeader(
        request, getBlockchainQueries().getBlockchain().getChainHead().getBlockHeader());
  }

  protected Object finalizedResult(final JsonRpcRequestContext request) {
    return posRelatedResult(request, BlockchainQueries::finalizedBlockHeader);
  }

  protected Object safeResult(final JsonRpcRequestContext request) {
    return posRelatedResult(request, BlockchainQueries::safeBlockHeader);
  }

  private Object posRelatedResult(
      final JsonRpcRequestContext request,
      final Function<BlockchainQueries, Optional<BlockHeader>> blockHeaderSupplier) {

    return blockHeaderSupplier
        .apply(blockchainQueries.get())
        .map(header -> resultByBlockHash(request, header.getBlockHash()))
        .orElseGet(
            () ->
                new JsonRpcErrorResponse(request.getRequest().getId(), RpcErrorType.UNKNOWN_BLOCK));
  }

  protected Object handleParamTypes(final JsonRpcRequestContext requestContext) {
    final BlockParameterOrBlockHash blockParameterOrBlockHash =
        blockParameterOrBlockHash(requestContext);

    final Object result;
    if (blockParameterOrBlockHash.isLatest()) {
      result = latestResult(requestContext);
    } else if (blockParameterOrBlockHash.isPending()) {
      result = pendingResult(requestContext);
    } else if (blockParameterOrBlockHash.isSafe()) {
      result = safeResult(requestContext);
    } else if (blockParameterOrBlockHash.isFinalized()) {
      result = finalizedResult(requestContext);
    } else if (blockParameterOrBlockHash.isNumeric() || blockParameterOrBlockHash.isEarliest()) {
      final OptionalLong blockNumber = blockParameterOrBlockHash.getNumber();
      if (blockNumber.isEmpty() || blockNumber.getAsLong() < 0) {
        return new JsonRpcErrorResponse(
            requestContext.getRequest().getId(), RpcErrorType.INVALID_BLOCK_NUMBER_PARAMS);
      } else if (blockNumber.getAsLong() > getBlockchainQueries().headBlockNumber()) {
        // return block not found if a future block is requested
        return blockNotFoundResponse(requestContext);
      }

      result =
          resultByBlockHash(
              requestContext,
              blockchainQueries
                  .get()
                  .getBlockHashByNumber(blockNumber.getAsLong())
                  .orElse(Hash.EMPTY));
    } else {
      Optional<Hash> blockHash = blockParameterOrBlockHash.getHash();
      if (blockHash.isEmpty()) {
        return blockNotFoundResponse(requestContext);
      }

      // return block not found if block hash does not find a block
      Optional<BlockHeader> maybeBlockHeader =
          getBlockchainQueries().getBlockHeaderByHash(blockHash.get());
      if (maybeBlockHeader.isEmpty()) {
        return blockNotFoundResponse(requestContext);
      }

      if (blockParameterOrBlockHash.getRequireCanonical()
          && !getBlockchainQueries().blockIsOnCanonicalChain(blockHash.get())) {
        return new JsonRpcErrorResponse(
            requestContext.getRequest().getId(), RpcErrorType.JSON_RPC_NOT_CANONICAL_ERROR);
      }

      result = resultByBlockHash(requestContext, blockHash.get());
    }

    return result;
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    Object response = handleParamTypes(requestContext);

    if (response instanceof JsonRpcResponse) {
      return (JsonRpcResponse) response;
    }

    return new JsonRpcSuccessResponse(requestContext.getRequest().getId(), response);
  }
}
