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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class DebugGetModifiedAccountsByHash extends AbstractDebugGetModifiedAccounts {

  public DebugGetModifiedAccountsByHash(final BlockchainQueries blockchainQueries) {
    super(blockchainQueries);
  }

  @Override
  public String getName() {
    return RpcMethod.DEBUG_GET_MODIFIED_ACCOUNTS_BY_HASH.getMethodName();
  }

  @Override
  protected Optional<JsonRpcError> validateParameters(final JsonRpcRequestContext requestContext) {
    if (requestContext.getRequest().getParamLength() == 1) {
      return getBlockHeader(requestContext, 0)
          .filter(blockHeader -> blockHeader.getNumber() == BlockHeader.GENESIS_BLOCK_NUMBER)
          .map(this::noParentError);
    }
    return Optional.empty();
  }

  @Override
  protected Optional<BlockHeader> startBlock(final JsonRpcRequestContext requestContext) {
    final Optional<BlockHeader> blockHeader = getBlockHeader(requestContext, 0);
    if (requestContext.getRequest().getParamLength() > 1) {
      return blockHeader;
    }
    return blockHeader.flatMap(this::parentBlock);
  }

  @Override
  protected Optional<BlockHeader> endBlock(final JsonRpcRequestContext requestContext) {
    return getBlockHeader(requestContext, 1).or(() -> getBlockHeader(requestContext, 0));
  }

  @Override
  protected HeaderRangeResult headersInRange(
      final BlockHeader startBlock, final BlockHeader endBlock) {
    final List<BlockHeader> headers = new ArrayList<>();
    Optional<BlockHeader> current = Optional.of(endBlock);
    while (current.isPresent() && current.get().getNumber() > startBlock.getNumber()) {
      headers.add(current.get());
      current = blockchainQueries().getBlockchain().getBlockHeader(current.get().getParentHash());
    }
    if (current.isEmpty()) {
      return HeaderRangeResult.error(RpcErrorType.BLOCK_NOT_FOUND);
    }
    if (!current.get().getBlockHash().equals(startBlock.getBlockHash())) {
      return HeaderRangeResult.error(RpcErrorType.INVALID_BLOCK_PARAMS);
    }
    Collections.reverse(headers);
    return HeaderRangeResult.success(headers);
  }

  private Optional<BlockHeader> parentBlock(final BlockHeader blockHeader) {
    if (blockHeader.getNumber() == BlockHeader.GENESIS_BLOCK_NUMBER) {
      return Optional.empty();
    }
    return blockchainQueries().getBlockchain().getBlockHeader(blockHeader.getParentHash());
  }

  private Optional<BlockHeader> getBlockHeader(
      final JsonRpcRequestContext requestContext, final int index) {
    try {
      return requestContext
          .getOptionalParameter(index, Hash.class)
          .flatMap(blockchainQueries().getBlockchain()::getBlockHeader);
    } catch (final JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block hash parameter (index " + index + ")",
          RpcErrorType.INVALID_BLOCK_HASH_PARAMS,
          e);
    }
  }
}
