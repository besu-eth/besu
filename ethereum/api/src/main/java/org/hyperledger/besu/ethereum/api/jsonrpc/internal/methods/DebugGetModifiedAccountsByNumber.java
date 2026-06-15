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

import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.Optional;

public class DebugGetModifiedAccountsByNumber extends AbstractDebugGetModifiedAccounts {

  public DebugGetModifiedAccountsByNumber(final BlockchainQueries blockchainQueries) {
    super(blockchainQueries);
  }

  @Override
  public String getName() {
    return RpcMethod.DEBUG_GET_MODIFIED_ACCOUNTS_BY_NUMBER.getMethodName();
  }

  @Override
  protected Optional<JsonRpcError> validateParameters(final JsonRpcRequestContext requestContext) {
    if (requestContext.getRequest().getParamLength() == 1
        && getBlockNumber(requestContext, 0)
            .filter(blockNumber -> blockNumber == BlockHeader.GENESIS_BLOCK_NUMBER)
            .isPresent()) {
      return Optional.of(
          new JsonRpcError(
              RpcErrorType.INVALID_BLOCK_PARAMS, "genesis block has no parent to diff against"));
    }
    return Optional.empty();
  }

  @Override
  protected Optional<BlockHeader> startBlock(final JsonRpcRequestContext requestContext) {
    return getBlockNumber(requestContext, 0)
        .map(
            blockNumber ->
                requestContext.getRequest().getParamLength() > 1 ? blockNumber : blockNumber - 1)
        .flatMap(blockchainQueries().getBlockchain()::getBlockHeader);
  }

  @Override
  protected Optional<BlockHeader> endBlock(final JsonRpcRequestContext requestContext) {
    return getBlockNumber(requestContext, 1)
        .or(() -> getBlockNumber(requestContext, 0))
        .flatMap(blockchainQueries().getBlockchain()::getBlockHeader);
  }

  private Optional<Long> getBlockNumber(
      final JsonRpcRequestContext requestContext, final int index) {
    try {
      return requestContext
          .getOptionalParameter(index, BlockParameter.class)
          .flatMap(blockParameter -> blockParameter.getBlockNumber(blockchainQueries()));
    } catch (final JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block parameter (index " + index + ")", RpcErrorType.INVALID_BLOCK_PARAMS, e);
    }
  }
}
