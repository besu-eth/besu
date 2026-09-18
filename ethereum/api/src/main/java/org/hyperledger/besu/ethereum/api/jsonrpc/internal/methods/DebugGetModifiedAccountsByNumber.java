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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;

import java.util.Optional;

public class DebugGetModifiedAccountsByNumber extends AbstractDebugGetModifiedAccounts {
  public DebugGetModifiedAccountsByNumber(
      final BlockchainQueries blockchainQueries, final TrieLogManager trieLogManager) {
    super(blockchainQueries, trieLogManager);
  }

  @Override
  public String getName() {
    return RpcMethod.DEBUG_GET_MODIFIED_ACCOUNTS_BY_NUMBER.getMethodName();
  }

  @Override
  protected Optional<BlockHeader> findHeader(final JsonRpcRequestContext request, final int index) {
    return blockchainQueries.getBlockchain().getBlockHeader(blockNumber(request, index));
  }

  /**
   * Renders a block number for an error message. The start block is hex and the end block is
   * decimal matching the geth format.
   */
  @Override
  protected String blockId(final JsonRpcRequestContext request, final int index) {
    final long number = blockNumber(request, index);
    return index == 0 ? Long.toHexString(number) : Long.toString(number);
  }

  private long blockNumber(final JsonRpcRequestContext request, final int index) {
    final BlockParameter blockParameter;
    try {
      blockParameter = request.getRequiredParameter(index, BlockParameter.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block number parameter (index " + index + ")",
          RpcErrorType.INVALID_BLOCK_NUMBER_PARAMS,
          e);
    }
    if (!blockParameter.isNumeric()) {
      throw new InvalidJsonRpcParameters(
          "Invalid block number parameter (index " + index + ")",
          RpcErrorType.INVALID_BLOCK_NUMBER_PARAMS);
    }
    return blockParameter.getNumber().get();
  }
}
