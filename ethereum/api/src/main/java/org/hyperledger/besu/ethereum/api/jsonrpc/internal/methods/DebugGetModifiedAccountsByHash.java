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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;

import java.util.Optional;

public class DebugGetModifiedAccountsByHash extends AbstractDebugGetModifiedAccounts {
  public DebugGetModifiedAccountsByHash(
      final BlockchainQueries blockchainQueries, final TrieLogManager trieLogManager) {
    super(blockchainQueries, trieLogManager);
  }

  @Override
  public String getName() {
    return RpcMethod.DEBUG_GET_MODIFIED_ACCOUNTS_BY_HASH.getMethodName();
  }

  @Override
  protected Optional<BlockHeader> findHeader(final JsonRpcRequestContext request, final int index) {
    return blockchainQueries.getBlockchain().getBlockHeader(blockHash(request, index));
  }

  @Override
  protected String blockId(final JsonRpcRequestContext request, final int index) {
    return blockHash(request, index).getBytes().toUnprefixedHexString();
  }

  private Hash blockHash(final JsonRpcRequestContext request, final int index) {
    try {
      return request.getRequiredParameter(index, Hash.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block hash parameter (index " + index + ")",
          RpcErrorType.INVALID_BLOCK_HASH_PARAMS,
          e);
    }
  }
}
