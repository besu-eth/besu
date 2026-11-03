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

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiValue;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog.LogTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public abstract class AbstractDebugGetModifiedAccounts implements JsonRpcMethod {

  protected final BlockchainQueries blockchainQueries;

  protected AbstractDebugGetModifiedAccounts(final BlockchainQueries blockchainQueries) {
    this.blockchainQueries = blockchainQueries;
  }

  @Override
  public abstract String getName();

  protected abstract Optional<BlockHeader> findHeader(
      final JsonRpcRequestContext request, final int index);

  @Override
  public final JsonRpcResponse response(final JsonRpcRequestContext request) {
    final int paramLength = request.getRequest().getParamLength();
    if (paramLength != 1 && paramLength != 2) {
      return new JsonRpcErrorResponse(
          request.getRequest().getId(), RpcErrorType.INVALID_PARAM_COUNT);
    }

    if (!(blockchainQueries.getWorldStateArchive() instanceof PathBasedWorldStateProvider provider)
        || provider.getWorldStateSharedSpec().isTrieDisabled()) {
      return new JsonRpcErrorResponse(
          request.getRequest().getId(), RpcErrorType.METHOD_NOT_ENABLED);
    }
    final TrieLogManager trieLogManager = provider.getTrieLogManager();

    final Optional<BlockHeader> maybeFirstHeader = findHeader(request, 0);
    if (maybeFirstHeader.isEmpty()) {
      return new JsonRpcErrorResponse(request.getRequest().getId(), RpcErrorType.BLOCK_NOT_FOUND);
    }

    final Blockchain blockchain = blockchainQueries.getBlockchain();
    final BlockHeader startHeader;
    final BlockHeader endHeader;
    if (paramLength == 1) {
      endHeader = maybeFirstHeader.get();
      final Optional<BlockHeader> maybeParent =
          blockchain.getBlockHeader(endHeader.getParentHash());
      if (maybeParent.isEmpty()) {
        return new JsonRpcErrorResponse(
            request.getRequest().getId(), RpcErrorType.PARENT_BLOCK_NOT_FOUND);
      }
      startHeader = maybeParent.get();
    } else {
      startHeader = maybeFirstHeader.get();
      final Optional<BlockHeader> maybeEndHeader = findHeader(request, 1);
      if (maybeEndHeader.isEmpty()) {
        return new JsonRpcErrorResponse(request.getRequest().getId(), RpcErrorType.BLOCK_NOT_FOUND);
      }
      endHeader = maybeEndHeader.get();
    }

    if (startHeader.getNumber() >= endHeader.getNumber()) {
      return new JsonRpcErrorResponse(
          request.getRequest().getId(), RpcErrorType.INVALID_BLOCK_RANGE);
    }

    if (endHeader.getNumber() - startHeader.getNumber() > trieLogManager.getMaxLayersToLoad()) {
      return new JsonRpcErrorResponse(
          request.getRequest().getId(), RpcErrorType.EXCEEDS_RPC_MAX_BLOCK_RANGE);
    }

    return collectModifiedAccounts(request, blockchain, trieLogManager, startHeader, endHeader);
  }

  private JsonRpcResponse collectModifiedAccounts(
      final JsonRpcRequestContext request,
      final Blockchain blockchain,
      final TrieLogManager trieLogManager,
      final BlockHeader startHeader,
      final BlockHeader endHeader) {
    final List<Hash> blockHashes = new ArrayList<>();
    BlockHeader header = endHeader;
    while (header.getNumber() > startHeader.getNumber()) {
      blockHashes.add(header.getBlockHash());
      final Optional<BlockHeader> maybeParent = blockchain.getBlockHeader(header.getParentHash());
      if (maybeParent.isEmpty()) {
        return new JsonRpcErrorResponse(
            request.getRequest().getId(), RpcErrorType.PARENT_BLOCK_NOT_FOUND);
      }
      header = maybeParent.get();
    }

    if (!header.getBlockHash().equals(startHeader.getBlockHash())) {
      return new JsonRpcErrorResponse(
          request.getRequest().getId(), RpcErrorType.INVALID_BLOCK_RANGE);
    }

    final Map<Address, BonsaiValue<AccountValue>> accounts = new HashMap<>();
    for (final Hash blockHash : blockHashes) {
      final Optional<TrieLog> maybeTrieLog = trieLogManager.getTrieLogLayer(blockHash);
      if (maybeTrieLog.isEmpty()) {
        return new JsonRpcErrorResponse(
            request.getRequest().getId(), RpcErrorType.WORLD_STATE_UNAVAILABLE);
      }

      final Map<Address, LogTuple<AccountValue>> accountChanges =
          maybeTrieLog.get().getAccountChanges();
      accountChanges.forEach(
          (address, change) ->
              accounts
                  .computeIfAbsent(address, __ -> new BonsaiValue<>(null, change.getUpdated()))
                  .setPrior(change.getPrior()));
    }

    final List<String> modified =
        accounts.entrySet().stream()
            .filter(entry -> !entry.getValue().isUnchanged())
            .map(entry -> entry.getKey().toString())
            .sorted()
            .toList();

    return new JsonRpcSuccessResponse(request.getRequest().getId(), modified);
  }
}
