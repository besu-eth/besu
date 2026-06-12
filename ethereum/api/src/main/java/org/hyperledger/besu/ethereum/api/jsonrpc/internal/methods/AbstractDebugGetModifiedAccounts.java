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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;

abstract class AbstractDebugGetModifiedAccounts implements JsonRpcMethod {

  private static final String METHOD_ONLY_SUPPORTED_WITH_BONSAI =
      "This method is only supported with Bonsai world state storage";

  private final Supplier<BlockchainQueries> blockchainQueries;

  AbstractDebugGetModifiedAccounts(final BlockchainQueries blockchainQueries) {
    this(Suppliers.ofInstance(blockchainQueries));
  }

  AbstractDebugGetModifiedAccounts(final Supplier<BlockchainQueries> blockchainQueries) {
    this.blockchainQueries = blockchainQueries;
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    final int parameterCount = requestContext.getRequest().getParamLength();
    if (parameterCount < 1 || parameterCount > 2) {
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(), RpcErrorType.INVALID_PARAM_COUNT);
    }
    final Optional<JsonRpcError> maybeValidationError = validateParameters(requestContext);
    if (maybeValidationError.isPresent()) {
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(), maybeValidationError.get());
    }

    final Optional<BlockHeader> maybeStartBlock = startBlock(requestContext);
    final Optional<BlockHeader> maybeEndBlock = endBlock(requestContext);
    if (maybeStartBlock.isEmpty() || maybeEndBlock.isEmpty()) {
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(), RpcErrorType.BLOCK_NOT_FOUND);
    }

    final BlockHeader startBlock = maybeStartBlock.get();
    final BlockHeader endBlock = maybeEndBlock.get();
    if (startBlock.getNumber() >= endBlock.getNumber()) {
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(),
          new JsonRpcError(
              RpcErrorType.INVALID_BLOCK_PARAMS, "start block must be less than end block"));
    }

    final WorldStateArchive worldStateArchive = blockchainQueries.get().getWorldStateArchive();
    if (!(worldStateArchive instanceof PathBasedWorldStateProvider pathBasedWorldStateProvider)) {
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(),
          new JsonRpcError(RpcErrorType.METHOD_NOT_FOUND, METHOD_ONLY_SUPPORTED_WITH_BONSAI));
    }

    final TrieLogManager trieLogManager = pathBasedWorldStateProvider.getTrieLogManager();
    final HeaderRangeResult headerRangeResult = headersInRange(startBlock, endBlock);
    if (headerRangeResult.errorType().isPresent()) {
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(), headerRangeResult.errorType().get());
    }

    final TreeSet<Address> modifiedAccounts = new TreeSet<>();
    for (final BlockHeader header : headerRangeResult.headers()) {
      final Optional<TrieLog> maybeTrieLog = trieLogManager.getTrieLogLayer(header.getBlockHash());
      if (maybeTrieLog.isEmpty()) {
        return new JsonRpcErrorResponse(
            requestContext.getRequest().getId(), RpcErrorType.WORLD_STATE_UNAVAILABLE);
      }
      addChangedAddresses(modifiedAccounts, maybeTrieLog.get());
    }

    final List<String> result = modifiedAccounts.stream().map(Address::toHexString).toList();
    return new JsonRpcSuccessResponse(requestContext.getRequest().getId(), result);
  }

  protected HeaderRangeResult headersInRange(
      final BlockHeader startBlock, final BlockHeader endBlock) {
    if (rangeSize(startBlock, endBlock) > TrieLogManager.LOG_RANGE_LIMIT) {
      return HeaderRangeResult.error(RpcErrorType.EXCEEDS_RPC_MAX_BLOCK_RANGE);
    }

    final List<BlockHeader> headers = new ArrayList<>();
    for (long blockNumber = startBlock.getNumber() + 1;
        blockNumber <= endBlock.getNumber();
        blockNumber++) {
      final Optional<BlockHeader> maybeHeader =
          blockchainQueries.get().getBlockchain().getBlockHeader(blockNumber);
      if (maybeHeader.isEmpty()) {
        return HeaderRangeResult.error(RpcErrorType.BLOCK_NOT_FOUND);
      }
      headers.add(maybeHeader.get());
    }
    return HeaderRangeResult.success(headers);
  }

  protected long rangeSize(final BlockHeader startBlock, final BlockHeader endBlock) {
    return endBlock.getNumber() - startBlock.getNumber();
  }

  private void addChangedAddresses(final Set<Address> modifiedAccounts, final TrieLog trieLog) {
    modifiedAccounts.addAll(trieLog.getAccountChanges().keySet());
    modifiedAccounts.addAll(trieLog.getCodeChanges().keySet());
    modifiedAccounts.addAll(trieLog.getStorageChanges().keySet());
  }

  protected BlockchainQueries blockchainQueries() {
    return blockchainQueries.get();
  }

  protected Optional<JsonRpcError> validateParameters(final JsonRpcRequestContext requestContext) {
    return Optional.empty();
  }

  protected abstract Optional<BlockHeader> startBlock(JsonRpcRequestContext requestContext);

  protected abstract Optional<BlockHeader> endBlock(JsonRpcRequestContext requestContext);

  protected record HeaderRangeResult(List<BlockHeader> headers, Optional<RpcErrorType> errorType) {
    static HeaderRangeResult success(final List<BlockHeader> headers) {
      return new HeaderRangeResult(headers, Optional.empty());
    }

    static HeaderRangeResult error(final RpcErrorType errorType) {
      return new HeaderRangeResult(List.of(), Optional.of(errorType));
    }
  }
}
