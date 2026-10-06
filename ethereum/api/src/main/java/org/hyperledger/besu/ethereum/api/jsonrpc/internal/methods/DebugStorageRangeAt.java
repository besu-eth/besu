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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameterOrBlockHash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.BlockReplay;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.Tracer;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.Tracer.TraceableState;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.DebugStorageRangeAtResult;
import org.hyperledger.besu.ethereum.api.query.BlockWithMetadata;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.TransactionWithMetadata;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.AccountStorageEntry;

import java.util.Collections;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import org.apache.tuweni.bytes.Bytes32;

public class DebugStorageRangeAt implements JsonRpcMethod {

  private final Supplier<BlockchainQueries> blockchainQueries;
  private final Supplier<BlockReplay> blockReplay;
  private final boolean shortValues;

  public DebugStorageRangeAt(
      final BlockchainQueries blockchainQueries, final BlockReplay blockReplay) {
    this(Suppliers.ofInstance(blockchainQueries), Suppliers.ofInstance(blockReplay), false);
  }

  public DebugStorageRangeAt(
      final Supplier<BlockchainQueries> blockchainQueries,
      final Supplier<BlockReplay> blockReplay,
      final boolean shortValues) {
    this.blockchainQueries = blockchainQueries;
    this.blockReplay = blockReplay;
    this.shortValues = shortValues;
  }

  @Override
  public String getName() {
    return RpcMethod.DEBUG_STORAGE_RANGE_AT.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    final BlockParameterOrBlockHash blockParameterOrBlockHash;
    try {
      blockParameterOrBlockHash =
          requestContext.getRequiredParameter(0, BlockParameterOrBlockHash.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block or block hash parameter (index 0)", RpcErrorType.INVALID_BLOCK_PARAMS, e);
    }
    final int transactionIndex;
    try {
      transactionIndex = requestContext.getRequiredParameter(1, Integer.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid transaction index parameter (index 1)",
          RpcErrorType.INVALID_TRANSACTION_INDEX_PARAMS,
          e);
    }
    final Address accountAddress;
    try {
      accountAddress = requestContext.getRequiredParameter(2, Address.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid account address parameter (index 2)", RpcErrorType.INVALID_ADDRESS_PARAMS, e);
    }
    final Hash startKey;
    try {
      startKey = Hash.fromHexStringLenient(requestContext.getRequiredParameter(3, String.class));
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid data start hash parameter (index 3)", RpcErrorType.INVALID_DATA_HASH_PARAMS, e);
    }
    final int limit;
    try {
      limit = requestContext.getRequiredParameter(4, Integer.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid limit parameter (index 4)", RpcErrorType.INVALID_TRANSACTION_LIMIT_PARAMS, e);
    }

    final Optional<Hash> blockHashOptional = hashFromParameter(blockParameterOrBlockHash);
    if (blockHashOptional.isEmpty()) {
      return emptyResponse(requestContext);
    }
    final Hash blockHash = blockHashOptional.get();
    final Optional<BlockHeader> blockHeaderOptional =
        blockchainQueries.get().blockByHash(blockHash).map(BlockWithMetadata::getHeader);
    if (blockHeaderOptional.isEmpty()) {
      return emptyResponse(requestContext);
    }

    final Optional<TransactionWithMetadata> maybeTransactionIndex =
        blockchainQueries.get().transactionByBlockHashAndIndex(blockHash, transactionIndex);

    return Tracer.processTracing(
            blockchainQueries.get(),
            Optional.of(blockHeaderOptional.get()),
            mutableWorldState -> {
              if (maybeTransactionIndex.isEmpty()) {
                return Optional.of(
                    extractStorageAt(
                        requestContext, accountAddress, startKey, limit, mutableWorldState));
              } else {
                return blockReplay
                    .get()
                    .afterTransactionInBlock(
                        mutableWorldState,
                        blockHash,
                        maybeTransactionIndex.get().getTransaction().getHash(),
                        (tx, txIndex, blk, chain, processor, gasPrice) ->
                            extractStorageAt(
                                requestContext,
                                accountAddress,
                                startKey,
                                limit,
                                mutableWorldState));
              }
            })
        .orElse(emptyResponse(requestContext));
  }

  private Optional<Hash> hashFromParameter(final BlockParameterOrBlockHash blockParameter) {
    if (blockParameter.isEarliest()) {
      return blockchainQueries.get().getBlockHashByNumber(0);
    } else if (blockParameter.isLatest() || blockParameter.isPending()) {
      return blockchainQueries
          .get()
          .latestBlockWithTxHashes()
          .map(block -> block.getHeader().getHash());
    } else if (blockParameter.isNumeric()) {
      return blockchainQueries.get().getBlockHashByNumber(blockParameter.getNumber().getAsLong());
    } else {
      return blockParameter.getHash();
    }
  }

  private JsonRpcSuccessResponse extractStorageAt(
      final JsonRpcRequestContext requestContext,
      final Address accountAddress,
      final Hash startKey,
      final int limit,
      final TraceableState worldState) {
    final Account account = worldState.get(accountAddress);
    final NavigableMap<Bytes32, AccountStorageEntry> entries =
        account.storageEntriesFrom(Bytes32.wrap(startKey.getBytes()), limit + 1);

    Bytes32 nextKey = null;
    if (entries.size() == limit + 1) {
      nextKey = entries.lastKey();
      entries.remove(nextKey);
    }
    return new JsonRpcSuccessResponse(
        requestContext.getRequest().getId(),
        new DebugStorageRangeAtResult(entries, nextKey, shortValues));
  }

  private JsonRpcSuccessResponse emptyResponse(final JsonRpcRequestContext requestContext) {
    return new JsonRpcSuccessResponse(
        requestContext.getRequest().getId(),
        new DebugStorageRangeAtResult(Collections.emptyNavigableMap(), null, shortValues));
  }
}
