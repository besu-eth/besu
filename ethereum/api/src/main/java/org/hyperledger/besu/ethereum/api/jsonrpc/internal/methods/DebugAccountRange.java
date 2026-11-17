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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.DebugAccountRangeAtResult;
import org.hyperledger.besu.ethereum.api.query.BlockWithMetadata;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.evm.worldstate.WorldState.StreamableAccount;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.base.Suppliers;
import org.apache.tuweni.bytes.Bytes32;

public class DebugAccountRange implements JsonRpcMethod {

  private final Supplier<BlockchainQueries> blockchainQueries;

  public DebugAccountRange(final BlockchainQueries blockchainQueries) {
    this(Suppliers.ofInstance(blockchainQueries));
  }

  public DebugAccountRange(final Supplier<BlockchainQueries> blockchainQueries) {
    this.blockchainQueries = blockchainQueries;
  }

  @Override
  public String getName() {
    return RpcMethod.DEBUG_ACCOUNT_RANGE.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    final BlockParameterOrBlockHash blockParameterOrBlockHash;
    try {
      blockParameterOrBlockHash =
          requestContext.getRequiredParameter(0, BlockParameterOrBlockHash.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block parameter or block hash parameter (index 0)",
          RpcErrorType.INVALID_BLOCK_PARAMS,
          e);
    }
    final String addressHash;
    try {
      addressHash = requestContext.getRequiredParameter(2, String.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid address hash parameter (index 2)", RpcErrorType.INVALID_ADDRESS_HASH_PARAMS, e);
    }
    final int maxResults;
    try {
      maxResults = requestContext.getRequiredParameter(3, Integer.TYPE);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid max results parameter (index 3)", RpcErrorType.INVALID_MAX_RESULTS_PARAMS, e);
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

    // TODO deal with mid-block locations
    return blockchainQueries
        .get()
        .getAndMapWorldState(
            blockHeaderOptional.get().getNumber(),
            state -> {
              final List<StreamableAccount> accounts =
                  state
                      .streamAccounts(Bytes32.fromHexStringLenient(addressHash), maxResults + 1)
                      .collect(Collectors.toList());
              Bytes32 nextKey = Bytes32.ZERO;
              if (accounts.size() == maxResults + 1) {
                nextKey = Bytes32.wrap(accounts.get(maxResults).getAddressHash().getBytes());
                accounts.remove(maxResults);
              }

              return Optional.of(
                  new JsonRpcSuccessResponse(
                      requestContext.getRequest().getId(),
                      new DebugAccountRangeAtResult(
                          accounts.stream()
                              .collect(
                                  Collectors.toMap(
                                      account -> account.getAddressHash().toString(),
                                      account ->
                                          account.getAddress().orElse(Address.ZERO).toString())),
                          nextKey.toString())));
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
    } else if (blockParameter.isFinalized()) {
      return blockchainQueries.get().finalizedBlockHeader().map(BlockHeader::getHash);
    } else if (blockParameter.isSafe()) {
      return blockchainQueries.get().safeBlockHeader().map(BlockHeader::getHash);
    } else if (blockParameter.isNumeric()) {
      return blockchainQueries.get().getBlockHashByNumber(blockParameter.getNumber().getAsLong());
    } else {
      return blockParameter.getHash();
    }
  }

  private JsonRpcSuccessResponse emptyResponse(final JsonRpcRequestContext requestContext) {
    return new JsonRpcSuccessResponse(
        requestContext.getRequest().getId(),
        new DebugAccountRangeAtResult(Collections.emptyNavigableMap(), null));
  }
}
