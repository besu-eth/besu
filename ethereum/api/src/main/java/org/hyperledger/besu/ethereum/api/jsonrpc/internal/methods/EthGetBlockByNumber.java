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
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlockResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlockResultFactory;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.plugin.data.SyncStatus;

import java.util.Optional;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EthGetBlockByNumber extends AbstractBlockParameterMethod {

  private final BlockResultFactory blockResult;
  private final boolean includeCoinbase;
  private static final Logger LOGGER = LoggerFactory.getLogger(EthGetBlockByNumber.class);
  private final Synchronizer synchronizer;

  public EthGetBlockByNumber(
      final BlockchainQueries blockchain,
      final BlockResultFactory blockResult,
      final Synchronizer synchronizer) {
    this(Suppliers.ofInstance(blockchain), blockResult, synchronizer, false);
  }

  public EthGetBlockByNumber(
      final Supplier<BlockchainQueries> blockchain,
      final BlockResultFactory blockResult,
      final Synchronizer synchronizer,
      final boolean includeCoinbase) {
    super(blockchain);
    this.blockResult = blockResult;
    this.synchronizer = synchronizer;
    this.includeCoinbase = includeCoinbase;
  }

  @Override
  public String getName() {
    return RpcMethod.ETH_GET_BLOCK_BY_NUMBER.getMethodName();
  }

  @Override
  protected BlockParameter blockParameter(final JsonRpcRequestContext request) {
    try {
      return request.getRequiredParameter(0, BlockParameter.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block parameter (index 0)", RpcErrorType.INVALID_BLOCK_NUMBER_PARAMS, e);
    }
  }

  @Override
  protected Object resultByBlockNumber(
      final JsonRpcRequestContext request, final long blockNumber) {
    if (isCompleteTransactions(request)) {
      return transactionComplete(blockNumber);
    }

    return transactionHash(blockNumber);
  }

  @Override
  protected Object latestResult(final JsonRpcRequestContext request) {

    final long headBlockNumber = blockchainQueriesSupplier.get().headBlockNumber();
    BlockHeader headHeader = blockchainQueriesSupplier.get().headBlockHeader();

    Hash block = headHeader.getHash();
    Hash stateRoot = headHeader.getStateRoot();

    if (blockchainQueriesSupplier
        .get()
        .getWorldStateArchive()
        .isWorldStateAvailable(stateRoot, block)) {
      final Optional<SyncStatus> maybeSyncStatus = this.synchronizer.getSyncStatus();
      if (maybeSyncStatus.isEmpty()) { // we are already in sync
        return resultByBlockNumber(request, headBlockNumber);
      } else { // out of sync, return highest pulled block
        long headishBlock = maybeSyncStatus.get().getCurrentBlock();
        return resultByBlockNumber(request, headishBlock);
      }
    }

    LOGGER.trace("no world state available for block {} returning genesis", headBlockNumber);
    return resultByBlockNumber(
        request,
        blockchainQueriesSupplier.get().getBlockchain().getGenesisBlock().getHeader().getNumber());
  }

  private BlockResult transactionComplete(final long blockNumber) {
    return getBlockchainQueries()
        .blockByNumber(blockNumber)
        .map(tx -> blockResult.transactionComplete(tx, includeCoinbase))
        .orElse(null);
  }

  private BlockResult transactionHash(final long blockNumber) {
    return getBlockchainQueries()
        .blockByNumberWithTxHashes(blockNumber)
        .map(tx -> blockResult.transactionHash(tx, includeCoinbase))
        .orElse(null);
  }

  private boolean isCompleteTransactions(final JsonRpcRequestContext request) {
    try {
      return request.getRequiredParameter(1, Boolean.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid return complete transaction parameter (index 1)",
          RpcErrorType.INVALID_RETURN_COMPLETE_TRANSACTION_PARAMS,
          e);
    }
  }
}
