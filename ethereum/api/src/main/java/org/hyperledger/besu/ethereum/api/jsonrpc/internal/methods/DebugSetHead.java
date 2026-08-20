/*
 * Copyright contributors to Hyperledger Besu.
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

import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.UNKNOWN_BLOCK;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameterOrBlockHash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;

import java.util.Optional;

import graphql.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebugSetHead extends AbstractBlockParameterOrBlockHashMethod {
  private final ProtocolContext protocolContext;
  private static final Logger LOG = LoggerFactory.getLogger(DebugSetHead.class);
  private static final int DEFAULT_MAX_TRIE_LOGS_TO_ROLL_AT_ONCE = 32;

  private final long maxTrieLogsToRollAtOnce;

  public DebugSetHead(final BlockchainQueries blockchain, final ProtocolContext protocolContext) {
    this(blockchain, protocolContext, DEFAULT_MAX_TRIE_LOGS_TO_ROLL_AT_ONCE);
  }

  @VisibleForTesting
  DebugSetHead(
      final BlockchainQueries blockchain,
      final ProtocolContext protocolContext,
      final long maxTrieLogsToRollAtOnce) {
    super(blockchain);
    this.protocolContext = protocolContext;
    this.maxTrieLogsToRollAtOnce = Math.abs(maxTrieLogsToRollAtOnce);
  }

  @Override
  public String getName() {
    return RpcMethod.DEBUG_SET_HEAD.getMethodName();
  }

  @Override
  protected BlockParameterOrBlockHash blockParameterOrBlockHash(
      final JsonRpcRequestContext requestContext) {
    try {
      return requestContext.getRequiredParameter(0, BlockParameterOrBlockHash.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block or block hash parameter (index 0)", RpcErrorType.INVALID_BLOCK_PARAMS, e);
    }
  }

  @Override
  protected Object resultByBlockHash(final JsonRpcRequestContext request, final Hash blockHash) {
    var blockchainQueries = getBlockchainQueries();
    var blockchain = protocolContext.getBlockchain();
    Optional<BlockHeader> maybeBlockHeader = blockchainQueries.getBlockHeaderByHash(blockHash);
    Optional<Boolean> maybeMoveWorldstate = shouldMoveWorldstate(request);

    if (maybeBlockHeader.isEmpty()) {
      return new JsonRpcErrorResponse(request.getRequest().getId(), UNKNOWN_BLOCK);
    }

    // Optionally move the worldstate to the specified blockhash, if it is present in the chain
    if (maybeMoveWorldstate.orElse(Boolean.FALSE)) {
      var archive = blockchainQueries.getWorldStateArchive();

      // Only BonsaiWorldState's need to be moved:
      if (archive instanceof BonsaiWorldStateProvider pathBasedArchive) {
        if (rollIncrementally(maybeBlockHeader.get(), blockchain, pathBasedArchive)) {
          return JsonRpcSuccessResponse.SUCCESS_RESULT;
        }
      }
    }

    // If we are not rolling incrementally or if there was an error incrementally rolling,
    // move the blockchain to the requested hash:
    blockchain.rewindToBlock(maybeBlockHeader.get().getBlockHash());

    return JsonRpcSuccessResponse.SUCCESS_RESULT;
  }

  private boolean rollIncrementally(
      final BlockHeader target,
      final MutableBlockchain blockchain,
      final BonsaiWorldStateProvider archive) {

    try {
      if (archive.isWorldStateAvailable(target.getStateRoot(), target.getBlockHash())) {
        // WARNING, this can be dangerous for a PathBasedWorldstate if a concurrent
        //          process attempts to move or modify the head worldstate.
        //          Ensure no block processing is occuring when using this feature.
        //          No engine-api, block import, sync, mining or other rpc calls should be running.

        Optional<BlockHeader> currentHead =
            archive
                .getWorldStateKeyValueStorage()
                .getWorldStateBlockHash()
                .flatMap(blockchain::getBlockHeader);

        while (currentHead.isPresent()
            && !target.getStateRoot().equals(currentHead.get().getStateRoot())) {
          long delta = currentHead.get().getNumber() - target.getNumber();

          if (maxTrieLogsToRollAtOnce < Math.abs(delta)) {
            // do we need to move forward or backward?
            long distanceToMove = (delta > 0) ? -maxTrieLogsToRollAtOnce : maxTrieLogsToRollAtOnce;

            // Add distanceToMove to the current block number to get the interim target header
            var interimHead =
                blockchain.getBlockHeader(currentHead.get().getNumber() + distanceToMove);

            interimHead.ifPresent(
                it -> {
                  blockchain.rewindToBlock(it.getBlockHash());
                  archive.getWorldState(withBlockHeaderAndUpdateNodeHead(it));
                  LOG.info("incrementally rolled worldstate to {}", it.toLogString());
                });
            currentHead = interimHead;

          } else {
            blockchain.rewindToBlock(target.getBlockHash());
            archive.getWorldState(withBlockHeaderAndUpdateNodeHead(target));
            currentHead = Optional.of(target);
            LOG.info("finished rolling worldstate to {}", target.toLogString());
          }
        }
      }

      return true;
    } catch (Exception ex) {
      LOG.error("Failed to incrementally roll blockchain to " + target.toLogString(), ex);
      return false;
    }
  }

  private Optional<Boolean> shouldMoveWorldstate(final JsonRpcRequestContext request) {
    try {
      return request.getOptionalParameter(1, Boolean.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid should move worldstate boolean parameter (index 1)",
          RpcErrorType.INVALID_PARAMS,
          e);
    }
  }
}
