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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameterOrBlockHash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.ExecutionWitnessResult;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiExecutionWitnessBuilder;

import java.util.Optional;

/**
 * Implements {@code debug_executionWitness}: reconstructs the EIP-8025 execution witness for a
 * previously-imported block by re-executing it against the persisted parent world state.
 *
 * <p>Re-execution is required (rather than reading a stored witness) because the {@code
 * BLOCKHASH}-accessed ancestor set is only observable at execution time and is not persisted
 * separately. The re-execution result carries the accessed-ancestor map via {@link
 * BlockProcessingOutputs#getAccessedAncestors()}, which {@link BonsaiExecutionWitnessBuilder} uses
 * to populate the {@code headers} list.
 *
 * <p>Error responses:
 *
 * <ul>
 *   <li>{@link RpcErrorType#BLOCK_NOT_FOUND} — the requested block or its parent header is not in
 *       the local chain (e.g. genesis, which has no on-chain parent).
 *   <li>{@link RpcErrorType#INTERNAL_ERROR} — block re-execution failed, or the world-state archive
 *       is not path-based (Bonsai), or the resulting witness has an empty {@code state} list.
 * </ul>
 */
public class DebugExecutionWitness extends AbstractBlockParameterOrBlockHashMethod {

  private final ProtocolContext protocolContext;
  private final ProtocolSchedule protocolSchedule;

  public DebugExecutionWitness(
      final BlockchainQueries blockchainQueries,
      final ProtocolContext protocolContext,
      final ProtocolSchedule protocolSchedule) {
    super(blockchainQueries);
    this.protocolContext = protocolContext;
    this.protocolSchedule = protocolSchedule;
  }

  @Override
  public String getName() {
    return RpcMethod.DEBUG_EXECUTION_WITNESS.getMethodName();
  }

  /** Extracts the block identifier (hash or tag) from request parameter index 0. */
  @Override
  protected BlockParameterOrBlockHash blockParameterOrBlockHash(
      final JsonRpcRequestContext request) {
    try {
      return request.getRequiredParameter(0, BlockParameterOrBlockHash.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block parameter (index 0)", RpcErrorType.INVALID_BLOCK_PARAMS, e);
    }
  }

  /**
   * Re-executes the block identified by {@code blockHash} against its parent world state, then
   * delegates witness construction to {@link BonsaiExecutionWitnessBuilder}. Returns a {@link
   * org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.ExecutionWitnessResult} on success,
   * or a {@link org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse}
   * if the block or parent is missing, re-execution fails, or the witness is empty.
   */
  @Override
  protected Object resultByBlockHash(final JsonRpcRequestContext request, final Hash blockHash) {
    final Object reqId = request.getRequest().getId();
    final Blockchain blockchain = getBlockchainQueries().getBlockchain();

    // Genesis has no on-chain parent, so it cannot be re-executed and is surfaced as not found.
    final Optional<Block> maybeBlock = blockchain.getBlockByHash(blockHash);
    if (maybeBlock.isEmpty()) {
      return new JsonRpcErrorResponse(reqId, RpcErrorType.BLOCK_NOT_FOUND);
    }
    final Block block = maybeBlock.get();
    final BlockHeader blockHeader = block.getHeader();

    final Optional<BlockHeader> maybeParent =
        blockchain.getBlockHeader(blockHeader.getParentHash());
    if (maybeParent.isEmpty()) {
      return new JsonRpcErrorResponse(reqId, RpcErrorType.BLOCK_NOT_FOUND);
    }
    final BlockHeader parentHeader = maybeParent.get();

    // Re-execution is needed to capture the BLOCKHASH-accessed
    // ancestor set, which is not persisted and must be observed at execution time.
    final BlockProcessingResult result =
        protocolSchedule
            .getByBlockHeader(blockHeader)
            .getBlockValidator()
            .validateAndProcessBlock(
                protocolContext,
                block,
                HeaderValidationMode.NONE,
                HeaderValidationMode.NONE,
                blockchain.getBlockAccessList(blockHash),
                false,
                false);

    if (!result.isSuccessful()) {
      return new JsonRpcErrorResponse(reqId, RpcErrorType.INTERNAL_ERROR);
    }

    final BonsaiExecutionWitnessBuilder.Witness witness;
    try {
      witness =
          new BonsaiExecutionWitnessBuilder()
              .buildWitness(
                  blockHeader,
                  parentHeader,
                  getBlockchainQueries().getWorldStateArchive(),
                  blockchain,
                  result.getYield());
      if (witness.state().isEmpty()) {
        return new JsonRpcErrorResponse(reqId, RpcErrorType.INTERNAL_ERROR);
      }
    } catch (final IllegalStateException e) {
      return new JsonRpcErrorResponse(reqId, RpcErrorType.INTERNAL_ERROR);
    }
    return new ExecutionWitnessResult(witness.state(), witness.codes(), witness.headers());
  }
}
