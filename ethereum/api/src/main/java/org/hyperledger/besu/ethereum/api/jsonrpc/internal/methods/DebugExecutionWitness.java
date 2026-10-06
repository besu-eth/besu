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
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiExecutionWitnessBuilder;

import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements {@code debug_executionWitness}: reconstructs the EIP-8025 execution witness for a
 * previously-imported block by re-executing it against the persisted parent world state.
 */
public class DebugExecutionWitness extends AbstractBlockParameterOrBlockHashMethod {

  private static final Logger LOG = LoggerFactory.getLogger(DebugExecutionWitness.class);

  private final ProtocolContext protocolContext;
  private final ProtocolSchedule protocolSchedule;
  private final Blockchain blockchain;

  public DebugExecutionWitness(
      final BlockchainQueries blockchainQueries,
      final ProtocolContext protocolContext,
      final ProtocolSchedule protocolSchedule) {
    super(blockchainQueries);
    this.protocolContext = protocolContext;
    this.protocolSchedule = protocolSchedule;
    blockchain = getBlockchainQueries().getBlockchain();
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
   * Re-executes the block identified by {@code blockHash}, then delegates witness construction to
   * {@link BonsaiExecutionWitnessBuilder}. Returns a {@link
   * org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.ExecutionWitnessResult} on success,
   * or a {@link org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse}
   * if the block or parent is missing, re-execution fails, or the witness is empty.
   */
  @Override
  protected Object resultByBlockHash(final JsonRpcRequestContext request, final Hash blockHash) {
    final Object reqId = request.getRequest().getId();

    // Genesis has no on-chain parent, so it cannot be re-executed and is surfaced as not found.
    final Optional<Block> maybeBlock = blockchain.getBlockByHash(blockHash);
    if (maybeBlock.isEmpty()) {
      return new JsonRpcErrorResponse(reqId, RpcErrorType.BLOCK_NOT_FOUND);
    }

    // The parent block must be present in order to re-execute the block against its parent state.
    final Block block = maybeBlock.get();
    final BlockHeader blockHeader = block.getHeader();
    if (blockchain.getBlockHeader(blockHeader.getParentHash()).isEmpty()) {
      return new JsonRpcErrorResponse(reqId, RpcErrorType.BLOCK_NOT_FOUND);
    }

    // Re-execute the block against its parent state. Validation is skipped (NONE/NONE) because the
    // block is already imported. Re-execution is what yields the two things the witness needs and
    // the database does not hold: the block access list, from which the codes are derived, and the
    // ancestors the BLOCKHASH lookup resolved. Both arrive on BlockProcessingOutputs.
    // shouldPersist=false keeps the world state unchanged; shouldRecordBadBlock=false suppresses
    // bad-block storage for what is known to be a valid, imported block.
    final BlockProcessingResult result =
        protocolSchedule
            .getByBlockHeader(blockHeader)
            .getBlockValidator()
            .validateAndProcessBlock(
                protocolContext,
                block,
                HeaderValidationMode.NONE,
                HeaderValidationMode.NONE,
                Optional.empty(),
                false,
                false);

    if (!result.isSuccessful()) {
      return new JsonRpcErrorResponse(reqId, RpcErrorType.INTERNAL_ERROR);
    }

    final BonsaiExecutionWitnessBuilder.Witness witness;
    try {
      // The block access list is required for witness generation
      final BlockAccessList blockAccessList =
          result
              .getYield()
              .flatMap(BlockProcessingOutputs::getBlockAccessList)
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "block access list is required for witness generation but was absent for block "
                              + blockHeader.getHash()));

      final Map<Long, Hash> accessedAncestors =
          result
              .getYield()
              .map(BlockProcessingOutputs::getAccessedAncestors)
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "block processing produced no yield for block " + blockHeader.getHash()));

      final BonsaiExecutionWitnessBuilder witnessBuilder =
          new BonsaiExecutionWitnessBuilder(
              getBlockchainQueries().getWorldStateArchive(), blockchain);
      witness = witnessBuilder.buildWitness(blockHeader, blockAccessList, accessedAncestors);
    } catch (final IllegalStateException e) {
      LOG.error("Failed to build execution witness for block {}", blockHeader.getHash(), e);
      return new JsonRpcErrorResponse(reqId, RpcErrorType.INTERNAL_ERROR);
    }

    if (witness.state().isEmpty()) {
      LOG.error("Empty witness state for block {}", blockHeader.getHash());
      return new JsonRpcErrorResponse(reqId, RpcErrorType.INTERNAL_ERROR);
    }
    return new ExecutionWitnessResult(witness.state(), witness.codes(), witness.headers());
  }
}
