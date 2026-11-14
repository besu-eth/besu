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

import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.OSAKA;

import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId;
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
import org.hyperledger.besu.ethereum.mainnet.BlockExecutionContext;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListFactory;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiExecutionWitnessBuilder;
import org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements {@code debug_executionWitness}: reconstructs the EIP-8025 execution witness for a
 * previously-imported block by re-executing it against the persisted parent world state. Only
 * blocks from Osaka on are supported.
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
   * if the block or parent is missing, the block predates Osaka, re-execution fails, or the witness
   * is empty.
   */
  @Override
  protected Object resultByBlockHash(final JsonRpcRequestContext request, final Hash blockHash) {
    final Object reqId = request.getRequest().getId();

    // Genesis has no on-chain parent, so it cannot be re-executed and is surfaced as not found.
    final Optional<Block> maybeBlock = blockchain.getBlockByHash(blockHash);
    if (maybeBlock.isEmpty()) {
      return new JsonRpcErrorResponse(reqId, RpcErrorType.BLOCK_NOT_FOUND);
    }

    final Block block = maybeBlock.get();
    final BlockHeader blockHeader = block.getHeader();
    if (!isWitnessSupported(blockHeader)) {
      return new JsonRpcErrorResponse(reqId, RpcErrorType.UNSUPPORTED_FORK);
    }

    // The parent block must be present in order to re-execute the block against its parent state.
    final Optional<BlockHeader> parentHeader =
        blockchain.getBlockHeader(blockHeader.getParentHash());
    if (parentHeader.isEmpty()) {
      return new JsonRpcErrorResponse(reqId, RpcErrorType.BLOCK_NOT_FOUND);
    }
    final WorldStateQueryParams worldStateQueryParams =
        WorldStateQueryParams.newBuilder()
            .withBlockHeader(parentHeader.get())
            .withShouldWorldStateUpdateHead(false)
            .build();

    try (final var worldState =
        protocolContext.getWorldStateArchive().getWorldState(worldStateQueryParams).orElse(null)) {

      final BlockProcessingResult result = processBlock(block, worldState);

      if (!result.isSuccessful() || result.getYield().isEmpty()) {
        return new JsonRpcErrorResponse(reqId, RpcErrorType.INTERNAL_ERROR);
      }
      final BlockProcessingOutputs outputs = result.getYield().get();
      final BonsaiExecutionWitnessBuilder.Witness witness;
      // The block access list is required for witness generation
      final BlockAccessList blockAccessList =
          outputs
              .getBlockAccessList()
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "block access list is required for witness generation but was absent for block "
                              + blockHeader.getHash()));
      final Map<Long, Hash> accessedAncestors = outputs.getAccessedAncestors();
      final BonsaiExecutionWitnessBuilder witnessBuilder =
          new BonsaiExecutionWitnessBuilder(
              getBlockchainQueries().getWorldStateArchive(), blockchain);
      witness = witnessBuilder.buildWitness(blockHeader, blockAccessList, accessedAncestors);
      if (witness.state().isEmpty()) {
        LOG.error("Empty witness state for block {}", blockHeader.getHash());
        return new JsonRpcErrorResponse(reqId, RpcErrorType.INTERNAL_ERROR);
      }
      return new ExecutionWitnessResult(witness.state(), witness.codes(), witness.headers());
    } catch (Exception ex) {
      LOG.error("Failed to build execution witness for block {}", blockHeader.getHash(), ex);
      return new JsonRpcErrorResponse(reqId, RpcErrorType.INTERNAL_ERROR);
    }
  }

  /**
   * Re-executes {@code block} on its parent's state to collect what the witness is built from: the
   * block access list, which records the accounts, storage and code the block touched, and the
   * ancestors its BLOCKHASH calls resolved.
   *
   * @param block the block to re-execute
   * @param worldState the state of the block's parent, which the execution modifies
   * @return the result of the execution, whose outputs carry the block access list and the accessed
   *     ancestors
   */
  private BlockProcessingResult processBlock(
      final Block block, final MutableWorldState worldState) {
    return protocolSchedule
        .getByBlockHeader(block.getHeader())
        .getBlockProcessor()
        .processBlock(
            BlockExecutionContext.builder()
                .protocolContext(protocolContext)
                .worldState(worldState)
                .block(block)
                .blockAccessList(Optional.empty())
                .blockAccessListFactoryProvider(DebugExecutionWitness::blockAccessListFactory)
                .build());
  }

  /**
   * Returns the block access list factory to re-execute a block of {@code protocolSpec} with. The
   * witness needs a block access list, which forks before Amsterdam do not define, so they get a
   * default factory; a fork that defines one keeps its own.
   *
   * @param protocolSpec the protocol spec of the block
   * @return the fork's block access list factory, or a default one
   */
  private static Optional<BlockAccessListFactory> blockAccessListFactory(
      final ProtocolSpec protocolSpec) {
    return protocolSpec
        .getBlockAccessListFactory()
        .or(() -> Optional.of(new BlockAccessListFactory()));
  }

  /**
   * Returns whether this method serves a witness for {@code blockHeader}: only blocks of Osaka or a
   * later fork are supported. The block's own fork decides, so a chain that schedules a later fork
   * without Osaka, such as Amsterdam alone, is still supported.
   *
   * @param blockHeader the header of the block
   * @return {@code true} if the block runs under Osaka or a later mainnet fork
   */
  private boolean isWitnessSupported(final BlockHeader blockHeader) {
    final HardforkId fork = protocolSchedule.getByBlockHeader(blockHeader).getHardforkId();
    return fork instanceof MainnetHardforkId mainnetFork && mainnetFork.compareTo(OSAKA) >= 0;
  }
}
