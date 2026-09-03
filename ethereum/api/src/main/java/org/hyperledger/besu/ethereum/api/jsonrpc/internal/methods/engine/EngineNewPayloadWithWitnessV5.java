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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.WitnessCodeReads;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV4;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.NewPayloadRequestParametersV3;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EngineExecutionWitnessResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EnginePayloadWithWitnessResult;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.WitnessCodeTracker;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiExecutionWitnessBuilder;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements {@code engine_newPayloadWithWitnessV5}: the same request/response shape as {@code
 * engine_newPayloadV5}, except that a successful response additionally carries the EIP-8025
 * execution witness for the imported block.
 *
 * <p>The witness is collected during the single import pass rather than by re-executing the block:
 * a {@link WitnessCodeTracker} is handed to block processing, which observes every EVM frame —
 * including system-contract calls (EIP-2935, EIP-4788, EIP-7002, EIP-7251) — and surfaces the
 * collected reads on the {@link BlockProcessingResult}.
 */
public final class EngineNewPayloadWithWitnessV5<
        EP extends ExecutionPayloadV4, NPRP extends NewPayloadRequestParametersV3<? extends EP>>
    extends EngineNewPayloadV5<EP, NPRP> {

  private static final Logger LOG = LoggerFactory.getLogger(EngineNewPayloadWithWitnessV5.class);

  public EngineNewPayloadWithWitnessV5(
      final ConstructorArguments constructorArguments,
      final HardforkId minSupportedFork,
      final HardforkId firstUnsupportedFork) {
    super(constructorArguments, minSupportedFork, firstUnsupportedFork);
  }

  @Override
  protected Logger logger() {
    return LOG;
  }

  @Override
  public String getName() {
    return RpcMethod.ENGINE_NEW_PAYLOAD_WITH_WITNESS_V5.getMethodName();
  }

  /** Imports the block with a witness collector attached to block processing. */
  @Override
  protected BlockProcessingResult rememberBlock(final Block block, final EP executionPayload) {
    return mergeCoordinator.rememberBlock(
        block,
        Optional.of(executionPayload.getBlockAccessList()),
        Optional.of(new WitnessCodeTracker()));
  }

  @Override
  protected JsonRpcResponse respondWithSuccess(
      final Object requestId,
      final ExecutionPayloadV1 param,
      final BlockHeader newBlockHeader,
      final BlockProcessingResult executionResult) {
    final Hash validHash = newBlockHeader.getHash();
    final Optional<BlockAccessList> blockAccessList =
        executionResult.getYield().flatMap(BlockProcessingOutputs::getBlockAccessList);
    final Optional<WitnessCodeReads> witnessCodeReads =
        executionResult.getYield().flatMap(BlockProcessingOutputs::getWitnessCodeReads);
    if (blockAccessList.isEmpty() || witnessCodeReads.isEmpty()) {
      LOG.debug("Witness data unavailable for imported block {}", validHash);
      return new JsonRpcErrorResponse(requestId, RpcErrorType.INTERNAL_ERROR);
    }

    try {
      final BonsaiExecutionWitnessBuilder.Witness witness =
          new BonsaiExecutionWitnessBuilder(
                  protocolContext.getWorldStateArchive(), protocolContext.getBlockchain())
              .buildWitness(newBlockHeader, blockAccessList.get(), witnessCodeReads.get());

      if (witness.state().isEmpty()) {
        LOG.debug("Empty witness state for imported block {}", validHash);
        return new JsonRpcErrorResponse(requestId, RpcErrorType.INTERNAL_ERROR);
      }

      return new JsonRpcSuccessResponse(
          requestId,
          new EnginePayloadWithWitnessResult(
              EngineStatus.VALID,
              validHash,
              Optional.empty(),
              new EngineExecutionWitnessResult(
                  witness.state(), witness.codes(), witness.headers())));
    } catch (final IllegalStateException e) {
      LOG.debug("Failed to build execution witness for block {}", validHash, e);
      return new JsonRpcErrorResponse(requestId, RpcErrorType.INTERNAL_ERROR);
    }
  }
}
