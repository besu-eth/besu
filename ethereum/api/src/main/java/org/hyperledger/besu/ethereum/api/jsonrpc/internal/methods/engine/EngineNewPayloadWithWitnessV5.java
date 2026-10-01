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

import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.VALID;

import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV4;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.NewPayloadRequestParametersV3;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EngineExecutionWitnessResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EnginePayloadWithWitnessResult;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiExecutionWitnessBuilder;

import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements {@code engine_newPayloadWithWitnessV5}: the same request/response shape as {@code
 * engine_newPayloadV5}, except that a successful response additionally carries the EIP-8025
 * execution witness for the imported block.
 *
 * <p>The witness is derived from the EIP-7928 block access list already produced by the single
 * import pass, the same way {@code debug_executionWitness} builds it on an already-imported block.
 * See {@link BonsaiExecutionWitnessBuilder} for the caveat this implies: {@code codes} is
 * over-approximated from the block access list's account changes rather than from instrumented
 * code-read tracking.
 */
public final class EngineNewPayloadWithWitnessV5<
        EP extends ExecutionPayloadV4, NPRP extends NewPayloadRequestParametersV3<? extends EP>>
    extends EngineNewPayloadV5<EP, NPRP> {

  private static final Logger LOG = LoggerFactory.getLogger(EngineNewPayloadWithWitnessV5.class);

  private final boolean witnessSupported;

  public EngineNewPayloadWithWitnessV5(
      final ConstructorArguments constructorArguments,
      final HardforkId minSupportedFork,
      final HardforkId firstUnsupportedFork) {
    super(constructorArguments, minSupportedFork, firstUnsupportedFork);
    this.witnessSupported =
        protocolContext.getWorldStateArchive() instanceof PathBasedWorldStateProvider;
  }

  /**
   * The witness can only be built from a path-based (Bonsai) world state. Otherwise the request is
   * refused before the payload is imported, rather than importing it and then failing to answer.
   */
  @Override
  public JsonRpcResponse syncResponse(final JsonRpcRequestContext requestContext) {
    if (!witnessSupported) {
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(), RpcErrorType.METHOD_NOT_ENABLED);
    }
    return super.syncResponse(requestContext);
  }

  @Override
  protected Logger logger() {
    return LOG;
  }

  @Override
  public String getName() {
    return RpcMethod.ENGINE_NEW_PAYLOAD_WITH_WITNESS_V5.getMethodName();
  }

  @Override
  protected JsonRpcResponse respondWithValid(
      final Object requestId,
      final ExecutionPayloadV1 param,
      final BlockHeader newBlockHeader,
      final BlockProcessingResult executionResult) {
    final Hash validHash = newBlockHeader.getHash();
    final Optional<BlockAccessList> blockAccessList =
        executionResult.getYield().flatMap(BlockProcessingOutputs::getBlockAccessList);
    if (blockAccessList.isEmpty()) {
      LOG.debug("Witness data unavailable for imported block {}", validHash);
      return new JsonRpcErrorResponse(requestId, RpcErrorType.INTERNAL_ERROR);
    }
    final Map<Long, Hash> accessedAncestors =
        executionResult
            .getYield()
            .map(BlockProcessingOutputs::getAccessedAncestors)
            .orElse(Map.of());

    final BonsaiExecutionWitnessBuilder.Witness witness;
    try {
      witness =
          new BonsaiExecutionWitnessBuilder(
                  protocolContext.getWorldStateArchive(), protocolContext.getBlockchain())
              .buildWitness(newBlockHeader, blockAccessList.get(), accessedAncestors);
    } catch (final RuntimeException e) {
      // the payload is valid: its block was validated and imported above. Not being able to build
      // the witness is a problem on Besu's side, so it is reported as an internal error
      LOG.warn("Failed to build execution witness for block {}", validHash, e);
      return new JsonRpcErrorResponse(requestId, RpcErrorType.INTERNAL_ERROR);
    }
    if (witness.state().isEmpty()) {
      LOG.debug("Empty witness state for imported block {}", validHash);
      return new JsonRpcErrorResponse(requestId, RpcErrorType.INTERNAL_ERROR);
    }

    logNewPayloadResponse(param, validHash, VALID);
    return new JsonRpcSuccessResponse(
        requestId,
        new EnginePayloadWithWitnessResult(
            VALID,
            validHash,
            Optional.empty(),
            new EngineExecutionWitnessResult(witness.state(), witness.codes(), witness.headers())));
  }
}
