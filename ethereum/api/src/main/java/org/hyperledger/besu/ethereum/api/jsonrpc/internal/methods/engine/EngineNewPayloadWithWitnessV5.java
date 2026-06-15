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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.VALID;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EnginePayloadWithWitnessResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.ExecutionWitnessResult;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiExecutionWitnessBuilder;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.Optional;

import io.vertx.core.Vertx;

/**
 * Implements {@code engine_newPayloadWithWitnessV5}: same request/response shape as {@code
 * engine_newPayloadV5} but the success response additionally carries an EIP-8025 execution witness
 * for the imported block.
 *
 * <p>The witness is assembled immediately after the block is imported by reading the trie log and
 * block access list produced during execution (both available via {@link
 * org.hyperledger.besu.ethereum.BlockProcessingOutputs}). If the witness cannot be built — for
 * example because the world-state archive is not path-based — a {@link JsonRpcErrorResponse} with
 * {@code INTERNAL_ERROR} is returned so callers receive a clear error rather than a silently empty
 * witness.
 */
public class EngineNewPayloadWithWitnessV5 extends EngineNewPayloadV5 {

  public EngineNewPayloadWithWitnessV5(
      final Vertx vertx,
      final ProtocolSchedule timestampSchedule,
      final ProtocolContext protocolContext,
      final MergeMiningCoordinator mergeCoordinator,
      final EthPeers ethPeers,
      final EngineCallListener engineCallListener,
      final MetricsSystem metricsSystem) {
    super(
        vertx,
        timestampSchedule,
        protocolContext,
        mergeCoordinator,
        ethPeers,
        engineCallListener,
        metricsSystem);
  }

  @Override
  public String getName() {
    return RpcMethod.ENGINE_NEW_PAYLOAD_WITH_WITNESS_V5.getMethodName();
  }

  /**
   * Builds the EIP-8025 witness and returns either a success response carrying {@link
   * EnginePayloadWithWitnessResult} or a {@link JsonRpcErrorResponse} with {@code INTERNAL_ERROR}
   * when the witness cannot be built or has an empty {@code state} list.
   */
  @Override
  protected JsonRpcResponse buildValidResponse(
      final Object reqId, final Hash latestValidHash, final BlockProcessingResult executionResult) {
    try {
      final BlockHeader blockHeader =
          protocolContext
              .getBlockchain()
              .getBlockHeader(latestValidHash)
              .orElseThrow(
                  () -> new IllegalStateException("Block header not found: " + latestValidHash));
      final BlockHeader parentHeader =
          protocolContext
              .getBlockchain()
              .getBlockHeader(blockHeader.getParentHash())
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "Parent header not found: " + blockHeader.getParentHash()));
      final BonsaiExecutionWitnessBuilder.Witness witness =
          new BonsaiExecutionWitnessBuilder()
              .buildWitness(
                  blockHeader,
                  parentHeader,
                  protocolContext.getWorldStateArchive(),
                  protocolContext.getBlockchain(),
                  executionResult.getYield());
      if (witness.state().isEmpty()) {
        return new JsonRpcErrorResponse(reqId, RpcErrorType.INTERNAL_ERROR);
      }
      return new JsonRpcSuccessResponse(
          reqId,
          new EnginePayloadWithWitnessResult(
              VALID,
              latestValidHash,
              Optional.empty(),
              new ExecutionWitnessResult(witness.state(), witness.codes(), witness.headers())));
    } catch (final IllegalStateException e) {
      return new JsonRpcErrorResponse(reqId, RpcErrorType.INTERNAL_ERROR);
    }
  }
}
