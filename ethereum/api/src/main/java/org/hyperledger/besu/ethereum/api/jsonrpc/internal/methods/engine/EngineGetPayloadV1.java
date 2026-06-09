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

import org.hyperledger.besu.consensus.merge.PayloadWrapper;
import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.consensus.merge.blockcreation.PayloadIdentifier;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlockResultFactory;
import org.hyperledger.besu.ethereum.blockcreation.BlockCreationTiming;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockWithReceipts;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;

import java.util.Optional;

import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public sealed class EngineGetPayloadV1 extends ExecutionEngineJsonRpcMethod
    permits EngineGetPayloadV2 {

  private static final Logger LOG = LoggerFactory.getLogger(EngineGetPayloadV1.class);

  private final MergeMiningCoordinator mergeMiningCoordinator;
  protected final BlockResultFactory blockResultFactory;

  public EngineGetPayloadV1(
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final Vertx vertx,
      final EngineCallListener engineCallListener,
      final MergeMiningCoordinator mergeMiningCoordinator,
      final BlockResultFactory blockResultFactory,
      final HardforkId minSupportedFork,
      final HardforkId firstUnsupportedFork) {
    super(
        protocolSchedule,
        protocolContext,
        vertx,
        engineCallListener,
        minSupportedFork,
        firstUnsupportedFork);
    this.mergeMiningCoordinator = mergeMiningCoordinator;
    this.blockResultFactory = blockResultFactory;
  }

  @Override
  public String getName() {
    return RpcMethod.ENGINE_GET_PAYLOAD_V1.getMethodName();
  }

  @Override
  public JsonRpcResponse syncResponse(final JsonRpcRequestContext request) {
    engineCallListener.executionEngineCalled();

    final PayloadIdentifier payloadId;
    try {
      payloadId = request.getRequiredParameter(0, PayloadIdentifier.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid payload ID parameter (index 0)", RpcErrorType.INVALID_PAYLOAD_ID_PARAMS, e);
    }

    Optional<PayloadWrapper> maybePayload = mergeContext.get().retrievePayloadById(payloadId);

    mergeMiningCoordinator.finalizeProposalById(payloadId);

    if (maybePayload.isPresent() && hasOnlyEmptyBlock(maybePayload.get())) {
      LOG.debug(
          "Only empty block available for payload {}, waiting for block building to complete",
          payloadId);
      mergeMiningCoordinator.awaitCurrentBuildCompletion(payloadId);
      maybePayload = mergeContext.get().retrievePayloadById(payloadId);
    }

    if (maybePayload.isPresent()) {
      final PayloadWrapper payload = maybePayload.get();
      final BlockWithReceipts proposal = payload.blockWithReceipts();
      LOG.trace("assembledBlock with receipts {}", proposal);
      ValidationResult<RpcErrorType> forkValidationResult =
          validateForkSupported(proposal.getHeader().getTimestamp());
      if (!forkValidationResult.isValid()) {
        return new JsonRpcErrorResponse(request.getRequest().getId(), forkValidationResult);
      }
      logProducedBlock(
          payload.blockWithReceipts().getBlock(),
          payload.getBlockCreationTimings(),
          payload.payloadIdentifier());
      return createResponse(request, payload);
    }
    return new JsonRpcErrorResponse(request.getRequest().getId(), RpcErrorType.UNKNOWN_PAYLOAD);
  }

  private boolean hasOnlyEmptyBlock(final PayloadWrapper payload) {
    return payload.blockWithReceipts().getBlock().getBody().getTransactions().isEmpty();
  }

  private void logProducedBlock(
      final Block block,
      final BlockCreationTiming blockCreationTiming,
      final PayloadIdentifier payloadIdentifier) {
    LOG.info(
        String.format(
            "Produced #%,d  (%s)| %4d tx%s | %,d (%01.1f%%) gas in %01.3fs | Timing(%s) | PayloadId %s",
            block.getHeader().getNumber(),
            block.getHash().toShortLogString(),
            block.getBody().getTransactions().size(),
            versionSpecificLogInfo(block),
            block.getHeader().getGasUsed(),
            (block.getHeader().getGasUsed() * 100.0) / block.getHeader().getGasLimit(),
            blockCreationTiming.end("awaitingRetrieval").toMillis() / 1000.0,
            blockCreationTiming,
            payloadIdentifier.toHexString()));
  }

  protected String versionSpecificLogInfo(final Block block) {
    return "";
  }

  protected JsonRpcResponse createResponse(
      final JsonRpcRequestContext request, final PayloadWrapper payload) {
    return new JsonRpcSuccessResponse(request.getRequest().getId(), createResponsePayload(payload));
  }

  protected Object createResponsePayload(final PayloadWrapper payload) {
    final Block block = payload.blockWithReceipts().getBlock();
    return new ExecutionPayloadV1(block.getHeader(), block.getBody().getTransactions());
  }
}
