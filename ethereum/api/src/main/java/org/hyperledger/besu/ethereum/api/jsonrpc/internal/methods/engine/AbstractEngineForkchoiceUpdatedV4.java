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

import static java.util.stream.Collectors.toList;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.AMSTERDAM;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BOGOTA;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.CANCUN;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.INVALID;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.SYNCING;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.VALID;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.WithdrawalsValidatorProvider.getWithdrawalsValidator;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator.ForkchoiceResult;
import org.hyperledger.besu.consensus.merge.blockcreation.PayloadIdentifier;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EngineForkchoiceUpdatedParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EnginePayloadAttributesParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.WithdrawalParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EngineUpdateForkchoiceResult;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggingEventBuilder;

/**
 * Abstract base for engine_forkchoiceUpdated implementations targeting the post execution-apis #786
 * spec (Amsterdam+). Differs from {@link AbstractEngineForkchoiceUpdated} in two ways:
 *
 * <ol>
 *   <li>The no-reorg "skip update" optimization (paris.md point 2) is narrowed to fire only when
 *       the head is an ancestor of the latest known finalized block (rather than any ancestor of
 *       the canonical head).
 *   <li>A new {@code -38006: Too deep reorg} error is returned when the requested reorg depth
 *       exceeds {@link MergeMiningCoordinator#MAX_REORG_DEPTH}.
 * </ol>
 */
public abstract class AbstractEngineForkchoiceUpdatedV4 extends ExecutionEngineJsonRpcMethod {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractEngineForkchoiceUpdatedV4.class);

  /** The merge mining coordinator. */
  protected final MergeMiningCoordinator mergeCoordinator;

  /** Cancun activation timestamp, if scheduled. */
  protected final Optional<Long> cancunMilestone;

  /** Amsterdam activation timestamp, if scheduled. */
  protected final Optional<Long> amsterdamMilestone;

  protected final Optional<Long> bogotaMilestone;

  /**
   * Instantiates a new V4-spec abstract engine forkchoice updated.
   *
   * @param vertx the vertx
   * @param protocolSchedule the protocol schedule
   * @param protocolContext the protocol context
   * @param mergeCoordinator the merge coordinator
   * @param engineCallListener the engine call listener
   */
  public AbstractEngineForkchoiceUpdatedV4(
      final Vertx vertx,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final MergeMiningCoordinator mergeCoordinator,
      final EngineCallListener engineCallListener) {
    super(vertx, protocolSchedule, protocolContext, engineCallListener);

    this.mergeCoordinator = mergeCoordinator;
    cancunMilestone = protocolSchedule.milestoneFor(CANCUN);
    amsterdamMilestone = protocolSchedule.milestoneFor(AMSTERDAM);
    bogotaMilestone = protocolSchedule.milestoneFor(BOGOTA);
  }

  /**
   * Validates the FCU parameter shape. Override to enforce version-specific requirements.
   *
   * @param forkchoiceUpdatedParameter the FCU parameter
   * @param maybePayloadAttributes the optional payload attributes
   * @return the validation result
   */
  protected ValidationResult<RpcErrorType> validateParameter(
      final EngineForkchoiceUpdatedParameter forkchoiceUpdatedParameter,
      final Optional<EnginePayloadAttributesParameter> maybePayloadAttributes) {
    return ValidationResult.valid();
  }

  @Override
  public JsonRpcResponse syncResponse(final JsonRpcRequestContext requestContext) {
    engineCallListener.executionEngineCalled();

    final Object requestId = requestContext.getRequest().getId();

    final EngineForkchoiceUpdatedParameter forkChoice;
    try {
      forkChoice = requestContext.getRequiredParameter(0, EngineForkchoiceUpdatedParameter.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid engine forkchoice updated parameter (index 0)",
          RpcErrorType.INVALID_ENGINE_FORKCHOICE_UPDATED_PARAMS,
          e);
    }

    final Optional<EnginePayloadAttributesParameter> maybePayloadAttributes;
    try {
      maybePayloadAttributes =
          requestContext.getOptionalParameter(1, EnginePayloadAttributesParameter.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid engine payload attributes parameter (index 1)",
          RpcErrorType.INVALID_ENGINE_FORKCHOICE_UPDATED_PAYLOAD_ATTRIBUTES,
          e);
    }

    LOG.debug("Forkchoice parameters {}", forkChoice);
    mergeContext
        .get()
        .fireNewUnverifiedForkchoiceEvent(
            forkChoice.getHeadBlockHash(),
            forkChoice.getSafeBlockHash(),
            forkChoice.getFinalizedBlockHash());

    if (mergeCoordinator.isBadBlock(forkChoice.getHeadBlockHash())) {
      logAtInfoFCUCall(INVALID, forkChoice);
      return new JsonRpcSuccessResponse(
          requestId,
          new EngineUpdateForkchoiceResult(
              INVALID,
              mergeCoordinator
                  .getLatestValidHashOfBadBlock(forkChoice.getHeadBlockHash())
                  .orElse(Hash.ZERO),
              null,
              Optional.of(forkChoice.getHeadBlockHash() + " is an invalid block")));
    }

    final Optional<BlockHeader> maybeNewHead =
        mergeCoordinator.getOrSyncHeadByHash(
            forkChoice.getHeadBlockHash(), forkChoice.getFinalizedBlockHash());

    if (maybeNewHead.isEmpty() || mergeContext.get().isSyncing()) {
      return syncingResponse(requestId, forkChoice);
    }

    if (!isValidForkchoiceState(
        forkChoice.getSafeBlockHash(), forkChoice.getFinalizedBlockHash(), maybeNewHead.get())) {
      logAtInfoFCUCall(INVALID, forkChoice);
      return new JsonRpcErrorResponse(requestId, RpcErrorType.INVALID_FORKCHOICE_STATE);
    }

    // https://github.com/ethereum/execution-apis/pull/786
    // paris.md point 6: reject when the implied reorg depth exceeds the
    // implementation-specific limit.
    final OptionalLong reorgDepth = mergeCoordinator.computeReorgDepth(maybeNewHead.get());
    if (reorgDepth.isPresent() && reorgDepth.getAsLong() > MergeMiningCoordinator.MAX_REORG_DEPTH) {
      LOG.atWarn()
          .setMessage("Rejecting forkchoiceUpdated: reorg depth {} exceeds limit {}")
          .addArgument(reorgDepth::getAsLong)
          .addArgument(MergeMiningCoordinator.MAX_REORG_DEPTH)
          .log();
      logAtInfoFCUCall(INVALID, forkChoice);
      return new JsonRpcErrorResponse(requestId, RpcErrorType.TOO_DEEP_REORG);
    }

    // https://github.com/ethereum/execution-apis/pull/786
    // paris.md point 2: skip the update only when the new head is an
    // ancestor of the latest known finalized block.
    if (mergeCoordinator.isAncestorOfFinalized(forkChoice.getHeadBlockHash())) {
      logAtInfoFCUCall(VALID, forkChoice);
      return new JsonRpcSuccessResponse(
          requestId,
          new EngineUpdateForkchoiceResult(
              VALID, forkChoice.getHeadBlockHash(), null, Optional.empty()));
    }

    final ForkchoiceResult forkchoiceResult =
        mergeCoordinator.updateForkChoiceWithoutLegacySkip(
            maybeNewHead.get(), forkChoice.getFinalizedBlockHash(), forkChoice.getSafeBlockHash());

    Optional<List<Withdrawal>> withdrawals = Optional.empty();
    if (maybePayloadAttributes.isPresent()) {
      final EnginePayloadAttributesParameter payloadAttributes = maybePayloadAttributes.get();
      withdrawals =
          maybePayloadAttributes.flatMap(
              pa ->
                  Optional.ofNullable(pa.getWithdrawals())
                      .map(
                          ws ->
                              ws.stream()
                                  .map(WithdrawalParameter::toWithdrawal)
                                  .collect(toList())));
      Optional<JsonRpcErrorResponse> maybeError =
          isPayloadAttributesValid(requestId, payloadAttributes);
      if (maybeError.isPresent()) {
        LOG.atWarn()
            .setMessage("RpcError {}: {}")
            .addArgument(maybeError.get().getErrorType())
            .addArgument(
                () ->
                    maybePayloadAttributes
                        .map(EnginePayloadAttributesParameter::serialize)
                        .orElse(null))
            .log();
        return maybeError.get();
      }
    }

    final BlockHeader newHead = maybeNewHead.get();
    if (maybePayloadAttributes.isPresent()) {
      Optional<JsonRpcErrorResponse> maybeError =
          isPayloadAttributeRelevantToNewHead(requestId, maybePayloadAttributes.get(), newHead);
      if (maybeError.isPresent()) {
        return maybeError.get();
      }
      if (!getWithdrawalsValidator(
              protocolSchedule.get(), newHead, maybePayloadAttributes.get().getTimestamp())
          .validateWithdrawals(withdrawals)) {
        return new JsonRpcErrorResponse(requestId, getInvalidPayloadAttributesError());
      }
    }

    ValidationResult<RpcErrorType> parameterValidationResult =
        validateParameter(forkChoice, maybePayloadAttributes);
    if (!parameterValidationResult.isValid()) {
      return new JsonRpcSuccessResponse(requestId, parameterValidationResult);
    }

    maybePayloadAttributes.ifPresentOrElse(
        this::logPayload, () -> LOG.debug("Payload attributes are null"));

    if (forkchoiceResult.shouldNotProceedToPayloadBuildProcess()) {
      logAtInfoFCUCall(INVALID, forkChoice);
      return handleNonValidForkchoiceUpdate(requestId, forkchoiceResult);
    }

    // begin preparing a block if we have a non-empty payload attributes param
    final Optional<List<Withdrawal>> finalWithdrawals = withdrawals;
    Optional<PayloadIdentifier> payloadId =
        maybePayloadAttributes.map(
            payloadAttributes ->
                mergeCoordinator.preparePayload(
                    newHead,
                    payloadAttributes.getTimestamp(),
                    payloadAttributes.getPrevRandao(),
                    payloadAttributes.getSuggestedFeeRecipient(),
                    finalWithdrawals,
                    Optional.ofNullable(payloadAttributes.getParentBeaconBlockRoot()),
                    Optional.ofNullable(payloadAttributes.getSlotNumber())));

    payloadId.ifPresent(
        pid ->
            LOG.atDebug()
                .setMessage("returning identifier {} for requested payload {}")
                .addArgument(pid::toHexString)
                .addArgument(
                    () -> maybePayloadAttributes.map(EnginePayloadAttributesParameter::serialize))
                .log());

    logAtInfoFCUCall(VALID, forkChoice);
    return new JsonRpcSuccessResponse(
        requestId,
        new EngineUpdateForkchoiceResult(
            VALID,
            forkchoiceResult.getNewHead().map(BlockHeader::getHash).orElse(null),
            payloadId.orElse(null),
            Optional.empty()));
  }

  /**
   * Validates payload attributes for the specific FCU version.
   *
   * @param requestId the request id
   * @param payloadAttribute the payload attribute
   * @return optional error response if invalid
   */
  protected abstract Optional<JsonRpcErrorResponse> isPayloadAttributesValid(
      final Object requestId, final EnginePayloadAttributesParameter payloadAttribute);

  /**
   * Validates that payload attributes are relevant for the new head (e.g. timestamp ordering).
   *
   * @param requestId the request id
   * @param payloadAttributes the payload attributes
   * @param headBlockHeader the head block header
   * @return optional error response if not relevant
   */
  protected Optional<JsonRpcErrorResponse> isPayloadAttributeRelevantToNewHead(
      final Object requestId,
      final EnginePayloadAttributesParameter payloadAttributes,
      final BlockHeader headBlockHeader) {

    if (payloadAttributes.getTimestamp() <= headBlockHeader.getTimestamp()) {
      LOG.warn(
          "Payload attributes timestamp is smaller than timestamp of header in fork choice update");
      return Optional.of(new JsonRpcErrorResponse(requestId, getInvalidPayloadAttributesError()));
    }

    return Optional.empty();
  }

  private JsonRpcResponse handleNonValidForkchoiceUpdate(
      final Object requestId, final ForkchoiceResult result) {
    final Optional<Hash> latestValid = result.getLatestValid();

    // IGNORE_UPDATE_TO_OLD_HEAD is unreachable here: updateForkChoiceWithoutLegacySkip never emits
    // it, and the narrowed skip is handled before the FCU call.
    if (result.getStatus() == ForkchoiceResult.Status.INVALID) {
      return new JsonRpcSuccessResponse(
          requestId,
          new EngineUpdateForkchoiceResult(
              INVALID, latestValid.orElse(null), null, result.getErrorMessage()));
    }
    throw new AssertionError(
        "ForkchoiceResult.Status "
            + result.getStatus()
            + " not handled in EngineForkchoiceUpdatedV4.handleNonValidForkchoiceUpdate");
  }

  private void logPayload(final EnginePayloadAttributesParameter payloadAttributes) {
    String message = "payloadAttributes: timestamp: {}, prevRandao: {}, suggestedFeeRecipient: {}";
    LoggingEventBuilder builder =
        LOG.atDebug()
            .setMessage(message)
            .addArgument(payloadAttributes::getTimestamp)
            .addArgument(() -> payloadAttributes.getPrevRandao().toHexString())
            .addArgument(
                () -> payloadAttributes.getSuggestedFeeRecipient().getBytes().toHexString());
    if (payloadAttributes.getWithdrawals() != null) {
      message += ", withdrawals: {}";
      builder =
          builder
              .setMessage(message)
              .addArgument(
                  payloadAttributes.getWithdrawals().stream()
                      .map(WithdrawalParameter::toString)
                      .collect(Collectors.joining(", ", "[", "]")));
    }
    if (payloadAttributes.getParentBeaconBlockRoot() != null) {
      message += ", parentBeaconBlockRoot: {}";
      builder =
          builder
              .setMessage(message)
              .addArgument(() -> payloadAttributes.getParentBeaconBlockRoot().toHexString());
    }
    builder.log();
  }

  private boolean isValidForkchoiceState(
      final Hash safeBlockHash, final Hash finalizedBlockHash, final BlockHeader newBlock) {
    Optional<BlockHeader> maybeFinalizedBlock = Optional.empty();

    if (!finalizedBlockHash.getBytes().isZero()) {
      maybeFinalizedBlock = protocolContext.getBlockchain().getBlockHeader(finalizedBlockHash);

      // if the finalized block hash is not zero, we always need to have its block, because we
      // only do this check once we have finished syncing
      if (maybeFinalizedBlock.isEmpty()) {
        return false;
      }

      // a valid finalized block must be an ancestor of the new head
      if (!mergeCoordinator.isDescendantOf(maybeFinalizedBlock.get(), newBlock)) {
        return false;
      }
    }

    // A zero value is only allowed, if the transition block is not yet finalized.
    // Once we have at least one finalized block, the transition block has either been finalized
    // directly or through one of its descendants.
    if (safeBlockHash.getBytes().isZero()) {
      return finalizedBlockHash.getBytes().isZero();
    }

    final Optional<BlockHeader> maybeSafeBlock =
        protocolContext.getBlockchain().getBlockHeader(safeBlockHash);

    // if the safe block hash is not zero, we always need to have its block, because we
    // only do this check once we have finished syncing
    if (maybeSafeBlock.isEmpty()) {
      return false;
    }

    // a valid safe block must be a descendant of the finalized block
    if (maybeFinalizedBlock.isPresent()
        && !mergeCoordinator.isDescendantOf(maybeFinalizedBlock.get(), maybeSafeBlock.get())) {
      return false;
    }

    // a valid safe block must be an ancestor of the new block
    return mergeCoordinator.isDescendantOf(maybeSafeBlock.get(), newBlock);
  }

  private JsonRpcResponse syncingResponse(
      final Object requestId, final EngineForkchoiceUpdatedParameter forkChoice) {

    logAtDebugFCUCall(SYNCING, forkChoice);
    return new JsonRpcSuccessResponse(
        requestId, new EngineUpdateForkchoiceResult(SYNCING, null, null, Optional.empty()));
  }

  /**
   * Returns the rpc error to use for invalid payload attributes.
   *
   * @return the rpc error
   */
  protected RpcErrorType getInvalidPayloadAttributesError() {
    return RpcErrorType.INVALID_PAYLOAD_ATTRIBUTES;
  }

  private static final String LOG_MESSAGE = "FCU({}) | head: {} | safe: {} | finalized: {}";

  private void logAtInfoFCUCall(
      final EngineStatus status, final EngineForkchoiceUpdatedParameter forkChoice) {
    LOG.info(
        LOG_MESSAGE,
        status.name(),
        forkChoice.getHeadBlockHash().toShortLogString(),
        forkChoice.getSafeBlockHash().toShortLogString(),
        forkChoice.getFinalizedBlockHash().toShortLogString());
  }

  private void logAtDebugFCUCall(
      final EngineStatus status, final EngineForkchoiceUpdatedParameter forkChoice) {
    LOG.debug(
        LOG_MESSAGE,
        status.name(),
        forkChoice.getHeadBlockHash(),
        forkChoice.getSafeBlockHash(),
        forkChoice.getFinalizedBlockHash());
  }
}
