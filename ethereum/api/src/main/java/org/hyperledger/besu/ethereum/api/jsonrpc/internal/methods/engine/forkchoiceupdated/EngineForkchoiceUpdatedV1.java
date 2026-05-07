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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.forkchoiceupdated;

import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.INVALID;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.SYNCING;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.VALID;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator.ForkchoiceResult;
import org.hyperledger.besu.consensus.merge.blockcreation.PayloadIdentifier;
import org.hyperledger.besu.consensus.merge.blockcreation.PreparePayloadArgsBuilder;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineCallListener;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.ForkSupportHelper;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.engine.ForkchoiceStateV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.engine.PayloadAttributesV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EngineUpdateForkchoiceResult;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;

import java.util.Optional;
import java.util.OptionalLong;

import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code engine_forkchoiceUpdatedV1} — Paris (The Merge).
 *
 * <p>Accepts {@link PayloadAttributesV1} (timestamp, prevRandao, suggestedFeeRecipient). Contains
 * the full FCU implementation; later versions extend and override specific hooks to introduce their
 * own concepts without modifying this class.
 *
 * <p>Parameterized so that V2 can extend this class while narrowing the payload type.
 *
 * @param <PA> the payload-attributes type this version accepts
 */
public sealed class EngineForkchoiceUpdatedV1<PA extends PayloadAttributesV1>
    extends ExecutionEngineJsonRpcMethod permits EngineForkchoiceUpdatedV2 {

  private static final Logger LOG = LoggerFactory.getLogger(EngineForkchoiceUpdatedV1.class);

  protected final MergeMiningCoordinator mergeCoordinator;

  private final Optional<Long> minForkMilestone;
  private final Optional<Long> maxForkMilestone;

  private final HardforkId minSupportedFork;
  private final HardforkId firstUnsupportedFork;

  public EngineForkchoiceUpdatedV1(
      final Vertx vertx,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final MergeMiningCoordinator mergeCoordinator,
      final EngineCallListener engineCallListener,
      final HardforkId minSupportedFork,
      final HardforkId firstUnsupportedFork) {
    super(vertx, protocolSchedule, protocolContext, engineCallListener);
    this.mergeCoordinator = mergeCoordinator;
    this.minSupportedFork = minSupportedFork;
    this.firstUnsupportedFork = firstUnsupportedFork;
    this.minForkMilestone =
        minSupportedFork != null
            ? protocolSchedule.milestoneFor(minSupportedFork)
            : Optional.empty();
    this.maxForkMilestone =
        firstUnsupportedFork != null
            ? protocolSchedule.milestoneFor(firstUnsupportedFork)
            : Optional.empty();
  }

  @Override
  public String getName() {
    return RpcMethod.ENGINE_FORKCHOICE_UPDATED_V1.getMethodName();
  }

  @SuppressWarnings("unchecked")
  protected Class<PA> getPayloadAttributesClass() {
    return (Class<PA>) PayloadAttributesV1.class;
  }

  @Override
  public JsonRpcResponse syncResponse(final JsonRpcRequestContext requestContext) {
    engineCallListener.executionEngineCalled();

    final Object requestId = requestContext.getRequest().getId();

    final ForkchoiceStateV1 forkChoice;
    try {
      forkChoice = requestContext.getRequiredParameter(0, ForkchoiceStateV1.class);
    } catch (JsonRpcParameter.JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid forkchoice state parameter (index 0)",
          RpcErrorType.INVALID_ENGINE_FORKCHOICE_UPDATED_PARAMS,
          e);
    }

    EngineForkchoiceUpdatedV1.LOG.debug("Forkchoice parameters {}", forkChoice);

    final Optional<PA> maybePayloadAttributes;
    try {
      maybePayloadAttributes = requestContext.getOptionalParameter(1, getPayloadAttributesClass());
    } catch (JsonRpcParameter.JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid payload attributes parameter (index 1)",
          RpcErrorType.INVALID_ENGINE_FORKCHOICE_UPDATED_PAYLOAD_ATTRIBUTES,
          e);
    }

    EngineForkchoiceUpdatedV1.LOG.debug("Payload attributers {}", maybePayloadAttributes);

    // Structural parameter check (-32602) — must happen before any FCU processing.
    final ValidationResult<RpcErrorType> structResult = validateForkchoiceStateParams(forkChoice);
    if (!structResult.isValid()) {
      return new JsonRpcSuccessResponse(requestId, structResult);
    }

    if (mergeCoordinator.isBadBlock(forkChoice.getHeadBlockHash())) {
      logFCU(INVALID, forkChoice);
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

    // this event is used to inform initial sync about chain progress
    mergeContext
        .get()
        .fireNewUnverifiedForkchoiceEvent(
            forkChoice.getHeadBlockHash(),
            forkChoice.getSafeBlockHash(),
            forkChoice.getFinalizedBlockHash());

    // 1. Client software MAY initiate a sync process if forkchoiceState.headBlockHash references an
    // unknown payload or a payload that can't be validated because data that are requisite for the
    // validation is missing. The sync process is specified in the Sync section.
    final Optional<BlockHeader> maybeNewHead =
        mergeCoordinator.getOrSyncHeadByHash(
            forkChoice.getHeadBlockHash(), forkChoice.getFinalizedBlockHash());

    if (maybeNewHead.isEmpty() || mergeContext.get().isSyncing()) {
      return syncingResponse(requestId, forkChoice);
    }

    final BlockHeader newHead = maybeNewHead.get();

    // 2. Client software MAY skip an update of the forkchoice state and MUST NOT begin a payload
    // build process if there is a known finalizedBlockHash and forkchoiceState.headBlockHash
    // references a VALID ancestor of the latest known finalized block, i.e. the ancestor passed
    // payload validation process and deemed VALID. In the case of such an event, client software
    if (mergeCoordinator.isAncestorOfFinalized(newHead)) {
      logFCU(VALID, forkChoice);
      return new JsonRpcSuccessResponse(
          requestId, new EngineUpdateForkchoiceResult(VALID, forkChoice.getHeadBlockHash()));
    }

    // 3. TODO? [SKIPPED] If forkchoiceState.headBlockHash references a PoW block, client software
    // MUST validate
    // this block with respect to terminal block conditions according to EIP-3675. This check maps
    // to the transition block validity section of the EIP. Additionally, if this validation fails,
    // client software MUST NOT update the forkchoice state and MUST NOT begin a payload build
    // process.

    // 4. Before updating the forkchoice state, client software MUST ensure the validity of the
    // payload referenced by forkchoiceState.headBlockHash, and MAY validate the payload while
    // processing the call. The validation process is specified in the Payload validation section.
    // If the validation process fails, client software MUST NOT update the forkchoice state and
    // MUST NOT begin a payload build process.

    // Step 4 is implicit since newHead is present, it means it has been already validated
    // in the previous newPayload call

    // 5. Client software MUST return -38002: Invalid forkchoice state error if the payload
    // referenced by forkchoiceState.headBlockHash is VALID and a payload referenced by either
    // forkchoiceState.finalizedBlockHash or forkchoiceState.safeBlockHash does not belong to the
    // chain defined by forkchoiceState.headBlockHash.
    if (!isValidForkchoiceState(
        forkChoice.getSafeBlockHash(), forkChoice.getFinalizedBlockHash(), newHead)) {
      logFCU(INVALID, forkChoice);
      return new JsonRpcErrorResponse(requestId, RpcErrorType.INVALID_FORKCHOICE_STATE);
    }

    // 6. Client software MUST return -38006: Too deep reorg error if the depth of reorg to
    // forkchoiceState.headBlockHash exceeds the limitation specific to the client software.
    final OptionalLong reorgDepth = mergeCoordinator.computeReorgDepth(newHead);
    if (reorgDepth.isPresent() && reorgDepth.getAsLong() > MergeMiningCoordinator.MAX_REORG_DEPTH) {
      EngineForkchoiceUpdatedV1.LOG
          .atWarn()
          .setMessage("Rejecting FCU: reorg depth {} exceeds limit {}")
          .addArgument(reorgDepth::getAsLong)
          .addArgument(MergeMiningCoordinator.MAX_REORG_DEPTH)
          .log();
      logFCU(INVALID, forkChoice);
      return new JsonRpcErrorResponse(requestId, RpcErrorType.TOO_DEEP_REORG);
    }

    // 7. Client software MUST update its forkchoice state if payloads referenced by
    // forkchoiceState.headBlockHash and forkchoiceState.finalizedBlockHash are VALID. The update is
    // specified as follows:
    final MergeMiningCoordinator.ForkchoiceResult forkchoiceResult =
        mergeCoordinator.updateForkChoiceWithoutLegacySkip(
            newHead, forkChoice.getFinalizedBlockHash(), forkChoice.getSafeBlockHash());

    // 8. Client software MUST process provided payloadAttributes after successfully applying the
    // forkchoiceState and only if the payload referenced by forkchoiceState.headBlockHash is VALID.
    // The processing flow is as follows:
    if (forkchoiceResult.shouldNotProceedToPayloadBuildProcess()) {
      logFCU(INVALID, forkChoice);
      return handleNonValidForkchoiceUpdate(requestId, forkchoiceResult);
    }

    PayloadIdentifier payloadId = null;
    if (maybePayloadAttributes.isPresent()) {
      final PA attrs = maybePayloadAttributes.get();

      // Version-specific payload field checks.
      final ValidationResult<RpcErrorType> attrResult = validatePayloadAttributes(newHead, attrs);
      if (!attrResult.isValid()) {
        EngineForkchoiceUpdatedV1.LOG
            .atWarn()
            .setMessage("Invalid payload attributes: {}")
            .addArgument(attrResult::getErrorMessage)
            .log();
        return new JsonRpcErrorResponse(requestId, attrResult);
      }

      // Fork-range check (-38005) is owned here; concrete versions never call
      // ForkSupportHelper directly.
      final ValidationResult<RpcErrorType> forkResult = validateForkRange(attrs.getTimestamp());
      if (!forkResult.isValid()) {
        return new JsonRpcErrorResponse(requestId, forkResult);
      }

      final PreparePayloadArgsBuilder preparePayloadArgsBuilder =
          new PreparePayloadArgsBuilder().parentHeader(newHead);
      setPreparePayloadArgs(preparePayloadArgsBuilder, attrs);
      payloadId = mergeCoordinator.preparePayload(preparePayloadArgsBuilder.build());

      EngineForkchoiceUpdatedV1.LOG
          .atDebug()
          .setMessage("Payload identifier {} for timestamp {}")
          .addArgument(payloadId::toHexString)
          .addArgument(() -> Long.toHexString(attrs.getTimestamp()))
          .log();
    }

    logFCU(VALID, forkChoice);
    return new JsonRpcSuccessResponse(
        requestId,
        new EngineUpdateForkchoiceResult(
            VALID,
            forkchoiceResult.getNewHead().map(BlockHeader::getHash).orElse(null),
            payloadId));
  }

  /** Validates version-specific payload attributes field requirements. */
  protected ValidationResult<RpcErrorType> validatePayloadAttributes(
      final BlockHeader newHead, final PA attrs) {
    return validatePayloadAttributesV1(newHead, attrs);
  }

  private ValidationResult<RpcErrorType> validatePayloadAttributesV1(
      final BlockHeader newHead, final PA attrs) {
    // 8.i. Verify that payloadAttributes.timestamp is greater than timestamp of a block referenced
    // by forkchoiceState.headBlockHash and return -38003: Invalid payload attributes on failure.
    if (attrs.getTimestamp() <= newHead.getTimestamp()) {
      return ValidationResult.invalid(
          getInvalidPayloadAttributesError(),
          "Payload attributes timestamp %d not greater than head block timestamp %d"
              .formatted(attrs.getTimestamp(), newHead.getTimestamp()));
    }
    return ValidationResult.valid();
  }

  /** Returns the error type to use for invalid payload attributes responses. */
  protected RpcErrorType getInvalidPayloadAttributesError() {
    return RpcErrorType.INVALID_PAYLOAD_ATTRIBUTES;
  }

  protected void setPreparePayloadArgs(
      final PreparePayloadArgsBuilder preparePayloadArgsBuilder, final PA attrs) {
    preparePayloadArgsBuilder
        .timestamp(attrs.getTimestamp())
        .prevRandao(attrs.getPrevRandao())
        .feeRecipient(attrs.getSuggestedFeeRecipient());
  }

  /**
   * Validates the structural shape of the forkchoice state parameter. Returns a non-valid result to
   * trigger a {@code -32602: Invalid params} response before any FCU processing occurs.
   *
   * <p>Default: accepts any (non-null) forkchoice state. V3+ overrides to require non-null hashes.
   */
  private ValidationResult<RpcErrorType> validateForkchoiceStateParams(
      final ForkchoiceStateV1 state) {
    if (state.getHeadBlockHash() == null) {
      return ValidationResult.invalid(RpcErrorType.INVALID_PARAMS, "Missing headBlockHash");
    }
    if (state.getSafeBlockHash() == null) {
      return ValidationResult.invalid(RpcErrorType.INVALID_PARAMS, "Missing safeBlockHash");
    }
    if (state.getFinalizedBlockHash() == null) {
      return ValidationResult.invalid(RpcErrorType.INVALID_PARAMS, "Missing finalizedBlockHash");
    }
    return ValidationResult.valid();
  }

  private ValidationResult<RpcErrorType> validateForkRange(final long timestamp) {
    return ForkSupportHelper.validateForkSupported(
        minSupportedFork, minForkMilestone, firstUnsupportedFork, maxForkMilestone, timestamp);
  }

  private boolean isValidForkchoiceState(
      final Hash safeBlockHash, final Hash finalizedBlockHash, final BlockHeader newBlock) {
    final Optional<BlockHeader> maybeFinalizedBlock;

    if (finalizedBlockHash.getBytes().isZero()) {
      maybeFinalizedBlock = Optional.empty();
    } else {
      maybeFinalizedBlock = protocolContext.getBlockchain().getBlockHeader(finalizedBlockHash);
      if (maybeFinalizedBlock.isEmpty()) {
        return false;
      }
      if (!mergeCoordinator.isDescendantOf(maybeFinalizedBlock.get(), newBlock)) {
        return false;
      }
    }

    if (safeBlockHash.getBytes().isZero()) {
      return finalizedBlockHash.getBytes().isZero();
    }

    final Optional<BlockHeader> maybeSafeBlock =
        protocolContext.getBlockchain().getBlockHeader(safeBlockHash);
    if (maybeSafeBlock.isEmpty()) {
      return false;
    }

    if (maybeFinalizedBlock.isPresent()
        && !mergeCoordinator.isDescendantOf(maybeFinalizedBlock.get(), maybeSafeBlock.get())) {
      return false;
    }

    return mergeCoordinator.isDescendantOf(maybeSafeBlock.get(), newBlock);
  }

  private JsonRpcResponse handleNonValidForkchoiceUpdate(
      final Object requestId, final ForkchoiceResult result) {
    final Optional<Hash> latestValid = result.getLatestValid();
    if (result.getStatus() == ForkchoiceResult.Status.INVALID) {
      return new JsonRpcSuccessResponse(
          requestId,
          new EngineUpdateForkchoiceResult(
              INVALID, latestValid.orElse(null), null, result.getErrorMessage()));
    }
    throw new AssertionError(
        "Unexpected ForkchoiceResult.Status: "
            + result.getStatus()
            + " (updateForkChoiceWithoutLegacySkip should not emit IGNORE_UPDATE_TO_OLD_HEAD)");
  }

  private JsonRpcResponse syncingResponse(
      final Object requestId, final ForkchoiceStateV1 forkChoice) {
    LOG.debug(
        "FCU({}) | head: {} | safe: {} | finalized: {}",
        SYNCING.name(),
        forkChoice.getHeadBlockHash(),
        forkChoice.getSafeBlockHash(),
        forkChoice.getFinalizedBlockHash());
    return new JsonRpcSuccessResponse(
        requestId, new EngineUpdateForkchoiceResult(SYNCING, null, null, Optional.empty()));
  }

  private void logFCU(final EngineStatus status, final ForkchoiceStateV1 forkChoice) {
    LOG.info(
        "FCU({}) | head: {} | safe: {} | finalized: {}",
        status.name(),
        forkChoice.getHeadBlockHash().toShortLogString(),
        forkChoice.getSafeBlockHash().toShortLogString(),
        forkChoice.getFinalizedBlockHash().toShortLogString());
  }
}
