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

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.consensus.merge.blockcreation.PreparePayloadArgsBuilder;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineCallListener;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.WithdrawalsValidatorProvider;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.engine.PayloadAttributesV2;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;

import java.util.Optional;

import io.vertx.core.Vertx;

/**
 * {@code engine_forkchoiceUpdatedV2} — Shanghai (Withdrawals).
 *
 * <p>Extends V1 with {@link PayloadAttributesV2}, introducing the {@code withdrawals} field.
 * Validates withdrawals presence/absence per fork via {@link WithdrawalsValidatorProvider}, and
 * passes extracted withdrawals to {@code preparePayload}.
 *
 * <p>Parameterized so that V3 can extend this class while narrowing the payload type.
 */
public sealed class EngineForkchoiceUpdatedV2<PA extends PayloadAttributesV2>
    extends EngineForkchoiceUpdatedV1<PA> permits EngineForkchoiceUpdatedV3 {

  public EngineForkchoiceUpdatedV2(
      final Vertx vertx,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final MergeMiningCoordinator mergeCoordinator,
      final EngineCallListener engineCallListener,
      final HardforkId minFork,
      final HardforkId maxFork) {
    super(
        vertx,
        protocolSchedule,
        protocolContext,
        mergeCoordinator,
        engineCallListener,
        minFork,
        maxFork);
  }

  @Override
  public String getName() {
    return RpcMethod.ENGINE_FORKCHOICE_UPDATED_V2.getMethodName();
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Class<PA> getPayloadAttributesClass() {
    return (Class<PA>) PayloadAttributesV2.class;
  }

  @Override
  protected ValidationResult<RpcErrorType> validatePayloadAttributes(
      final BlockHeader newHead, final PA attrs) {
    final ValidationResult<RpcErrorType> r = super.validatePayloadAttributes(newHead, attrs);
    return r.isValid() ? validatePayloadAttributesV2(newHead, attrs) : r;
  }

  private ValidationResult<RpcErrorType> validatePayloadAttributesV2(
      final BlockHeader newHead, final PayloadAttributesV2 attrs) {
    return WithdrawalsValidatorProvider.getWithdrawalsValidator(
                protocolSchedule.get(), newHead, attrs.getTimestamp())
            .validateWithdrawals(Optional.of(attrs.getWithdrawals()))
        ? ValidationResult.valid()
        : ValidationResult.invalid(getInvalidPayloadAttributesError(), "Invalid withdrawals");
  }

  @Override
  protected void setPreparePayloadArgs(
      final PreparePayloadArgsBuilder preparePayloadArgsBuilder, final PA attrs) {
    super.setPreparePayloadArgs(preparePayloadArgsBuilder, attrs);
    preparePayloadArgsBuilder.withdrawals(attrs.getWithdrawals());
  }
}
