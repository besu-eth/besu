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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.engine.PayloadAttributesV4;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;

import io.vertx.core.Vertx;

/**
 * {@code engine_forkchoiceUpdatedV4} — Amsterdam (EIP-7843 Slot Number).
 *
 * <p>Extends V3 with {@link PayloadAttributesV4}, adding the mandatory {@code slotNumber} field.
 *
 * <p>Parameterized so that a future V5 can extend this class without modifying it (beyond updating
 * the upper-bound fork in the public constructor below).
 *
 * <h3>Adding V5</h3>
 *
 * <ol>
 *   <li>Create {@code PayloadAttributesV5 extends PayloadAttributesV4} with the new field.
 *   <li>Create {@code EngineForkchoiceUpdatedV5 extends
 *       EngineForkchoiceUpdatedV4<PayloadAttributesV5>}.
 *   <li>Update the public constructor below: change {@code Optional.empty()} to {@code
 *       Optional.of(V5_FORK)}.
 * </ol>
 */
public final class EngineForkchoiceUpdatedV4<PA extends PayloadAttributesV4>
    extends EngineForkchoiceUpdatedV3<PA> {

  public EngineForkchoiceUpdatedV4(
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
    return RpcMethod.ENGINE_FORKCHOICE_UPDATED_V4.getMethodName();
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Class<PA> getPayloadAttributesClass() {
    return (Class<PA>) PayloadAttributesV4.class;
  }

  /**
   * V4 requires {@code slotNumber} in addition to everything V3 requires. Delegates to V3 first
   * (which checks {@code parentBeaconBlockRoot} and timestamp), then adds its own check.
   *
   * <p>{@code PA} is bounded to {@link PayloadAttributesV4}, so {@code getSlotNumber()} is
   * available without a cast.
   */
  @Override
  protected ValidationResult<RpcErrorType> validatePayloadAttributes(
      final BlockHeader newHead, final PA attrs) {
    final ValidationResult<RpcErrorType> r = super.validatePayloadAttributes(newHead, attrs);
    return r.isValid() ? validatePayloadAttributesV4(attrs) : r;
  }

  private ValidationResult<RpcErrorType> validatePayloadAttributesV4(final PA attrs) {
    if (attrs.getSlotNumber() < 0) {
      return ValidationResult.invalid(
          RpcErrorType.INVALID_SLOT_NUMBER_PARAMS, "Invalid slotNumber");
    }
    return ValidationResult.valid();
  }

  @Override
  protected void setPreparePayloadArgs(
      final PreparePayloadArgsBuilder preparePayloadArgsBuilder, final PA attrs) {
    super.setPreparePayloadArgs(preparePayloadArgsBuilder, attrs);
    preparePayloadArgsBuilder.slotNumber(attrs.getSlotNumber());
  }
}
