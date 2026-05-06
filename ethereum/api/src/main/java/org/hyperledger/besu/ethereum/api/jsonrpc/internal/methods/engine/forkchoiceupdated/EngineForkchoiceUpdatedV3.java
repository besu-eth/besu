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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.engine.PayloadAttributesV3;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;

import io.vertx.core.Vertx;

/**
 * {@code engine_forkchoiceUpdatedV3} — Cancun (Beacon Block Root).
 *
 * <p>Extends V2 with {@link PayloadAttributesV3}, adding the mandatory {@code
 * parentBeaconBlockRoot} field. Also introduces strict null-checking on all three forkchoice state
 * hashes ({@code -32602}).
 *
 * <p>Parameterized so that V4 can extend this class while narrowing the payload type.
 */
public sealed class EngineForkchoiceUpdatedV3<PA extends PayloadAttributesV3>
    extends EngineForkchoiceUpdatedV2<PA> permits EngineForkchoiceUpdatedV4 {

  public EngineForkchoiceUpdatedV3(
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
    return RpcMethod.ENGINE_FORKCHOICE_UPDATED_V3.getMethodName();
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Class<PA> getPayloadAttributesClass() {
    return (Class<PA>) PayloadAttributesV3.class;
  }

  /**
   * V3 requires {@code parentBeaconBlockRoot} to be present. Delegates to V2 for any prior-version
   * checks, then adds its own.
   *
   * <p>{@code PA} is bounded to {@link PayloadAttributesV3}, so {@code getParentBeaconBlockRoot()}
   * is available without a cast.
   */
  @Override
  protected ValidationResult<RpcErrorType> validatePayloadAttributes(
      final BlockHeader newHead, final PA attrs) {
    final ValidationResult<RpcErrorType> r = super.validatePayloadAttributes(newHead, attrs);
    return r.isValid() ? validatePayloadAttributesV3(attrs) : r;
  }

  private ValidationResult<RpcErrorType> validatePayloadAttributesV3(final PA attrs) {
    if (attrs.getParentBeaconBlockRoot().isEmpty()) {
      return ValidationResult.invalid(
          getInvalidPayloadAttributesError(), "Missing parentBeaconBlockRoot");
    }
    if (attrs.getTimestamp() == 0) {
      return ValidationResult.invalid(getInvalidPayloadAttributesError(), "Zero timestamp");
    }
    return ValidationResult.valid();
  }

  @Override
  protected void setPreparePayloadArgs(
      final PreparePayloadArgsBuilder preparePayloadArgsBuilder, final PA attrs) {
    super.setPreparePayloadArgs(preparePayloadArgsBuilder, attrs);
    preparePayloadArgsBuilder.parentBeaconBlockRoot(attrs.getParentBeaconBlockRoot());
  }
}
