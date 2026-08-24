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

import org.hyperledger.besu.consensus.merge.blockcreation.PreparePayloadArgsBuilder;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ForkchoiceUpdatedRequestParametersV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ForkchoiceUpdatedRequestParametersV2;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.PayloadAttributesV4;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code engine_forkchoiceUpdatedV4} — Amsterdam (EIP-7843 Slot Number, EIP-8070 Custody Columns).
 *
 * <p>Extends V3 with {@link PayloadAttributesV4}, adding the mandatory {@code slotNumber} field.
 * Also adds an optional {@code custodyColumns} 3rd top-level request parameter (EIP-8070), handled
 * via {@link #readRequestParameters(JsonRpcRequestContext)} / {@link
 * #applyUnverifiedRequestParameters(ForkchoiceUpdatedRequestParametersV2)} rather than a {@code
 * syncResponse} override — see {@link EngineForkchoiceUpdatedV1}'s class for the parameter-parsing
 * pattern this follows.
 *
 * <p>Parameterized so that a future V5 can extend this class without modifying it (beyond updating
 * the upper-bound fork in the public constructor below).
 *
 * <h3>Adding V5</h3>
 *
 * <ol>
 *   <li>Create {@code PayloadAttributesV5 extends PayloadAttributesV4} with the new field.
 *   <li>Create {@code EngineForkchoiceUpdatedV5 extends
 *       EngineForkchoiceUpdatedV4<PayloadAttributesV5, ...>}.
 *   <li>Update the public constructor below: change {@code Optional.empty()} to {@code
 *       Optional.of(V5_FORK)}.
 * </ol>
 */
public final class EngineForkchoiceUpdatedV4<
        PA extends PayloadAttributesV4,
        FRP extends ForkchoiceUpdatedRequestParametersV2<? extends PA>>
    extends EngineForkchoiceUpdatedV3<PA, FRP> {

  private static final Logger LOG = LoggerFactory.getLogger(EngineForkchoiceUpdatedV4.class);
  private static final int CUSTODY_COLUMNS_BYTE_LENGTH = 16;

  private final TransactionPool transactionPool;

  @Override
  protected Logger logger() {
    return LOG;
  }

  public EngineForkchoiceUpdatedV4(
      final ConstructorArguments constructorArguments,
      final HardforkId minFork,
      final HardforkId maxFork) {
    super(constructorArguments, minFork, maxFork);
    this.transactionPool = constructorArguments.transactionPool();
  }

  @Override
  public String getName() {
    return RpcMethod.ENGINE_FORKCHOICE_UPDATED_V4.getMethodName();
  }

  /**
   * V4 adds an optional {@code custodyColumns} 3rd parameter (Amsterdam / EIP-8070): reads and
   * shape-validates it (16 bytes if present), decorating V1..V3's request parameters.
   */
  @Override
  @SuppressWarnings("unchecked")
  protected FRP readRequestParameters(final JsonRpcRequestContext requestContext) {
    final ForkchoiceUpdatedRequestParametersV1<? extends PA> requestParameters =
        super.readRequestParameters(requestContext);
    final Optional<Bytes> custodyColumns = readCustodyColumns(requestContext);
    return (FRP) new ForkchoiceUpdatedRequestParametersV2<>(requestParameters, custodyColumns);
  }

  private Optional<Bytes> readCustodyColumns(final JsonRpcRequestContext requestContext) {
    final Optional<Bytes> custodyColumns;
    try {
      custodyColumns = requestContext.getOptionalParameter(2, Bytes.class);
    } catch (JsonRpcParameter.JsonRpcParameterException e) {
      throw new InvalidRequestParametersException(
          "Invalid custodyColumns parameter (index 2)",
          RpcErrorType.INVALID_CUSTODY_COLUMNS_PARAMS,
          e);
    }
    // If custodyColumns is provided (non-null), the following rules apply:
    // custodyColumns MUST be a 16-byte DATA value. If it is not, the client software MUST return
    // -32602: Invalid params.
    if (custodyColumns.isPresent() && custodyColumns.get().size() != CUSTODY_COLUMNS_BYTE_LENGTH) {
      throw new InvalidRequestParametersException(
          "custodyColumns must be %d bytes, got %d"
              .formatted(CUSTODY_COLUMNS_BYTE_LENGTH, custodyColumns.get().size()),
          RpcErrorType.INVALID_CUSTODY_COLUMNS_PARAMS);
    }
    return custodyColumns;
  }

  /**
   * Adopts {@code custodyColumns} into {@link TransactionPool} when present. Custody-set adoption
   * MUST NOT affect the main forkchoice-update processing flow, so failures here are logged and
   * swallowed rather than propagated.
   */
  @Override
  protected void applyUnverifiedRequestParameters(final FRP requestParameters) {
    super.applyUnverifiedRequestParameters(requestParameters);
    // The Execution client MUST run custody set update independently to the fork choice update,
    // i.e. execution time errors occurred during custody set update MUST NOT affect the main
    // processing flow of this method.
    requestParameters
        .custodyColumns()
        .ifPresent(
            custodyColumns -> {
              try {
                transactionPool.updateBlobCustodyColumns(custodyColumns);
              } catch (final RuntimeException e) {
                logger().warn("Failed to adopt updated blob custody columns", e);
              }
            });
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
    if (attrs.getSlotNumber() == null || attrs.getSlotNumber() < 0) {
      return ValidationResult.invalid(
          RpcErrorType.INVALID_SLOT_NUMBER_PARAMS, "Invalid slotNumber");
    }
    if (attrs.getTargetGasLimit() == null) {
      return ValidationResult.invalid(
          RpcErrorType.INVALID_TARGET_GAS_LIMIT_PARAMS, "Missing target gas limit field");
    }
    return ValidationResult.valid();
  }

  @Override
  protected void setPreparePayloadArgs(
      final PreparePayloadArgsBuilder preparePayloadArgsBuilder, final PA attrs) {
    super.setPreparePayloadArgs(preparePayloadArgsBuilder, attrs);
    preparePayloadArgsBuilder.slotNumber(attrs.getSlotNumber());
    preparePayloadArgsBuilder.targetGasLimit(attrs.getTargetGasLimit());
  }
}
