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

import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.INVALID;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV2;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.NewPayloadRequestParametersV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import io.vertx.core.Vertx;

public sealed class EngineNewPayloadV2<
        EP extends ExecutionPayloadV2, NPRP extends NewPayloadRequestParametersV1<? extends EP>>
    extends EngineNewPayloadV1<EP, NPRP> permits EngineNewPayloadV3 {

  private final Optional<Long> shanghaiTimestamp;

  public EngineNewPayloadV2(
      final Vertx vertx,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final MergeMiningCoordinator mergeCoordinator,
      final EthPeers ethPeers,
      final EngineCallListener engineCallListener,
      final MetricsSystem metricsSystem,
      final HardforkId minSupportedFork,
      final HardforkId firstUnsupportedFork) {
    super(
        vertx,
        protocolSchedule,
        protocolContext,
        mergeCoordinator,
        ethPeers,
        engineCallListener,
        metricsSystem,
        minSupportedFork,
        firstUnsupportedFork);
    shanghaiTimestamp = protocolSchedule.milestoneFor(HardforkId.MainnetHardforkId.SHANGHAI);
  }

  @Override
  public String getName() {
    return RpcMethod.ENGINE_NEW_PAYLOAD_V2.getMethodName();
  }

  @Override
  protected EngineStatus getInvalidBlockHashStatus() {
    return INVALID;
  }

  @Override
  protected Class<? extends ExecutionPayloadV1> getPayloadParameterClass() {
    return ExecutionPayloadV2.class;
  }

  @Override
  protected ValidationResult<RpcErrorType> validateNewBlock(
      final Block newBlock,
      final ProtocolSpec protocolSpec,
      final BlockHeader parentHeader,
      final NPRP requestParameters) {
    final ValidationResult<RpcErrorType> result =
        super.validateNewBlock(newBlock, protocolSpec, parentHeader, requestParameters);
    return result.isValid() ? validateExecutionPayloadV2(newBlock, protocolSpec) : result;
  }

  private ValidationResult<RpcErrorType> validateExecutionPayloadV2(
      final Block newBlock, final ProtocolSpec protocolSpec) {
    // engine_newPayloadV2 is peculiar since it allows 2 different versions of execution payload,
    // so we need to check the timestamp for withdrawal validation.

    // Spec: executionPayload: instance of ExecutionPayloadV1 | ExecutionPayloadV2, where:
    // ExecutionPayloadV1 MUST be used if the timestamp value is lower than the Shanghai timestamp,
    // ExecutionPayloadV2 MUST be used if the timestamp value is greater or equal to the
    // Shanghai timestamp,
    // Client software MUST return -32602: Invalid params error if the wrong version of the
    // structure is used in the method call.
    if (newBlock.getHeader().getTimestamp() < shanghaiTimestamp.orElse(0L)) {
      if (newBlock.getBody().getWithdrawals().isPresent()) {
        return ValidationResult.invalid(
            RpcErrorType.INVALID_PARAMS,
            "Withdrawals must not be present before Shanghai hardfork");
      }
    } else {
      if (newBlock.getBody().getWithdrawals().isEmpty()) {
        return ValidationResult.invalid(
            RpcErrorType.INVALID_PARAMS, "Withdrawals must be present after Shanghai hardfork");
      }
    }

    return protocolSpec
            .getWithdrawalsValidator()
            .validateWithdrawals(newBlock.getBody().getWithdrawals())
        ? ValidationResult.valid()
        : ValidationResult.invalid(RpcErrorType.INVALID_WITHDRAWALS_PARAMS);
  }

  @Override
  protected void setBlockHeaderFields(
      final BlockHeaderBuilder blockHeaderBuilder, final NPRP requestParameters) {
    super.setBlockHeaderFields(blockHeaderBuilder, requestParameters);
    final ExecutionPayloadV2 executionPayloadV2 = requestParameters.payloadParameter();
    if (executionPayloadV2.getWithdrawals() != null) {
      blockHeaderBuilder.withdrawalsRoot(
          BodyValidation.withdrawalsRoot(executionPayloadV2.getWithdrawals()));
    }
  }

  @Override
  protected BlockBody createBlockBody(final EP executionPayload) {
    return new BlockBody(
        executionPayload.getTransactions(),
        Collections.emptyList(),
        Optional.ofNullable(executionPayload.getWithdrawals()));
  }

  @Override
  protected void appendVersionSpecificLogInfo(
      final StringBuilder message, final List<Object> messageArgs, final Block block) {
    super.appendVersionSpecificLogInfo(message, messageArgs, block);
    block
        .getBody()
        .getWithdrawals()
        .ifPresent(
            withdrawals -> {
              message.append("| %2d ws");
              messageArgs.add(withdrawals.size());
            });
  }
}
