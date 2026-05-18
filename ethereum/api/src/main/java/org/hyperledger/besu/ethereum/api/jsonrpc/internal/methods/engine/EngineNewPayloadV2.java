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
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.WithdrawalsValidatorProvider.getWithdrawalsValidator;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV2;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.NewPayloadRequestParametersV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.WithdrawalParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import io.vertx.core.Vertx;

public sealed class EngineNewPayloadV2 extends EngineNewPayloadV1 permits EngineNewPayloadV3 {

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
  protected VersionSpecificPayloadData createVersionSpecificPayloadData(
      final NewPayloadRequestParametersV1 requestParameters)
      throws InvalidVersionSpecificPayloadException {
    final ExecutionPayloadV2 payloadParameter =
        (ExecutionPayloadV2) requestParameters.payloadParameter();
    final Optional<List<Withdrawal>> maybeWithdrawals =
        Optional.ofNullable(payloadParameter.getWithdrawals())
            .map(ws -> ws.stream().map(WithdrawalParameter::toWithdrawal).toList());
    return new V2PayloadData(maybeWithdrawals);
  }

  @Override
  protected ValidationResult<RpcErrorType> validateVersionSpecificPayloadData(
      final NewPayloadRequestParametersV1 requestParameters,
      final VersionSpecificPayloadData versionSpecificPayloadData) {
    final ValidationResult<RpcErrorType> result =
        super.validateVersionSpecificPayloadData(requestParameters, versionSpecificPayloadData);
    if (!result.isValid()) {
      return result;
    }
    final ExecutionPayloadV2 payloadParameter =
        (ExecutionPayloadV2) requestParameters.payloadParameter();
    final V2PayloadData v2PayloadData = (V2PayloadData) versionSpecificPayloadData;
    if (!getWithdrawalsValidator(
            protocolSchedule.get(),
            payloadParameter.getTimestamp(),
            payloadParameter.getBlockNumber())
        .validateWithdrawals(v2PayloadData.maybeWithdrawals())) {
      return ValidationResult.invalid(RpcErrorType.INVALID_WITHDRAWALS_PARAMS);
    }
    return ValidationResult.valid();
  }

  @Override
  protected void setVersionSpecificBlockHeaderFields(
      final BlockHeaderBuilder blockHeaderBuilder,
      final NewPayloadRequestParametersV1 requestParameters,
      final VersionSpecificPayloadData versionSpecificPayloadData) {
    super.setVersionSpecificBlockHeaderFields(
        blockHeaderBuilder, requestParameters, versionSpecificPayloadData);
    final V2PayloadData v2PayloadData = (V2PayloadData) versionSpecificPayloadData;
    blockHeaderBuilder.withdrawalsRoot(
        v2PayloadData.maybeWithdrawals().map(BodyValidation::withdrawalsRoot).orElse(null));
  }

  @Override
  protected BlockBody createBlockBody(
      final List<Transaction> transactions,
      final VersionSpecificPayloadData versionSpecificPayloadData) {
    final V2PayloadData v2PayloadData = (V2PayloadData) versionSpecificPayloadData;
    return new BlockBody(transactions, Collections.emptyList(), v2PayloadData.maybeWithdrawals());
  }

  @Override
  protected void appendVersionSpecificLogInfo(
      final StringBuilder message,
      final List<Object> messageArgs,
      final Block block,
      final List<Transaction> transactions,
      final VersionSpecificPayloadData versionSpecificPayloadData) {
    super.appendVersionSpecificLogInfo(
        message, messageArgs, block, transactions, versionSpecificPayloadData);
    final V2PayloadData v2PayloadData = (V2PayloadData) versionSpecificPayloadData;
    v2PayloadData
        .maybeWithdrawals()
        .ifPresent(
            withdrawals -> {
              message.append("| %2d ws");
              messageArgs.add(withdrawals.size());
            });
  }

  protected static class V2PayloadData extends VersionSpecificPayloadData {
    private final Optional<List<Withdrawal>> maybeWithdrawals;

    protected V2PayloadData(final Optional<List<Withdrawal>> maybeWithdrawals) {
      this.maybeWithdrawals = maybeWithdrawals;
    }

    protected Optional<List<Withdrawal>> maybeWithdrawals() {
      return maybeWithdrawals;
    }
  }
}
