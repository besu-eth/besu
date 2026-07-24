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

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.CANCUN;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.INVALID;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineTestSupport.fromErrorResp;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.WithdrawalParameterTestFixture.WITHDRAWAL_PARAM_1;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.INVALID_PARAMS;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ConstructorArgumentsBuilder;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.WithdrawalParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.WithdrawalsValidator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.UnaryOperator;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class EngineNewPayloadV2Test extends EngineNewPayloadV1Test {

  @Override
  protected EngineNewPayloadV1<?, ?> createMethodInstance() {
    return new EngineNewPayloadV2<>(
        new ConstructorArgumentsBuilder()
            .protocolSchedule(protocolSchedule)
            .protocolContext(protocolContext)
            .vertx(vertx)
            .engineCallListener(engineCallListener)
            .mergeCoordinator(mergeCoordinator)
            .ethPeers(ethPeers)
            .metricsSystem(new NoOpMetricsSystem())
            .build(),
        null,
        CANCUN);
  }

  @Override
  protected long getMinSupportedTimestamp() {
    return shanghaiHardfork.milestone();
  }

  @Override
  protected OptionalLong getMaxSupportedTimestamp() {
    return OptionalLong.of(cancunHardfork.milestone() - 1);
  }

  @Override
  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_newPayloadV2");
  }

  @Test
  public void shouldReturnValidIfWithdrawalsIsNotNull_WhenWithdrawalsAllowed() {
    final List<Withdrawal> withdrawals = List.of(WITHDRAWAL_PARAM_1.toWithdrawal());
    BlockHeader mockHeader =
        setupPayloadV2(
            getMinSupportedTimestamp(),
            new BlockProcessingResult(Optional.of(new BlockProcessingOutputs(null, List.of()))),
            withdrawals);
    var resp = resp(requestParams(mockEnginePayloadParam(mockHeader, emptyList(), withdrawals)));

    assertValidResponse(mockHeader, resp);
  }

  @Test
  public void shouldReturnValidIfWithdrawalsIsNull_WhenWithdrawalsProhibited() {
    final List<Withdrawal> withdrawals = null;
    BlockHeader mockHeader =
        setupPayloadV2(
            getMinSupportedTimestamp() / 2,
            new BlockProcessingResult(Optional.of(new BlockProcessingOutputs(null, List.of()))),
            withdrawals);
    var resp = resp(requestParams(mockEnginePayloadParam(mockHeader, emptyList(), withdrawals)));

    assertValidResponse(mockHeader, resp);
  }

  @Test
  public void shouldReturnInvalidIfWithdrawalsIsNotNull_WhenWithdrawalsProhibited() {
    final List<Withdrawal> withdrawals = List.of();
    BlockHeader mockHeader =
        setupPayloadV2(
            getMinSupportedTimestamp() / 2,
            new BlockProcessingResult(Optional.of(new BlockProcessingOutputs(null, List.of()))),
            withdrawals);
    var resp = resp(requestParams(mockEnginePayloadParam(mockHeader, emptyList(), withdrawals)));

    final JsonRpcError jsonRpcError = fromErrorResp(resp);
    assertThat(jsonRpcError.getCode()).isEqualTo(INVALID_PARAMS.getCode());
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnInvalidIfWithdrawalsIsNull_WhenWithdrawalsAllowed() {
    final List<Withdrawal> withdrawals = null;
    BlockHeader mockHeader =
        setupPayloadV2(
            getMinSupportedTimestamp(),
            new BlockProcessingResult(Optional.of(new BlockProcessingOutputs(null, List.of()))),
            withdrawals);
    var resp = resp(requestParams(mockEnginePayloadParam(mockHeader, emptyList(), withdrawals)));

    assertThat(fromErrorResp(resp).getCode()).isEqualTo(INVALID_PARAMS.getCode());
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  protected Map<String, Object> mockEnginePayloadParam(
      final BlockHeader header, final List<String> txs, final List<Withdrawal> withdrawals) {
    var param = super.mockEnginePayloadParam(header, txs);
    if (withdrawals != null) {
      param.put(
          "withdrawals",
          withdrawals.stream()
              .map(WithdrawalParameter::fromWithdrawal)
              .map(WithdrawalParameter::asJsonObject)
              .map(JsonObject::getMap)
              .toList());
    } else {
      param.remove("withdrawals");
    }
    return param;
  }

  @Override
  protected void setDefaultExecutionPayloadFields(
      final Map<String, Object> payload, final BlockHeader header, final List<String> txs) {
    super.setDefaultExecutionPayloadFields(payload, header, txs);
    if (header.getTimestamp() >= shanghaiHardfork.milestone()) {
      payload.put("withdrawals", List.of());
    }
  }

  @Override
  protected ExecutionEngineJsonRpcMethod.EngineStatus getExpectedInvalidBlockHashStatus() {
    return INVALID;
  }

  protected BlockHeader setupPayloadV2(
      final long timestamp, final BlockProcessingResult value, final List<Withdrawal> withdrawals) {

    return setupPayloadV2(timestamp, value, withdrawals, UnaryOperator.identity());
  }

  protected BlockHeader setupPayloadV2(
      final long timestamp,
      final BlockProcessingResult value,
      final List<Withdrawal> withdrawals,
      final UnaryOperator<BlockHeaderTestFixture> versionSpecificModifier) {

    return super.setupPayloadV1(
        timestamp,
        value,
        fixture -> versionSpecificModifier.apply(setWithdrawalsRoot(fixture, withdrawals)));
  }

  private BlockHeaderTestFixture setWithdrawalsRoot(
      final BlockHeaderTestFixture fixture, final List<Withdrawal> withdrawals) {
    return fixture.withdrawalsRoot(
        withdrawals != null ? BodyValidation.withdrawalsRoot(withdrawals) : null);
  }

  @Override
  protected BlockHeaderTestFixture versionSpecificBlockHeaderFixture(final long timestamp) {
    BlockHeaderTestFixture baseFixture = super.versionSpecificBlockHeaderFixture(timestamp);
    if (timestamp >= shanghaiHardfork.milestone()) {
      baseFixture.withdrawalsRoot(BodyValidation.withdrawalsRoot(List.of()));
      when(protocolSpec.getWithdrawalsValidator())
          .thenReturn(new WithdrawalsValidator.AllowedWithdrawals());
    } else {
      baseFixture.withdrawalsRoot(null);
      when(protocolSpec.getWithdrawalsValidator())
          .thenReturn(new WithdrawalsValidator.ProhibitedWithdrawals());
    }
    return baseFixture;
  }
}
