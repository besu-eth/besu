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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.CANCUN;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.INVALID;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineTestSupport.fromErrorResp;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.WithdrawalParameterTestFixture.WITHDRAWAL_PARAM_1;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.INVALID_PARAMS;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.INVALID_REQUEST;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.UNSUPPORTED_FORK;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.parameters.UnsignedLongParameter;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV2;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.WithdrawalParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.mainnet.ScheduledProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.WithdrawalsValidator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class EngineNewPayloadV2Test extends EngineNewPayloadV1Test {

  protected Set<ScheduledProtocolSpec.Hardfork> supportedHardforks() {
    return Set.of(shanghaiHardfork);
  }

  public EngineNewPayloadV2Test() {}

  @Override
  @BeforeEach
  public void before() {
    super.before();
    createMethod();
  }

  @Override
  protected EngineNewPayloadV1 createMethodInstance() {
    return new EngineNewPayloadV2(
        vertx,
        protocolSchedule,
        protocolContext,
        mergeCoordinator,
        ethPeers,
        engineCallListener,
        new NoOpMetricsSystem(),
        null,
        CANCUN);
  }

  @Override
  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_newPayloadV2");
  }

  @Test
  public void shouldReturnValidIfWithdrawalsIsNotNull_WhenWithdrawalsAllowed() {
    final List<WithdrawalParameter> withdrawalsParam = List.of(WITHDRAWAL_PARAM_1);
    final List<Withdrawal> withdrawals = List.of(WITHDRAWAL_PARAM_1.toWithdrawal());
    when(protocolSpec.getWithdrawalsValidator())
        .thenReturn(new WithdrawalsValidator.AllowedWithdrawals());
    BlockHeader mockHeader =
        setupValidPayload(
            new BlockProcessingResult(Optional.of(new BlockProcessingOutputs(null, List.of()))),
            Optional.of(withdrawals));
    lenient()
        .when(blockchain.getBlockHeader(mockHeader.getParentHash()))
        .thenReturn(Optional.of(mock(BlockHeader.class)));
    var resp = resp(mockEnginePayload(mockHeader, Collections.emptyList(), withdrawalsParam));

    assertValidResponse(mockHeader, resp);
  }

  @Test
  public void shouldReturnValidIfWithdrawalsIsNull_WhenWithdrawalsProhibited() {
    final List<WithdrawalParameter> withdrawals = null;
    when(protocolSpec.getWithdrawalsValidator())
        .thenReturn(new WithdrawalsValidator.ProhibitedWithdrawals());
    BlockHeader mockHeader =
        setupValidPayload(
            new BlockProcessingResult(Optional.of(new BlockProcessingOutputs(null, List.of()))),
            Optional.empty());
    lenient()
        .when(blockchain.getBlockHeader(mockHeader.getParentHash()))
        .thenReturn(Optional.of(mock(BlockHeader.class)));
    var resp = resp(mockEnginePayload(mockHeader, Collections.emptyList(), withdrawals));

    assertValidResponse(mockHeader, resp);
  }

  @Test
  public void shouldReturnInvalidIfWithdrawalsIsNotNull_WhenWithdrawalsProhibited() {
    final List<WithdrawalParameter> withdrawals = List.of();
    lenient()
        .when(protocolSpec.getWithdrawalsValidator())
        .thenReturn(new WithdrawalsValidator.ProhibitedWithdrawals());

    var resp =
        resp(
            mockEnginePayload(
                createBlockHeader(Optional.of(Collections.emptyList())),
                Collections.emptyList(),
                withdrawals));

    final JsonRpcError jsonRpcError = fromErrorResp(resp);
    assertThat(jsonRpcError.getCode()).isEqualTo(INVALID_PARAMS.getCode());
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldValidateBlobGasUsedCorrectly() {
    BlockHeader blockHeader =
        createBlockHeaderFixture(Optional.of(Collections.emptyList()))
            .blobGasUsed(100L)
            .buildHeader();
    final Map<String, Object> payload =
        executionPayloadV2Json(blockHeader, Collections.emptyList(), List.of());
    payload.put("blobGasUsed", "0x64");

    var resp = resp(payload);
    final JsonRpcError jsonRpcError = fromErrorResp(resp);
    assertThat(jsonRpcError.getCode()).isEqualTo(INVALID_REQUEST.getCode());
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldValidateExcessBlobGasCorrectly() {
    BlockHeader blockHeader =
        createBlockHeaderFixture(Optional.of(Collections.emptyList()))
            .excessBlobGas(BlobGas.MAX_BLOB_GAS)
            .buildHeader();
    final Map<String, Object> payload =
        executionPayloadV2Json(blockHeader, Collections.emptyList(), List.of());
    payload.put("excessBlobGas", BlobGas.MAX_BLOB_GAS.toHexString());

    var resp = resp(payload);

    final JsonRpcError jsonRpcError = fromErrorResp(resp);
    assertThat(jsonRpcError.getCode()).isEqualTo(INVALID_REQUEST.getCode());
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnInvalidIfWithdrawalsIsNull_WhenWithdrawalsAllowed() {
    final List<WithdrawalParameter> withdrawals = null;
    when(protocolSpec.getWithdrawalsValidator())
        .thenReturn(new WithdrawalsValidator.AllowedWithdrawals());

    var resp =
        resp(
            mockEnginePayload(
                createBlockHeader(Optional.empty()), Collections.emptyList(), withdrawals));

    assertThat(fromErrorResp(resp).getCode()).isEqualTo(INVALID_PARAMS.getCode());
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnUnsupportedForkIfBlockTimestampIsAfterCancunMilestone() {
    // Cancun started at timestamp 30
    final long blockTimestamp = 31L;
    BlockHeader blockHeader =
        createBlockHeaderFixture(Optional.of(Collections.emptyList()))
            .timestamp(blockTimestamp)
            .buildHeader();

    var resp = resp(mockEnginePayload(blockHeader, Collections.emptyList(), List.of()));

    final JsonRpcError jsonRpcError = fromErrorResp(resp);
    assertThat(jsonRpcError.getCode()).isEqualTo(UNSUPPORTED_FORK.getCode());
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Override
  protected ExecutionPayloadV2 mockEnginePayload(final BlockHeader header, final List<String> txs) {
    return executionPayloadV2(header, txs, null);
  }

  @Override
  protected ExecutionPayloadV2 mockEnginePayload(
      final BlockHeader header,
      final List<String> txs,
      final List<WithdrawalParameter> withdrawals) {
    return executionPayloadV2(header, txs, withdrawals);
  }

  private JsonRpcResponse resp(final Map<String, Object> payload) {
    return method.response(
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", this.method.getName(), new Object[] {payload})));
  }

  private Map<String, Object> executionPayloadV2Json(
      final BlockHeader header,
      final List<String> txs,
      final List<WithdrawalParameter> withdrawals) {
    return new HashMap<>(
        Map.ofEntries(
            Map.entry("blockHash", header.getHash()),
            Map.entry("parentHash", header.getParentHash()),
            Map.entry("feeRecipient", header.getCoinbase()),
            Map.entry("stateRoot", header.getStateRoot()),
            Map.entry("blockNumber", new UnsignedLongParameter(header.getNumber())),
            Map.entry("baseFeePerGas", header.getBaseFee().map(w -> w.toHexString()).orElse("0x0")),
            Map.entry("gasLimit", new UnsignedLongParameter(header.getGasLimit())),
            Map.entry("gasUsed", new UnsignedLongParameter(header.getGasUsed())),
            Map.entry("timestamp", new UnsignedLongParameter(header.getTimestamp())),
            Map.entry(
                "extraData",
                header.getExtraData() == null ? "0x" : header.getExtraData().toHexString()),
            Map.entry("receiptsRoot", header.getReceiptsRoot()),
            Map.entry("logsBloom", header.getLogsBloom()),
            Map.entry("prevRandao", header.getPrevRandao().map(Bytes32::toHexString).orElse("0x0")),
            Map.entry("transactions", txs),
            Map.entry("withdrawals", withdrawals)));
  }

  @Override
  protected ExecutionPayloadV2 mockEnginePayload(
      final BlockHeader header,
      final List<String> txs,
      final List<WithdrawalParameter> withdrawals,
      final String blockAccessList) {
    return executionPayloadV2(header, txs, withdrawals);
  }

  @Override
  protected ExecutionEngineJsonRpcMethod.EngineStatus getExpectedInvalidBlockHashStatus() {
    return INVALID;
  }
}
