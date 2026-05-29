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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.VALID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator.ForkchoiceResult;
import org.hyperledger.besu.consensus.merge.blockcreation.PayloadIdentifier;
import org.hyperledger.besu.consensus.merge.blockcreation.PreparePayloadArgsBuilder;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EngineForkchoiceUpdatedParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EnginePayloadAttributesParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.WithdrawalParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EngineUpdateForkchoiceResult;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.WithdrawalsValidator;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class EngineForkchoiceUpdatedV4Test extends AbstractEngineForkchoiceUpdatedTest {

  private static final Bytes32 MOCK_PBSR = Bytes32.fromHexStringLenient("0xBEEF");
  private static final String MOCK_SLOT = "0x1";

  public EngineForkchoiceUpdatedV4Test() {
    super(
        (vertx,
            protocolSchedule1,
            protocolContext,
            mergeCoordinator1,
            transactionPool1,
            engineCallListener) ->
            new EngineForkchoiceUpdatedV4(
                vertx,
                protocolSchedule1,
                protocolContext,
                mergeCoordinator1,
                transactionPool1,
                engineCallListener));
  }

  @Override
  protected EnginePayloadAttributesParameter createPayloadParams(
      final long timestamp, final List<WithdrawalParameter> withdrawals) {
    return new EnginePayloadAttributesParameter(
        String.valueOf(timestamp),
        Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
        Address.ECREC.toHexString(),
        withdrawals,
        MOCK_PBSR.toHexString(),
        MOCK_SLOT,
        "0x1c9c380",
        null);
  }

  @Override
  protected Optional<Bytes32> getParentBeaconBlockRoot(
      final EnginePayloadAttributesParameter params) {
    return Optional.ofNullable(params.getParentBeaconBlockRoot());
  }

  @Override
  protected Optional<Long> getSlotNumber(final EnginePayloadAttributesParameter params) {
    return Optional.ofNullable(params.getSlotNumber());
  }

  @Override
  protected long defaultPayloadTimestamp() {
    return AMSTERDAM_MILESTONE + 1;
  }

  @Override
  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_forkchoiceUpdatedV4");
  }

  @Override
  @Test
  public void shouldIgnoreUpdateToOldHeadAndNotPreparePayload() {
    // V4 (per execution-apis #786) skips the forkchoice update only when the new head is an
    // ancestor of the latest known finalized block, not the legacy "old head" optimization. Verify
    // that V4 returns VALID without preparing a payload for that scenario.
    BlockHeader mockHeader =
        blockHeaderBuilder.timestamp(defaultPayloadTimestamp() - 1).buildHeader();

    when(mergeCoordinator.getOrSyncHeadByHash(mockHeader.getHash(), Hash.ZERO))
        .thenReturn(Optional.of(mockHeader));
    when(mergeCoordinator.isAncestorOfFinalized(mockHeader.getHash())).thenReturn(true);

    var payloadParams = createPayloadParams(defaultPayloadTimestamp(), null);

    var resp =
        (JsonRpcSuccessResponse)
            resp(
                new EngineForkchoiceUpdatedParameter(
                    mockHeader.getBlockHash(), Hash.ZERO, Hash.ZERO),
                Optional.of(payloadParams));

    var forkchoiceRes = (EngineUpdateForkchoiceResult) resp.getResult();

    verify(mergeCoordinator, never())
        .preparePayload(any(MergeMiningCoordinator.PreparePayloadArgs.class));

    assertThat(forkchoiceRes.getPayloadStatus().getStatus()).isEqualTo(VALID);
    assertThat(forkchoiceRes.getPayloadStatus().getError()).isNull();
    assertThat(forkchoiceRes.getPayloadId()).isNull();
  }

  @Test
  public void shouldReturnInvalidSlotNumberWhenMissingSlotNumber() {
    when(protocolSpec.getWithdrawalsValidator())
        .thenReturn(new WithdrawalsValidator.AllowedWithdrawals());
    BlockHeader mockParent =
        blockHeaderBuilder.timestamp(defaultPayloadTimestamp() - 2).number(9L).buildHeader();
    BlockHeader mockHeader =
        blockHeaderBuilder
            .timestamp(defaultPayloadTimestamp() - 1)
            .number(10L)
            .parentHash(mockParent.getHash())
            .buildHeader();
    setupValidForkchoiceUpdate(mockHeader);

    var payloadParams =
        new EnginePayloadAttributesParameter(
            String.valueOf(defaultPayloadTimestamp()),
            Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
            Address.ECREC.toString(),
            List.of(),
            MOCK_PBSR.toHexString(),
            null,
            "0x1c9c380",
            List.of());

    var resp =
        resp(
            new EngineForkchoiceUpdatedParameter(
                mockHeader.getHash(), Hash.ZERO, mockParent.getHash()),
            Optional.of(payloadParams));

    assertThat(resp.getType()).isEqualTo(RpcResponseType.ERROR);
    assertThat(((JsonRpcErrorResponse) resp).getErrorType())
        .isEqualTo(RpcErrorType.INVALID_SLOT_NUMBER_PARAMS);
  }

  @Test
  public void shouldReturnValidPayloadAttributesWhenSlotNumberPresent() {
    when(protocolSpec.getWithdrawalsValidator())
        .thenReturn(new WithdrawalsValidator.AllowedWithdrawals());
    BlockHeader mockParent =
        blockHeaderBuilder.timestamp(defaultPayloadTimestamp() - 2).number(9L).buildHeader();
    BlockHeader mockHeader =
        blockHeaderBuilder
            .timestamp(defaultPayloadTimestamp() - 1)
            .number(10L)
            .parentHash(mockParent.getHash())
            .buildHeader();
    setupValidForkchoiceUpdate(mockHeader);

    var payloadParams =
        new EnginePayloadAttributesParameter(
            String.valueOf(defaultPayloadTimestamp()),
            Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
            Address.ECREC.toString(),
            List.of(),
            MOCK_PBSR.toHexString(),
            MOCK_SLOT,
            "0x1c9c380",
            null);

    var mockPayloadId =
        PayloadIdentifier.forPayloadParams(
            mockHeader.getHash(),
            payloadParams.getTimestamp(),
            payloadParams.getPrevRandao(),
            payloadParams.getSuggestedFeeRecipient(),
            Optional.of(List.of()),
            getParentBeaconBlockRoot(payloadParams),
            getSlotNumber(payloadParams),
            Optional.ofNullable(payloadParams.getTargetGasLimit()),
            List.of());

    when(mergeCoordinator.preparePayload(
            new PreparePayloadArgsBuilder()
                .parentHeader(mockHeader)
                .timestamp(payloadParams.getTimestamp())
                .prevRandao(payloadParams.getPrevRandao())
                .feeRecipient(Address.ECREC)
                .withdrawals(List.of())
                .parentBeaconBlockRoot(getParentBeaconBlockRoot(payloadParams))
                .slotNumber(getSlotNumber(payloadParams))
                .targetGasLimit(Optional.ofNullable(payloadParams.getTargetGasLimit()))
                .inclusionListTransactions(List.of())
                .build()))
        .thenReturn(mockPayloadId);

    assertSuccessWithPayloadForForkchoiceResult(
        new EngineForkchoiceUpdatedParameter(mockHeader.getHash(), Hash.ZERO, mockParent.getHash()),
        Optional.of(payloadParams),
        ForkchoiceResult.withResult(Optional.empty(), Optional.of(mockHeader)),
        VALID);
  }

  @Test
  public void shouldReturnInvalidTargetGasLimitParamsWhenTargetGasLimitMissing() {
    final BlockHeader head =
        blockHeaderBuilder.number(50L).timestamp(AMSTERDAM_MILESTONE + 1).buildHeader();
    when(mergeCoordinator.getOrSyncHeadByHash(head.getHash(), Hash.ZERO))
        .thenReturn(Optional.of(head));
    when(mergeCoordinator.computeReorgDepth(head)).thenReturn(OptionalLong.of(0L));

    final EngineForkchoiceUpdatedParameter param =
        new EngineForkchoiceUpdatedParameter(head.getHash(), Hash.ZERO, Hash.ZERO);

    final EnginePayloadAttributesParameter attrs =
        new EnginePayloadAttributesParameter(
            "0x" + Long.toHexString(head.getTimestamp() + 1),
            Bytes32.ZERO.toHexString(),
            Address.ZERO.toHexString(),
            null,
            Bytes32.ZERO.toHexString(),
            "0x1",
            null,
            null);

    final JsonRpcResponse resp = resp(param, Optional.of(attrs));

    assertThat(resp.getType()).isEqualTo(RpcResponseType.ERROR);
    final JsonRpcErrorResponse err = (JsonRpcErrorResponse) resp;
    assertThat(err.getErrorType()).isEqualTo(RpcErrorType.INVALID_TARGET_GAS_LIMIT_PARAMS);
  }

  @Override
  protected String getMethodName() {
    return RpcMethod.ENGINE_FORKCHOICE_UPDATED_V4.getMethodName();
  }

  @Override
  protected RpcErrorType expectedInvalidPayloadError() {
    return RpcErrorType.INVALID_PAYLOAD_ATTRIBUTES;
  }
}
