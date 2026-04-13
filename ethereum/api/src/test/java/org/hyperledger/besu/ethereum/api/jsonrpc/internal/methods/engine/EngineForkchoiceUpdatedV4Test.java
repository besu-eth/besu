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
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.AMSTERDAM;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.VALID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator.ForkchoiceResult;
import org.hyperledger.besu.consensus.merge.blockcreation.PayloadIdentifier;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.GWei;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EngineForkchoiceUpdatedParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EnginePayloadAttributesParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.WithdrawalParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EngineUpdateForkchoiceResult;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.mainnet.WithdrawalsValidator;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class EngineForkchoiceUpdatedV4Test extends AbstractEngineForkchoiceUpdatedTest {

  private static final long AMSTERDAM_MILESTONE = 100L;
  private static final Bytes32 MOCK_PBSR = Bytes32.fromHexStringLenient("0xBEEF");
  private static final String MOCK_SLOT = "0x01";

  public EngineForkchoiceUpdatedV4Test() {
    super(EngineForkchoiceUpdatedV4::new);
  }

  @Override
  @BeforeEach
  public void before() {
    when(protocolSchedule.milestoneFor(AMSTERDAM)).thenReturn(Optional.of(AMSTERDAM_MILESTONE));
    super.before();
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

  @Test
  public void shouldReturnInvalidPayloadAttributesWhenMissingParentBeaconBlockRoot() {
    BlockHeader mockParent = blockHeaderBuilder.number(9L).buildHeader();
    BlockHeader mockHeader =
        blockHeaderBuilder.number(10L).parentHash(mockParent.getHash()).buildHeader();
    setupValidForkchoiceUpdate(mockHeader);

    var payloadParams =
        new EnginePayloadAttributesParameter(
            String.valueOf(defaultPayloadTimestamp()),
            Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
            Address.ECREC.toString(),
            null,
            null,
            MOCK_SLOT,
            null);

    var resp =
        resp(
            new EngineForkchoiceUpdatedParameter(
                mockHeader.getHash(), Hash.ZERO, mockParent.getHash()),
            Optional.of(payloadParams));

    assertThat(resp.getType()).isEqualTo(RpcResponseType.ERROR);
    assertThat(((JsonRpcErrorResponse) resp).getErrorType())
        .isEqualTo(RpcErrorType.INVALID_PAYLOAD_ATTRIBUTES);
  }

  @Test
  public void shouldReturnInvalidSlotNumberWhenMissingSlotNumber() {
    BlockHeader mockParent = blockHeaderBuilder.number(9L).buildHeader();
    BlockHeader mockHeader =
        blockHeaderBuilder.number(10L).parentHash(mockParent.getHash()).buildHeader();
    setupValidForkchoiceUpdate(mockHeader);

    var payloadParams =
        new EnginePayloadAttributesParameter(
            String.valueOf(defaultPayloadTimestamp()),
            Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
            Address.ECREC.toString(),
            null,
            MOCK_PBSR.toHexString(),
            null,
            null);

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
  public void shouldReturnValidWithNullPayloadAttributes() {
    BlockHeader mockHeader = blockHeaderBuilder.timestamp(0).buildHeader();
    when(mergeCoordinator.getOrSyncHeadByHash(mockHeader.getHash(), Hash.ZERO))
        .thenReturn(Optional.of(mockHeader));

    assertSuccessWithPayloadForForkchoiceResult(
        new EngineForkchoiceUpdatedParameter(mockHeader.getHash(), Hash.ZERO, Hash.ZERO),
        Optional.empty(),
        ForkchoiceResult.withResult(Optional.empty(), Optional.of(mockHeader)),
        VALID);
  }

  // Override abstract test: V4 requires PBSR and slotNumber
  @Override
  @Test
  public void shouldReturnValidWithoutFinalizedWithPayload() {
    BlockHeader mockHeader = blockHeaderBuilder.timestamp(0).buildHeader();
    when(mergeCoordinator.getOrSyncHeadByHash(mockHeader.getHash(), Hash.ZERO))
        .thenReturn(Optional.of(mockHeader));

    var payloadParams = v4PayloadAttributes(defaultPayloadTimestamp(), null, null);
    var mockPayloadId = mockPayloadId(mockHeader, payloadParams);

    when(mergeCoordinator.preparePayload(
            mockHeader,
            payloadParams.getTimestamp(),
            payloadParams.getPrevRandao(),
            Address.ECREC,
            Optional.empty(),
            Optional.ofNullable(payloadParams.getParentBeaconBlockRoot()),
            Optional.ofNullable(payloadParams.getSlotNumber()),
            Optional.empty()))
        .thenReturn(mockPayloadId);

    assertSuccessWithPayloadForForkchoiceResult(
        new EngineForkchoiceUpdatedParameter(mockHeader.getHash(), Hash.ZERO, Hash.ZERO),
        Optional.of(payloadParams),
        ForkchoiceResult.withResult(Optional.empty(), Optional.of(mockHeader)),
        VALID);
  }

  // Override abstract test: V4 requires PBSR and slotNumber
  @Override
  @Test
  public void shouldIgnoreUpdateToOldHeadAndNotPreparePayload() {
    BlockHeader mockHeader = blockHeaderBuilder.timestamp(0).buildHeader();

    when(mergeCoordinator.getOrSyncHeadByHash(mockHeader.getHash(), Hash.ZERO))
        .thenReturn(Optional.of(mockHeader));

    var ignoreOldHeadUpdateRes = ForkchoiceResult.withIgnoreUpdateToOldHead(mockHeader);
    when(mergeCoordinator.updateForkChoice(any(), any(), any())).thenReturn(ignoreOldHeadUpdateRes);

    var payloadParams = v4PayloadAttributes(defaultPayloadTimestamp(), null, null);

    var resp =
        (JsonRpcSuccessResponse)
            resp(
                new EngineForkchoiceUpdatedParameter(
                    mockHeader.getBlockHash(), Hash.ZERO, Hash.ZERO),
                Optional.of(payloadParams));

    var forkchoiceRes = (EngineUpdateForkchoiceResult) resp.getResult();

    verify(mergeCoordinator, never())
        .preparePayload(any(), any(), any(), any(), any(), any(), any(), any());

    assertThat(forkchoiceRes.getPayloadStatus().getStatus()).isEqualTo(VALID);
    assertThat(forkchoiceRes.getPayloadStatus().getError()).isNull();
    assertThat(forkchoiceRes.getPayloadId()).isNull();
  }

  // Override abstract test: V4 requires PBSR and slotNumber
  @Override
  @Test
  public void shouldReturnValidIfWithdrawalsIsNull_WhenWithdrawalsProhibited() {
    BlockHeader mockParent =
        blockHeaderBuilder.timestamp(AMSTERDAM_MILESTONE).number(9L).buildHeader();
    BlockHeader mockHeader =
        blockHeaderBuilder.number(10L).parentHash(mockParent.getHash()).buildHeader();
    setupValidForkchoiceUpdate(mockHeader);

    var payloadParams = v4PayloadAttributes(mockHeader.getTimestamp() + 1, null, null);
    var mockPayloadId = mockPayloadId(mockHeader, payloadParams);

    when(mergeCoordinator.preparePayload(
            mockHeader,
            payloadParams.getTimestamp(),
            payloadParams.getPrevRandao(),
            Address.ECREC,
            Optional.empty(),
            Optional.ofNullable(payloadParams.getParentBeaconBlockRoot()),
            Optional.ofNullable(payloadParams.getSlotNumber()),
            Optional.empty()))
        .thenReturn(mockPayloadId);

    assertSuccessWithPayloadForForkchoiceResult(
        new EngineForkchoiceUpdatedParameter(mockHeader.getHash(), Hash.ZERO, mockParent.getHash()),
        Optional.of(payloadParams),
        ForkchoiceResult.withResult(Optional.empty(), Optional.of(mockHeader)),
        VALID);
  }

  // Override abstract test: V4 requires PBSR and slotNumber
  @Override
  @Test
  public void shouldReturnValidIfWithdrawalsIsNotNull_WhenWithdrawalsAllowed() {
    when(protocolSpec.getWithdrawalsValidator())
        .thenReturn(new WithdrawalsValidator.AllowedWithdrawals());

    BlockHeader mockParent =
        blockHeaderBuilder.timestamp(AMSTERDAM_MILESTONE).number(9L).buildHeader();
    BlockHeader mockHeader =
        blockHeaderBuilder.number(10L).parentHash(mockParent.getHash()).buildHeader();
    setupValidForkchoiceUpdate(mockHeader);

    var withdrawalParameters =
        List.of(
            new WithdrawalParameter(
                "0x1",
                "0x10000",
                "0x0100000000000000000000000000000000000000",
                GWei.ONE.toHexString()));

    var payloadParams =
        new EnginePayloadAttributesParameter(
            String.valueOf(mockHeader.getTimestamp() + 1),
            Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
            Address.ECREC.toString(),
            withdrawalParameters,
            MOCK_PBSR.toHexString(),
            MOCK_SLOT,
            null);

    final Optional<List<Withdrawal>> withdrawals =
        Optional.of(
            withdrawalParameters.stream()
                .map(WithdrawalParameter::toWithdrawal)
                .collect(Collectors.toList()));

    var mockPayloadId =
        PayloadIdentifier.forPayloadParams(
            mockHeader.getHash(),
            payloadParams.getTimestamp(),
            payloadParams.getPrevRandao(),
            payloadParams.getSuggestedFeeRecipient(),
            withdrawals,
            Optional.ofNullable(payloadParams.getParentBeaconBlockRoot()));

    when(mergeCoordinator.preparePayload(
            mockHeader,
            payloadParams.getTimestamp(),
            payloadParams.getPrevRandao(),
            Address.ECREC,
            withdrawals,
            Optional.ofNullable(payloadParams.getParentBeaconBlockRoot()),
            Optional.ofNullable(payloadParams.getSlotNumber()),
            Optional.empty()))
        .thenReturn(mockPayloadId);

    assertSuccessWithPayloadForForkchoiceResult(
        new EngineForkchoiceUpdatedParameter(mockHeader.getHash(), Hash.ZERO, mockParent.getHash()),
        Optional.of(payloadParams),
        ForkchoiceResult.withResult(Optional.empty(), Optional.of(mockHeader)),
        VALID);
  }

  // Override abstract test: V4 requires PBSR and slotNumber
  @Override
  @Test
  public void shouldReturnValidIfProtocolScheduleIsEmpty() {
    when(protocolSchedule.getForNextBlockHeader(any(), anyLong())).thenReturn(null);

    BlockHeader mockParent =
        blockHeaderBuilder.timestamp(AMSTERDAM_MILESTONE).number(9L).buildHeader();
    BlockHeader mockHeader =
        blockHeaderBuilder.number(10L).parentHash(mockParent.getHash()).buildHeader();
    setupValidForkchoiceUpdate(mockHeader);

    var payloadParams = v4PayloadAttributes(mockHeader.getTimestamp() + 1, null, null);
    var mockPayloadId = mockPayloadId(mockHeader, payloadParams);

    when(mergeCoordinator.preparePayload(
            mockHeader,
            payloadParams.getTimestamp(),
            payloadParams.getPrevRandao(),
            Address.ECREC,
            Optional.empty(),
            Optional.ofNullable(payloadParams.getParentBeaconBlockRoot()),
            Optional.ofNullable(payloadParams.getSlotNumber()),
            Optional.empty()))
        .thenReturn(mockPayloadId);

    assertSuccessWithPayloadForForkchoiceResult(
        new EngineForkchoiceUpdatedParameter(mockHeader.getHash(), Hash.ZERO, mockParent.getHash()),
        Optional.of(payloadParams),
        ForkchoiceResult.withResult(Optional.empty(), Optional.of(mockHeader)),
        VALID);
  }

  @Test
  public void shouldReturnValidWithInclusionListTransactions() {
    BlockHeader mockParent =
        blockHeaderBuilder.timestamp(AMSTERDAM_MILESTONE).number(9L).buildHeader();
    BlockHeader mockHeader =
        blockHeaderBuilder.number(10L).parentHash(mockParent.getHash()).buildHeader();
    setupValidForkchoiceUpdate(mockHeader);

    List<String> ilTxs = List.of("0xdead", "0xbeef");

    var payloadParams =
        new EnginePayloadAttributesParameter(
            String.valueOf(mockHeader.getTimestamp() + 1),
            Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
            Address.ECREC.toString(),
            null,
            MOCK_PBSR.toHexString(),
            MOCK_SLOT,
            ilTxs);

    var mockPayloadId = mockPayloadId(mockHeader, payloadParams);

    when(mergeCoordinator.updateForkChoice(any(), any(), any()))
        .thenReturn(ForkchoiceResult.withResult(Optional.empty(), Optional.of(mockHeader)));
    when(mergeCoordinator.preparePayload(
            mockHeader,
            payloadParams.getTimestamp(),
            payloadParams.getPrevRandao(),
            Address.ECREC,
            Optional.empty(),
            Optional.ofNullable(payloadParams.getParentBeaconBlockRoot()),
            Optional.ofNullable(payloadParams.getSlotNumber()),
            Optional.of(List.of(Bytes.fromHexString("0xdead"), Bytes.fromHexString("0xbeef")))))
        .thenReturn(mockPayloadId);

    var resp =
        resp(
            new EngineForkchoiceUpdatedParameter(
                mockHeader.getHash(), Hash.ZERO, mockParent.getHash()),
            Optional.of(payloadParams));

    assertThat(resp.getType()).isEqualTo(RpcResponseType.SUCCESS);
    var result = (EngineUpdateForkchoiceResult) ((JsonRpcSuccessResponse) resp).getResult();
    assertThat(result.getPayloadStatus().getStatus()).isEqualTo(VALID);
    assertThat(result.getPayloadId()).isNotNull();
  }

  @Test
  public void shouldReturnValidWithNullInclusionListTransactions() {
    BlockHeader mockParent =
        blockHeaderBuilder.timestamp(AMSTERDAM_MILESTONE).number(9L).buildHeader();
    BlockHeader mockHeader =
        blockHeaderBuilder.number(10L).parentHash(mockParent.getHash()).buildHeader();
    setupValidForkchoiceUpdate(mockHeader);

    var payloadParams = v4PayloadAttributes(mockHeader.getTimestamp() + 1, null, null);
    var mockPayloadId = mockPayloadId(mockHeader, payloadParams);

    when(mergeCoordinator.updateForkChoice(any(), any(), any()))
        .thenReturn(ForkchoiceResult.withResult(Optional.empty(), Optional.of(mockHeader)));
    when(mergeCoordinator.preparePayload(
            mockHeader,
            payloadParams.getTimestamp(),
            payloadParams.getPrevRandao(),
            Address.ECREC,
            Optional.empty(),
            Optional.ofNullable(payloadParams.getParentBeaconBlockRoot()),
            Optional.ofNullable(payloadParams.getSlotNumber()),
            Optional.empty()))
        .thenReturn(mockPayloadId);

    var resp =
        resp(
            new EngineForkchoiceUpdatedParameter(
                mockHeader.getHash(), Hash.ZERO, mockParent.getHash()),
            Optional.of(payloadParams));

    assertThat(resp.getType()).isEqualTo(RpcResponseType.SUCCESS);
    var result = (EngineUpdateForkchoiceResult) ((JsonRpcSuccessResponse) resp).getResult();
    assertThat(result.getPayloadStatus().getStatus()).isEqualTo(VALID);
    assertThat(result.getPayloadId()).isNotNull();
  }

  @Override
  protected String getMethodName() {
    return RpcMethod.ENGINE_FORKCHOICE_UPDATED_V4.getMethodName();
  }

  @Override
  protected RpcErrorType expectedInvalidPayloadError() {
    return RpcErrorType.INVALID_PAYLOAD_ATTRIBUTES;
  }

  private EnginePayloadAttributesParameter v4PayloadAttributes(
      final long timestamp,
      final List<WithdrawalParameter> withdrawals,
      final List<String> inclusionListTransactions) {
    return new EnginePayloadAttributesParameter(
        String.valueOf(timestamp),
        Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
        Address.ECREC.toString(),
        withdrawals,
        MOCK_PBSR.toHexString(),
        MOCK_SLOT,
        inclusionListTransactions);
  }

  private PayloadIdentifier mockPayloadId(
      final BlockHeader header, final EnginePayloadAttributesParameter payloadParams) {
    return PayloadIdentifier.forPayloadParams(
        header.getHash(),
        payloadParams.getTimestamp(),
        payloadParams.getPrevRandao(),
        payloadParams.getSuggestedFeeRecipient(),
        Optional.empty(),
        Optional.ofNullable(payloadParams.getParentBeaconBlockRoot()));
  }
}
