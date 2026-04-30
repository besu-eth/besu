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
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.consensus.merge.blockcreation.PayloadIdentifier;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EngineForkchoiceUpdatedParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EnginePayloadAttributesParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.WithdrawalParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.WithdrawalsValidator;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.util.List;
import java.util.Optional;

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
        (vertx, protocolSchedule1, protocolContext, mergeCoordinator1, engineCallListener) ->
            new EngineForkchoiceUpdatedV4(
                vertx,
                protocolSchedule1,
                protocolContext,
                mergeCoordinator1,
                transactionPool,
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
            List.of());

    when(mergeCoordinator.preparePayload(
            mockHeader,
            payloadParams.getTimestamp(),
            payloadParams.getPrevRandao(),
            Address.ECREC,
            Optional.of(List.of()),
            getParentBeaconBlockRoot(payloadParams),
            getSlotNumber(payloadParams),
            List.of()))
        .thenReturn(mockPayloadId);

    assertSuccessWithPayloadForForkchoiceResult(
        new EngineForkchoiceUpdatedParameter(mockHeader.getHash(), Hash.ZERO, mockParent.getHash()),
        Optional.of(payloadParams),
        MergeMiningCoordinator.ForkchoiceResult.withResult(
            Optional.empty(), Optional.of(mockHeader)),
        VALID);
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
