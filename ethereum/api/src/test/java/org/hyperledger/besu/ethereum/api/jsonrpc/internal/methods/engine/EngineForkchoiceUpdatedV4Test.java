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
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.AMSTERDAM;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ConstructorArgumentsBuilder;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ForkchoiceStateV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.PayloadAttributesV4;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.Collections;
import java.util.Optional;
import java.util.OptionalLong;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EngineForkchoiceUpdatedV4Test extends EngineForkchoiceUpdatedV3Test {

  @Override
  protected EngineForkchoiceUpdatedV1<?> createMethodInstance() {
    // V4 has no upper bound (null maxFork = open-ended).
    return new EngineForkchoiceUpdatedV4<>(
        new ConstructorArgumentsBuilder()
            .protocolSchedule(protocolSchedule)
            .protocolContext(protocolContext)
            .vertx(vertx)
            .engineCallListener(engineCallListener)
            .mergeCoordinator(mergeCoordinator)
            .build(),
        AMSTERDAM,
        null);
  }

  @Override
  protected long getMinSupportedTimestamp() {
    return amsterdamHardfork.milestone();
  }

  @Override
  protected OptionalLong getMaxSupportedTimestamp() {
    return OptionalLong.empty();
  }

  @Override
  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_forkchoiceUpdatedV4");
  }

  @Override
  protected String getMethodName() {
    return RpcMethod.ENGINE_FORKCHOICE_UPDATED_V4.getMethodName();
  }

  @Override
  protected Object validPayloadAttributesForBlock(final BlockHeader head) {
    return new PayloadAttributesV4(
        String.valueOf(head.getTimestamp() + 1),
        Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
        Address.ECREC.toString(),
        Collections.emptyList(),
        Bytes32.ZERO.toHexString(),
        "0x1",
        "0x1C9C380");
  }

  @Override
  protected Object invalidTimestampPayloadAttributesForBlock(final BlockHeader head) {
    return new PayloadAttributesV4(
        String.valueOf(head.getTimestamp()),
        Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
        Address.ECREC.toString(),
        Collections.emptyList(),
        Bytes32.ZERO.toHexString(),
        "0x1",
        null);
  }

  @Override
  protected Object payloadAttributesWithNullWithdrawalsForBlock(final BlockHeader head) {
    return new PayloadAttributesV4(
        String.valueOf(head.getTimestamp() + 1),
        Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
        Address.ECREC.toString(),
        null,
        Bytes32.ZERO.toHexString(),
        "0x1",
        null);
  }

  // ---- V4-specific tests ----

  @Test
  public void shouldReturnInvalidSlotNumberParamsForNegativeSlotNumber() {
    final BlockHeader mockHeader = setupValidForkchoiceUpdate();

    final PayloadAttributesV4 payloadWithNegativeSlot =
        new PayloadAttributesV4(
            String.valueOf(mockHeader.getTimestamp() + 1),
            Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
            Address.ECREC.toString(),
            Collections.emptyList(),
            Bytes32.ZERO.toHexString(),
            "-0x1",
            null);

    final JsonRpcResponse resp =
        resp(
            new ForkchoiceStateV1(mockHeader.getBlockHash(), Hash.ZERO, Hash.ZERO),
            Optional.of(payloadWithNegativeSlot));

    assertThat(resp).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) resp).getErrorType())
        .isEqualTo(RpcErrorType.INVALID_SLOT_NUMBER_PARAMS);
  }

  @Test
  public void shouldReturnValidForZeroSlotNumber() {
    final BlockHeader mockHeader = setupValidForkchoiceUpdate();

    final PayloadAttributesV4 payloadWithZeroSlot =
        new PayloadAttributesV4(
            String.valueOf(mockHeader.getTimestamp() + 1),
            Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
            Address.ECREC.toString(),
            Collections.emptyList(),
            Bytes32.ZERO.toHexString(),
            "0x0",
            "0x1C9C380");

    final JsonRpcResponse resp =
        resp(
            new ForkchoiceStateV1(mockHeader.getBlockHash(), Hash.ZERO, Hash.ZERO),
            Optional.of(payloadWithZeroSlot));

    assertThat(resp).isInstanceOf(JsonRpcSuccessResponse.class);
  }

  @Test
  public void shouldReturnInvalidTargetGasLimitParamsWhenTargetGasLimitMissing() {
    final BlockHeader mockHeader = setupValidForkchoiceUpdate();

    final PayloadAttributesV4 payloadWithMissingTargetGasLimit =
        new PayloadAttributesV4(
            String.valueOf(mockHeader.getTimestamp() + 1),
            Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
            Address.ECREC.toString(),
            Collections.emptyList(),
            Bytes32.ZERO.toHexString(),
            "0x1",
            null);

    final JsonRpcResponse resp =
        resp(
            new ForkchoiceStateV1(mockHeader.getBlockHash(), Hash.ZERO, Hash.ZERO),
            Optional.of(payloadWithMissingTargetGasLimit));

    assertThat(resp).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) resp).getErrorType())
        .isEqualTo(RpcErrorType.INVALID_TARGET_GAS_LIMIT_PARAMS);
  }

  @Test
  public void shouldForwardTargetGasLimitToPreparePayload() {
    final BlockHeader mockHeader = setupValidForkchoiceUpdate();
    final long targetGasLimitValue = 30_000_000L;

    final PayloadAttributesV4 attrs =
        new PayloadAttributesV4(
            String.valueOf(mockHeader.getTimestamp() + 1),
            Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
            Address.ECREC.toString(),
            Collections.emptyList(),
            Bytes32.ZERO.toHexString(),
            "0x1",
            "0x" + Long.toHexString(targetGasLimitValue));

    resp(
        new ForkchoiceStateV1(mockHeader.getBlockHash(), Hash.ZERO, Hash.ZERO), Optional.of(attrs));

    final ArgumentCaptor<MergeMiningCoordinator.PreparePayloadArgs> captor =
        ArgumentCaptor.forClass(MergeMiningCoordinator.PreparePayloadArgs.class);
    verify(mergeCoordinator).preparePayload(captor.capture());
    assertThat(captor.getValue().targetGasLimit()).contains(targetGasLimitValue);
  }
}
