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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.forkchoiceupdated;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.AMSTERDAM;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator.ForkchoiceResult;
import org.hyperledger.besu.consensus.merge.blockcreation.PayloadIdentifier;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.engine.ForkchoiceStateV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.engine.PayloadAttributesV4;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.Collections;
import java.util.Optional;
import java.util.OptionalLong;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EngineForkchoiceUpdatedV4Test extends EngineForkchoiceUpdatedV3Test {

  @Override
  protected EngineForkchoiceUpdatedV1<?> createMethodInstance() {
    // V4 has no upper bound (null maxFork = open-ended).
    return new EngineForkchoiceUpdatedV4<>(
        vertx,
        protocolSchedule,
        protocolContext,
        mergeCoordinator,
        engineCallListener,
        AMSTERDAM,
        null);
  }

  @Override
  @BeforeEach
  public void before() {
    super.before();
    // Set blockHeaderBuilder default timestamp to AMSTERDAM_MILESTONE so inherited tests that call
    // blockHeaderBuilder.buildHeader() produce payloads >= AMSTERDAM_MILESTONE (V4's lower bound).
    blockHeaderBuilder.timestamp(AMSTERDAM_MILESTONE);
    createMethod();
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
        "0x1");
  }

  @Override
  protected Object invalidTimestampPayloadAttributesForBlock(final BlockHeader head) {
    return new PayloadAttributesV4(
        String.valueOf(head.getTimestamp()),
        Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
        Address.ECREC.toString(),
        Collections.emptyList(),
        Bytes32.ZERO.toHexString(),
        "0x1");
  }

  // Inherited from V3Test: V4 has no upper bound, so Amsterdam timestamps are valid (open-ended).
  @Override
  @Test
  public void shouldReturnUnsupportedForkIfBlockTimestampIsAfterAmsterdamMilestone() {}

  // Inherited from V3Test: Cancun timestamps (< Amsterdam) are below V4's minimum — not applicable.
  @Override
  @Test
  public void shouldReturnValidForTimestampInCancunWindow() {}

  // ---- V4-specific tests ----

  @Test
  public void shouldReturnUnsupportedForkIfBlockTimestampIsBeforeAmsterdamMilestone() {
    // head.timestamp = AMSTERDAM_MILESTONE-2 so payload = AMSTERDAM_MILESTONE-1 (> head) and
    // AMSTERDAM_MILESTONE-1 < AMSTERDAM_MILESTONE triggers the min-bound UNSUPPORTED_FORK
    final BlockHeader mockHeader =
        blockHeaderBuilder.timestamp(AMSTERDAM_MILESTONE - 2).buildHeader();
    when(mergeCoordinator.getOrSyncHeadByHash(mockHeader.getHash(), Hash.ZERO))
        .thenReturn(Optional.of(mockHeader));
    when(mergeCoordinator.isDescendantOf(any(), any())).thenReturn(true);
    when(mergeCoordinator.computeReorgDepth(any())).thenReturn(OptionalLong.empty());
    when(mergeCoordinator.updateForkChoiceWithoutLegacySkip(any(), any(), any()))
        .thenReturn(mock(ForkchoiceResult.class));

    final JsonRpcResponse resp =
        resp(
            new ForkchoiceStateV1(mockHeader.getBlockHash(), Hash.ZERO, Hash.ZERO),
            Optional.of(validPayloadAttributesForBlock(mockHeader)));

    final JsonRpcError jsonRpcError =
        Optional.of(resp)
            .map(JsonRpcErrorResponse.class::cast)
            .map(JsonRpcErrorResponse::getError)
            .get();
    assertThat(jsonRpcError.getCode()).isEqualTo(RpcErrorType.UNSUPPORTED_FORK.getCode());
  }

  @Test
  public void shouldReturnValidForTimestampAtAmsterdamMilestone() {
    final BlockHeader mockHeader = blockHeaderBuilder.timestamp(AMSTERDAM_MILESTONE).buildHeader();
    when(mergeCoordinator.getOrSyncHeadByHash(mockHeader.getHash(), Hash.ZERO))
        .thenReturn(Optional.of(mockHeader));
    when(mergeCoordinator.isDescendantOf(any(), any())).thenReturn(true);
    when(mergeCoordinator.computeReorgDepth(any())).thenReturn(OptionalLong.empty());
    when(mergeCoordinator.preparePayload(any())).thenReturn(new PayloadIdentifier(1337L));
    when(mergeCoordinator.updateForkChoiceWithoutLegacySkip(any(), any(), any()))
        .thenReturn(ForkchoiceResult.withResult(Optional.empty(), Optional.of(mockHeader)));

    final JsonRpcResponse resp =
        resp(
            new ForkchoiceStateV1(mockHeader.getBlockHash(), Hash.ZERO, Hash.ZERO),
            Optional.of(validPayloadAttributesForBlock(mockHeader)));

    assertThat(resp).isInstanceOf(JsonRpcSuccessResponse.class);
  }

  @Test
  public void shouldReturnInvalidSlotNumberParamsForNegativeSlotNumber() {
    final BlockHeader mockHeader =
        blockHeaderBuilder.timestamp(AMSTERDAM_MILESTONE + 1).buildHeader();
    when(mergeCoordinator.getOrSyncHeadByHash(mockHeader.getHash(), Hash.ZERO))
        .thenReturn(Optional.of(mockHeader));
    when(mergeCoordinator.isDescendantOf(any(), any())).thenReturn(true);
    when(mergeCoordinator.computeReorgDepth(any())).thenReturn(OptionalLong.empty());
    when(mergeCoordinator.updateForkChoiceWithoutLegacySkip(any(), any(), any()))
        .thenReturn(mock(ForkchoiceResult.class));

    final PayloadAttributesV4 payloadWithNegativeSlot =
        new PayloadAttributesV4(
            String.valueOf(mockHeader.getTimestamp() + 1),
            Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
            Address.ECREC.toString(),
            Collections.emptyList(),
            Bytes32.ZERO.toHexString(),
            "-0x1");

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
    final BlockHeader mockHeader =
        blockHeaderBuilder.timestamp(AMSTERDAM_MILESTONE + 1).buildHeader();
    when(mergeCoordinator.getOrSyncHeadByHash(mockHeader.getHash(), Hash.ZERO))
        .thenReturn(Optional.of(mockHeader));
    when(mergeCoordinator.isDescendantOf(any(), any())).thenReturn(true);
    when(mergeCoordinator.computeReorgDepth(any())).thenReturn(OptionalLong.empty());
    when(mergeCoordinator.preparePayload(any())).thenReturn(new PayloadIdentifier(1337L));
    when(mergeCoordinator.updateForkChoiceWithoutLegacySkip(any(), any(), any()))
        .thenReturn(ForkchoiceResult.withResult(Optional.empty(), Optional.of(mockHeader)));

    final PayloadAttributesV4 payloadWithZeroSlot =
        new PayloadAttributesV4(
            String.valueOf(mockHeader.getTimestamp() + 1),
            Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
            Address.ECREC.toString(),
            Collections.emptyList(),
            Bytes32.ZERO.toHexString(),
            "0x0");

    final JsonRpcResponse resp =
        resp(
            new ForkchoiceStateV1(mockHeader.getBlockHash(), Hash.ZERO, Hash.ZERO),
            Optional.of(payloadWithZeroSlot));

    assertThat(resp).isInstanceOf(JsonRpcSuccessResponse.class);
  }
}
