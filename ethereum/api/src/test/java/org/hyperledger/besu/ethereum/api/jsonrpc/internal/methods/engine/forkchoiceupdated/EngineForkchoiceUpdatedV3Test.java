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
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.CANCUN;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator.ForkchoiceResult;
import org.hyperledger.besu.consensus.merge.blockcreation.PayloadIdentifier;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.engine.ForkchoiceStateV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.engine.PayloadAttributesV3;
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
public class EngineForkchoiceUpdatedV3Test extends EngineForkchoiceUpdatedV2Test {

  static final long AMSTERDAM_MILESTONE = 2_000_000L;

  @Override
  protected EngineForkchoiceUpdatedV1<?> createMethodInstance() {
    return new EngineForkchoiceUpdatedV3<>(
        vertx,
        protocolSchedule,
        protocolContext,
        mergeCoordinator,
        engineCallListener,
        CANCUN,
        AMSTERDAM);
  }

  @Override
  @BeforeEach
  public void before() {
    super.before();
    // AMSTERDAM upper bound; set blockHeaderBuilder default timestamp to CANCUN_MILESTONE so
    // inherited tests that call buildHeader() produce payloads in the valid [CANCUN, AMSTERDAM)
    // window.
    when(protocolSchedule.milestoneFor(AMSTERDAM)).thenReturn(Optional.of(AMSTERDAM_MILESTONE));
    blockHeaderBuilder.timestamp(CANCUN_MILESTONE);
    createMethod();
  }

  @Override
  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_forkchoiceUpdatedV3");
  }

  @Override
  protected String getMethodName() {
    return RpcMethod.ENGINE_FORKCHOICE_UPDATED_V3.getMethodName();
  }

  @Override
  protected Object validPayloadAttributesForBlock(final BlockHeader head) {
    return new PayloadAttributesV3(
        String.valueOf(head.getTimestamp() + 1),
        Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
        Address.ECREC.toString(),
        Collections.emptyList(),
        Bytes32.ZERO.toHexString());
  }

  @Override
  protected Object invalidTimestampPayloadAttributesForBlock(final BlockHeader head) {
    return new PayloadAttributesV3(
        String.valueOf(head.getTimestamp()),
        Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
        Address.ECREC.toString(),
        Collections.emptyList(),
        Bytes32.ZERO.toHexString());
  }

  // Inherited from V2Test: in V3, Cancun-era timestamps are within the valid [CANCUN, AMSTERDAM)
  // window — the V2 upper-bound scenario no longer applies here.
  @Override
  @Test
  public void shouldReturnUnsupportedForkIfBlockTimestampIsAfterCancunMilestone() {}

  // ---- V3-specific tests ----

  @Test
  public void shouldReturnUnsupportedForkIfBlockTimestampIsBeforeCancunMilestone() {
    // head.timestamp = CANCUN_MILESTONE-2 so payload = CANCUN_MILESTONE-1 (> head) and
    // CANCUN_MILESTONE-1 < CANCUN_MILESTONE triggers the min-bound UNSUPPORTED_FORK
    final BlockHeader mockHeader = blockHeaderBuilder.timestamp(CANCUN_MILESTONE - 2).buildHeader();
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
  public void shouldReturnUnsupportedForkIfBlockTimestampIsAfterAmsterdamMilestone() {
    // head.timestamp = AMSTERDAM_MILESTONE so payload = AMSTERDAM_MILESTONE+1 (> head) and
    // AMSTERDAM_MILESTONE+1 >= AMSTERDAM_MILESTONE triggers the max-bound UNSUPPORTED_FORK
    final BlockHeader mockHeader = blockHeaderBuilder.timestamp(AMSTERDAM_MILESTONE).buildHeader();
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
  public void shouldReturnValidForTimestampInCancunWindow() {
    final BlockHeader mockHeader = blockHeaderBuilder.timestamp(CANCUN_MILESTONE).buildHeader();
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
}
