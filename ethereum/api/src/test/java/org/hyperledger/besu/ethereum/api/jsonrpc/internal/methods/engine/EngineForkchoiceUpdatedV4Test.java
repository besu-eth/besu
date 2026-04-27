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
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.CANCUN;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.VALID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.MergeContext;
import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator.ForkchoiceResult;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EngineForkchoiceUpdatedParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EnginePayloadAttributesParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EngineUpdateForkchoiceResult;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.WithdrawalsValidator;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Stream;

import io.vertx.core.Vertx;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class EngineForkchoiceUpdatedV4Test {

  private static final Vertx vertx = Vertx.vertx();
  private static final long CANCUN_MILESTONE = 1_000_000L;
  private static final long AMSTERDAM_MILESTONE = 2_000_000L;

  private final BlockHeaderTestFixture blockHeaderBuilder =
      new BlockHeaderTestFixture().baseFeePerGas(Wei.ONE);

  @Mock private ProtocolSpec protocolSpec;
  @Mock private ProtocolSchedule protocolSchedule;
  @Mock private ProtocolContext protocolContext;
  @Mock private MergeContext mergeContext;
  @Mock private MergeMiningCoordinator mergeCoordinator;
  @Mock private MutableBlockchain blockchain;
  @Mock private EngineCallListener engineCallListener;

  private EngineForkchoiceUpdatedV4 method;

  @BeforeEach
  public void before() {
    when(protocolContext.safeConsensusContext(Mockito.any())).thenReturn(Optional.of(mergeContext));
    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    when(protocolSpec.getWithdrawalsValidator())
        .thenReturn(new WithdrawalsValidator.AllowedWithdrawals());
    when(protocolSchedule.getForNextBlockHeader(any(), anyLong())).thenReturn(protocolSpec);
    when(protocolSchedule.milestoneFor(CANCUN)).thenReturn(Optional.of(CANCUN_MILESTONE));
    when(protocolSchedule.milestoneFor(AMSTERDAM)).thenReturn(Optional.of(AMSTERDAM_MILESTONE));
    method =
        new EngineForkchoiceUpdatedV4(
            vertx, protocolSchedule, protocolContext, mergeCoordinator, engineCallListener);
  }

  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_forkchoiceUpdatedV4");
  }

  @Test
  public void shouldSkipUpdateWhenHeadIsAncestorOfFinalized() {
    final BlockHeader finalized =
        blockHeaderBuilder.number(100L).timestamp(AMSTERDAM_MILESTONE + 1).buildHeader();
    final BlockHeader head =
        blockHeaderBuilder.number(50L).timestamp(AMSTERDAM_MILESTONE + 1).buildHeader();
    setupValidForkchoiceState(head, finalized);
    when(mergeCoordinator.computeReorgDepth(head)).thenReturn(OptionalLong.of(0L));
    when(mergeCoordinator.isAncestorOfFinalized(head.getHash())).thenReturn(true);

    final EngineForkchoiceUpdatedParameter param =
        new EngineForkchoiceUpdatedParameter(
            head.getHash(), finalized.getHash(), finalized.getHash());

    final JsonRpcResponse resp = resp(param, Optional.empty());

    assertThat(resp.getType()).isEqualTo(RpcResponseType.SUCCESS);

    final EngineUpdateForkchoiceResult result =
        (EngineUpdateForkchoiceResult) ((JsonRpcSuccessResponse) resp).getResult();
    assertThat(result.getPayloadStatus().getStatus()).isEqualTo(VALID);
    assertThat(result.getPayloadStatus().getLatestValidHashAsString())
        .isEqualTo(head.getHash().toHexString());
    assertThat(result.getPayloadId()).isNull();

    verify(mergeCoordinator, never()).updateForkChoiceWithoutLegacySkip(any(), any(), any());
    verify(mergeCoordinator, never()).updateForkChoice(any(), any(), any());
  }

  @Test
  public void shouldNotSkipUpdateWhenHeadIsNotAncestorOfFinalized() {
    final BlockHeader finalized =
        blockHeaderBuilder.number(100L).timestamp(AMSTERDAM_MILESTONE + 1).buildHeader();
    final BlockHeader head =
        blockHeaderBuilder.number(150L).timestamp(AMSTERDAM_MILESTONE + 1).buildHeader();

    setupValidForkchoiceState(head, finalized);
    when(mergeCoordinator.computeReorgDepth(head)).thenReturn(OptionalLong.of(0L));
    when(mergeCoordinator.isAncestorOfFinalized(head.getHash())).thenReturn(false);
    when(mergeCoordinator.updateForkChoiceWithoutLegacySkip(
            head, finalized.getHash(), finalized.getHash()))
        .thenReturn(ForkchoiceResult.withResult(Optional.of(finalized), Optional.of(head)));

    final EngineForkchoiceUpdatedParameter param =
        new EngineForkchoiceUpdatedParameter(
            head.getHash(), finalized.getHash(), finalized.getHash());

    final JsonRpcResponse resp = resp(param, Optional.empty());

    assertThat(resp.getType()).isEqualTo(RpcResponseType.SUCCESS);
    verify(mergeCoordinator, times(1))
        .updateForkChoiceWithoutLegacySkip(head, finalized.getHash(), finalized.getHash());
    verify(mergeCoordinator, never()).updateForkChoice(any(), any(), any());
  }

  @Test
  public void shouldNotConsultIsAncestorOfFinalizedWhenFinalizedHashIsZero() {
    final BlockHeader head =
        blockHeaderBuilder.number(50L).timestamp(AMSTERDAM_MILESTONE + 1).buildHeader();

    when(mergeCoordinator.getOrSyncHeadByHash(head.getHash(), Hash.ZERO))
        .thenReturn(Optional.of(head));
    when(mergeCoordinator.computeReorgDepth(head)).thenReturn(OptionalLong.of(0L));
    when(mergeCoordinator.updateForkChoiceWithoutLegacySkip(head, Hash.ZERO, Hash.ZERO))
        .thenReturn(ForkchoiceResult.withResult(Optional.empty(), Optional.of(head)));

    final EngineForkchoiceUpdatedParameter param =
        new EngineForkchoiceUpdatedParameter(head.getHash(), Hash.ZERO, Hash.ZERO);

    final JsonRpcResponse resp = resp(param, Optional.empty());

    assertThat(resp.getType()).isEqualTo(RpcResponseType.SUCCESS);
    verify(mergeCoordinator, never()).isAncestorOfFinalized(any());
    verify(mergeCoordinator, times(1))
        .updateForkChoiceWithoutLegacySkip(head, Hash.ZERO, Hash.ZERO);
  }

  @Test
  public void shouldRejectWithTooDeepReorgWhenDepthExceedsLimit() {
    final BlockHeader head =
        blockHeaderBuilder.number(20_000L).timestamp(AMSTERDAM_MILESTONE + 1).buildHeader();
    when(mergeCoordinator.getOrSyncHeadByHash(head.getHash(), Hash.ZERO))
        .thenReturn(Optional.of(head));
    when(mergeCoordinator.computeReorgDepth(head))
        .thenReturn(OptionalLong.of(MergeMiningCoordinator.MAX_REORG_DEPTH + 1));

    final EngineForkchoiceUpdatedParameter param =
        new EngineForkchoiceUpdatedParameter(head.getHash(), Hash.ZERO, Hash.ZERO);

    final JsonRpcResponse resp = resp(param, Optional.empty());

    assertThat(resp.getType()).isEqualTo(RpcResponseType.ERROR);
    final JsonRpcErrorResponse errorResp = (JsonRpcErrorResponse) resp;
    assertThat(errorResp.getErrorType()).isEqualTo(RpcErrorType.TOO_DEEP_REORG);
    assertThat(errorResp.getError().getCode()).isEqualTo(-38006);
    assertThat(errorResp.getError().getMessage()).isEqualTo("Too deep reorg");

    verify(mergeCoordinator, never()).updateForkChoiceWithoutLegacySkip(any(), any(), any());
    verify(mergeCoordinator, never()).updateForkChoice(any(), any(), any());
    verify(mergeCoordinator, never()).isAncestorOfFinalized(any());
  }

  @Test
  public void shouldAcceptWhenReorgDepthEqualsLimit() {
    final BlockHeader head =
        blockHeaderBuilder.number(20_000L).timestamp(AMSTERDAM_MILESTONE + 1).buildHeader();
    when(mergeCoordinator.getOrSyncHeadByHash(head.getHash(), Hash.ZERO))
        .thenReturn(Optional.of(head));
    when(mergeCoordinator.computeReorgDepth(head))
        .thenReturn(OptionalLong.of(MergeMiningCoordinator.MAX_REORG_DEPTH));
    when(mergeCoordinator.updateForkChoiceWithoutLegacySkip(head, Hash.ZERO, Hash.ZERO))
        .thenReturn(ForkchoiceResult.withResult(Optional.empty(), Optional.of(head)));

    final EngineForkchoiceUpdatedParameter param =
        new EngineForkchoiceUpdatedParameter(head.getHash(), Hash.ZERO, Hash.ZERO);

    final JsonRpcResponse resp = resp(param, Optional.empty());

    assertThat(resp.getType()).isEqualTo(RpcResponseType.SUCCESS);
    verify(mergeCoordinator, times(1))
        .updateForkChoiceWithoutLegacySkip(head, Hash.ZERO, Hash.ZERO);
  }

  @Test
  public void shouldAcceptWhenReorgDepthIsEmpty() {
    final BlockHeader head =
        blockHeaderBuilder.number(50L).timestamp(AMSTERDAM_MILESTONE + 1).buildHeader();
    when(mergeCoordinator.getOrSyncHeadByHash(head.getHash(), Hash.ZERO))
        .thenReturn(Optional.of(head));
    when(mergeCoordinator.computeReorgDepth(head)).thenReturn(OptionalLong.empty());
    when(mergeCoordinator.updateForkChoiceWithoutLegacySkip(head, Hash.ZERO, Hash.ZERO))
        .thenReturn(ForkchoiceResult.withResult(Optional.empty(), Optional.of(head)));

    final EngineForkchoiceUpdatedParameter param =
        new EngineForkchoiceUpdatedParameter(head.getHash(), Hash.ZERO, Hash.ZERO);

    final JsonRpcResponse resp = resp(param, Optional.empty());

    assertThat(resp.getType()).isEqualTo(RpcResponseType.SUCCESS);
    verify(mergeCoordinator, times(1))
        .updateForkChoiceWithoutLegacySkip(head, Hash.ZERO, Hash.ZERO);
  }

  private void setupValidForkchoiceState(final BlockHeader head, final BlockHeader finalized) {
    when(blockchain.getBlockHeader(finalized.getHash())).thenReturn(Optional.of(finalized));
    when(mergeCoordinator.getOrSyncHeadByHash(head.getHash(), finalized.getHash()))
        .thenReturn(Optional.of(head));
    when(mergeCoordinator.isDescendantOf(any(), any())).thenReturn(true);
  }

  private JsonRpcResponse resp(
      final EngineForkchoiceUpdatedParameter forkchoiceParam,
      final Optional<EnginePayloadAttributesParameter> payloadParam) {
    return method.response(
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0",
                RpcMethod.ENGINE_FORKCHOICE_UPDATED_V4.getMethodName(),
                Stream.concat(Stream.of(forkchoiceParam), payloadParam.stream()).toArray())));
  }
}
