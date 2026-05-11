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
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.INVALID;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.SYNCING;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.VALID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.MergeContext;
import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator.ForkchoiceResult;
import org.hyperledger.besu.consensus.merge.blockcreation.PayloadIdentifier;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineCallListener;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.engine.ForkchoiceStateV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.engine.PayloadAttributesV1;
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
import org.apache.tuweni.bytes.Bytes32;
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
public class EngineForkchoiceUpdatedV1Test {

  protected EngineForkchoiceUpdatedV1<?> method;

  protected static final Vertx vertx = Vertx.vertx();
  protected static final Hash mockHash = Hash.hash(Bytes32.fromHexStringLenient("0x1337deadbeef"));

  protected static final ForkchoiceStateV1 mockFcuParam =
      new ForkchoiceStateV1(mockHash, mockHash, mockHash);

  protected final BlockHeaderTestFixture blockHeaderBuilder =
      new BlockHeaderTestFixture().baseFeePerGas(Wei.ONE);

  @Mock protected ProtocolSpec protocolSpec;
  @Mock protected ProtocolSchedule protocolSchedule;
  @Mock protected ProtocolContext protocolContext;
  @Mock protected MergeContext mergeContext;
  @Mock protected MergeMiningCoordinator mergeCoordinator;
  @Mock protected MutableBlockchain blockchain;
  @Mock protected EngineCallListener engineCallListener;

  @BeforeEach
  public void before() {
    when(protocolContext.safeConsensusContext(Mockito.any())).thenReturn(Optional.of(mergeContext));
    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    when(protocolSpec.getWithdrawalsValidator())
        .thenReturn(new WithdrawalsValidator.ProhibitedWithdrawals());
    when(protocolSchedule.getForNextBlockHeader(any(), anyLong())).thenReturn(protocolSpec);
    createMethod();
  }

  /** Returns the method factory for the version under test. Overridden by each subclass. */
  protected EngineForkchoiceUpdatedV1<?> createMethodInstance() {
    return new EngineForkchoiceUpdatedV1<>(
        vertx, protocolSchedule, protocolContext, mergeCoordinator, engineCallListener, null, null);
  }

  protected final void createMethod() {
    this.method = createMethodInstance();
  }

  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_forkchoiceUpdatedV1");
  }

  protected String getMethodName() {
    return RpcMethod.ENGINE_FORKCHOICE_UPDATED_V1.getMethodName();
  }

  protected Object validPayloadAttributesForBlock(final BlockHeader head) {
    return new PayloadAttributesV1(
        String.valueOf(head.getTimestamp() + 1),
        Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
        "0x0000000000000000000000000000000000000001");
  }

  protected Object invalidTimestampPayloadAttributesForBlock(final BlockHeader head) {
    return new PayloadAttributesV1(
        String.valueOf(head.getTimestamp()),
        Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
        "0x0000000000000000000000000000000000000001");
  }

  // ---- shared tests ----

  @Test
  public void shouldReturnSyncingIfForwardSync() {
    when(mergeContext.isSyncing()).thenReturn(true);
    assertSuccessWithPayloadForForkchoiceResult(
        mockFcuParam, Optional.empty(), mock(ForkchoiceResult.class), SYNCING);
  }

  @Test
  public void shouldReturnSyncingIfMissingNewHead() {
    assertSuccessWithPayloadForForkchoiceResult(
        mockFcuParam, Optional.empty(), mock(ForkchoiceResult.class), SYNCING);
  }

  @Test
  public void shouldReturnSyncingOnHeadNotFound() {
    assertSuccessWithPayloadForForkchoiceResult(
        mockFcuParam, Optional.empty(), mock(ForkchoiceResult.class), SYNCING);
  }

  @Test
  public void shouldReturnInvalidWithLatestValidHashOnBadBlock() {
    final BlockHeader mockParent = blockHeaderBuilder.buildHeader();
    blockHeaderBuilder.parentHash(mockParent.getHash());
    final BlockHeader mockHeader = blockHeaderBuilder.buildHeader();
    final Hash latestValidHash = Hash.hash(Bytes32.fromHexStringLenient("0xcafebabe"));
    when(mergeCoordinator.isBadBlock(mockHeader.getHash())).thenReturn(true);
    when(mergeCoordinator.getLatestValidHashOfBadBlock(mockHeader.getHash()))
        .thenReturn(Optional.of(latestValidHash));

    final JsonRpcResponse resp =
        resp(
            new ForkchoiceStateV1(
                mockHeader.getHash(), mockHeader.getParentHash(), mockHeader.getParentHash()),
            Optional.empty());

    assertThat(resp.getType()).isEqualTo(RpcResponseType.SUCCESS);
    final EngineUpdateForkchoiceResult result =
        (EngineUpdateForkchoiceResult) ((JsonRpcSuccessResponse) resp).getResult();
    assertThat(result.getPayloadStatus().getStatus()).isEqualTo(INVALID);
    assertThat(result.getPayloadStatus().getLatestValidHash())
        .isEqualTo(Optional.of(latestValidHash));
    assertThat(result.getPayloadId()).isNull();
    verify(engineCallListener, times(1)).executionEngineCalled();
    verify(mergeContext, never()).fireNewUnverifiedForkchoiceEvent(any(), any(), any());
  }

  @Test
  public void shouldReturnValidWithoutFinalizedOrPayload() {
    final BlockHeader mockHeader = blockHeaderBuilder.buildHeader();
    when(mergeCoordinator.getOrSyncHeadByHash(mockHeader.getHash(), Hash.ZERO))
        .thenReturn(Optional.of(mockHeader));

    assertSuccessWithPayloadForForkchoiceResult(
        new ForkchoiceStateV1(mockHeader.getHash(), Hash.ZERO, Hash.ZERO),
        Optional.empty(),
        ForkchoiceResult.withResult(Optional.empty(), Optional.of(mockHeader)),
        VALID);
  }

  @Test
  public void shouldReturnValidWithNewHeadAndFinalizedNoPayload() {
    final BlockHeader mockParent = blockHeaderBuilder.number(9L).buildHeader();
    final BlockHeader mockHeader =
        blockHeaderBuilder.number(10L).parentHash(mockParent.getHash()).buildHeader();
    setupValidForkchoiceUpdate(mockHeader);

    assertSuccessWithPayloadForForkchoiceResult(
        new ForkchoiceStateV1(mockHeader.getHash(), mockParent.getHash(), Hash.ZERO),
        Optional.empty(),
        ForkchoiceResult.withResult(Optional.of(mockParent), Optional.of(mockHeader)),
        VALID);
  }

  @Test
  public void shouldReturnInvalidForkchoiceStateIfFinalizedBlockIsUnknown() {
    final BlockHeader newHead = blockHeaderBuilder.buildHeader();
    final Hash finalizedBlockHash = Hash.hash(Bytes32.fromHexStringLenient("0x424abcdef"));

    when(blockchain.getBlockHeader(finalizedBlockHash)).thenReturn(Optional.empty());
    when(mergeContext.isSyncing()).thenReturn(false);
    when(mergeCoordinator.getOrSyncHeadByHash(newHead.getHash(), finalizedBlockHash))
        .thenReturn(Optional.of(newHead));

    final JsonRpcResponse resp =
        resp(
            new ForkchoiceStateV1(newHead.getBlockHash(), finalizedBlockHash, finalizedBlockHash),
            Optional.empty());

    assertInvalidForkchoiceState(resp);
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnInvalidForkchoiceStateIfFinalizedBlockIsNotAnAncestorOfNewHead() {
    final BlockHeader finalized = blockHeaderBuilder.buildHeader();
    final BlockHeader newHead = blockHeaderBuilder.buildHeader();

    when(blockchain.getBlockHeader(finalized.getHash())).thenReturn(Optional.of(finalized));
    when(mergeContext.isSyncing()).thenReturn(false);
    when(mergeCoordinator.getOrSyncHeadByHash(newHead.getHash(), finalized.getBlockHash()))
        .thenReturn(Optional.of(newHead));
    when(mergeCoordinator.isDescendantOf(finalized, newHead)).thenReturn(false);

    final JsonRpcResponse resp =
        resp(
            new ForkchoiceStateV1(
                newHead.getBlockHash(), finalized.getBlockHash(), finalized.getBlockHash()),
            Optional.empty());

    assertInvalidForkchoiceState(resp);
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnInvalidForkchoiceStateIfSafeHeadZeroWithFinalizedBlock() {
    final BlockHeader parent = blockHeaderBuilder.buildHeader();
    final BlockHeader newHead =
        blockHeaderBuilder
            .parentHash(parent.getHash())
            .timestamp(parent.getTimestamp())
            .buildHeader();

    when(blockchain.getBlockHeader(parent.getHash())).thenReturn(Optional.of(parent));
    when(mergeContext.isSyncing()).thenReturn(false);
    when(mergeCoordinator.getOrSyncHeadByHash(newHead.getHash(), parent.getBlockHash()))
        .thenReturn(Optional.of(newHead));

    final JsonRpcResponse resp =
        resp(
            new ForkchoiceStateV1(newHead.getBlockHash(), Hash.ZERO, parent.getBlockHash()),
            Optional.empty());

    assertInvalidForkchoiceState(resp);
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnInvalidForkchoiceStateIfSafeBlockIsUnknown() {
    final BlockHeader finalized = blockHeaderBuilder.buildHeader();
    final BlockHeader newHead =
        blockHeaderBuilder
            .parentHash(finalized.getHash())
            .timestamp(finalized.getTimestamp())
            .buildHeader();
    final Hash safeBlockBlockHash = Hash.hash(Bytes32.fromHexStringLenient("0x424abcdef"));

    when(blockchain.getBlockHeader(finalized.getHash())).thenReturn(Optional.of(finalized));
    when(blockchain.getBlockHeader(safeBlockBlockHash)).thenReturn(Optional.empty());
    when(mergeContext.isSyncing()).thenReturn(false);
    when(mergeCoordinator.getOrSyncHeadByHash(newHead.getHash(), finalized.getBlockHash()))
        .thenReturn(Optional.of(newHead));
    when(mergeCoordinator.isDescendantOf(finalized, newHead)).thenReturn(true);

    final JsonRpcResponse resp =
        resp(
            new ForkchoiceStateV1(
                newHead.getBlockHash(), safeBlockBlockHash, finalized.getBlockHash()),
            Optional.empty());

    assertInvalidForkchoiceState(resp);
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnInvalidForkchoiceStateIfSafeBlockIsNotADescendantOfFinalized() {
    final BlockHeader finalized = blockHeaderBuilder.buildHeader();
    final BlockHeader newHead = blockHeaderBuilder.buildHeader();
    final BlockHeader safeBlock = blockHeaderBuilder.buildHeader();

    when(blockchain.getBlockHeader(newHead.getHash())).thenReturn(Optional.of(newHead));
    when(blockchain.getBlockHeader(finalized.getHash())).thenReturn(Optional.of(finalized));
    when(blockchain.getBlockHeader(safeBlock.getHash())).thenReturn(Optional.of(safeBlock));
    when(mergeContext.isSyncing()).thenReturn(false);
    when(mergeCoordinator.getOrSyncHeadByHash(newHead.getHash(), finalized.getBlockHash()))
        .thenReturn(Optional.of(newHead));
    when(mergeCoordinator.isDescendantOf(finalized, newHead)).thenReturn(true);
    when(mergeCoordinator.isDescendantOf(finalized, safeBlock)).thenReturn(false);

    final JsonRpcResponse resp =
        resp(
            new ForkchoiceStateV1(
                newHead.getBlockHash(), safeBlock.getBlockHash(), finalized.getBlockHash()),
            Optional.empty());

    assertInvalidForkchoiceState(resp);
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnInvalidForkchoiceStateIfSafeBlockIsNotAnAncestorOfNewHead() {
    final BlockHeader finalized = blockHeaderBuilder.buildHeader();
    final BlockHeader newHead = blockHeaderBuilder.buildHeader();
    final BlockHeader safeBlock = blockHeaderBuilder.buildHeader();

    when(blockchain.getBlockHeader(newHead.getHash())).thenReturn(Optional.of(newHead));
    when(blockchain.getBlockHeader(finalized.getHash())).thenReturn(Optional.of(finalized));
    when(blockchain.getBlockHeader(safeBlock.getHash())).thenReturn(Optional.of(safeBlock));
    when(mergeContext.isSyncing()).thenReturn(false);
    when(mergeCoordinator.getOrSyncHeadByHash(newHead.getHash(), finalized.getBlockHash()))
        .thenReturn(Optional.of(newHead));
    when(mergeCoordinator.isDescendantOf(finalized, newHead)).thenReturn(true);
    when(mergeCoordinator.isDescendantOf(finalized, safeBlock)).thenReturn(true);
    when(mergeCoordinator.isDescendantOf(safeBlock, newHead)).thenReturn(false);

    final JsonRpcResponse resp =
        resp(
            new ForkchoiceStateV1(
                newHead.getBlockHash(), safeBlock.getBlockHash(), finalized.getBlockHash()),
            Optional.empty());

    assertInvalidForkchoiceState(resp);
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnInvalidWhenForkchoiceResultIsInvalid() {
    final BlockHeader mockParent = blockHeaderBuilder.number(9L).buildHeader();
    final BlockHeader mockHeader =
        blockHeaderBuilder.number(10L).parentHash(mockParent.getHash()).buildHeader();
    setupValidForkchoiceUpdate(mockHeader);

    when(mergeCoordinator.updateForkChoiceWithoutLegacySkip(
            mockHeader, mockParent.getHash(), mockParent.getHash()))
        .thenReturn(
            ForkchoiceResult.withFailure(
                ForkchoiceResult.Status.INVALID,
                "new head timestamp not greater than parent",
                Optional.of(mockParent.getHash())));

    final ForkchoiceStateV1 param =
        new ForkchoiceStateV1(
            mockHeader.getBlockHash(), mockParent.getBlockHash(), mockParent.getBlockHash());

    final EngineUpdateForkchoiceResult resp = fromSuccessResp(resp(param, Optional.empty()));

    assertThat(resp.getPayloadStatus().getStatus()).isEqualTo(INVALID);
    assertThat(resp.getPayloadStatus().getLatestValidHash()).isPresent();
    assertThat(resp.getPayloadStatus().getLatestValidHash().get())
        .isEqualTo(mockParent.getBlockHash());
    assertThat(resp.getPayloadStatus().getError())
        .isEqualTo("new head timestamp not greater than parent");
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnValidWithoutFinalizedWithPayload() {
    final BlockHeader mockHeader = blockHeaderBuilder.buildHeader();
    when(mergeCoordinator.getOrSyncHeadByHash(mockHeader.getHash(), Hash.ZERO))
        .thenReturn(Optional.of(mockHeader));
    when(mergeCoordinator.preparePayload(any())).thenReturn(new PayloadIdentifier(1337L));

    final var res =
        assertSuccessWithPayloadForForkchoiceResult(
            new ForkchoiceStateV1(mockHeader.getHash(), Hash.ZERO, Hash.ZERO),
            Optional.of(validPayloadAttributesForBlock(mockHeader)),
            ForkchoiceResult.withResult(Optional.empty(), Optional.of(mockHeader)),
            VALID);

    assertThat(res.getPayloadId()).isNotNull();
  }

  @Test
  public void shouldReturnInvalidIfPayloadTimestampNotGreaterThanHead() {
    final BlockHeader mockParent = blockHeaderBuilder.timestamp(99).number(9L).buildHeader();
    final BlockHeader mockHeader =
        blockHeaderBuilder.number(10L).parentHash(mockParent.getHash()).buildHeader();
    setupValidForkchoiceUpdate(mockHeader);

    final JsonRpcResponse resp =
        resp(
            new ForkchoiceStateV1(mockHeader.getHash(), mockParent.getHash(), Hash.ZERO),
            Optional.of(invalidTimestampPayloadAttributesForBlock(mockHeader)));

    assertInvalidForkchoiceState(resp, RpcErrorType.INVALID_PAYLOAD_ATTRIBUTES);
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldSkipUpdateWhenHeadIsAncestorOfFinalized() {
    final BlockHeader finalized = blockHeaderBuilder.number(100L).buildHeader();
    final BlockHeader head = blockHeaderBuilder.number(50L).buildHeader();
    setupValidForkchoiceState(head, finalized);
    when(mergeCoordinator.isAncestorOfFinalized(head)).thenReturn(true);

    final JsonRpcResponse resp =
        resp(
            new ForkchoiceStateV1(head.getHash(), finalized.getHash(), finalized.getHash()),
            Optional.empty());

    assertThat(resp.getType()).isEqualTo(RpcResponseType.SUCCESS);
    final EngineUpdateForkchoiceResult result =
        (EngineUpdateForkchoiceResult) ((JsonRpcSuccessResponse) resp).getResult();
    assertThat(result.getPayloadStatus().getStatus()).isEqualTo(VALID);
    assertThat(result.getPayloadStatus().getLatestValidHashAsString())
        .isEqualTo(head.getHash().toHexString());
    assertThat(result.getPayloadId()).isNull();

    verify(mergeCoordinator, never()).updateForkChoiceWithoutLegacySkip(any(), any(), any());
  }

  @Test
  public void shouldNotSkipUpdateWhenHeadIsNotAncestorOfFinalized() {
    final BlockHeader finalized = blockHeaderBuilder.number(100L).buildHeader();
    final BlockHeader head = blockHeaderBuilder.number(150L).buildHeader();

    setupValidForkchoiceState(head, finalized);
    when(mergeCoordinator.isAncestorOfFinalized(head)).thenReturn(false);
    when(mergeCoordinator.updateForkChoiceWithoutLegacySkip(
            head, finalized.getHash(), finalized.getHash()))
        .thenReturn(ForkchoiceResult.withResult(Optional.of(finalized), Optional.of(head)));

    final JsonRpcResponse resp =
        resp(
            new ForkchoiceStateV1(head.getHash(), finalized.getHash(), finalized.getHash()),
            Optional.empty());

    assertThat(resp.getType()).isEqualTo(RpcResponseType.SUCCESS);
    verify(mergeCoordinator, times(1))
        .updateForkChoiceWithoutLegacySkip(head, finalized.getHash(), finalized.getHash());
  }

  @Test
  public void shouldNotSkipUpdateWhenFinalizedHashIsZero() {
    final BlockHeader head = blockHeaderBuilder.number(50L).buildHeader();

    when(mergeCoordinator.getOrSyncHeadByHash(head.getHash(), Hash.ZERO))
        .thenReturn(Optional.of(head));
    when(mergeCoordinator.computeReorgDepth(head)).thenReturn(OptionalLong.empty());
    when(mergeCoordinator.isAncestorOfFinalized(head)).thenReturn(false);
    when(mergeCoordinator.updateForkChoiceWithoutLegacySkip(head, Hash.ZERO, Hash.ZERO))
        .thenReturn(ForkchoiceResult.withResult(Optional.empty(), Optional.of(head)));

    final JsonRpcResponse resp =
        resp(new ForkchoiceStateV1(head.getHash(), Hash.ZERO, Hash.ZERO), Optional.empty());

    assertThat(resp.getType()).isEqualTo(RpcResponseType.SUCCESS);
    verify(mergeCoordinator, times(1))
        .updateForkChoiceWithoutLegacySkip(head, Hash.ZERO, Hash.ZERO);
  }

  @Test
  public void shouldRejectWithTooDeepReorgWhenDepthExceedsLimit() {
    final BlockHeader head = blockHeaderBuilder.number(20_000L).buildHeader();
    when(mergeCoordinator.getOrSyncHeadByHash(head.getHash(), Hash.ZERO))
        .thenReturn(Optional.of(head));
    when(mergeCoordinator.computeReorgDepth(head))
        .thenReturn(OptionalLong.of(MergeMiningCoordinator.MAX_REORG_DEPTH + 1));

    final JsonRpcResponse resp =
        resp(new ForkchoiceStateV1(head.getHash(), Hash.ZERO, Hash.ZERO), Optional.empty());

    assertThat(resp.getType()).isEqualTo(RpcResponseType.ERROR);
    final JsonRpcErrorResponse errorResp = (JsonRpcErrorResponse) resp;
    assertThat(errorResp.getErrorType()).isEqualTo(RpcErrorType.TOO_DEEP_REORG);

    verify(mergeCoordinator, never()).updateForkChoiceWithoutLegacySkip(any(), any(), any());
  }

  @Test
  public void shouldAcceptWhenReorgDepthEqualsLimit() {
    final BlockHeader head = blockHeaderBuilder.number(20_000L).buildHeader();
    when(mergeCoordinator.getOrSyncHeadByHash(head.getHash(), Hash.ZERO))
        .thenReturn(Optional.of(head));
    when(mergeCoordinator.computeReorgDepth(head))
        .thenReturn(OptionalLong.of(MergeMiningCoordinator.MAX_REORG_DEPTH));
    when(mergeCoordinator.updateForkChoiceWithoutLegacySkip(head, Hash.ZERO, Hash.ZERO))
        .thenReturn(ForkchoiceResult.withResult(Optional.empty(), Optional.of(head)));

    final JsonRpcResponse resp =
        resp(new ForkchoiceStateV1(head.getHash(), Hash.ZERO, Hash.ZERO), Optional.empty());

    assertThat(resp.getType()).isEqualTo(RpcResponseType.SUCCESS);
    verify(mergeCoordinator, times(1))
        .updateForkChoiceWithoutLegacySkip(head, Hash.ZERO, Hash.ZERO);
  }

  @Test
  public void shouldAcceptWhenReorgDepthIsEmpty() {
    final BlockHeader head = blockHeaderBuilder.number(50L).buildHeader();
    when(mergeCoordinator.getOrSyncHeadByHash(head.getHash(), Hash.ZERO))
        .thenReturn(Optional.of(head));
    when(mergeCoordinator.computeReorgDepth(head)).thenReturn(OptionalLong.empty());
    when(mergeCoordinator.updateForkChoiceWithoutLegacySkip(head, Hash.ZERO, Hash.ZERO))
        .thenReturn(ForkchoiceResult.withResult(Optional.empty(), Optional.of(head)));

    final JsonRpcResponse resp =
        resp(new ForkchoiceStateV1(head.getHash(), Hash.ZERO, Hash.ZERO), Optional.empty());

    assertThat(resp.getType()).isEqualTo(RpcResponseType.SUCCESS);
    verify(mergeCoordinator, times(1))
        .updateForkChoiceWithoutLegacySkip(head, Hash.ZERO, Hash.ZERO);
  }

  // ---- helpers ----

  protected void setupValidForkchoiceUpdate(final BlockHeader mockHeader) {
    when(blockchain.getBlockHeader(any())).thenReturn(Optional.of(mockHeader));
    when(mergeCoordinator.getOrSyncHeadByHash(eq(mockHeader.getHash()), any()))
        .thenReturn(Optional.of(mockHeader));
    when(mergeCoordinator.isDescendantOf(any(), any())).thenReturn(true);
    when(mergeCoordinator.computeReorgDepth(mockHeader)).thenReturn(OptionalLong.empty());
    when(mergeCoordinator.updateForkChoiceWithoutLegacySkip(any(), any(), any()))
        .thenReturn(mock(ForkchoiceResult.class));
  }

  protected void setupValidForkchoiceState(final BlockHeader head, final BlockHeader finalized) {
    when(blockchain.getBlockHeader(finalized.getHash())).thenReturn(Optional.of(finalized));
    when(mergeCoordinator.getOrSyncHeadByHash(head.getHash(), finalized.getHash()))
        .thenReturn(Optional.of(head));
    when(mergeCoordinator.isDescendantOf(any(), any())).thenReturn(true);
    when(mergeCoordinator.computeReorgDepth(head)).thenReturn(OptionalLong.empty());
  }

  protected EngineUpdateForkchoiceResult assertSuccessWithPayloadForForkchoiceResult(
      final ForkchoiceStateV1 fcuParam,
      final Optional<Object> payloadParam,
      final ForkchoiceResult forkchoiceResult,
      final EngineStatus expectedStatus) {
    return assertSuccessWithPayloadForForkchoiceResult(
        fcuParam, payloadParam, forkchoiceResult, expectedStatus, Optional.empty());
  }

  protected EngineUpdateForkchoiceResult assertSuccessWithPayloadForForkchoiceResult(
      final ForkchoiceStateV1 fcuParam,
      final Optional<Object> payloadParam,
      final ForkchoiceResult forkchoiceResult,
      final EngineStatus expectedStatus,
      final Optional<Hash> maybeLatestValidHash) {

    when(mergeCoordinator.updateForkChoiceWithoutLegacySkip(
            any(BlockHeader.class), any(Hash.class), any(Hash.class)))
        .thenReturn(forkchoiceResult);

    final JsonRpcResponse resp = resp(fcuParam, payloadParam);
    final EngineUpdateForkchoiceResult res = fromSuccessResp(resp);

    assertThat(res.getPayloadStatus().getStatusAsString()).isEqualTo(expectedStatus.name());

    if (expectedStatus.equals(VALID)) {
      assertThat(res.getPayloadStatus().getLatestValidHash())
          .isEqualTo(forkchoiceResult.getNewHead().map(BlockHeader::getBlockHash));
      assertThat(res.getPayloadStatus().getError()).isNullOrEmpty();
      if (payloadParam.isPresent()) {
        assertThat(res.getPayloadId()).isNotNull();
      } else {
        assertThat(res.getPayloadId()).isNull();
      }
    } else {
      assertThat(res.getPayloadStatus().getLatestValidHash()).isEqualTo(maybeLatestValidHash);
      assertThat(res.getPayloadId()).isNull();
    }

    verify(mergeContext)
        .fireNewUnverifiedForkchoiceEvent(
            fcuParam.getHeadBlockHash(),
            fcuParam.getSafeBlockHash(),
            fcuParam.getFinalizedBlockHash());

    verify(engineCallListener, times(1)).executionEngineCalled();

    return res;
  }

  protected JsonRpcResponse resp(
      final ForkchoiceStateV1 forkchoiceParam, final Optional<Object> payloadParam) {
    return method.response(
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0",
                getMethodName(),
                Stream.concat(Stream.of(forkchoiceParam), payloadParam.stream()).toArray())));
  }

  private EngineUpdateForkchoiceResult fromSuccessResp(final JsonRpcResponse resp) {
    assertThat(resp.getType()).isEqualTo(RpcResponseType.SUCCESS);
    return Optional.of(resp)
        .map(JsonRpcSuccessResponse.class::cast)
        .map(JsonRpcSuccessResponse::getResult)
        .map(EngineUpdateForkchoiceResult.class::cast)
        .get();
  }

  protected void assertInvalidForkchoiceState(final JsonRpcResponse resp) {
    assertInvalidForkchoiceState(resp, RpcErrorType.INVALID_FORKCHOICE_STATE);
  }

  protected void assertInvalidForkchoiceState(
      final JsonRpcResponse resp, final RpcErrorType jsonRpcError) {
    assertThat(resp.getType()).isEqualTo(RpcResponseType.ERROR);
    final JsonRpcErrorResponse errorResp = (JsonRpcErrorResponse) resp;
    assertThat(errorResp.getErrorType()).isEqualTo(jsonRpcError);
    assertThat(errorResp.getError().getMessage()).isEqualTo(jsonRpcError.getMessage());
  }
}
