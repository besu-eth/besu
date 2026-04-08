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
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.SYNCING;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.VALID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.MergeContext;
import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator.ForkchoiceResult;
import org.hyperledger.besu.consensus.merge.blockcreation.PayloadIdentifier;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.GWei;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EngineForkchoiceUpdatedParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EnginePayloadAttributesParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.WithdrawalParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EngineUpdateForkchoiceResult;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.WithdrawalsValidator;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.vertx.core.Vertx;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public abstract class AbstractEngineForkchoiceUpdatedTest {

  @FunctionalInterface
  interface MethodFactory {
    AbstractEngineForkchoiceUpdated create(
        final Vertx vertx,
        final ProtocolSchedule protocolSchedule,
        final ProtocolContext protocolContext,
        final MergeMiningCoordinator mergeCoordinator,
        final EngineCallListener engineCallListener);
  }

  private final MethodFactory methodFactory;
  protected AbstractEngineForkchoiceUpdated method;

  public AbstractEngineForkchoiceUpdatedTest(final MethodFactory methodFactory) {
    this.methodFactory = methodFactory;
  }

  private static final Vertx vertx = Vertx.vertx();
  private static final Hash mockHash = Hash.hash(Bytes32.fromHexStringLenient("0x1337deadbeef"));
  protected static final long CANCUN_MILESTONE = 1_000_000L;

  private static final EngineForkchoiceUpdatedParameter mockFcuParam =
      new EngineForkchoiceUpdatedParameter(mockHash, mockHash, mockHash);

  protected final BlockHeaderTestFixture blockHeaderBuilder =
      new BlockHeaderTestFixture().baseFeePerGas(Wei.ONE);

  @Mock private ProtocolSpec protocolSpec;
  @Mock protected ProtocolSchedule protocolSchedule;
  @Mock private ProtocolContext protocolContext;

  @Mock private MergeContext mergeContext;

  @Mock protected MergeMiningCoordinator mergeCoordinator;

  @Mock protected MutableBlockchain blockchain;

  @Mock private EngineCallListener engineCallListener;

  @BeforeEach
  public void before() {
    when(protocolContext.safeConsensusContext(Mockito.any())).thenReturn(Optional.of(mergeContext));
    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    when(protocolSpec.getWithdrawalsValidator())
        .thenReturn(new WithdrawalsValidator.ProhibitedWithdrawals());
    when(protocolSchedule.getForNextBlockHeader(any(), anyLong())).thenReturn(protocolSpec);
    when(protocolSchedule.milestoneFor(CANCUN)).thenReturn(Optional.of(CANCUN_MILESTONE));
    this.method =
        methodFactory.create(
            vertx, protocolSchedule, protocolContext, mergeCoordinator, engineCallListener);
  }

  @Test
  public abstract void shouldReturnExpectedMethodName();

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
  public void shouldReturnInvalidWithLatestValidHashOnBadBlock() {
    BlockHeader mockParent = blockHeaderBuilder.buildHeader();
    blockHeaderBuilder.parentHash(mockParent.getHash());
    BlockHeader mockHeader = blockHeaderBuilder.buildHeader();
    Hash latestValidHash = Hash.hash(Bytes32.fromHexStringLenient("0xcafebabe"));
    when(blockchain.getBlockHeader(mockHeader.getHash())).thenReturn(Optional.of(mockHeader));
    when(blockchain.getBlockHeader(mockHeader.getParentHash())).thenReturn(Optional.of(mockParent));
    when(mergeCoordinator.getOrSyncHeadByHash(any(), any())).thenReturn(Optional.of(mockHeader));
    when(mergeCoordinator.isBadBlock(mockHeader.getHash())).thenReturn(true);
    when(mergeCoordinator.isDescendantOf(any(), any())).thenReturn(true);
    when(mergeCoordinator.getLatestValidHashOfBadBlock(mockHeader.getHash()))
        .thenReturn(Optional.of(latestValidHash));

    assertSuccessWithPayloadForForkchoiceResult(
        new EngineForkchoiceUpdatedParameter(
            mockHeader.getHash(), mockHeader.getParentHash(), mockHeader.getParentHash()),
        Optional.empty(),
        mock(ForkchoiceResult.class),
        INVALID,
        Optional.of(latestValidHash));
  }

  @Test
  public void shouldReturnSyncingOnHeadNotFound() {
    assertSuccessWithPayloadForForkchoiceResult(
        mockFcuParam, Optional.empty(), mock(ForkchoiceResult.class), SYNCING);
  }

  @Test
  public void shouldReturnValidWithoutFinalizedOrPayload() {
    BlockHeader mockHeader = blockHeaderBuilder.buildHeader();
    when(mergeCoordinator.getOrSyncHeadByHash(mockHeader.getHash(), Hash.ZERO))
        .thenReturn(Optional.of(mockHeader));

    assertSuccessWithPayloadForForkchoiceResult(
        new EngineForkchoiceUpdatedParameter(mockHeader.getHash(), Hash.ZERO, Hash.ZERO),
        Optional.empty(),
        ForkchoiceResult.withResult(Optional.empty(), Optional.of(mockHeader)),
        VALID);
  }

  @Test
  public void shouldReturnInvalidOnOldTimestamp() {
    BlockHeader parent = blockHeaderBuilder.buildHeader();
    BlockHeader mockHeader =
        blockHeaderBuilder
            .parentHash(parent.getHash())
            .timestamp(parent.getTimestamp())
            .buildHeader();
    when(blockchain.getBlockHeader(parent.getHash())).thenReturn(Optional.of(parent));
    when(mergeCoordinator.isDescendantOf(any(), any())).thenReturn(true);
    when(mergeContext.isSyncing()).thenReturn(false);
    when(mergeCoordinator.getOrSyncHeadByHash(mockHeader.getHash(), parent.getHash()))
        .thenReturn(Optional.of(mockHeader));
    when(mergeCoordinator.updateForkChoice(mockHeader, parent.getHash(), parent.getHash()))
        .thenReturn(
            ForkchoiceResult.withFailure(
                ForkchoiceResult.Status.INVALID,
                "new head timestamp not greater than parent",
                Optional.of(parent.getHash())));

    EngineForkchoiceUpdatedParameter param =
        new EngineForkchoiceUpdatedParameter(
            mockHeader.getBlockHash(), parent.getBlockHash(), parent.getBlockHash());

    EngineUpdateForkchoiceResult resp = fromSuccessResp(resp(param, Optional.empty()));

    assertThat(resp.getPayloadStatus().getStatus()).isEqualTo(INVALID);
    assertThat(resp.getPayloadStatus().getLatestValidHash()).isPresent();
    assertThat(resp.getPayloadStatus().getLatestValidHash().get()).isEqualTo(parent.getBlockHash());
    assertThat(resp.getPayloadStatus().getError())
        .isEqualTo("new head timestamp not greater than parent");
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnValidWithNewHeadAndFinalizedNoPayload() {
    BlockHeader mockParent = blockHeaderBuilder.number(9L).buildHeader();
    BlockHeader mockHeader =
        blockHeaderBuilder.number(10L).parentHash(mockParent.getHash()).buildHeader();
    setupValidForkchoiceUpdate(mockHeader);

    assertSuccessWithPayloadForForkchoiceResult(
        new EngineForkchoiceUpdatedParameter(mockHeader.getHash(), Hash.ZERO, mockParent.getHash()),
        Optional.empty(),
        ForkchoiceResult.withResult(Optional.of(mockParent), Optional.of(mockHeader)),
        VALID);
  }

  @Test
  public void shouldReturnValidWithoutFinalizedWithPayload() {
    BlockHeader mockHeader = blockHeaderBuilder.buildHeader();
    when(mergeCoordinator.getOrSyncHeadByHash(mockHeader.getHash(), Hash.ZERO))
        .thenReturn(Optional.of(mockHeader));

    var payloadParams =
        new EnginePayloadAttributesParameter(
            String.valueOf(mockHeader.getTimestamp() + 1),
            Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
            Address.ECREC.toString(),
            null,
            null,
            null);
    var mockPayloadId =
        PayloadIdentifier.forPayloadParams(
            mockHeader.getHash(),
            payloadParams.getTimestamp(),
            payloadParams.getPrevRandao(),
            payloadParams.getSuggestedFeeRecipient(),
            Optional.empty(),
            Optional.empty());

    when(mergeCoordinator.preparePayload(
            mockHeader,
            payloadParams.getTimestamp(),
            payloadParams.getPrevRandao(),
            Address.ECREC,
            Optional.empty(),
            Optional.empty(),
            Optional.empty()))
        .thenReturn(mockPayloadId);

    var res =
        assertSuccessWithPayloadForForkchoiceResult(
            new EngineForkchoiceUpdatedParameter(mockHeader.getHash(), Hash.ZERO, Hash.ZERO),
            Optional.of(payloadParams),
            ForkchoiceResult.withResult(Optional.empty(), Optional.of(mockHeader)),
            VALID);

    assertThat(res.getPayloadId()).isEqualTo(mockPayloadId.toHexString());
  }

  @Test
  public void shouldReturnInvalidForkchoiceStateIfFinalizedBlockIsUnknown() {
    BlockHeader newHead = blockHeaderBuilder.buildHeader();
    Hash finalizedBlockHash = Hash.hash(Bytes32.fromHexStringLenient("0x424abcdef"));

    when(blockchain.getBlockHeader(finalizedBlockHash)).thenReturn(Optional.empty());
    when(mergeContext.isSyncing()).thenReturn(false);
    when(mergeCoordinator.getOrSyncHeadByHash(newHead.getHash(), finalizedBlockHash))
        .thenReturn(Optional.of(newHead));

    var resp =
        resp(
            new EngineForkchoiceUpdatedParameter(
                newHead.getBlockHash(), finalizedBlockHash, finalizedBlockHash),
            Optional.empty());

    assertInvalidForkchoiceState(resp);
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnInvalidForkchoiceStateIfFinalizedBlockIsNotAnAncestorOfNewHead() {
    BlockHeader finalized = blockHeaderBuilder.buildHeader();
    BlockHeader newHead = blockHeaderBuilder.buildHeader();

    when(blockchain.getBlockHeader(newHead.getHash())).thenReturn(Optional.of(newHead));
    when(blockchain.getBlockHeader(finalized.getHash())).thenReturn(Optional.of(finalized));
    when(mergeContext.isSyncing()).thenReturn(false);
    when(mergeCoordinator.getOrSyncHeadByHash(newHead.getHash(), finalized.getBlockHash()))
        .thenReturn(Optional.of(newHead));
    when(mergeCoordinator.isDescendantOf(finalized, newHead)).thenReturn(false);

    var resp =
        resp(
            new EngineForkchoiceUpdatedParameter(
                newHead.getBlockHash(), finalized.getBlockHash(), finalized.getBlockHash()),
            Optional.empty());

    assertInvalidForkchoiceState(resp);
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnInvalidForkchoiceStateIfSafeHeadZeroWithFinalizedBlock() {
    BlockHeader parent = blockHeaderBuilder.buildHeader();
    BlockHeader newHead =
        blockHeaderBuilder
            .parentHash(parent.getHash())
            .timestamp(parent.getTimestamp())
            .buildHeader();

    when(blockchain.getBlockHeader(parent.getHash())).thenReturn(Optional.of(parent));
    when(mergeContext.isSyncing()).thenReturn(false);
    when(mergeCoordinator.getOrSyncHeadByHash(newHead.getHash(), parent.getBlockHash()))
        .thenReturn(Optional.of(newHead));

    var resp =
        resp(
            new EngineForkchoiceUpdatedParameter(
                newHead.getBlockHash(), parent.getBlockHash(), Hash.ZERO),
            Optional.empty());

    assertInvalidForkchoiceState(resp);
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnInvalidForkchoiceStateIfSafeBlockIsUnknown() {
    BlockHeader finalized = blockHeaderBuilder.buildHeader();
    BlockHeader newHead =
        blockHeaderBuilder
            .parentHash(finalized.getHash())
            .timestamp(finalized.getTimestamp())
            .buildHeader();
    Hash safeBlockBlockHash = Hash.hash(Bytes32.fromHexStringLenient("0x424abcdef"));

    when(blockchain.getBlockHeader(finalized.getHash())).thenReturn(Optional.of(finalized));
    when(blockchain.getBlockHeader(safeBlockBlockHash)).thenReturn(Optional.empty());
    when(mergeContext.isSyncing()).thenReturn(false);
    when(mergeCoordinator.getOrSyncHeadByHash(newHead.getHash(), finalized.getBlockHash()))
        .thenReturn(Optional.of(newHead));
    when(mergeCoordinator.isDescendantOf(finalized, newHead)).thenReturn(true);

    var resp =
        resp(
            new EngineForkchoiceUpdatedParameter(
                newHead.getBlockHash(), finalized.getBlockHash(), safeBlockBlockHash),
            Optional.empty());

    assertInvalidForkchoiceState(resp);
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnInvalidForkchoiceStateIfSafeBlockIsNotADescendantOfFinalized() {
    BlockHeader finalized = blockHeaderBuilder.buildHeader();
    BlockHeader newHead = blockHeaderBuilder.buildHeader();
    BlockHeader safeBlock = blockHeaderBuilder.buildHeader();

    when(blockchain.getBlockHeader(newHead.getHash())).thenReturn(Optional.of(newHead));
    when(blockchain.getBlockHeader(finalized.getHash())).thenReturn(Optional.of(finalized));
    when(blockchain.getBlockHeader(safeBlock.getHash())).thenReturn(Optional.of(safeBlock));
    when(mergeContext.isSyncing()).thenReturn(false);
    when(mergeCoordinator.getOrSyncHeadByHash(newHead.getHash(), finalized.getBlockHash()))
        .thenReturn(Optional.of(newHead));
    when(mergeCoordinator.isDescendantOf(finalized, newHead)).thenReturn(true);
    when(mergeCoordinator.isDescendantOf(finalized, safeBlock)).thenReturn(false);

    var resp =
        resp(
            new EngineForkchoiceUpdatedParameter(
                newHead.getBlockHash(), finalized.getBlockHash(), safeBlock.getBlockHash()),
            Optional.empty());

    assertInvalidForkchoiceState(resp);
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnInvalidForkchoiceStateIfSafeBlockIsNotAnAncestorOfNewHead() {
    BlockHeader finalized = blockHeaderBuilder.buildHeader();
    BlockHeader newHead = blockHeaderBuilder.buildHeader();
    BlockHeader safeBlock = blockHeaderBuilder.buildHeader();

    when(blockchain.getBlockHeader(newHead.getHash())).thenReturn(Optional.of(newHead));
    when(blockchain.getBlockHeader(finalized.getHash())).thenReturn(Optional.of(finalized));
    when(blockchain.getBlockHeader(safeBlock.getHash())).thenReturn(Optional.of(safeBlock));
    when(mergeContext.isSyncing()).thenReturn(false);
    when(mergeCoordinator.getOrSyncHeadByHash(newHead.getHash(), finalized.getBlockHash()))
        .thenReturn(Optional.of(newHead));
    when(mergeCoordinator.isDescendantOf(finalized, newHead)).thenReturn(true);
    when(mergeCoordinator.isDescendantOf(finalized, safeBlock)).thenReturn(true);
    when(mergeCoordinator.isDescendantOf(safeBlock, newHead)).thenReturn(false);

    var resp =
        resp(
            new EngineForkchoiceUpdatedParameter(
                newHead.getBlockHash(), finalized.getBlockHash(), safeBlock.getBlockHash()),
            Optional.empty());

    assertInvalidForkchoiceState(resp);
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldIgnoreUpdateToOldHeadAndNotPreparePayload() {
    BlockHeader mockHeader = blockHeaderBuilder.buildHeader();

    when(mergeCoordinator.getOrSyncHeadByHash(mockHeader.getHash(), Hash.ZERO))
        .thenReturn(Optional.of(mockHeader));

    var ignoreOldHeadUpdateRes = ForkchoiceResult.withIgnoreUpdateToOldHead(mockHeader);
    when(mergeCoordinator.updateForkChoice(any(), any(), any())).thenReturn(ignoreOldHeadUpdateRes);

    var payloadParams =
        new EnginePayloadAttributesParameter(
            String.valueOf(mockHeader.getTimestamp() + 1),
            Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
            Address.ECREC.toString(),
            null,
            null,
            null);

    var resp =
        (JsonRpcSuccessResponse)
            resp(
                new EngineForkchoiceUpdatedParameter(
                    mockHeader.getBlockHash(), Hash.ZERO, Hash.ZERO),
                Optional.of(payloadParams));

    var forkchoiceRes = (EngineUpdateForkchoiceResult) resp.getResult();

    verify(mergeCoordinator, never())
        .preparePayload(any(), any(), any(), any(), any(), any(), any());

    assertThat(forkchoiceRes.getPayloadStatus().getStatus()).isEqualTo(VALID);
    assertThat(forkchoiceRes.getPayloadStatus().getError()).isNull();
    assertThat(forkchoiceRes.getPayloadStatus().getLatestValidHashAsString())
        .isEqualTo(mockHeader.getHash().getBytes().toHexString());
    assertThat(forkchoiceRes.getPayloadId()).isNull();
    assertThat(forkchoiceRes.getPayloadId()).isNull();
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnInvalidIfPayloadTimestampNotGreaterThanHead() {
    BlockHeader mockParent = blockHeaderBuilder.timestamp(99).number(9L).buildHeader();
    BlockHeader mockHeader =
        blockHeaderBuilder.number(10L).parentHash(mockParent.getHash()).buildHeader();
    setupValidForkchoiceUpdate(mockHeader);

    final long timestampNotGreaterThanHead = mockHeader.getTimestamp();

    var payloadParams =
        new EnginePayloadAttributesParameter(
            String.valueOf(timestampNotGreaterThanHead),
            Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
            Address.ECREC.toString(),
            emptyList(),
            null,
            null);

    var resp =
        resp(
            new EngineForkchoiceUpdatedParameter(
                mockHeader.getHash(), Hash.ZERO, mockParent.getHash()),
            Optional.of(payloadParams));

    assertInvalidForkchoiceState(resp, expectedInvalidPayloadError());
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnInvalidIfWithdrawalsIsNotNull_WhenWithdrawalsProhibited() {
    BlockHeader mockParent = blockHeaderBuilder.number(9L).buildHeader();
    BlockHeader mockHeader =
        blockHeaderBuilder.number(10L).parentHash(mockParent.getHash()).buildHeader();
    setupValidForkchoiceUpdate(mockHeader);

    var payloadParams =
        new EnginePayloadAttributesParameter(
            String.valueOf(mockHeader.getTimestamp() + 1),
            Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
            Address.ECREC.toString(),
            emptyList(),
            null,
            null);

    var resp =
        resp(
            new EngineForkchoiceUpdatedParameter(
                mockHeader.getHash(), Hash.ZERO, mockParent.getHash()),
            Optional.of(payloadParams));

    assertInvalidForkchoiceState(resp, expectedInvalidPayloadError());
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnValidIfWithdrawalsIsNull_WhenWithdrawalsProhibited() {
    BlockHeader mockParent = blockHeaderBuilder.number(9L).buildHeader();
    BlockHeader mockHeader =
        blockHeaderBuilder.number(10L).parentHash(mockParent.getHash()).buildHeader();
    setupValidForkchoiceUpdate(mockHeader);

    var payloadParams =
        new EnginePayloadAttributesParameter(
            String.valueOf(mockHeader.getTimestamp() + 1),
            Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
            Address.ECREC.toString(),
            null,
            null,
            null);

    var mockPayloadId =
        PayloadIdentifier.forPayloadParams(
            mockHeader.getHash(),
            payloadParams.getTimestamp(),
            payloadParams.getPrevRandao(),
            payloadParams.getSuggestedFeeRecipient(),
            Optional.empty(),
            Optional.empty());

    when(mergeCoordinator.preparePayload(
            mockHeader,
            payloadParams.getTimestamp(),
            payloadParams.getPrevRandao(),
            Address.ECREC,
            Optional.empty(),
            Optional.empty(),
            Optional.empty()))
        .thenReturn(mockPayloadId);

    assertSuccessWithPayloadForForkchoiceResult(
        new EngineForkchoiceUpdatedParameter(mockHeader.getHash(), Hash.ZERO, mockParent.getHash()),
        Optional.of(payloadParams),
        ForkchoiceResult.withResult(Optional.empty(), Optional.of(mockHeader)),
        VALID);
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnInvalidIfWithdrawalsIsNull_WhenWithdrawalsAllowed() {
    when(protocolSpec.getWithdrawalsValidator())
        .thenReturn(new WithdrawalsValidator.AllowedWithdrawals());

    BlockHeader mockParent = blockHeaderBuilder.number(9L).buildHeader();
    BlockHeader mockHeader =
        blockHeaderBuilder.number(10L).parentHash(mockParent.getHash()).buildHeader();
    setupValidForkchoiceUpdate(mockHeader);

    var payloadParams =
        new EnginePayloadAttributesParameter(
            String.valueOf(mockHeader.getTimestamp() + 1),
            Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
            Address.ECREC.toString(),
            null,
            null,
            null);

    var resp =
        resp(
            new EngineForkchoiceUpdatedParameter(
                mockHeader.getHash(), Hash.ZERO, mockParent.getHash()),
            Optional.of(payloadParams));

    assertInvalidForkchoiceState(resp, expectedInvalidPayloadError());
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnValidIfWithdrawalsIsNotNull_WhenWithdrawalsAllowed() {
    when(protocolSpec.getWithdrawalsValidator())
        .thenReturn(new WithdrawalsValidator.AllowedWithdrawals());

    BlockHeader mockParent = blockHeaderBuilder.number(9L).buildHeader();
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
            null,
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
            Optional.empty());

    when(mergeCoordinator.preparePayload(
            mockHeader,
            payloadParams.getTimestamp(),
            payloadParams.getPrevRandao(),
            Address.ECREC,
            withdrawals,
            Optional.empty(),
            Optional.empty()))
        .thenReturn(mockPayloadId);

    assertSuccessWithPayloadForForkchoiceResult(
        new EngineForkchoiceUpdatedParameter(mockHeader.getHash(), Hash.ZERO, mockParent.getHash()),
        Optional.of(payloadParams),
        ForkchoiceResult.withResult(Optional.empty(), Optional.of(mockHeader)),
        VALID);
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnValidIfProtocolScheduleIsEmpty() {
    when(protocolSchedule.getForNextBlockHeader(any(), anyLong())).thenReturn(null);

    BlockHeader mockParent = blockHeaderBuilder.number(9L).buildHeader();
    BlockHeader mockHeader =
        blockHeaderBuilder.number(10L).parentHash(mockParent.getHash()).buildHeader();
    setupValidForkchoiceUpdate(mockHeader);

    var payloadParams =
        new EnginePayloadAttributesParameter(
            String.valueOf(mockHeader.getTimestamp() + 1),
            Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
            Address.ECREC.toString(),
            null,
            null,
            null);

    var mockPayloadId =
        PayloadIdentifier.forPayloadParams(
            mockHeader.getHash(),
            payloadParams.getTimestamp(),
            payloadParams.getPrevRandao(),
            payloadParams.getSuggestedFeeRecipient(),
            Optional.empty(),
            Optional.empty());

    when(mergeCoordinator.preparePayload(
            mockHeader,
            payloadParams.getTimestamp(),
            payloadParams.getPrevRandao(),
            Address.ECREC,
            Optional.empty(),
            Optional.empty(),
            Optional.empty()))
        .thenReturn(mockPayloadId);

    assertSuccessWithPayloadForForkchoiceResult(
        new EngineForkchoiceUpdatedParameter(mockHeader.getHash(), Hash.ZERO, mockParent.getHash()),
        Optional.of(payloadParams),
        ForkchoiceResult.withResult(Optional.empty(), Optional.of(mockHeader)),
        VALID);
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  protected void setupValidForkchoiceUpdate(final BlockHeader mockHeader) {
    when(blockchain.getBlockHeader(any())).thenReturn(Optional.of(mockHeader));
    when(mergeCoordinator.getOrSyncHeadByHash(mockHeader.getHash(), Hash.ZERO))
        .thenReturn(Optional.of(mockHeader));
    when(mergeCoordinator.isDescendantOf(any(), any())).thenReturn(true);
  }

  protected EngineUpdateForkchoiceResult assertSuccessWithPayloadForForkchoiceResult(
      final EngineForkchoiceUpdatedParameter fcuParam,
      final Optional<EnginePayloadAttributesParameter> payloadParam,
      final ForkchoiceResult forkchoiceResult,
      final EngineStatus expectedStatus) {
    return assertSuccessWithPayloadForForkchoiceResult(
        fcuParam, payloadParam, forkchoiceResult, expectedStatus, Optional.empty());
  }

  protected EngineUpdateForkchoiceResult assertSuccessWithPayloadForForkchoiceResult(
      final EngineForkchoiceUpdatedParameter fcuParam,
      final Optional<EnginePayloadAttributesParameter> payloadParam,
      final ForkchoiceResult forkchoiceResult,
      final EngineStatus expectedStatus,
      final Optional<Hash> maybeLatestValidHash) {

    // result from mergeCoordinator has no new finalized, new head:
    when(mergeCoordinator.updateForkChoice(
            any(BlockHeader.class), any(Hash.class), any(Hash.class)))
        .thenReturn(forkchoiceResult);
    var resp = resp(fcuParam, payloadParam);
    var res = fromSuccessResp(resp);

    assertThat(res.getPayloadStatus().getStatusAsString()).isEqualTo(expectedStatus.name());

    if (expectedStatus.equals(VALID)) {
      // check conditions when response is valid
      assertThat(res.getPayloadStatus().getLatestValidHash())
          .isEqualTo(forkchoiceResult.getNewHead().map(BlockHeader::getBlockHash));
      assertThat(res.getPayloadStatus().getError()).isNullOrEmpty();
      if (payloadParam.isPresent()) {
        assertThat(res.getPayloadId()).isNotNull();
      } else {
        assertThat(res.getPayloadId()).isNull();
      }
    } else {
      // assert null latest valid and payload identifier:
      assertThat(res.getPayloadStatus().getLatestValidHash()).isEqualTo(maybeLatestValidHash);
      assertThat(res.getPayloadId()).isNull();
    }

    // assert that listeners are always notified
    verify(mergeContext)
        .fireNewUnverifiedForkchoiceEvent(
            fcuParam.getHeadBlockHash(),
            fcuParam.getSafeBlockHash(),
            fcuParam.getFinalizedBlockHash());

    verify(engineCallListener, times(1)).executionEngineCalled();

    return res;
  }

  protected RpcErrorType expectedInvalidPayloadError() {
    return RpcErrorType.INVALID_WITHDRAWALS_PARAMS;
  }

  protected JsonRpcResponse resp(
      final EngineForkchoiceUpdatedParameter forkchoiceParam,
      final Optional<EnginePayloadAttributesParameter> payloadParam) {
    return method.response(
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0",
                getMethodName(),
                Stream.concat(Stream.of(forkchoiceParam), payloadParam.stream()).toArray())));
  }

  abstract String getMethodName();

  private EngineUpdateForkchoiceResult fromSuccessResp(final JsonRpcResponse resp) {
    assertThat(resp.getType()).isEqualTo(RpcResponseType.SUCCESS);
    return Optional.of(resp)
        .map(JsonRpcSuccessResponse.class::cast)
        .map(JsonRpcSuccessResponse::getResult)
        .map(EngineUpdateForkchoiceResult.class::cast)
        .get();
  }

  private void assertInvalidForkchoiceState(final JsonRpcResponse resp) {
    assertInvalidForkchoiceState(resp, RpcErrorType.INVALID_FORKCHOICE_STATE);
  }

  private void assertInvalidForkchoiceState(
      final JsonRpcResponse resp, final RpcErrorType jsonRpcError) {
    assertThat(resp.getType()).isEqualTo(RpcResponseType.ERROR);

    var errorResp = (JsonRpcErrorResponse) resp;
    assertThat(errorResp.getErrorType()).isEqualTo(jsonRpcError);
    assertThat(errorResp.getError().getMessage()).isEqualTo(jsonRpcError.getMessage());
  }

  protected long defaultPayloadTimestamp() {
    return 1;
  }
}
