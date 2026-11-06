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
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BOGOTA;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.VALID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ConstructorArgumentsBuilder;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.PayloadStatusV2;
import org.hyperledger.besu.ethereum.chain.InclusionListStatus;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionEncoder;
import org.hyperledger.besu.ethereum.mainnet.inclusionlist.InclusionListTransactions;
import org.hyperledger.besu.ethereum.mainnet.inclusionlist.InclusionListValidationResult;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.Executor;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests engine_newPayloadV6, which validates the payload against an inclusion list (EIP-7805), and
 * inherits the tests of the previous versions, run with an empty inclusion list.
 */
public class EngineNewPayloadV6Test extends EngineNewPayloadV5Test {

  // backs the inclusion list status stored with the blocks
  private final Map<Hash, InclusionListStatus> inclusionListStatusByBlockHash = new HashMap<>();

  @BeforeEach
  @Override
  public void before() {
    super.before();
    // V6 remembers a block with its inclusion list: like in production, it ends up in the
    // remembering without inclusion list, which the inherited tests stub and verify
    doAnswer(
            invocation ->
                ((MergeMiningCoordinator) invocation.getMock())
                    .rememberBlock(
                        invocation.getArgument(0),
                        invocation.getArgument(1),
                        invocation.<Executor>getArgument(3)))
        .when(mergeCoordinator)
        .rememberBlock(any(), any(), any(), any(Executor.class));
    when(blockchain.getInclusionListStatus(any()))
        .thenAnswer(
            invocation ->
                Optional.ofNullable(inclusionListStatusByBlockHash.get(invocation.getArgument(0))));
  }

  @Override
  protected EngineNewPayloadV1<?, ?> createMethodInstance() {
    return new EngineNewPayloadV6<>(
        new ConstructorArgumentsBuilder()
            .protocolSchedule(protocolSchedule)
            .protocolContext(protocolContext)
            .vertx(vertx)
            .engineCallListener(engineCallListener)
            .mergeCoordinator(mergeCoordinator)
            .ethPeers(ethPeers)
            .metricsSystem(new NoOpMetricsSystem())
            .transactionPool(transactionPool)
            .maxRequestBlocks(0)
            .build(),
        BOGOTA,
        null);
  }

  @Override
  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_newPayloadV6");
  }

  @Override
  protected long getMinSupportedTimestamp() {
    return bogotaHardfork.milestone();
  }

  @Override
  protected OptionalLong getMaxSupportedTimestamp() {
    return OptionalLong.empty();
  }

  @Override
  @Test
  public void shouldReturnSuccessOnAlreadyPresent() {
    // a payload is only already present if it was validated against the same inclusion list
    when(blockchain.getInclusionListStatus(any()))
        .thenReturn(
            Optional.of(
                new InclusionListStatus(InclusionListTransactions.EMPTY.getChecksum(), true)));
    super.shouldReturnSuccessOnAlreadyPresent();
  }

  @Override
  protected Object[] getVersionSpecificDefaultParams() {
    return ArrayUtils.addAll(super.getVersionSpecificDefaultParams(), emptyList());
  }

  /** The inherited tests that pass the parameters of V5 send an empty inclusion list. */
  @Override
  protected JsonRpcResponse resp(final Object... params) {
    return super.resp(params.length == 4 ? ArrayUtils.add(params, emptyList()) : params);
  }

  @Override
  protected JsonRpcResponse respV4(
      final Map<String, Object> payloadParam, final List<String> requestsParam) {
    return resp(
        payloadParam, emptyVersionedHashesParam(), zeroParentBeaconBlockRootParam(), requestsParam);
  }

  @Test
  public void reportsSatisfiedEmptyInclusionList() {
    final BlockHeader payloadHeader = setupValidPayloadHeader();

    final PayloadStatusV2 status =
        fromSuccessRespV2(callNewPayload(payloadHeader, emptyList(), emptyList()));
    assertThat(status.getStatus()).isEqualTo(VALID);
    assertThat(status.getInclusionListSatisfied()).isTrue();

    runQueuedWrites();
    assertThat(inclusionListStatusByBlockHash.get(payloadHeader.getHash()))
        .isEqualTo(new InclusionListStatus(InclusionListTransactions.EMPTY.getChecksum(), true));
  }

  @Test
  public void reportsAndStoresUnsatisfiedInclusionList() {
    final BlockHeader payloadHeader = setupValidPayloadHeader();
    stubBlockProcessingResult(InclusionListValidationResult.unsatisfied("unsatisfied"));
    final Transaction ilTx = createLegacyTransaction(0, Wei.of(100));

    final PayloadStatusV2 newPayloadStatus =
        fromSuccessRespV2(callNewPayload(payloadHeader, emptyList(), List.of(encode(ilTx))));
    assertThat(newPayloadStatus.getStatus()).isEqualTo(VALID);
    assertThat(newPayloadStatus.getInclusionListSatisfied()).isFalse();
    // the inclusion list transactions are validated during block processing
    verify(mergeCoordinator)
        .rememberBlock(
            any(), any(), eq(Optional.of(new InclusionListTransactions(List.of(ilTx)))), any());

    runQueuedWrites();
    assertThat(inclusionListStatusByBlockHash.get(payloadHeader.getHash()))
        .isEqualTo(
            new InclusionListStatus(
                new InclusionListTransactions(List.of(ilTx)).getChecksum(), false));
  }

  @Test
  public void skipsInclusionListTransactionsThatCannotBeDecoded() {
    final BlockHeader payloadHeader = setupValidPayloadHeader();
    final Transaction ilTx = createLegacyTransaction(0, Wei.of(100));
    final String encodedIlTx = encode(ilTx);

    final PayloadStatusV2 status =
        fromSuccessRespV2(
            callNewPayload(
                payloadHeader,
                emptyList(),
                List.of(
                    "0xnot-hex",
                    encodedIlTx.substring(0, encodedIlTx.length() - 10),
                    encodedIlTx)));
    assertThat(status.getStatus()).isEqualTo(VALID);

    verify(mergeCoordinator)
        .rememberBlock(
            any(), any(), eq(Optional.of(new InclusionListTransactions(List.of(ilTx)))), any());
  }

  @Test
  public void rejectsAnInclusionListThatIsNotAList() {
    final BlockHeader payloadHeader = setupValidPayloadHeader();

    final JsonRpcResponse response = callNewPayload(payloadHeader, emptyList(), "0x01");

    assertThat(response.getType()).isEqualTo(RpcResponseType.ERROR);
    assertThat(((JsonRpcErrorResponse) response).getErrorType())
        .isEqualTo(RpcErrorType.INVALID_ENGINE_NEW_PAYLOAD_PARAMS);
    assertThat(((JsonRpcErrorResponse) response).getError().getData())
        .contains("Invalid inclusion list transactions parameters");
    verify(mergeCoordinator, never()).rememberBlock(any(), any(), any(), any());
  }

  @Test
  public void reportsStoredStatusOfAlreadyPresentPayloadWithSameInclusionList() {
    final BlockHeader payloadHeader = setupValidPayloadHeader();
    stubBlockProcessingResult(InclusionListValidationResult.unsatisfied("unsatisfied"));
    final String ilTx = encode(createLegacyTransaction(0, Wei.of(100)));

    final PayloadStatusV2 firstStatus =
        fromSuccessRespV2(callNewPayload(payloadHeader, emptyList(), List.of(ilTx)));
    assertThat(firstStatus.getInclusionListSatisfied()).isFalse();
    runQueuedWrites();

    // the payload is now part of the chain
    when(blockchain.getBlockByHash(payloadHeader.getHash()))
        .thenReturn(Optional.of(mock(Block.class)));

    // sent again with the same inclusion list, answered with the stored status
    final PayloadStatusV2 resentStatus =
        fromSuccessRespV2(callNewPayload(payloadHeader, emptyList(), List.of(ilTx)));
    assertThat(resentStatus.getStatus()).isEqualTo(VALID);
    assertThat(resentStatus.getInclusionListSatisfied()).isFalse();

    // the block was processed only the first time
    verify(mergeCoordinator, times(1)).rememberBlock(any(), any(), any(), any());
  }

  @Test
  public void processesAgainAlreadyPresentPayloadWithAnotherInclusionList() {
    // the same block can be sent with different inclusion lists, e.g. tests that share a genesis
    final BlockHeader payloadHeader = setupValidPayloadHeader();
    final Transaction notIncludableTx = createLegacyTransaction(0, Wei.of(100));
    final Transaction includableTx = createLegacyTransaction(1, Wei.of(100));

    final PayloadStatusV2 firstStatus =
        fromSuccessRespV2(
            callNewPayload(payloadHeader, emptyList(), List.of(encode(notIncludableTx))));
    assertThat(firstStatus.getInclusionListSatisfied()).isTrue();
    runQueuedWrites();

    // the payload is now part of the chain
    when(blockchain.getBlockByHash(payloadHeader.getHash()))
        .thenReturn(Optional.of(mock(Block.class)));
    stubBlockProcessingResult(InclusionListValidationResult.unsatisfied("unsatisfied"));

    // sent again with another inclusion list, it is processed again with it
    final PayloadStatusV2 resentStatus =
        fromSuccessRespV2(
            callNewPayload(payloadHeader, emptyList(), List.of(encode(includableTx))));
    assertThat(resentStatus.getStatus()).isEqualTo(VALID);
    assertThat(resentStatus.getInclusionListSatisfied()).isFalse();
    runQueuedWrites();
    assertThat(inclusionListStatusByBlockHash.get(payloadHeader.getHash()))
        .isEqualTo(
            new InclusionListStatus(
                new InclusionListTransactions(List.of(includableTx)).getChecksum(), false));

    verify(mergeCoordinator)
        .rememberBlock(
            any(),
            any(),
            eq(Optional.of(new InclusionListTransactions(List.of(notIncludableTx)))),
            any());
    verify(mergeCoordinator)
        .rememberBlock(
            any(),
            any(),
            eq(Optional.of(new InclusionListTransactions(List.of(includableTx)))),
            any());
  }

  @Test
  public void processesAlreadyPresentPayloadWithoutInclusionListStatus() {
    final BlockHeader payloadHeader = setupValidPayloadHeader();
    // present in the chain, e.g. imported by sync, but never validated against an inclusion list
    when(blockchain.getBlockByHash(payloadHeader.getHash()))
        .thenReturn(Optional.of(mock(Block.class)));
    stubBlockProcessingResult(InclusionListValidationResult.unsatisfied("unsatisfied"));

    final PayloadStatusV2 status =
        fromSuccessRespV2(
            callNewPayload(
                payloadHeader,
                emptyList(),
                List.of(encode(createLegacyTransaction(0, Wei.of(100))))));
    assertThat(status.getStatus()).isEqualTo(VALID);
    assertThat(status.getInclusionListSatisfied()).isFalse();

    verify(mergeCoordinator, times(1)).rememberBlock(any(), any(), any(), any());
  }

  private BlockHeader setupValidPayloadHeader() {
    final BlockHeader header = setupPayloadV1(getMinSupportedTimestamp());
    stubBlockProcessingResult(InclusionListValidationResult.valid());
    return header;
  }

  /**
   * Stubs the block processing result, and the writes that the merge coordinator hands to the block
   * writer for a valid block, that store the inclusion list status after the block.
   */
  private void stubBlockProcessingResult(
      final InclusionListValidationResult inclusionListValidationResult) {
    doAnswer(
            invocation -> {
              final Block block = invocation.getArgument(0);
              final Optional<InclusionListTransactions> inclusionListTransactions =
                  invocation.getArgument(2);
              invocation
                  .<Executor>getArgument(3)
                  .execute(
                      () ->
                          inclusionListTransactions.ifPresent(
                              il ->
                                  inclusionListStatusByBlockHash.put(
                                      block.getHash(),
                                      new InclusionListStatus(
                                          il.getChecksum(),
                                          inclusionListValidationResult.isValid()))));
              return new BlockProcessingResult(
                  Optional.of(
                      new BlockProcessingOutputs(
                          null,
                          List.of(),
                          Optional.empty(),
                          Optional.empty(),
                          0,
                          Map.of(),
                          inclusionListValidationResult)));
            })
        .when(mergeCoordinator)
        .rememberBlock(any(), any(), any(), any(Executor.class));
  }

  /**
   * Every ordered engine call first runs the writes queued before it, like the block and its
   * inclusion list status, stored after the newPayload reply. Calls the method without parameters,
   * which only fails after running them.
   */
  private void runQueuedWrites() {
    method.response(
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", method.getName(), new Object[0])));
  }

  private JsonRpcResponse callNewPayload(
      final BlockHeader header, final List<String> payloadTxs, final Object inclusionListTxs) {
    return resp(
        mockEnginePayloadParam(header, payloadTxs),
        emptyVersionedHashesParam(),
        zeroParentBeaconBlockRootParam(),
        emptyRequestsParam(),
        inclusionListTxs);
  }

  private PayloadStatusV2 fromSuccessRespV2(final JsonRpcResponse resp) {
    return (PayloadStatusV2) fromSuccessResp(resp);
  }

  private static String encode(final Transaction transaction) {
    return TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.BLOCK_BODY)
        .toHexString();
  }

  private static Transaction createLegacyTransaction(final long nonce, final Wei gasPrice) {
    return new TransactionTestFixture()
        .to(Optional.of(Address.ZERO))
        .type(TransactionType.FRONTIER)
        .chainId(Optional.of(BigInteger.valueOf(42)))
        .nonce(nonce)
        .gasPrice(gasPrice)
        .gasLimit(21000)
        .createTransaction(senderKeys);
  }
}
