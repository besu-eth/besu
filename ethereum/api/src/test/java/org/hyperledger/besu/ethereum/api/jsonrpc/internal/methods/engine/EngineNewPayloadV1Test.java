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
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.SHANGHAI;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.ACCEPTED;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.INVALID;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.INVALID_BLOCK_HASH;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.SYNCING;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.VALID;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineTestSupport.fromErrorResp;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.INVALID_PARAMS;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.UNSUPPORTED_FORK;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.MergeContext;
import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.PayloadStatusV1;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;

import io.vertx.core.Vertx;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class EngineNewPayloadV1Test extends AbstractScheduledApiTest {
  protected EngineNewPayloadV1<?, ?> method;

  public EngineNewPayloadV1Test() {}

  protected static final Vertx vertx = Vertx.vertx();
  protected static final Hash mockHash = Hash.hash(Bytes32.fromHexStringLenient("0x1337deadbeef"));

  @Mock protected ProtocolSpec protocolSpec;

  @Mock protected ProtocolContext protocolContext;

  @Mock protected MergeContext mergeContext;

  @Mock protected MergeMiningCoordinator mergeCoordinator;

  @Mock protected MutableBlockchain blockchain;

  @Mock protected EthPeers ethPeers;

  @Mock protected EngineCallListener engineCallListener;

  @BeforeEach
  @Override
  public void before() {
    super.before();
    when(protocolContext.safeConsensusContext(any())).thenReturn(Optional.of(mergeContext));
    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    when(protocolSchedule.getByBlockHeader(any())).thenReturn(protocolSpec);
    lenient().when(ethPeers.peerCount()).thenReturn(1);
    createMethod();
  }

  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_newPayloadV1");
  }

  protected EngineNewPayloadV1<?, ?> createMethodInstance() {
    return new EngineNewPayloadV1<>(
        protocolSchedule,
        protocolContext,
        vertx,
        engineCallListener,
        mergeCoordinator,
        ethPeers,
        new NoOpMetricsSystem(),
        null,
        SHANGHAI);
  }

  private void createMethod() {
    this.method = createMethodInstance();
  }

  protected long getMinSupportedTimestamp() {
    return parisHardfork.milestone();
  }

  protected OptionalLong getMaxSupportedTimestamp() {
    return OptionalLong.of(shanghaiHardfork.milestone() - 1);
  }

  @Test
  public void shouldReturnValid() {
    BlockHeader mockHeader =
        setupPayloadV1(
            getMinSupportedTimestamp(),
            new BlockProcessingResult(Optional.of(new BlockProcessingOutputs(null, List.of()))));
    var resp = resp(requestParams(mockEnginePayloadParam(mockHeader, emptyList())));

    assertValidResponse(mockHeader, resp);
  }

  @Test
  public void shouldReturnInvalidOnBlockExecutionError() {
    BlockHeader mockHeader =
        setupPayloadV1(getMinSupportedTimestamp(), new BlockProcessingResult("error 42"));
    var resp = resp(requestParams(mockEnginePayloadParam(mockHeader, emptyList())));

    PayloadStatusV1 res = fromSuccessResp(resp);
    assertThat(res.getLatestValidHash().get()).isEqualTo(mockHash);
    assertThat(res.getStatusAsString()).isEqualTo(INVALID.name());
    assertThat(res.getError()).isEqualTo("error 42");
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnAcceptedOnLatestValidAncestorEmpty() {
    BlockHeader mockHeader = setupPayloadV1(getMinSupportedTimestamp());
    when(mergeCoordinator.getLatestValidAncestor(any(BlockHeader.class)))
        .thenReturn(Optional.empty());

    var resp = resp(requestParams(mockEnginePayloadParam(mockHeader, emptyList())));

    PayloadStatusV1 res = fromSuccessResp(resp);
    assertThat(res.getLatestValidHash()).isEmpty();
    assertThat(res.getStatusAsString()).isEqualTo(ACCEPTED.name());
    assertThat(res.getError()).isNull();
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnSuccessOnAlreadyPresent() {
    BlockHeader mockHeader = setupPayloadV1(getMinSupportedTimestamp());
    Block mockBlock = new Block(mockHeader, new BlockBody(emptyList(), emptyList()));

    when(blockchain.getBlockByHash(any())).thenReturn(Optional.of(mockBlock));

    var resp = resp(requestParams(mockEnginePayloadParam(mockHeader, emptyList())));

    assertValidResponse(mockHeader, resp);
  }

  @Test
  public void shouldReturnInvalidWithLatestValidHashIsABadBlock() {
    BlockHeader mockHeader = createBlockHeader(getMinSupportedTimestamp());
    Hash latestValidHash = Hash.hash(Bytes32.fromHexStringLenient("0xcafebabe"));

    when(mergeCoordinator.isBadBlock(mockHeader.getHash())).thenReturn(true);
    when(mergeCoordinator.getLatestValidHashOfBadBlock(mockHeader.getHash()))
        .thenReturn(Optional.of(latestValidHash));

    var resp = resp(requestParams(mockEnginePayloadParam(mockHeader, emptyList())));

    PayloadStatusV1 res = fromSuccessResp(resp);
    assertThat(res.getLatestValidHash()).isEqualTo(Optional.of(latestValidHash));
    assertThat(res.getStatusAsString()).isEqualTo(INVALID.name());
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldNotReturnInvalidOnStorageException() {
    BlockHeader mockHeader =
        setupPayloadV1(
            getMinSupportedTimestamp(),
            new BlockProcessingResult(Optional.empty(), new StorageException("database bedlam")));
    var resp = resp(requestParams(mockEnginePayloadParam(mockHeader, emptyList())));

    fromErrorResp(resp);
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldNotReturnInvalidOnHandledMerkleTrieException() {
    BlockHeader mockHeader =
        setupPayloadV1(
            getMinSupportedTimestamp(),
            new BlockProcessingResult(Optional.empty(), new MerkleTrieException("missing leaf")));

    var resp = resp(requestParams(mockEnginePayloadParam(mockHeader, emptyList())));

    verify(engineCallListener, times(1)).executionEngineCalled();

    fromErrorResp(resp);
  }

  @Test
  public void shouldNotReturnInvalidOnThrownMerkleTrieException() {
    BlockHeader mockHeader = setupPayloadV1(getMinSupportedTimestamp());
    when(mergeCoordinator.rememberBlock(any(), any()))
        .thenThrow(new MerkleTrieException("missing leaf"));

    var resp = resp(requestParams(mockEnginePayloadParam(mockHeader, emptyList())));

    verify(engineCallListener, times(1)).executionEngineCalled();

    fromErrorResp(resp);
  }

  @Test
  public void shouldReturnInvalidBlockHashOnBadHashParameter() {
    BlockHeader mockHeader = spy(createBlockHeader(getMinSupportedTimestamp()));
    when(mergeCoordinator.getLatestValidAncestor(mockHeader.getBlockHash()))
        .thenReturn(Optional.empty());
    when(blockchain.getBlockHeader(mockHeader.getParentHash()))
        .thenReturn(Optional.of(mock(BlockHeader.class)));
    when(mockHeader.getHash()).thenReturn(Hash.fromHexStringLenient("0x1337"));
    var resp = resp(requestParams(mockEnginePayloadParam(mockHeader, emptyList())));

    PayloadStatusV1 res = fromSuccessResp(resp);
    assertThat(res.getStatusAsString()).isEqualTo(getExpectedInvalidBlockHashStatus().name());
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldCheckBlockValidityBeforeCheckingByHashForExisting() {
    BlockHeader realHeader = createBlockHeader(getMinSupportedTimestamp());
    BlockHeader paramHeader = spy(realHeader);
    when(paramHeader.getHash()).thenReturn(Hash.fromHexStringLenient("0x1337"));

    var resp = resp(requestParams(mockEnginePayloadParam(paramHeader, emptyList())));

    PayloadStatusV1 res = fromSuccessResp(resp);
    assertThat(res.getLatestValidHash()).isEmpty();
    assertThat(res.getStatusAsString()).isEqualTo(getExpectedInvalidBlockHashStatus().name());
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnInvalidOnMalformedTransactions() {
    BlockHeader mockHeader = setupPayloadV1(getMinSupportedTimestamp());

    var executionPayload = mockEnginePayloadParam(mockHeader, List.of("0xDEAD", "0xBEEF"));

    var resp = resp(requestParams(executionPayload));

    PayloadStatusV1 res = fromSuccessResp(resp);
    assertThat(res.getLatestValidHash()).isEmpty();
    assertThat(res.getStatusAsString()).isEqualTo(INVALID.name());
    assertThat(res.getError()).startsWith("Failed to decode transactions from block parameter");
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldRespondWithSyncingDuringForwardSync() {
    BlockHeader mockHeader = setupPayloadV1(getMinSupportedTimestamp());
    when(mergeContext.isSyncing()).thenReturn(Boolean.TRUE);
    var resp = resp(requestParams(mockEnginePayloadParam(mockHeader, emptyList())));

    PayloadStatusV1 res = fromSuccessResp(resp);
    assertThat(res.getError()).isNull();
    assertThat(res.getStatusAsString()).isEqualTo(SYNCING.name());
    assertThat(res.getLatestValidHash()).isEmpty();
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldRespondWithSyncingDuringBackwardsSync() {
    BlockHeader mockHeader = createBlockHeader(getMinSupportedTimestamp());
    when(mergeCoordinator.appendNewPayloadToSync(any()))
        .thenReturn(CompletableFuture.completedFuture(null));
    var resp = resp(requestParams(mockEnginePayloadParam(mockHeader, emptyList())));

    PayloadStatusV1 res = fromSuccessResp(resp);
    assertThat(res.getLatestValidHash()).isEmpty();
    assertThat(res.getStatusAsString()).isEqualTo(SYNCING.name());
    assertThat(res.getError()).isNull();
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldRespondWithInvalidIfExtraDataIsNull() {
    BlockHeader realHeader = createBlockHeader(getMinSupportedTimestamp());
    BlockHeader paramHeader = spy(realHeader);
    when(paramHeader.getHash()).thenReturn(Hash.fromHexStringLenient("0x1337"));
    when(paramHeader.getExtraData().toHexString()).thenReturn(null);

    var resp = resp(requestParams(mockEnginePayloadParam(paramHeader, emptyList())));

    final JsonRpcError jsonRpcError = fromErrorResp(resp);
    assertThat(jsonRpcError.getCode()).isEqualTo(INVALID_PARAMS.getCode());
    assertThat(jsonRpcError.getData())
        .startsWith("Failed to decode extraData from block parameter");
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnInvalidWhenBadBlock() {
    when(mergeCoordinator.isBadBlock(any(Hash.class))).thenReturn(true);
    BlockHeader mockHeader = createBlockHeader(getMinSupportedTimestamp());
    var resp = resp(requestParams(mockEnginePayloadParam(mockHeader, emptyList())));
    PayloadStatusV1 res = fromSuccessResp(resp);
    assertThat(res.getLatestValidHash()).contains(Hash.ZERO);
    assertThat(res.getStatusAsString()).isEqualTo(INVALID.name());
    assertThat(res.getError()).isEqualTo("Block already present in bad block manager.");
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnUnsupportedForkIfBlockTimestampIsAfterSupportedForkWindow() {
    getMaxSupportedTimestamp()
        .ifPresent(
            maxSupportedTimestamp -> {
              BlockHeader mockHeader =
                  setupPayloadV1(
                      maxSupportedTimestamp + 1,
                      new BlockProcessingResult(
                          Optional.of(new BlockProcessingOutputs(null, List.of()))));

              var resp = resp(requestParams(mockEnginePayloadParam(mockHeader, emptyList())));

              final JsonRpcError jsonRpcError = fromErrorResp(resp);
              assertThat(jsonRpcError.getCode()).isEqualTo(UNSUPPORTED_FORK.getCode());
              verify(engineCallListener, times(1)).executionEngineCalled();
            });
  }

  protected Object[] requestParams(final Map<String, Object> payloadParams) {
    return ArrayUtils.addAll(new Object[] {payloadParams}, getVersionSpecificDefaultParams());
  }

  protected Object[] getVersionSpecificDefaultParams() {
    return new Object[0];
  }

  protected JsonRpcResponse resp(final Object... params) {
    return method.response(
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", this.method.getName(), params)));
  }

  protected Map<String, Object> mockEnginePayloadParam(
      final BlockHeader header, final List<String> txs) {
    final Map<String, Object> payload = new HashMap<>();
    setDefaultExecutionPayloadFields(payload, header, txs);
    return payload;
  }

  protected void setDefaultExecutionPayloadFields(
      final Map<String, Object> payload, final BlockHeader header, final List<String> txs) {
    payload.put("blockHash", header.getHash().toHexString());
    payload.put("parentHash", header.getParentHash().toHexString());
    payload.put("feeRecipient", header.getCoinbase().getBytes().toHexString());
    payload.put("stateRoot", header.getStateRoot().toHexString());
    payload.put("blockNumber", Bytes.ofUnsignedLong(header.getNumber()).toHexString());
    payload.put("baseFeePerGas", header.getBaseFee().map(Wei::toHexString).orElse("0x0"));
    payload.put("gasLimit", Bytes.ofUnsignedLong(header.getGasLimit()).toHexString());
    payload.put("gasUsed", Bytes.ofUnsignedLong(header.getGasUsed()).toHexString());
    payload.put("timestamp", Bytes.ofUnsignedLong(header.getTimestamp()).toHexString());
    payload.put(
        "extraData", header.getExtraData() == null ? null : header.getExtraData().toHexString());
    payload.put("receiptsRoot", header.getReceiptsRoot().toHexString());
    payload.put("logsBloom", header.getLogsBloom().toHexString());
    payload.put("prevRandao", header.getPrevRandao().map(Bytes32::toHexString).orElse("0x0"));
    payload.put("transactions", txs);
  }

  protected ExecutionEngineJsonRpcMethod.EngineStatus getExpectedInvalidBlockHashStatus() {
    return INVALID_BLOCK_HASH;
  }

  protected PayloadStatusV1 fromSuccessResp(final JsonRpcResponse resp) {
    if (resp.getType().equals(RpcResponseType.ERROR)) {
      final JsonRpcError jsonRpcError = fromErrorResp(resp);
      throw new AssertionError(
          "Expected success but was error with message: " + jsonRpcError.getMessage());
    }
    assertThat(resp.getType()).isEqualTo(RpcResponseType.SUCCESS);
    return Optional.of(resp)
        .map(JsonRpcSuccessResponse.class::cast)
        .map(JsonRpcSuccessResponse::getResult)
        .map(PayloadStatusV1.class::cast)
        .get();
  }

  protected BlockHeader setupPayloadV1(final long timestamp) {
    return setupPayloadV1(timestamp, null, UnaryOperator.identity());
  }

  protected BlockHeader setupPayloadV1(final long timestamp, final BlockProcessingResult value) {

    return setupPayloadV1(timestamp, value, UnaryOperator.identity());
  }

  protected BlockHeader setupPayloadV1(
      final long timestamp,
      final BlockProcessingResult value,
      final UnaryOperator<BlockHeaderTestFixture> versionSpecificModifier) {

    BlockHeader mockHeader = createBlockHeader(timestamp, versionSpecificModifier);
    when(blockchain.getBlockByHash(mockHeader.getHash())).thenReturn(Optional.empty());
    when(blockchain.getBlockHeader(mockHeader.getParentHash()))
        .thenReturn(Optional.of(mock(BlockHeader.class)));
    when(mergeCoordinator.getLatestValidAncestor(any(BlockHeader.class)))
        .thenReturn(Optional.of(mockHash));
    when(mergeCoordinator.rememberBlock(any(), any())).thenReturn(value);
    return mockHeader;
  }

  protected BlockHeader createBlockHeader(final long timestamp) {
    return versionSpecificBlockHeaderFixture(timestamp).buildHeader();
  }

  protected BlockHeader createBlockHeader(
      final long timestamp, final UnaryOperator<BlockHeaderTestFixture> versionSpecificModifier) {
    return versionSpecificModifier
        .apply(versionSpecificBlockHeaderFixture(timestamp))
        .buildHeader();
  }

  protected BlockHeaderTestFixture versionSpecificBlockHeaderFixture(final long timestamp) {
    BlockHeader parentBlockHeader =
        new BlockHeaderTestFixture().timestamp(timestamp - 1).baseFeePerGas(Wei.ONE).buildHeader();
    return new BlockHeaderTestFixture()
        .baseFeePerGas(Wei.ONE)
        .parentHash(parentBlockHeader.getParentHash())
        .number(parentBlockHeader.getNumber() + 1)
        .timestamp(parentBlockHeader.getTimestamp() + 1);
  }

  protected void assertValidResponse(final BlockHeader mockHeader, final JsonRpcResponse resp) {
    PayloadStatusV1 res = fromSuccessResp(resp);
    assertThat(res.getLatestValidHash().get()).isEqualTo(mockHeader.getHash());
    assertThat(res.getStatusAsString()).isEqualTo(VALID.name());
    assertThat(res.getError()).isNull();
    verify(engineCallListener, times(1)).executionEngineCalled();
  }
}
