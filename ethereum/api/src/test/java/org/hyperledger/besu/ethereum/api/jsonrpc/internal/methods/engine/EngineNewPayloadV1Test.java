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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.MergeContext;
import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.datatypes.parameters.UnsignedLongParameter;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV2;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV3;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV4;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.WithdrawalParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.PayloadStatusV1;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.WithdrawalsValidator;
import org.hyperledger.besu.ethereum.mainnet.requests.ProhibitedRequestValidator;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

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
public class EngineNewPayloadV1Test extends AbstractScheduledApiTest {

  protected EngineNewPayloadV1 method;
  protected Optional<Bytes32> maybeParentBeaconBlockRoot = Optional.empty();

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
    when(protocolContext.safeConsensusContext(Mockito.any())).thenReturn(Optional.of(mergeContext));
    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    lenient()
        .when(protocolSpec.getWithdrawalsValidator())
        .thenReturn(new WithdrawalsValidator.ProhibitedWithdrawals());
    mockProhibitedRequestsValidator();
    lenient().when(protocolSchedule.getByBlockHeader(any())).thenReturn(protocolSpec);
    lenient().when(ethPeers.peerCount()).thenReturn(1);
    createMethod();
  }

  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_newPayloadV1");
  }

  protected EngineNewPayloadV1 createMethodInstance() {
    return new EngineNewPayloadV1(
        vertx,
        protocolSchedule,
        protocolContext,
        mergeCoordinator,
        ethPeers,
        engineCallListener,
        new NoOpMetricsSystem(),
        null,
        SHANGHAI);
  }

  protected final void createMethod() {
    this.method = createMethodInstance();
  }

  @Test
  public void shouldReturnValid() {
    BlockHeader mockHeader =
        setupValidPayload(
            new BlockProcessingResult(Optional.of(new BlockProcessingOutputs(null, List.of()))),
            Optional.empty());
    lenient()
        .when(blockchain.getBlockHeader(mockHeader.getParentHash()))
        .thenReturn(Optional.of(mock(BlockHeader.class)));
    var resp = resp(mockEnginePayload(mockHeader, emptyList()));

    assertValidResponse(mockHeader, resp);
  }

  @Test
  public void shouldReturnInvalidOnBlockExecutionError() {
    BlockHeader mockHeader =
        setupValidPayload(new BlockProcessingResult("error 42"), Optional.empty());
    lenient()
        .when(blockchain.getBlockHeader(mockHeader.getParentHash()))
        .thenReturn(Optional.of(mock(BlockHeader.class)));
    var resp = resp(mockEnginePayload(mockHeader, emptyList()));

    PayloadStatusV1 res = fromSuccessResp(resp);
    assertThat(res.getLatestValidHash().get()).isEqualTo(mockHash);
    assertThat(res.getStatusAsString()).isEqualTo(INVALID.name());
    assertThat(res.getError()).isEqualTo("error 42");
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnAcceptedOnLatestValidAncestorEmpty() {
    BlockHeader mockHeader = createBlockHeader(Optional.empty());
    when(blockchain.getBlockByHash(mockHeader.getHash())).thenReturn(Optional.empty());
    when(blockchain.getBlockHeader(mockHeader.getParentHash()))
        .thenReturn(Optional.of(mock(BlockHeader.class)));
    when(mergeCoordinator.getLatestValidAncestor(any(BlockHeader.class)))
        .thenReturn(Optional.empty());

    var resp = resp(mockEnginePayload(mockHeader, emptyList()));

    PayloadStatusV1 res = fromSuccessResp(resp);
    assertThat(res.getLatestValidHash()).isEmpty();
    assertThat(res.getStatusAsString()).isEqualTo(ACCEPTED.name());
    assertThat(res.getError()).isNull();
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnSuccessOnAlreadyPresent() {
    BlockHeader mockHeader = createBlockHeader(Optional.empty());
    Block mockBlock = new Block(mockHeader, new BlockBody(emptyList(), emptyList()));

    when(blockchain.getBlockByHash(any())).thenReturn(Optional.of(mockBlock));

    var resp = resp(mockEnginePayload(mockHeader, emptyList()));

    assertValidResponse(mockHeader, resp);
  }

  @Test
  public void shouldReturnInvalidWithLatestValidHashIsABadBlock() {
    BlockHeader mockHeader = createBlockHeader(Optional.empty());
    Hash latestValidHash = Hash.hash(Bytes32.fromHexStringLenient("0xcafebabe"));

    when(blockchain.getBlockByHash(mockHeader.getHash())).thenReturn(Optional.empty());
    when(mergeCoordinator.isBadBlock(mockHeader.getHash())).thenReturn(true);
    when(mergeCoordinator.getLatestValidHashOfBadBlock(mockHeader.getHash()))
        .thenReturn(Optional.of(latestValidHash));

    var resp = resp(mockEnginePayload(mockHeader, emptyList()));

    PayloadStatusV1 res = fromSuccessResp(resp);
    assertThat(res.getLatestValidHash()).isEqualTo(Optional.of(latestValidHash));
    assertThat(res.getStatusAsString()).isEqualTo(INVALID.name());
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldNotReturnInvalidOnStorageException() {
    BlockHeader mockHeader =
        setupValidPayload(
            new BlockProcessingResult(Optional.empty(), new StorageException("database bedlam")),
            Optional.empty());
    lenient()
        .when(blockchain.getBlockHeader(mockHeader.getParentHash()))
        .thenReturn(Optional.of(mock(BlockHeader.class)));
    var resp = resp(mockEnginePayload(mockHeader, emptyList()));

    fromErrorResp(resp);
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldNotReturnInvalidOnHandledMerkleTrieException() {
    BlockHeader mockHeader =
        setupValidPayload(
            new BlockProcessingResult(Optional.empty(), new MerkleTrieException("missing leaf")),
            Optional.empty());

    lenient()
        .when(blockchain.getBlockHeader(mockHeader.getParentHash()))
        .thenReturn(Optional.of(mock(BlockHeader.class)));
    var resp = resp(mockEnginePayload(mockHeader, emptyList()));

    verify(engineCallListener, times(1)).executionEngineCalled();

    fromErrorResp(resp);
  }

  @Test
  public void shouldNotReturnInvalidOnThrownMerkleTrieException() {
    BlockHeader mockHeader = createBlockHeader(Optional.empty());
    when(blockchain.getBlockByHash(mockHeader.getHash())).thenReturn(Optional.empty());
    when(blockchain.getBlockHeader(mockHeader.getParentHash()))
        .thenReturn(Optional.of(mock(BlockHeader.class)));
    when(mergeCoordinator.getLatestValidAncestor(any(BlockHeader.class)))
        .thenReturn(Optional.of(mockHash));
    when(mergeCoordinator.rememberBlock(any(), any()))
        .thenThrow(new MerkleTrieException("missing leaf"));

    var resp = resp(mockEnginePayload(mockHeader, emptyList()));

    verify(engineCallListener, times(1)).executionEngineCalled();

    fromErrorResp(resp);
  }

  @Test
  public void shouldReturnInvalidBlockHashOnBadHashParameter() {
    BlockHeader mockHeader = spy(createBlockHeader(Optional.empty()));
    lenient()
        .when(mergeCoordinator.getLatestValidAncestor(mockHeader.getBlockHash()))
        .thenReturn(Optional.empty());
    lenient()
        .when(blockchain.getBlockHeader(mockHeader.getParentHash()))
        .thenReturn(Optional.of(mock(BlockHeader.class)));
    lenient().when(mockHeader.getHash()).thenReturn(Hash.fromHexStringLenient("0x1337"));
    var resp = resp(mockEnginePayload(mockHeader, emptyList()));

    PayloadStatusV1 res = fromSuccessResp(resp);
    assertThat(res.getStatusAsString()).isEqualTo(getExpectedInvalidBlockHashStatus().name());
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldCheckBlockValidityBeforeCheckingByHashForExisting() {
    BlockHeader realHeader = createBlockHeader(Optional.empty());
    BlockHeader paramHeader = spy(realHeader);
    when(paramHeader.getHash()).thenReturn(Hash.fromHexStringLenient("0x1337"));

    var resp = resp(mockEnginePayload(paramHeader, emptyList()));

    PayloadStatusV1 res = fromSuccessResp(resp);
    assertThat(res.getLatestValidHash()).isEmpty();
    assertThat(res.getStatusAsString()).isEqualTo(getExpectedInvalidBlockHashStatus().name());
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnInvalidOnMalformedTransactions() {
    BlockHeader mockHeader = createBlockHeader(Optional.empty());
    when(mergeCoordinator.getLatestValidAncestor(any(Hash.class)))
        .thenReturn(Optional.of(mockHash));

    var resp = resp(mockEnginePayload(mockHeader, List.of("0xDEAD", "0xBEEF")));

    PayloadStatusV1 res = fromSuccessResp(resp);
    assertThat(res.getLatestValidHash().get()).isEqualTo(mockHash);
    assertThat(res.getStatusAsString()).isEqualTo(INVALID.name());
    assertThat(res.getError()).isEqualTo("Failed to decode transactions from block parameter");
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldRespondWithSyncingDuringForwardSync() {
    BlockHeader mockHeader = createBlockHeader(Optional.empty());
    when(mergeContext.isSyncing()).thenReturn(Boolean.TRUE);
    var resp = resp(mockEnginePayload(mockHeader, emptyList()));

    PayloadStatusV1 res = fromSuccessResp(resp);
    assertThat(res.getError()).isNull();
    assertThat(res.getStatusAsString()).isEqualTo(SYNCING.name());
    assertThat(res.getLatestValidHash()).isEmpty();
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldRespondWithSyncingDuringBackwardsSync() {
    BlockHeader mockHeader = createBlockHeader(Optional.empty());
    when(mergeCoordinator.appendNewPayloadToSync(any()))
        .thenReturn(CompletableFuture.completedFuture(null));
    var resp = resp(mockEnginePayload(mockHeader, emptyList()));

    PayloadStatusV1 res = fromSuccessResp(resp);
    assertThat(res.getLatestValidHash()).isEmpty();
    assertThat(res.getStatusAsString()).isEqualTo(SYNCING.name());
    assertThat(res.getError()).isNull();
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldRespondWithInvalidIfExtraDataIsNull() {
    BlockHeader realHeader = createBlockHeader(Optional.empty());
    BlockHeader paramHeader = spy(realHeader);
    when(paramHeader.getHash()).thenReturn(Hash.fromHexStringLenient("0x1337"));
    when(paramHeader.getExtraData().toHexString()).thenReturn(null);

    var resp = resp(mockEnginePayload(paramHeader, emptyList()));

    PayloadStatusV1 res = fromSuccessResp(resp);
    assertThat(res.getLatestValidHash()).isEmpty();
    assertThat(res.getStatusAsString()).isEqualTo(INVALID.name());
    assertThat(res.getError()).isEqualTo("Field extraData must not be null");
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnInvalidWhenBadBlock() {
    when(mergeCoordinator.isBadBlock(any(Hash.class))).thenReturn(true);
    BlockHeader mockHeader = createBlockHeader(Optional.empty());
    var resp = resp(mockEnginePayload(mockHeader, emptyList()));
    when(protocolSpec.getWithdrawalsValidator())
        .thenReturn(new WithdrawalsValidator.AllowedWithdrawals());
    PayloadStatusV1 res = fromSuccessResp(resp);
    assertThat(res.getLatestValidHash()).contains(Hash.ZERO);
    assertThat(res.getStatusAsString()).isEqualTo(INVALID.name());
    assertThat(res.getError()).isEqualTo("Block already present in bad block manager.");
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnValidIfProtocolScheduleIsEmpty() {
    when(protocolSchedule.getByBlockHeader(any())).thenReturn(null);
    BlockHeader mockHeader =
        setupValidPayload(
            new BlockProcessingResult(Optional.of(new BlockProcessingOutputs(null, List.of()))),
            Optional.empty());
    lenient()
        .when(blockchain.getBlockHeader(mockHeader.getParentHash()))
        .thenReturn(Optional.of(mock(BlockHeader.class)));
    var resp = resp(mockEnginePayload(mockHeader, emptyList()));

    assertValidResponse(mockHeader, resp);
  }

  protected JsonRpcResponse resp(final ExecutionPayloadV1 payload) {
    Object[] params =
        maybeParentBeaconBlockRoot
            .map(bytes32 -> new Object[] {payload, null, bytes32.toHexString()})
            .orElseGet(() -> new Object[] {payload});
    return method.response(
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", this.method.getName(), params)));
  }

  protected ExecutionPayloadV1 mockEnginePayload(final BlockHeader header, final List<String> txs) {
    return executionPayloadV1(header, txs);
  }

  protected ExecutionPayloadV1 mockEnginePayload(
      final BlockHeader header,
      final List<String> txs,
      final List<WithdrawalParameter> withdrawals) {
    return executionPayloadV1(header, txs);
  }

  protected ExecutionPayloadV1 mockEnginePayload(
      final BlockHeader header,
      final List<String> txs,
      final List<WithdrawalParameter> withdrawals,
      final String blockAccessList) {
    return executionPayloadV1(header, txs);
  }

  protected ExecutionPayloadV1 executionPayloadV1(
      final BlockHeader header, final List<String> txs) {
    final ExecutionPayloadV1 payload = new ExecutionPayloadV1();
    setExecutionPayloadV1Fields(payload, header, txs);
    return payload;
  }

  protected ExecutionPayloadV2 executionPayloadV2(
      final BlockHeader header,
      final List<String> txs,
      final List<WithdrawalParameter> withdrawals) {
    final ExecutionPayloadV2 payload = new ExecutionPayloadV2();
    setExecutionPayloadV1Fields(payload, header, txs);
    payload.setWithdrawals(withdrawals);
    return payload;
  }

  protected ExecutionPayloadV3 executionPayloadV3(
      final BlockHeader header,
      final List<String> txs,
      final List<WithdrawalParameter> withdrawals) {
    final ExecutionPayloadV3 payload = new ExecutionPayloadV3();
    setExecutionPayloadV1Fields(payload, header, txs);
    payload.setWithdrawals(withdrawals);
    payload.setBlobGasUsed(header.getBlobGasUsed().map(UnsignedLongParameter::new).orElse(null));
    payload.setExcessBlobGas(header.getExcessBlobGas().map(BlobGas::toHexString).orElse(null));
    return payload;
  }

  protected ExecutionPayloadV4 executionPayloadV4(
      final BlockHeader header,
      final List<String> txs,
      final List<WithdrawalParameter> withdrawals,
      final String blockAccessList) {
    final ExecutionPayloadV4 payload = new ExecutionPayloadV4();
    setExecutionPayloadV1Fields(payload, header, txs);
    payload.setWithdrawals(withdrawals);
    payload.setBlobGasUsed(header.getBlobGasUsed().map(UnsignedLongParameter::new).orElse(null));
    payload.setExcessBlobGas(header.getExcessBlobGas().map(BlobGas::toHexString).orElse(null));
    payload.setBlockAccessList(blockAccessList);
    payload.setSlotNumber(
        header.getOptionalSlotNumber().map(UnsignedLongParameter::new).orElse(null));
    return payload;
  }

  protected void setExecutionPayloadV1Fields(
      final ExecutionPayloadV1 payload, final BlockHeader header, final List<String> txs) {
    payload.setBlockHash(header.getHash());
    payload.setParentHash(header.getParentHash());
    payload.setFeeRecipient(header.getCoinbase());
    payload.setStateRoot(header.getStateRoot());
    payload.setBlockNumber(new UnsignedLongParameter(header.getNumber()));
    payload.setBaseFeePerGas(header.getBaseFee().map(w -> w.toHexString()).orElse("0x0"));
    payload.setGasLimit(new UnsignedLongParameter(header.getGasLimit()));
    payload.setGasUsed(new UnsignedLongParameter(header.getGasUsed()));
    payload.setTimestamp(new UnsignedLongParameter(header.getTimestamp()));
    payload.setExtraData(
        header.getExtraData() == null ? null : header.getExtraData().toHexString());
    payload.setReceiptsRoot(header.getReceiptsRoot());
    payload.setLogsBloom(header.getLogsBloom());
    payload.setPrevRandao(header.getPrevRandao().map(Bytes32::toHexString).orElse("0x0"));
    payload.setTransactions(txs);
  }

  protected BlockHeader setupValidPayload(
      final BlockProcessingResult value, final Optional<List<Withdrawal>> maybeWithdrawals) {

    BlockHeader mockHeader = createBlockHeader(maybeWithdrawals);
    when(blockchain.getBlockByHash(mockHeader.getHash())).thenReturn(Optional.empty());
    when(mergeCoordinator.getLatestValidAncestor(any(BlockHeader.class)))
        .thenReturn(Optional.of(mockHash));
    when(mergeCoordinator.rememberBlock(any(), any())).thenReturn(value);
    return mockHeader;
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

  protected BlockHeader createBlockHeader(final Optional<List<Withdrawal>> maybeWithdrawals) {
    return createBlockHeaderFixture(maybeWithdrawals).buildHeader();
  }

  protected BlockHeaderTestFixture createBlockHeaderFixture(
      final Optional<List<Withdrawal>> maybeWithdrawals) {
    BlockHeader parentBlockHeader =
        new BlockHeaderTestFixture().baseFeePerGas(Wei.ONE).buildHeader();
    return new BlockHeaderTestFixture()
        .baseFeePerGas(Wei.ONE)
        .parentHash(parentBlockHeader.getParentHash())
        .number(parentBlockHeader.getNumber() + 1)
        .timestamp(parentBlockHeader.getTimestamp() + 1)
        .withdrawalsRoot(maybeWithdrawals.map(BodyValidation::withdrawalsRoot).orElse(null))
        .parentBeaconBlockRoot(maybeParentBeaconBlockRoot);
  }

  protected void assertValidResponse(final BlockHeader mockHeader, final JsonRpcResponse resp) {
    PayloadStatusV1 res = fromSuccessResp(resp);
    assertThat(res.getLatestValidHash().get()).isEqualTo(mockHeader.getHash());
    assertThat(res.getStatusAsString()).isEqualTo(VALID.name());
    assertThat(res.getError()).isNull();
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  private void mockProhibitedRequestsValidator() {
    var validator = new ProhibitedRequestValidator();
    when(protocolSpec.getRequestsValidator()).thenReturn(validator);
  }
}
