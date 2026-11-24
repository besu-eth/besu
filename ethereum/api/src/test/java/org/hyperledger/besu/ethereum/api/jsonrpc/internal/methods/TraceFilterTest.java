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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryWorldState;
import static org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture.getGenesisConfigOptions;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.FilterParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.BlockTracer;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.MainnetProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TraceFilterTest {

  private TraceFilter method;

  @Mock Supplier<BlockTracer> blockTracerSupplier;
  @Mock ProtocolSchedule protocolSchedule;
  @Mock BlockchainQueries blockchainQueries;
  @Mock Blockchain blockchain;
  @Mock WorldStateArchive worldStateArchive;

  private static final BlockHeader BLOCK_HEADER =
      new BlockHeaderTestFixture().number(19).buildHeader();
  private static final BlockHeader HEAD_HEADER =
      new BlockHeaderTestFixture().number(20).stateRoot(Hash.ZERO).buildHeader();
  private static final BlockHeader GENESIS_HEADER =
      new BlockHeaderTestFixture().number(0).parentHash(Hash.ZERO).buildHeader();

  @ParameterizedTest
  @CsvSource({"0, 1001, 1000", "1, 6002, 1000", "1000, 3000, 500"})
  public void shouldFailIfParamsExceedMaxRange(
      final long fromBlock, final long toBlock, final long maxFilterRange) {
    final FilterParameter filterParameter =
        new FilterParameter(
            new BlockParameter(fromBlock),
            new BlockParameter(toBlock),
            null,
            null,
            null,
            null,
            null,
            null,
            null);

    JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", "trace_filter", new Object[] {filterParameter}));

    // Mock headBlockNumber for validation with a value higher than toBlock
    when(blockchainQueries.headBlockNumber()).thenReturn(Math.max(toBlock + 1000, 10000L));

    method =
        new TraceFilter(
            protocolSchedule,
            blockchainQueries,
            maxFilterRange,
            new NoOpMetricsSystem(),
            new DeterministicEthScheduler());

    final JsonRpcResponse response = method.response(request);
    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);

    final JsonRpcErrorResponse errorResponse = (JsonRpcErrorResponse) response;
    assertThat(errorResponse.getErrorType()).isEqualTo(RpcErrorType.EXCEEDS_RPC_MAX_BLOCK_RANGE);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void shouldRejectBlockHashWithBlockRange(final boolean withFromBlock) {
    final BlockParameter bound = new BlockParameter(19);
    final JsonRpcResponse response =
        traceFilter(
            new FilterParameter(
                withFromBlock ? bound : null,
                withFromBlock ? null : bound,
                null,
                null,
                null,
                null,
                BLOCK_HEADER.getHash(),
                null,
                null));

    assertError(response, RpcErrorType.INVALID_FILTER_PARAMS);
    verifyNoInteractions(blockchainQueries);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 10})
  public void shouldReturnBlockNotFoundForUnknownBlockHash(final int count) {
    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getBlockHeader(BLOCK_HEADER.getHash())).thenReturn(Optional.empty());

    assertError(
        traceFilter(blockHashFilter(BLOCK_HEADER.getHash(), count)), RpcErrorType.BLOCK_NOT_FOUND);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 10})
  public void shouldReturnBlockNotFoundForNoncanonicalBlockHash(final int count) {
    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getBlockHeader(BLOCK_HEADER.getHash())).thenReturn(Optional.of(BLOCK_HEADER));
    when(blockchain.blockIsOnCanonicalChain(BLOCK_HEADER.getHash())).thenReturn(false);

    assertError(
        traceFilter(blockHashFilter(BLOCK_HEADER.getHash(), count)), RpcErrorType.BLOCK_NOT_FOUND);
    verify(blockchainQueries, never()).getAndMapWorldState(any(Hash.class), any());
  }

  @Test
  public void shouldReturnPrunedHistoryUnavailableForBlockHashWithoutBody() {
    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getBlockHeader(BLOCK_HEADER.getHash())).thenReturn(Optional.of(BLOCK_HEADER));
    when(blockchain.blockIsOnCanonicalChain(BLOCK_HEADER.getHash())).thenReturn(true);
    when(blockchain.getBlockBody(BLOCK_HEADER.getHash())).thenReturn(Optional.empty());

    assertError(
        traceFilter(blockHashFilter(BLOCK_HEADER.getHash(), 0)),
        RpcErrorType.PRUNED_HISTORY_UNAVAILABLE);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 10})
  public void shouldReturnWorldStateUnavailableForBlockHashWithoutParentStateWhenHeadUnavailable(
      final int count) {
    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchainQueries.getWorldStateArchive()).thenReturn(worldStateArchive);
    when(blockchain.getBlockHeader(BLOCK_HEADER.getHash())).thenReturn(Optional.of(BLOCK_HEADER));
    when(blockchain.blockIsOnCanonicalChain(BLOCK_HEADER.getHash())).thenReturn(true);
    when(blockchain.getBlockBody(BLOCK_HEADER.getHash()))
        .thenReturn(Optional.of(BlockBody.empty()));
    when(blockchainQueries.getAndMapWorldState(any(Hash.class), any()))
        .thenReturn(Optional.empty());
    when(blockchain.getChainHeadHeader()).thenReturn(HEAD_HEADER);
    when(worldStateArchive.isWorldStateAvailable(HEAD_HEADER.getStateRoot(), HEAD_HEADER.getHash()))
        .thenReturn(false);

    assertError(
        traceFilter(blockHashFilter(BLOCK_HEADER.getHash(), count)),
        RpcErrorType.WORLD_STATE_UNAVAILABLE);
    // the resolved block is traced on its own parent state, never looked up again by number
    verify(blockchainQueries).getAndMapWorldState(eq(BLOCK_HEADER.getParentHash()), any());
    verify(blockchain, never()).getBlockByNumber(anyLong());
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 10})
  public void shouldReturnPrunedHistoryUnavailableForBlockHashWithoutParentStateWhenHeadAvailable(
      final int count) {
    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchainQueries.getWorldStateArchive()).thenReturn(worldStateArchive);
    when(blockchain.getBlockHeader(BLOCK_HEADER.getHash())).thenReturn(Optional.of(BLOCK_HEADER));
    when(blockchain.blockIsOnCanonicalChain(BLOCK_HEADER.getHash())).thenReturn(true);
    when(blockchain.getBlockBody(BLOCK_HEADER.getHash()))
        .thenReturn(Optional.of(BlockBody.empty()));
    when(blockchainQueries.getAndMapWorldState(any(Hash.class), any()))
        .thenReturn(Optional.empty());
    when(blockchain.getChainHeadHeader()).thenReturn(HEAD_HEADER);
    when(worldStateArchive.isWorldStateAvailable(HEAD_HEADER.getStateRoot(), HEAD_HEADER.getHash()))
        .thenReturn(true);

    assertError(
        traceFilter(blockHashFilter(BLOCK_HEADER.getHash(), count)),
        RpcErrorType.PRUNED_HISTORY_UNAVAILABLE);
    verify(blockchainQueries).getAndMapWorldState(eq(BLOCK_HEADER.getParentHash()), any());
    verify(blockchain, never()).getBlockByNumber(anyLong());
  }

  @Test
  public void shouldReturnEmptyResultForGenesisBlockHash() {
    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getBlockHeader(GENESIS_HEADER.getHash()))
        .thenReturn(Optional.of(GENESIS_HEADER));
    when(blockchain.blockIsOnCanonicalChain(GENESIS_HEADER.getHash())).thenReturn(true);
    when(blockchain.getBlockBody(GENESIS_HEADER.getHash()))
        .thenReturn(Optional.of(BlockBody.empty()));

    final JsonRpcResponse response = traceFilter(blockHashFilter(GENESIS_HEADER.getHash(), 10));

    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    assertThat((ArrayNode) ((JsonRpcSuccessResponse) response).getResult()).isEmpty();
    verify(blockchainQueries, never()).getAndMapWorldState(any(Hash.class), any());
  }

  private static FilterParameter blockHashFilter(final Hash blockHash, final int count) {
    return new FilterParameter(null, null, null, null, null, null, blockHash, null, count);
  }

  private JsonRpcResponse traceFilter(final FilterParameter filterParameter) {
    method =
        new TraceFilter(
            protocolSchedule,
            blockchainQueries,
            0L,
            new NoOpMetricsSystem(),
            new DeterministicEthScheduler());
    return method.response(
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", "trace_filter", new Object[] {filterParameter})));
  }

  private static void assertError(final JsonRpcResponse response, final RpcErrorType errorType) {
    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) response).getErrorType()).isEqualTo(errorType);
  }

  @Nested
  class UnavailableParentStateTests {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Address RECIPIENT =
        Address.fromHexString("0x2000000000000000000000000000000000000002");
    private static final Address BENEFICIARY =
        Address.fromHexString("0x3000000000000000000000000000000000000003");

    private final Blockchain chain = mock(Blockchain.class);
    private final WorldStateArchive archive = mock(WorldStateArchive.class);
    private final KeyPair keys = SignatureAlgorithmFactory.getInstance().generateKeyPair();
    private final Address sender = Address.extract(keys.getPublicKey());
    private final DeterministicEthScheduler scheduler = new DeterministicEthScheduler();

    private ProtocolSchedule schedule;
    private BlockchainQueries queries;
    private TraceFilter filterMethod;
    private BlockHeader parent;
    private Block block;
    private Transaction transaction;

    @BeforeEach
    void setUp() {
      schedule =
          MainnetProtocolSchedule.fromConfig(
              getGenesisConfigOptions("/prague_all_milestones_zero.json"),
              MiningConfiguration.newDefault(),
              new BadBlockManager(),
              false,
              BalConfiguration.DEFAULT,
              new NoOpMetricsSystem());
      queries = new BlockchainQueries(schedule, chain, archive, MiningConfiguration.newDefault());
      filterMethod = new TraceFilter(schedule, queries, 0L, new NoOpMetricsSystem(), scheduler);
      parent =
          new BlockHeaderTestFixture()
              .number(0)
              .parentHash(Hash.ZERO)
              .timestamp(1)
              .gasLimit(30_000_000)
              .baseFeePerGas(Wei.ONE)
              .excessBlobGas(BlobGas.ZERO)
              .blobGasUsed(0L)
              .buildHeader();
      lenient().when(chain.getChainHeadBlockNumber()).thenReturn(10L);
      lenient().when(chain.getBlockHeader(parent.getHash())).thenReturn(Optional.of(parent));
      lenient()
          .when(chain.getBlockByNumber(0))
          .thenReturn(Optional.of(new Block(parent, BlockBody.empty())));
      lenient()
          .when(archive.getWorldState(any()))
          .thenAnswer(invocation -> Optional.of(newState()));
      transaction = transaction(0);
      setBlock(List.of(transaction));
    }

    @AfterEach
    void tearDown() {
      scheduler.stop();
    }

    @Test
    void blockRangeShouldReturnPrunedHistoryWhenParentStateIsUnavailableAndHeadStateIsAvailable() {
      unavailableParentState(true);

      final JsonRpcResponse response = responseForRange(1, 1);

      assertError(response, RpcErrorType.PRUNED_HISTORY_UNAVAILABLE);
      assertThat(RpcErrorType.PRUNED_HISTORY_UNAVAILABLE.getCode()).isEqualTo(4444);
    }

    @Test
    void blockRangeShouldReturnWorldStateUnavailableWhenHeadStateIsAlsoUnavailable() {
      unavailableParentState(false);

      final JsonRpcResponse response = responseForRange(1, 1);

      assertError(response, RpcErrorType.WORLD_STATE_UNAVAILABLE);
      assertThat(RpcErrorType.WORLD_STATE_UNAVAILABLE.getCode()).isEqualTo(-32000);
    }

    @Test
    void blockRangeShouldReturnInternalErrorWhenTracingThrowsAndStateIsAvailable() {
      useFailingScheduler();

      final JsonRpcResponse response = responseForRange(1, 1);

      assertError(response, RpcErrorType.INTERNAL_ERROR);
      assertThat(RpcErrorType.INTERNAL_ERROR.getCode()).isEqualTo(-32603);
    }

    @Test
    void blockRangeShouldTraceBlockWhenParentStateIsAvailable() {
      final JsonNode result = successResult(responseForRange(1, 1));

      assertThat(result).isNotEmpty();
      assertThat(result.toString()).contains(transaction.getHash().toHexString());
    }

    @Test
    void blockRangeShouldReturnEmptyTraceForGenesisBlock() {
      final JsonNode result = successResult(responseForRange(0, 0));

      assertThat(result).isEmpty();
    }

    @Test
    void blockHashShouldReturnPrunedHistoryWhenParentStateIsUnavailableAndHeadStateIsAvailable() {
      unavailableParentState(true);

      final JsonRpcResponse response = responseForBlockHash(block.getHash());

      assertError(response, RpcErrorType.PRUNED_HISTORY_UNAVAILABLE);
      assertThat(RpcErrorType.PRUNED_HISTORY_UNAVAILABLE.getCode()).isEqualTo(4444);
    }

    @Test
    void blockHashShouldReturnWorldStateUnavailableWhenHeadStateIsAlsoUnavailable() {
      unavailableParentState(false);

      final JsonRpcResponse response = responseForBlockHash(block.getHash());

      assertError(response, RpcErrorType.WORLD_STATE_UNAVAILABLE);
      assertThat(RpcErrorType.WORLD_STATE_UNAVAILABLE.getCode()).isEqualTo(-32000);
    }

    @Test
    void blockHashShouldReturnInternalErrorWhenTracingThrowsAndStateIsAvailable() {
      useFailingScheduler();

      final JsonRpcResponse response = responseForBlockHash(block.getHash());

      assertError(response, RpcErrorType.INTERNAL_ERROR);
      assertThat(RpcErrorType.INTERNAL_ERROR.getCode()).isEqualTo(-32603);
    }

    @Test
    void blockHashShouldTraceBlockWhenParentStateIsAvailable() {
      final JsonNode result = successResult(responseForBlockHash(block.getHash()));

      assertThat(result).isNotEmpty();
      assertThat(result.toString()).contains(transaction.getHash().toHexString());
    }

    private void unavailableParentState(final boolean headStateAvailable) {
      when(archive.getWorldState(any())).thenReturn(Optional.empty());
      final BlockHeader head =
          new BlockHeaderTestFixture().number(10).stateRoot(Hash.ZERO).buildHeader();
      when(chain.getChainHeadHeader()).thenReturn(head);
      when(archive.isWorldStateAvailable(head.getStateRoot(), head.getHash()))
          .thenReturn(headStateAvailable);
    }

    private void useFailingScheduler() {
      final EthScheduler failingScheduler = mock(EthScheduler.class);
      when(failingScheduler.startPipeline(any()))
          .thenReturn(
              CompletableFuture.failedFuture(new RuntimeException("trace pipeline failed")));
      filterMethod =
          new TraceFilter(schedule, queries, 0L, new NoOpMetricsSystem(), failingScheduler);
      final BlockHeader head =
          new BlockHeaderTestFixture().number(10).stateRoot(Hash.ZERO).buildHeader();
      lenient().when(chain.getChainHeadHeader()).thenReturn(head);
      lenient()
          .when(archive.isWorldStateAvailable(head.getStateRoot(), head.getHash()))
          .thenReturn(true);
    }

    private JsonRpcResponse responseForRange(final long fromBlock, final long toBlock) {
      final FilterParameter filterParameter =
          new FilterParameter(
              new BlockParameter(fromBlock),
              new BlockParameter(toBlock),
              null,
              null,
              null,
              null,
              null,
              null,
              null);
      return filterMethod.response(
          new JsonRpcRequestContext(
              new JsonRpcRequest("2.0", "trace_filter", new Object[] {filterParameter})));
    }

    private JsonRpcResponse responseForBlockHash(final Hash blockHash) {
      return filterMethod.response(
          new JsonRpcRequestContext(
              new JsonRpcRequest(
                  "2.0", "trace_filter", new Object[] {blockHashFilter(blockHash, 10)})));
    }

    private JsonNode successResult(final JsonRpcResponse response) {
      assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
      final Object result = ((JsonRpcSuccessResponse) response).getResult();
      assertThat(result).isInstanceOf(ArrayNode.class);
      return MAPPER.valueToTree(result);
    }

    private MutableWorldState newState() {
      final MutableWorldState state = createInMemoryWorldState();
      final WorldUpdater updater = state.updater();
      updater.getOrCreate(sender).setBalance(Wei.of(1_000_000_000));
      updater.commit();
      return state;
    }

    private Transaction transaction(final int nonce) {
      return new TransactionTestFixture()
          .sender(sender)
          .nonce(nonce)
          .to(Optional.of(RECIPIENT))
          .gasLimit(100_000)
          .gasPrice(Wei.of(2))
          .value(Wei.of(7))
          .chainId(Optional.of(BigInteger.valueOf(20211)))
          .createTransaction(keys);
    }

    private void setBlock(final List<Transaction> transactions) {
      final BlockHeader header =
          new BlockHeaderTestFixture()
              .parentHash(parent.getHash())
              .number(1)
              .timestamp(2)
              .gasLimit(30_000_000)
              .coinbase(BENEFICIARY)
              .baseFeePerGas(Wei.ONE)
              .parentBeaconBlockRoot(Optional.of(Bytes32.fromHexString("0x1234")))
              .excessBlobGas(BlobGas.ZERO)
              .blobGasUsed(0L)
              .buildHeader();
      block = new Block(header, new BlockBody(transactions, List.of()));
      lenient().when(chain.getBlockByNumber(1)).thenReturn(Optional.of(block));
      lenient().when(chain.getBlockHeader(block.getHash())).thenReturn(Optional.of(header));
      lenient().when(chain.blockIsOnCanonicalChain(block.getHash())).thenReturn(true);
      lenient().when(chain.getBlockBody(block.getHash())).thenReturn(Optional.of(block.getBody()));
    }
  }
}
