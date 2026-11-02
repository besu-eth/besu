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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
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
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.util.Optional;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.node.ArrayNode;
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

  private static final BlockHeader BLOCK_HEADER =
      new BlockHeaderTestFixture().number(19).buildHeader();
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
  public void shouldReturnWorldStateUnavailableForBlockHashWithoutParentState(final int count) {
    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getBlockHeader(BLOCK_HEADER.getHash())).thenReturn(Optional.of(BLOCK_HEADER));
    when(blockchain.blockIsOnCanonicalChain(BLOCK_HEADER.getHash())).thenReturn(true);
    when(blockchain.getBlockBody(BLOCK_HEADER.getHash()))
        .thenReturn(Optional.of(BlockBody.empty()));
    when(blockchainQueries.getAndMapWorldState(any(Hash.class), any()))
        .thenReturn(Optional.empty());

    assertError(
        traceFilter(blockHashFilter(BLOCK_HEADER.getHash(), count)),
        RpcErrorType.WORLD_STATE_UNAVAILABLE);
    // the resolved block is traced on its own parent state, never looked up again by number
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
}
