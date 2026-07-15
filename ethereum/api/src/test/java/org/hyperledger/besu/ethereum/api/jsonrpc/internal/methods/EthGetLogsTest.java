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
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.FilterParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.ArrayList;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EthGetLogsTest {

  private EthGetLogs method;

  @Mock BlockchainQueries blockchainQueries;
  long maxLogRange;

  @BeforeEach
  public void setUp() {
    method = new EthGetLogs(blockchainQueries, maxLogRange);
  }

  @Test
  public void shouldReturnCorrectMethodName() {
    assertThat(method.getName()).isEqualTo("eth_getLogs");
  }

  @Test
  public void shouldReturnErrorWhenMissingParams() {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", "eth_getLogs", new Object[] {}));

    final Throwable thrown = catchThrowable(() -> method.response(request));
    assertThat(thrown)
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .hasMessage("Invalid filter parameter (index 0)");

    verifyNoInteractions(blockchainQueries);
  }

  @Test
  public void shouldQueryFinalizedBlock() {
    final long blockNumber = 7L;
    final JsonRpcRequestContext request = buildRequest("finalized", "finalized");

    final BlockHeader blockHeader = mock(BlockHeader.class);

    when(blockchainQueries.finalizedBlockHeader()).thenReturn(Optional.of(blockHeader));
    when(blockHeader.getNumber()).thenReturn(blockNumber);
    when(blockchainQueries.headBlockNumber()).thenReturn(100L);
    when(blockchainQueries.matchingLogs(anyLong(), anyLong(), any(), any()))
        .thenReturn(new ArrayList<>());

    final JsonRpcResponse response = method.response(request);
    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);

    verify(blockchainQueries).matchingLogs(eq(blockNumber), eq(blockNumber), any(), any());
    verify(blockchainQueries, times(1)).headBlockNumber();
    verify(blockchainQueries, never()).safeBlockHeader();
  }

  @Test
  public void shouldQueryLatestBlock() {
    final long latestBlockNumber = 7L;
    final JsonRpcRequestContext request = buildRequest("latest", "latest");

    when(blockchainQueries.headBlockNumber()).thenReturn(latestBlockNumber);
    when(blockchainQueries.matchingLogs(anyLong(), anyLong(), any(), any()))
        .thenReturn(new ArrayList<>());

    final JsonRpcResponse response = method.response(request);
    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);

    verify(blockchainQueries)
        .matchingLogs(eq(latestBlockNumber), eq(latestBlockNumber), any(), any());
    verify(blockchainQueries, never()).finalizedBlockHeader();
    verify(blockchainQueries, never()).safeBlockHeader();
  }

  @Test
  public void shouldQuerySafeBlock() {
    final long blockNumber = 7L;
    final JsonRpcRequestContext request = buildRequest("safe", "safe");

    final BlockHeader blockHeader = mock(BlockHeader.class);

    when(blockchainQueries.safeBlockHeader()).thenReturn(Optional.of(blockHeader));
    when(blockHeader.getNumber()).thenReturn(blockNumber);
    when(blockchainQueries.headBlockNumber()).thenReturn(100L);
    when(blockchainQueries.matchingLogs(anyLong(), anyLong(), any(), any()))
        .thenReturn(new ArrayList<>());

    final JsonRpcResponse response = method.response(request);
    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);

    verify(blockchainQueries).matchingLogs(eq(blockNumber), eq(blockNumber), any(), any());
    verify(blockchainQueries, never()).finalizedBlockHeader();
    verify(blockchainQueries, times(1)).headBlockNumber();
  }

  @Test
  public void shouldQueryNumericBlock() {
    final long fromBlock = 3L;
    final long toBlock = 10L;
    final JsonRpcRequestContext request = buildRequest(fromBlock, toBlock);

    when(blockchainQueries.headBlockNumber()).thenReturn(100L);
    when(blockchainQueries.matchingLogs(anyLong(), anyLong(), any(), any()))
        .thenReturn(new ArrayList<>());

    final JsonRpcResponse response = method.response(request);
    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);

    verify(blockchainQueries).matchingLogs(eq(fromBlock), eq(toBlock), any(), any());

    verify(blockchainQueries, never()).finalizedBlockHeader();
    verify(blockchainQueries, times(1)).headBlockNumber();
    verify(blockchainQueries, never()).safeBlockHeader();
  }

  @Test
  public void shouldQueryEarliestToNumeric() {
    final long genesisBlock = 0L;
    final long latestBlock = 50L;
    final JsonRpcRequestContext request = buildRequest("earliest", latestBlock);

    when(blockchainQueries.headBlockNumber()).thenReturn(100L);
    when(blockchainQueries.matchingLogs(anyLong(), anyLong(), any(), any()))
        .thenReturn(new ArrayList<>());

    final JsonRpcResponse response = method.response(request);
    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);

    verify(blockchainQueries).matchingLogs(eq(genesisBlock), eq(latestBlock), any(), any());
    verify(blockchainQueries, never()).finalizedBlockHeader();
    verify(blockchainQueries, never()).safeBlockHeader();
  }

  @Test
  public void shouldQueryWrappedNumeric() {
    final long fromBlock = 50L;
    final long toBlock = 100L;
    final JsonRpcRequestContext request =
        buildRequest("0x" + Long.toHexString(fromBlock), "0x" + Long.toHexString(toBlock));

    when(blockchainQueries.headBlockNumber()).thenReturn(150L);
    when(blockchainQueries.matchingLogs(anyLong(), anyLong(), any(), any()))
        .thenReturn(new ArrayList<>());

    final JsonRpcResponse response = method.response(request);
    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);

    verify(blockchainQueries).matchingLogs(eq(fromBlock), eq(toBlock), any(), any());
    verify(blockchainQueries, never()).finalizedBlockHeader();
    verify(blockchainQueries, times(1)).headBlockNumber();
    verify(blockchainQueries, never()).safeBlockHeader();
  }

  @Test
  public void shouldQueryWrappedNumericToNumeric() {
    final long fromBlock = 50L;
    final long toBlock = 100L;
    final JsonRpcRequestContext request = buildRequest("0x" + Long.toHexString(fromBlock), toBlock);

    when(blockchainQueries.headBlockNumber()).thenReturn(150L);
    when(blockchainQueries.matchingLogs(anyLong(), anyLong(), any(), any()))
        .thenReturn(new ArrayList<>());

    final JsonRpcResponse response = method.response(request);
    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);

    verify(blockchainQueries).matchingLogs(eq(fromBlock), eq(toBlock), any(), any());
    verify(blockchainQueries, never()).finalizedBlockHeader();
    verify(blockchainQueries, times(1)).headBlockNumber();
    verify(blockchainQueries, never()).safeBlockHeader();
  }

  @Test
  public void shouldQueryEarliestToLatest() {
    final long genesisBlock = 0L;
    final long latestBlock = 50L;
    final JsonRpcRequestContext request = buildRequest("earliest", "latest");

    when(blockchainQueries.headBlockNumber()).thenReturn(latestBlock);
    when(blockchainQueries.matchingLogs(anyLong(), anyLong(), any(), any()))
        .thenReturn(new ArrayList<>());

    final JsonRpcResponse response = method.response(request);
    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);

    verify(blockchainQueries).matchingLogs(eq(genesisBlock), eq(latestBlock), any(), any());
    verify(blockchainQueries, never()).finalizedBlockHeader();
    verify(blockchainQueries, never()).safeBlockHeader();
  }

  @Test
  public void shouldQuerySafeToFinalized() {
    final long finalizedBlockNumber = 50L;
    final long safeBlockNumber = 25L;
    final JsonRpcRequestContext request = buildRequest("safe", "finalized");

    final BlockHeader finalizedBlockHeader = mock(BlockHeader.class);
    final BlockHeader safeBlockHeader = mock(BlockHeader.class);

    when(blockchainQueries.finalizedBlockHeader()).thenReturn(Optional.of(finalizedBlockHeader));
    when(blockchainQueries.safeBlockHeader()).thenReturn(Optional.of(safeBlockHeader));
    when(finalizedBlockHeader.getNumber()).thenReturn(finalizedBlockNumber);
    when(safeBlockHeader.getNumber()).thenReturn(safeBlockNumber);
    when(blockchainQueries.headBlockNumber())
        .thenReturn(100L); // Latest block higher than finalized
    when(blockchainQueries.matchingLogs(anyLong(), anyLong(), any(), any()))
        .thenReturn(new ArrayList<>());

    final JsonRpcResponse response = method.response(request);
    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);

    verify(blockchainQueries)
        .matchingLogs(eq(safeBlockNumber), eq(finalizedBlockNumber), any(), any());
    verify(blockchainQueries, times(1)).finalizedBlockHeader();
    verify(blockchainQueries, times(1)).safeBlockHeader();
    verify(blockchainQueries, times(1)).headBlockNumber();
  }

  @Test
  public void shouldFailIfNoSafeBlock() {
    final JsonRpcRequestContext request = buildRequest("safe", "finalized");

    when(blockchainQueries.safeBlockHeader()).thenReturn(Optional.empty());

    final JsonRpcResponse response = method.response(request);
    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
  }

  @Test
  public void shouldFailIfNoFinalizedBlock() {
    final JsonRpcRequestContext request = buildRequest("finalized", "safe");

    when(blockchainQueries.finalizedBlockHeader()).thenReturn(Optional.empty());

    final JsonRpcResponse response = method.response(request);
    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
  }

  @Test
  public void shouldFailIfParamsExceedMaxRange() {
    final JsonRpcRequestContext request = buildRequest(0, 50);
    maxLogRange = 20L;
    method = new EthGetLogs(blockchainQueries, maxLogRange);
    when(blockchainQueries.headBlockNumber()).thenReturn(100L);
    final JsonRpcResponse response = method.response(request);
    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    final JsonRpcErrorResponse errorResponse = (JsonRpcErrorResponse) response;
    assertThat(errorResponse.getErrorType()).isEqualTo(RpcErrorType.EXCEEDS_RPC_MAX_BLOCK_RANGE);
  }

  @Test
  public void shouldFailIfFromBlockGreaterThanToBlock() {
    final JsonRpcRequestContext request = buildRequest(10, 9);
    final JsonRpcResponse response = method.response(request);
    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    final JsonRpcErrorResponse errorResponse = (JsonRpcErrorResponse) response;
    assertThat(errorResponse.getErrorType()).isEqualTo(RpcErrorType.INVALID_PARAMS);
  }

  @Test
  public void shouldFailIfToBlockGreaterThanLatestBlock() {
    final long latestBlockNumber = 100L;
    final long toBlock = 150L; // Greater than latest
    when(blockchainQueries.headBlockNumber()).thenReturn(latestBlockNumber);

    final JsonRpcRequestContext request = buildRequest(50, toBlock);
    final JsonRpcResponse response = method.response(request);
    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    final JsonRpcErrorResponse errorResponse = (JsonRpcErrorResponse) response;
    assertThat(errorResponse.getErrorType()).isEqualTo(RpcErrorType.INVALID_PARAMS);
  }

  @Test
  public void shouldFailIfBlockHashNotFound() {
    final org.hyperledger.besu.datatypes.Hash mockHash =
        org.hyperledger.besu.datatypes.Hash.fromHexString(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");

    final FilterParameter filterParameter =
        new FilterParameter(null, null, null, null, null, null, mockHash, null, null);

    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", "eth_getLogs", new Object[] {filterParameter}));

    when(blockchainQueries.getBlockHeaderByHash(mockHash)).thenReturn(Optional.empty());

    final Throwable thrown = catchThrowable(() -> method.response(request));

    assertThat(thrown).isInstanceOf(InvalidJsonRpcParameters.class).hasMessage("Block not found");

    final InvalidJsonRpcParameters ex = (InvalidJsonRpcParameters) thrown;
    assertThat(ex.getRpcErrorType()).isEqualTo(RpcErrorType.BLOCK_NOT_FOUND);

    verify(blockchainQueries).getBlockHeaderByHash(mockHash);
  }

  private JsonRpcRequestContext buildRequest(final long fromBlock, final long toBlock) {
    final FilterParameter filterParameter =
        buildFilterParameter(new BlockParameter(fromBlock), new BlockParameter(toBlock));
    return new JsonRpcRequestContext(
        new JsonRpcRequest("2.0", "eth_getLogs", new Object[] {filterParameter}));
  }

  private JsonRpcRequestContext buildRequest(final String fromBlock, final long toBlock) {
    final FilterParameter filterParameter =
        buildFilterParameter(new BlockParameter(fromBlock), new BlockParameter(toBlock));
    return new JsonRpcRequestContext(
        new JsonRpcRequest("2.0", "eth_getLogs", new Object[] {filterParameter}));
  }

  private JsonRpcRequestContext buildRequest(final String fromBlock, final String toBlock) {
    final FilterParameter filterParameter =
        buildFilterParameter(new BlockParameter(fromBlock), new BlockParameter(toBlock));
    return new JsonRpcRequestContext(
        new JsonRpcRequest("2.0", "eth_getLogs", new Object[] {filterParameter}));
  }

  private FilterParameter buildFilterParameter(
      final BlockParameter fromBlock, final BlockParameter toBlock) {
    return new FilterParameter(fromBlock, toBlock, null, null, null, null, null, null, null);
  }
}
