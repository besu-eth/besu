/*
 * Copyright ConsenSys AG.
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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.cache.TransactionLogBloomCacher;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class AdminLogsRemoveCacheTest {
  @Mock private BlockchainQueries blockchainQueries;
  @Mock private Blockchain blockchain;
  @Mock private TransactionLogBloomCacher transactionLogBloomCacher;
  @Mock private Block block;
  @Captor private ArgumentCaptor<Long> fromBlock;
  @Captor private ArgumentCaptor<Long> toBlock;

  private AdminLogsRemoveCache adminLogsRemoveCache;

  @BeforeEach
  public void setup() {
    adminLogsRemoveCache = new AdminLogsRemoveCache(blockchainQueries);
  }

  @Test
  public void testParameterized() {
    long blockNumber = 1000L;
    when(blockchainQueries.headBlockNumber()).thenReturn(blockNumber);

    final Object[][] testVector = {
      {new String[] {}, 0L, blockNumber},
      {new String[] {"earliest"}, 0L, 0L},
      {new String[] {"latest"}, blockNumber, blockNumber},
      {new String[] {"pending"}, blockNumber, blockNumber},
      {new String[] {"0x50"}, 0x50L, 0x50L},
      {new String[] {"earliest", "earliest"}, 0L, 0L},
      {new String[] {"earliest", "latest"}, 0L, blockNumber},
      {new String[] {"latest", "latest"}, blockNumber, blockNumber},
      {new String[] {"pending", "latest"}, blockNumber, blockNumber},
      {new String[] {"0x50", "latest"}, 0x50L, blockNumber},
      {new String[] {"earliest", "pending"}, 0L, blockNumber},
      {new String[] {"latest", "pending"}, blockNumber, blockNumber},
      {new String[] {"pending", "pending"}, blockNumber, blockNumber},
      {new String[] {"0x50", "pending"}, 0x50L, blockNumber},
      {new String[] {"earliest", "0x100"}, 0L, 0x100L},
      {new String[] {"0x50", "0x100"}, 0x50L, 0x100L},
    };

    for (final Object[] test : testVector) {
      System.out.println("Vector - " + List.of((String[]) test[0]));
      testMockedResult((String[]) test[0], (Long) test[1], (Long) test[2]);
    }
  }

  private void testMockedResult(
      final String[] args, final long expectedFromBlock, final long expectedToBlock) {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", "admin_generateLogBloomCache", args));
    final Map<String, String> response = new HashMap<>();
    response.put("Status", "Cache Removed");
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(request.getRequest().getId(), response);

    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getBlockByNumber(anyLong())).thenReturn(Optional.of(block));
    when(blockchainQueries.getTransactionLogBloomCacher())
        .thenReturn(Optional.of(transactionLogBloomCacher));
    when(transactionLogBloomCacher.removeSegments(fromBlock.capture(), toBlock.capture()))
        .thenReturn(true);

    final JsonRpcResponse actualResponse = adminLogsRemoveCache.response(request);

    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(expectedResponse);
    assertThat(fromBlock.getValue()).isEqualTo(expectedFromBlock);
    assertThat(toBlock.getValue()).isEqualTo(expectedToBlock);
  }

  @Test
  public void safeAndFinalizedTagsResolveToTheirBlockNumbers() {
    final BlockHeader safeHeader = new BlockHeaderTestFixture().number(900L).buildHeader();
    final BlockHeader finalizedHeader = new BlockHeaderTestFixture().number(800L).buildHeader();
    when(blockchainQueries.safeBlockHeader()).thenReturn(Optional.of(safeHeader));
    when(blockchainQueries.finalizedBlockHeader()).thenReturn(Optional.of(finalizedHeader));
    when(blockchainQueries.getTransactionLogBloomCacher())
        .thenReturn(Optional.of(transactionLogBloomCacher));
    when(transactionLogBloomCacher.removeSegments(fromBlock.capture(), toBlock.capture()))
        .thenReturn(true);

    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", "admin_logsRemoveCache", new String[] {"finalized", "safe"}));
    adminLogsRemoveCache.response(request);

    assertThat(fromBlock.getValue()).isEqualTo(800L);
    assertThat(toBlock.getValue()).isEqualTo(900L);
  }

  @ParameterizedTest
  @ValueSource(strings = {"safe", "finalized"})
  public void unknownSafeOrFinalizedStartBlockReturnsUnknownBlock(final String tag) {
    stubUnknownSafeOrFinalized(tag);

    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", "admin_logsRemoveCache", new String[] {tag}));
    final JsonRpcResponse expectedResponse =
        new JsonRpcErrorResponse(request.getRequest().getId(), RpcErrorType.UNKNOWN_BLOCK);

    assertThat(adminLogsRemoveCache.response(request))
        .usingRecursiveComparison()
        .isEqualTo(expectedResponse);
  }

  @ParameterizedTest
  @ValueSource(strings = {"safe", "finalized"})
  public void unknownSafeOrFinalizedStopBlockReturnsUnknownBlock(final String tag) {
    stubUnknownSafeOrFinalized(tag);

    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", "admin_logsRemoveCache", new String[] {"earliest", tag}));
    final JsonRpcResponse expectedResponse =
        new JsonRpcErrorResponse(request.getRequest().getId(), RpcErrorType.UNKNOWN_BLOCK);

    assertThat(adminLogsRemoveCache.response(request))
        .usingRecursiveComparison()
        .isEqualTo(expectedResponse);
  }

  private void stubUnknownSafeOrFinalized(final String tag) {
    if ("finalized".equals(tag)) {
      when(blockchainQueries.finalizedBlockHeader()).thenReturn(Optional.empty());
    } else {
      when(blockchainQueries.safeBlockHeader()).thenReturn(Optional.empty());
    }
  }

  @Test
  public void requestCacheRemovedTest() {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", "admin_logsRemoveCache", new String[] {}));
    final Map<String, String> response = new HashMap<>();
    response.put("Status", "Cache Removed");
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(request.getRequest().getId(), response);

    when(blockchainQueries.getTransactionLogBloomCacher())
        .thenReturn(Optional.of(transactionLogBloomCacher));
    when(blockchainQueries.headBlockNumber()).thenReturn(1000L);
    when(transactionLogBloomCacher.removeSegments(anyLong(), anyLong())).thenReturn(true);

    final JsonRpcResponse actualResponse = adminLogsRemoveCache.response(request);

    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void requestCacheRemovalSkippedWhileCachingTest() {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", "admin_logsRemoveCache", new String[] {}));
    final JsonRpcResponse expectedResponse =
        new JsonRpcErrorResponse(
            request.getRequest().getId(), RpcErrorType.CACHE_REMOVAL_IN_PROGRESS);

    when(blockchainQueries.getTransactionLogBloomCacher())
        .thenReturn(Optional.of(transactionLogBloomCacher));
    when(blockchainQueries.headBlockNumber()).thenReturn(1000L);
    when(transactionLogBloomCacher.removeSegments(anyLong(), anyLong())).thenReturn(false);

    final JsonRpcResponse actualResponse = adminLogsRemoveCache.response(request);

    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void requestCacheRemovalFailedTest() {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", "admin_logsRemoveCache", new String[] {}));
    final JsonRpcResponse expectedResponse =
        new JsonRpcErrorResponse(request.getRequest().getId(), RpcErrorType.CACHE_REMOVAL_FAILED);

    when(blockchainQueries.getTransactionLogBloomCacher())
        .thenReturn(Optional.of(transactionLogBloomCacher));
    when(blockchainQueries.headBlockNumber()).thenReturn(1000L);
    when(transactionLogBloomCacher.removeSegments(anyLong(), anyLong()))
        .thenThrow(new UncheckedIOException(new IOException("Deletion failed")));

    assertThat(adminLogsRemoveCache.response(request))
        .usingRecursiveComparison()
        .isEqualTo(expectedResponse);
  }

  @Test
  public void requestBlockNumberNotFoundTest() {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", "admin_logsRemoveCache", new String[] {"0x75bcd15"}));
    final JsonRpcResponse expectedResponse =
        new JsonRpcErrorResponse(request.getRequest().getId(), RpcErrorType.UNKNOWN_BLOCK);

    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getBlockByNumber(anyLong())).thenReturn(Optional.empty());

    assertThat(adminLogsRemoveCache.response(request))
        .usingRecursiveComparison()
        .isEqualTo(expectedResponse);
  }

  @Test
  public void requestStopBlockNumberNotFoundTest() {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", "admin_logsRemoveCache", new String[] {"0x1", "0x75bcd15"}));
    final JsonRpcResponse expectedResponse =
        new JsonRpcErrorResponse(request.getRequest().getId(), RpcErrorType.UNKNOWN_BLOCK);

    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getBlockByNumber(1L)).thenReturn(Optional.of(block));
    when(blockchain.getBlockByNumber(0x75bcd15L)).thenReturn(Optional.empty());

    assertThat(adminLogsRemoveCache.response(request))
        .usingRecursiveComparison()
        .isEqualTo(expectedResponse);
  }

  @Test
  public void requestBlockRangeInvalidTest() {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", "admin_logsRemoveCache", new String[] {"0x20", "0x1"}));
    final JsonRpcResponse expectedResponse =
        new JsonRpcErrorResponse(
            request.getRequest().getId(), RpcErrorType.INVALID_BLOCK_NUMBER_PARAMS);

    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getBlockByNumber(anyLong())).thenReturn(Optional.of(block));

    final JsonRpcResponse actualResponse = adminLogsRemoveCache.response(request);

    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(expectedResponse);
  }
}
