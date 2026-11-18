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
import org.hyperledger.besu.ethereum.api.query.cache.TransactionLogBloomCacher.CachingStatus;
import org.hyperledger.besu.ethereum.chain.Blockchain;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class AdminLogsRepairCacheTest {
  @Mock private BlockchainQueries blockchainQueries;
  @Mock private Blockchain blockchain;
  @Mock private TransactionLogBloomCacher transactionLogBloomCacher;
  @Mock private CachingStatus cachingStatus;

  private AdminLogsRepairCache adminLogsRepairCache;

  @BeforeEach
  public void setup() {
    adminLogsRepairCache = new AdminLogsRepairCache(blockchainQueries);
  }

  @Test
  public void requestStartedTest() {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", "admin_logsRepairCache", new String[] {}));
    final Map<String, String> response = new HashMap<>();
    response.put("Status", "Started");
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(request.getRequest().getId(), response);

    when(blockchainQueries.getTransactionLogBloomCacher())
        .thenReturn(Optional.of(transactionLogBloomCacher));
    when(transactionLogBloomCacher.getCachingStatus()).thenReturn(cachingStatus);
    when(cachingStatus.isCaching()).thenReturn(false);

    final JsonRpcResponse actualResponse = adminLogsRepairCache.response(request);

    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void requestAlreadyRunningTest() {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", "admin_logsRepairCache", new String[] {}));
    final Map<String, String> response = new HashMap<>();
    response.put("Status", "Already running");
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(request.getRequest().getId(), response);

    when(blockchainQueries.getTransactionLogBloomCacher())
        .thenReturn(Optional.of(transactionLogBloomCacher));
    when(transactionLogBloomCacher.getCachingStatus()).thenReturn(cachingStatus);
    when(cachingStatus.isCaching()).thenReturn(true);

    final JsonRpcResponse actualResponse = adminLogsRepairCache.response(request);

    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void requestBlockNumberNotFoundTest() {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", "admin_logsRepairCache", new String[] {"123456789"}));
    final JsonRpcResponse expectedResponse =
        new JsonRpcErrorResponse(request.getRequest().getId(), RpcErrorType.UNKNOWN_BLOCK);

    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getBlockByNumber(anyLong())).thenReturn(Optional.empty());

    assertThat(adminLogsRepairCache.response(request))
        .usingRecursiveComparison()
        .isEqualTo(expectedResponse);
  }
}
