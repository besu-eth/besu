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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionPoolResult;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TxPoolStatusTest {

  @Mock private TransactionPool transactionPool;
  private TxPoolStatus method;
  private static final String JSON_RPC_VERSION = "2.0";
  private static final String TXPOOL_STATUS_METHOD = "txpool_status";

  @BeforeEach
  public void setUp() {
    method = new TxPoolStatus(transactionPool);
  }

  @Test
  public void returnsCorrectMethodName() {
    assertThat(method.getName()).isEqualTo(TXPOOL_STATUS_METHOD);
  }

  @Test
  public void shouldReturnZeroCountsWhenPoolIsEmpty() {
    when(transactionPool.getStatus()).thenReturn(new PendingTransactions.Status(0, 0));

    final TransactionPoolResult<Long> result = invokeMethod();

    assertThat(result.getPending()).isEqualTo(0L);
    assertThat(result.getQueued()).isEqualTo(0L);
  }

  @Test
  public void shouldReturnCorrectCountsWithPendingAndQueuedTransactions() {
    when(transactionPool.getStatus()).thenReturn(new PendingTransactions.Status(10, 7));

    final TransactionPoolResult<Long> result = invokeMethod();

    assertThat(result.getPending()).isEqualTo(10L);
    assertThat(result.getQueued()).isEqualTo(7L);
  }

  @Test
  public void shouldReturnCorrectCountsWithOnlyPendingTransactions() {
    when(transactionPool.getStatus()).thenReturn(new PendingTransactions.Status(5, 0));

    final TransactionPoolResult<Long> result = invokeMethod();

    assertThat(result.getPending()).isEqualTo(5L);
    assertThat(result.getQueued()).isEqualTo(0L);
  }

  @Test
  public void shouldReturnCorrectCountsWithOnlyQueuedTransactions() {
    when(transactionPool.getStatus()).thenReturn(new PendingTransactions.Status(0, 3));

    final TransactionPoolResult<Long> result = invokeMethod();

    assertThat(result.getPending()).isEqualTo(0L);
    assertThat(result.getQueued()).isEqualTo(3L);
  }

  @Test
  public void shouldReturnLargeValues() {
    when(transactionPool.getStatus()).thenReturn(new PendingTransactions.Status(256, 4096));

    final TransactionPoolResult<Long> result = invokeMethod();

    assertThat(result.getPending()).isEqualTo(256L);
    assertThat(result.getQueued()).isEqualTo(4096L);
  }

  @SuppressWarnings("unchecked")
  private TransactionPoolResult<Long> invokeMethod() {
    final JsonRpcSuccessResponse response =
        (JsonRpcSuccessResponse) method.response(buildRequest());
    return (TransactionPoolResult<Long>) response.getResult();
  }

  private JsonRpcRequestContext buildRequest() {
    return new JsonRpcRequestContext(
        new JsonRpcRequest(JSON_RPC_VERSION, TXPOOL_STATUS_METHOD, new Object[] {}));
  }
}
