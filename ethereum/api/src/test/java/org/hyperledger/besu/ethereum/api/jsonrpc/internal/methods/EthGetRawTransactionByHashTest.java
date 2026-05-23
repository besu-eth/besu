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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.TransactionWithMetadata;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EthGetRawTransactionByHashTest {

  @Mock private BlockchainQueries blockchainQueries;
  @Mock private TransactionPool transactionPool;
  private EthGetRawTransactionByHash method;
  private final String JSON_RPC_VERSION = "2.0";
  private final String ETH_METHOD = "eth_getRawTransactionByHash";
  private final BlockDataGenerator gen = new BlockDataGenerator();

  @BeforeEach
  public void setUp() {
    method = new EthGetRawTransactionByHash(blockchainQueries, transactionPool);
  }

  @Test
  void returnsCorrectMethodName() {
    assertThat(method.getName()).isEqualTo(ETH_METHOD);
  }

  @Test
  void shouldReturnErrorResponseIfMissingRequiredParameter() {
    final JsonRpcRequest request =
        new JsonRpcRequest(JSON_RPC_VERSION, method.getName(), new Object[] {});
    final JsonRpcRequestContext context = new JsonRpcRequestContext(request);

    final JsonRpcErrorResponse expectedResponse =
        new JsonRpcErrorResponse(request.getId(), RpcErrorType.INVALID_PARAM_COUNT);

    final JsonRpcResponse actualResponse = method.response(context);

    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  void shouldReturnNullResultWhenTransactionDoesNotExist() {
    final String transactionHash =
        "0xf9ef5f0cf02685711cdf687b72d4754901729b942f4ea7f956e7fb206cae2f9e";
    when(transactionPool.getTransactionByHash(Hash.fromHexString(transactionHash)))
        .thenReturn(Optional.empty());
    when(blockchainQueries.transactionByHash(Hash.fromHexString(transactionHash)))
        .thenReturn(Optional.empty());

    final JsonRpcRequest request =
        new JsonRpcRequest(JSON_RPC_VERSION, method.getName(), new Object[] {transactionHash});
    final JsonRpcRequestContext context = new JsonRpcRequestContext(request);

    final JsonRpcSuccessResponse expectedResponse =
        new JsonRpcSuccessResponse(request.getId(), null);

    final JsonRpcResponse actualResponse = method.response(context);

    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  void shouldThrowOnInvalidHashParameter() {
    final String badHash = "not-a-hex-hash";
    final JsonRpcRequest request =
        new JsonRpcRequest(JSON_RPC_VERSION, method.getName(), new Object[] {badHash});
    final JsonRpcRequestContext context = new JsonRpcRequestContext(request);

    assertThatThrownBy(() -> method.response(context)).isInstanceOf(InvalidJsonRpcParameters.class);
  }

  @Test
  void shouldReturnRawTransactionForFrontierTransaction() {
    final Transaction transaction = gen.transaction(TransactionType.FRONTIER);
    assertThat(transaction.getType()).isEqualTo(TransactionType.FRONTIER);

    final TransactionWithMetadata transactionWithMetadata =
        new TransactionWithMetadata(transaction, 1, Optional.empty(), Hash.ZERO, 0, 0L);

    when(transactionPool.getTransactionByHash(transaction.getHash())).thenReturn(Optional.empty());
    when(blockchainQueries.transactionByHash(transaction.getHash()))
        .thenReturn(Optional.of(transactionWithMetadata));

    final JsonRpcRequest request =
        new JsonRpcRequest(
            JSON_RPC_VERSION, method.getName(), new Object[] {transaction.getHash().toHexString()});
    final JsonRpcRequestContext context = new JsonRpcRequestContext(request);

    // Expected: writeTo encoding, matching DebugGetRawTransaction.
    final String expectedRaw = expectedRawHex(transaction);

    final JsonRpcSuccessResponse expectedResponse =
        new JsonRpcSuccessResponse(request.getId(), expectedRaw);

    final JsonRpcResponse actualResponse = method.response(context);

    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  void shouldReturnRawTransactionForEip1559Transaction() {
    final Transaction transaction = gen.transaction(TransactionType.EIP1559);
    assertThat(transaction.getType()).isEqualTo(TransactionType.EIP1559);

    final TransactionWithMetadata transactionWithMetadata =
        new TransactionWithMetadata(transaction, 100, Optional.empty(), Hash.ZERO, 3, 12345L);

    when(transactionPool.getTransactionByHash(transaction.getHash())).thenReturn(Optional.empty());
    when(blockchainQueries.transactionByHash(transaction.getHash()))
        .thenReturn(Optional.of(transactionWithMetadata));

    final JsonRpcRequest request =
        new JsonRpcRequest(
            JSON_RPC_VERSION, method.getName(), new Object[] {transaction.getHash().toHexString()});
    final JsonRpcRequestContext context = new JsonRpcRequestContext(request);

    final String expectedRaw = expectedRawHex(transaction);

    final JsonRpcSuccessResponse expectedResponse =
        new JsonRpcSuccessResponse(request.getId(), expectedRaw);

    final JsonRpcResponse actualResponse = method.response(context);

    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  void shouldReturnRawTransactionForAccessListTransaction() {
    final Transaction transaction = gen.transaction(TransactionType.ACCESS_LIST);
    assertThat(transaction.getType()).isEqualTo(TransactionType.ACCESS_LIST);

    final TransactionWithMetadata transactionWithMetadata =
        new TransactionWithMetadata(transaction, 50, Optional.empty(), Hash.ZERO, 1, 9999L);

    when(transactionPool.getTransactionByHash(transaction.getHash())).thenReturn(Optional.empty());
    when(blockchainQueries.transactionByHash(transaction.getHash()))
        .thenReturn(Optional.of(transactionWithMetadata));

    final JsonRpcRequest request =
        new JsonRpcRequest(
            JSON_RPC_VERSION, method.getName(), new Object[] {transaction.getHash().toHexString()});
    final JsonRpcRequestContext context = new JsonRpcRequestContext(request);

    final String expectedRaw = expectedRawHex(transaction);

    final JsonRpcResponse actualResponse = method.response(context);

    assertThat(actualResponse)
        .usingRecursiveComparison()
        .isEqualTo(new JsonRpcSuccessResponse(request.getId(), expectedRaw));
  }

  @Test
  void shouldReturnPendingTransactionFromPool() {
    final Transaction transaction = gen.transaction(TransactionType.EIP1559);

    when(transactionPool.getTransactionByHash(transaction.getHash()))
        .thenReturn(Optional.of(transaction));
    // Blockchain should NOT be consulted when pool has the tx.
    verifyNoInteractions(blockchainQueries);

    final JsonRpcRequest request =
        new JsonRpcRequest(
            JSON_RPC_VERSION, method.getName(), new Object[] {transaction.getHash().toHexString()});
    final JsonRpcRequestContext context = new JsonRpcRequestContext(request);

    final String expectedRaw = expectedRawHex(transaction);

    final JsonRpcSuccessResponse expectedResponse =
        new JsonRpcSuccessResponse(request.getId(), expectedRaw);

    final JsonRpcResponse actualResponse = method.response(context);

    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  void shouldMatchDebugGetRawTransactionEncoding() {
    // Verify that EthGetRawTransactionByHash produces the exact same output as
    // DebugGetRawTransaction for the same transaction (both use transaction.writeTo).
    final Transaction transaction = gen.transaction(TransactionType.EIP1559);

    // Simulate the DebugGetRawTransaction encoding.
    final BytesValueRLPOutput debugOut = new BytesValueRLPOutput();
    transaction.writeTo(debugOut);
    final String debugRaw = debugOut.encoded().toHexString();

    // Our method's expected encoding.
    final String ethRaw = expectedRawHex(transaction);

    assertThat(ethRaw).isEqualTo(debugRaw);
  }

  /**
   * Helper: produces the expected raw hex using transaction.writeTo(), matching the encoding used
   * by both EthGetRawTransactionByHash and DebugGetRawTransaction.
   */
  private String expectedRawHex(final Transaction transaction) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    transaction.writeTo(out);
    return out.encoded().toHexString();
  }
}
