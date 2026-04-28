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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.Quantity;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionBaseResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionWithMetadataResult;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.TransactionWithMetadata;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.SenderPendingTransactionsData;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EthGetTransactionBySenderAndNonceTest {

  private static final String JSON_RPC_VERSION = "2.0";
  private static final String ETH_METHOD = "eth_getTransactionBySenderAndNonce";

  @Mock private BlockchainQueries blockchainQueries;
  @Mock private TransactionPool transactionPool;
  @Mock private Blockchain blockchain;

  private EthGetTransactionBySenderAndNonce method;

  @BeforeEach
  void setUp() {
    method = new EthGetTransactionBySenderAndNonce(blockchainQueries, transactionPool);
  }

  @Test
  void returnsCorrectMethodName() {
    assertThat(method.getName()).isEqualTo(ETH_METHOD);
  }

  @Test
  void shouldReturnErrorResponseIfMissingRequiredParameters() {
    final JsonRpcRequest request =
        new JsonRpcRequest(JSON_RPC_VERSION, method.getName(), new Object[] {"0x1"});
    final JsonRpcRequestContext context = new JsonRpcRequestContext(request);

    final JsonRpcErrorResponse expectedResponse =
        new JsonRpcErrorResponse(request.getId(), RpcErrorType.INVALID_PARAM_COUNT);

    final JsonRpcResponse actualResponse = method.response(context);

    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  void shouldReturnPendingTransactionWhenPresentInTransactionPool() {
    final var transaction = new BlockDataGenerator().transaction();
    final Address sender = transaction.getSender();
    final long nonce = transaction.getNonce();

    when(transactionPool.getPendingTransactionsFor(sender))
        .thenReturn(
            new SenderPendingTransactionsData(
                sender, nonce, List.of(new PendingTransaction.Local(transaction))));

    final JsonRpcRequest request =
        new JsonRpcRequest(
            JSON_RPC_VERSION,
            method.getName(),
            new Object[] {sender.toHexString(), Quantity.create(nonce)});
    final JsonRpcRequestContext context = new JsonRpcRequestContext(request);

    final JsonRpcSuccessResponse expectedResponse =
        new JsonRpcSuccessResponse(request.getId(), new TransactionBaseResult(transaction));

    final JsonRpcResponse actualResponse = method.response(context);

    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  void shouldReturnCompleteTransactionWhenPresentInBlockchain() {
    final var transaction = new BlockDataGenerator().transaction();
    final Address sender = transaction.getSender();
    final long nonce = transaction.getNonce();

    final long foundAtBlock = 10L;
    final long headBlock = 20L;

    final TransactionWithMetadata transactionWithMetadata =
        new TransactionWithMetadata(transaction, foundAtBlock, Optional.empty(), Hash.ZERO, 0, 0L);

    when(transactionPool.getPendingTransactionsFor(sender))
        .thenReturn(SenderPendingTransactionsData.empty(sender));

    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getChainHeadBlockNumber()).thenReturn(headBlock);

    when(blockchainQueries.getTransactionCount(eq(sender), anyLong()))
        .thenAnswer(
            invocation -> {
              final long block = invocation.getArgument(1);
              return block >= foundAtBlock ? nonce + 1 : nonce;
            });

    when(blockchainQueries.getTransactionCount(foundAtBlock)).thenReturn(Optional.of(1));
    when(blockchainQueries.transactionByBlockNumberAndIndex(foundAtBlock, 0))
        .thenReturn(Optional.of(transactionWithMetadata));

    final JsonRpcRequest request =
        new JsonRpcRequest(
            JSON_RPC_VERSION,
            method.getName(),
            new Object[] {sender.toHexString(), Quantity.create(nonce)});
    final JsonRpcRequestContext context = new JsonRpcRequestContext(request);

    final JsonRpcSuccessResponse expectedResponse =
        new JsonRpcSuccessResponse(
            request.getId(), new TransactionWithMetadataResult(transactionWithMetadata));

    final JsonRpcResponse actualResponse = method.response(context);

    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  void shouldReturnNullWhenTransactionDoesNotExist() {
    final Address sender = Address.fromHexString("0x0000000000000000000000000000000000000001");
    final long nonce = 7L;

    when(transactionPool.getPendingTransactionsFor(sender))
        .thenReturn(SenderPendingTransactionsData.empty(sender));

    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getChainHeadBlockNumber()).thenReturn(20L);
    when(blockchainQueries.getTransactionCount(sender, 20L)).thenReturn(nonce);

    final JsonRpcRequest request =
        new JsonRpcRequest(
            JSON_RPC_VERSION,
            method.getName(),
            new Object[] {sender.toHexString(), Quantity.create(nonce)});
    final JsonRpcRequestContext context = new JsonRpcRequestContext(request);

    final JsonRpcSuccessResponse expectedResponse =
        new JsonRpcSuccessResponse(request.getId(), null);

    final JsonRpcResponse actualResponse = method.response(context);

    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(expectedResponse);
  }
}
