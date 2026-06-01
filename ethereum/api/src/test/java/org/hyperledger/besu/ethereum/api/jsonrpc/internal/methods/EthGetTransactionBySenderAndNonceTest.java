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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionPendingResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionWithMetadataResult;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.TransactionWithMetadata;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.SenderPendingTransactionsData;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EthGetTransactionBySenderAndNonceTest {

  private static final String VALID_TRANSACTION =
      "0xb8c404f8c1018080078307a12094a94f5374fce5edbc8e2a8697c15331677e6ebf0b8080c0f85cf85a809400000000000000000000000000000000000010008080a0dbcff17ff6c249f13b334fa86bcbaa1afd9f566ca9b06e4ea5fab9bdde9a9202a05c34c9d8af5b20e4a425fc1daf2d9d484576857eaf1629145b4686bac733868e01a0d61673cd58ffa5fc605c3215aa4647fa3afbea1d1f577e08402442992526d980a0063068ca818025c7b8493d0623cb70ef3a2ba4b3e2ae0af1146d1c9b065c0aff";

  private static final String JSON_RPC_VERSION = "2.0";
  private static final String METHOD = "eth_getTransactionBySenderAndNonce";

  @Mock private BlockchainQueries blockchainQueries;
  @Mock private TransactionPool transactionPool;

  private EthGetTransactionBySenderAndNonce method;

  @BeforeEach
  void setUp() {
    method = new EthGetTransactionBySenderAndNonce(blockchainQueries, transactionPool);
  }

  @Test
  void returnsCorrectMethodName() {
    assertThat(method.getName()).isEqualTo(METHOD);
  }

  @Test
  void shouldReturnErrorWhenParamCountIsWrong() {
    final JsonRpcRequest request = new JsonRpcRequest(JSON_RPC_VERSION, METHOD, new Object[] {});
    final JsonRpcResponse actual = method.response(new JsonRpcRequestContext(request));
    assertThat(actual)
        .usingRecursiveComparison()
        .isEqualTo(new JsonRpcErrorResponse(request.getId(), RpcErrorType.INVALID_PARAM_COUNT));
  }

  @Test
  void shouldReturnNullWhenTransactionNotFoundInPoolOrChain() {
    final Address sender = Address.fromHexString("0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b");
    final long nonce = 1L;
    when(transactionPool.getPendingTransactionsFor(sender))
        .thenReturn(SenderPendingTransactionsData.empty(sender));
    when(blockchainQueries.transactionBySenderAndNonce(sender, nonce)).thenReturn(Optional.empty());

    final JsonRpcRequest request =
        new JsonRpcRequest(JSON_RPC_VERSION, METHOD, new Object[] {sender.toHexString(), "0x1"});
    final JsonRpcResponse actual = method.response(new JsonRpcRequestContext(request));

    assertThat(actual)
        .usingRecursiveComparison()
        .isEqualTo(new JsonRpcSuccessResponse(request.getId(), null));
  }

  @Test
  void shouldReturnPendingTransactionWhenFoundInPool() {
    final Transaction tx = Transaction.readFrom(Bytes.fromHexString(VALID_TRANSACTION));
    final Address sender = tx.getSender();
    final long nonce = tx.getNonce();
    final PendingTransaction pendingTx = new PendingTransaction.Local(tx);

    when(transactionPool.getPendingTransactionsFor(sender))
        .thenReturn(new SenderPendingTransactionsData(sender, nonce, List.of(pendingTx)));

    final JsonRpcRequest request =
        new JsonRpcRequest(
            JSON_RPC_VERSION,
            METHOD,
            new Object[] {sender.toHexString(), "0x" + Long.toHexString(nonce)});
    final JsonRpcResponse actual = method.response(new JsonRpcRequestContext(request));

    assertThat(actual)
        .usingRecursiveComparison()
        .isEqualTo(new JsonRpcSuccessResponse(request.getId(), new TransactionPendingResult(tx)));
  }

  @Test
  void shouldReturnMinedTransactionWhenFoundInChain() {
    final Transaction tx = Transaction.readFrom(Bytes.fromHexString(VALID_TRANSACTION));
    final Address sender = tx.getSender();
    final long nonce = tx.getNonce();
    final TransactionWithMetadata txWithMeta =
        new TransactionWithMetadata(tx, 1L, Optional.empty(), Hash.ZERO, 0, 0L);

    when(transactionPool.getPendingTransactionsFor(sender))
        .thenReturn(SenderPendingTransactionsData.empty(sender));
    when(blockchainQueries.transactionBySenderAndNonce(sender, nonce))
        .thenReturn(Optional.of(txWithMeta));

    final JsonRpcRequest request =
        new JsonRpcRequest(
            JSON_RPC_VERSION,
            METHOD,
            new Object[] {sender.toHexString(), "0x" + Long.toHexString(nonce)});
    final JsonRpcResponse actual = method.response(new JsonRpcRequestContext(request));

    assertThat(actual)
        .usingRecursiveComparison()
        .isEqualTo(
            new JsonRpcSuccessResponse(
                request.getId(), new TransactionWithMetadataResult(txWithMeta)));
  }
}
