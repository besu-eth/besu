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

import static org.hyperledger.besu.evm.account.Account.MAX_NONCE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.BlockTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.BlockTracer;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.Tracer;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.DebugAccountAtResult;
import org.hyperledger.besu.ethereum.api.query.BlockWithMetadata;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.TransactionWithMetadata;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.tracing.TraceFrame;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.Collections;
import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DebugAccountAtTest {
  @Mock private BlockTracer blockTracer;
  @Mock private BlockchainQueries blockchainQueries;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private BlockWithMetadata<TransactionWithMetadata, Hash> blockWithMetadata;

  @Mock private BlockHeader blockHeader;
  @Mock private TransactionWithMetadata transactionWithMetadata;
  @Mock private BlockTrace blockTrace;
  @Mock private TransactionTrace transactionTrace;
  @Mock private TraceFrame traceFrame;
  @Mock private Transaction transaction;
  @Mock private WorldUpdater worldUpdater;
  @Mock private MutableWorldState worldState;

  @Mock private Account account;

  private static DebugAccountAt debugAccountAt;

  @BeforeEach
  void init() {
    debugAccountAt = new DebugAccountAt(blockchainQueries, () -> blockTracer);
  }

  @Test
  void nameShouldBeDebugAccountAt() {
    Assertions.assertThat(debugAccountAt.getName()).isEqualTo("debug_accountAt");
  }

  @Test
  void shouldReturnNullWhenBlockNotFound() {
    Mockito.when(blockchainQueries.getBlockHeaderByHash(any())).thenReturn(Optional.empty());

    final Object[] params =
        new Object[] {Hash.ZERO.getBytes().toHexString(), 0, Address.ZERO.getBytes().toHexString()};
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", "debug_accountAt", params));
    final JsonRpcResponse response = debugAccountAt.response(request);

    Assertions.assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    Assertions.assertThat(((JsonRpcSuccessResponse) response).getResult()).isNull();
  }

  @Test
  void testInvalidParamsResponseEmptyList() {
    setupMockBlock();
    Mockito.when(blockWithMetadata.getTransactions()).thenReturn(Collections.emptyList());

    final Object[] params =
        new Object[] {Hash.ZERO.getBytes().toHexString(), 0, Address.ZERO.getBytes().toHexString()};
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", "debug_accountAt", params));
    final JsonRpcResponse response = debugAccountAt.response(request);

    Assertions.assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    Assertions.assertThat(((JsonRpcErrorResponse) response).getErrorType())
        .isEqualByComparingTo(RpcErrorType.INVALID_TRANSACTION_PARAMS);
  }

  @Test
  void testInvalidParamsResponseNegative() {
    setupMockBlock();
    Mockito.when(blockWithMetadata.getTransactions())
        .thenReturn(Collections.singletonList(transactionWithMetadata));

    final Object[] params =
        new Object[] {
          Hash.ZERO.getBytes().toHexString(), -1, Address.ZERO.getBytes().toHexString()
        };
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", "debug_accountAt", params));
    final JsonRpcResponse response = debugAccountAt.response(request);

    Assertions.assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    Assertions.assertThat(((JsonRpcErrorResponse) response).getErrorType())
        .isEqualByComparingTo(RpcErrorType.INVALID_TRANSACTION_PARAMS);
  }

  @Test
  void testInvalidParamsResponseTooHigh() {
    setupMockBlock();
    Mockito.when(blockWithMetadata.getTransactions())
        .thenReturn(Collections.singletonList(transactionWithMetadata));

    final Object[] params =
        new Object[] {Hash.ZERO.getBytes().toHexString(), 2, Address.ZERO.getBytes().toHexString()};
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", "debug_accountAt", params));
    final JsonRpcResponse response = debugAccountAt.response(request);

    Assertions.assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    Assertions.assertThat(((JsonRpcErrorResponse) response).getErrorType())
        .isEqualByComparingTo(RpcErrorType.INVALID_TRANSACTION_PARAMS);
  }

  @Test
  void testTransactionNotFoundResponse() {
    doAnswer(
            invocation ->
                invocation
                    .<Function<MutableWorldState, Optional<? extends JsonRpcResponse>>>getArgument(
                        1)
                    .apply(worldState))
        .when(blockchainQueries)
        .getAndMapWorldState(any(), any());

    setupMockBlock();
    Mockito.when(blockWithMetadata.getTransactions())
        .thenReturn(Collections.singletonList(transactionWithMetadata));

    final Object[] params =
        new Object[] {Hash.ZERO.getBytes().toHexString(), 0, Address.ZERO.getBytes().toHexString()};
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", "debug_accountAt", params));
    final JsonRpcResponse response = debugAccountAt.response(request);

    Assertions.assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    Assertions.assertThat(((JsonRpcErrorResponse) response).getErrorType())
        .isEqualByComparingTo(RpcErrorType.TRANSACTION_NOT_FOUND);
  }

  @Test
  void testNoAccountFoundResponse() {
    doAnswer(
            invocation ->
                invocation
                    .<Function<MutableWorldState, Optional<? extends JsonRpcResponse>>>getArgument(
                        1)
                    .apply(worldState))
        .when(blockchainQueries)
        .getAndMapWorldState(any(), any());

    setupMockTransaction();
    setupMockBlock();

    final Object[] params =
        new Object[] {Hash.ZERO.getBytes().toHexString(), 0, Address.ZERO.getBytes().toHexString()};
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", "debug_accountAt", params));

    final JsonRpcResponse response = debugAccountAt.response(request);

    Assertions.assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    Assertions.assertThat(((JsonRpcErrorResponse) response).getErrorType())
        .isEqualByComparingTo(RpcErrorType.NO_ACCOUNT_FOUND);
  }

  @Test
  void shouldBeSuccessfulWhenTransactionsAndAccountArePresent() {
    doAnswer(
            invocation ->
                invocation
                    .<Function<MutableWorldState, Optional<? extends JsonRpcResponse>>>getArgument(
                        1)
                    .apply(worldState))
        .when(blockchainQueries)
        .getAndMapWorldState(any(), any());

    final String codeString =
        "0x608060405234801561001057600080fd5b506004361061002b5760003560e01c8063b27b880414610030575b";
    final Bytes code = Bytes.fromHexString(codeString);
    final long nonce = MAX_NONCE - 1;
    final String balanceString = "0xffff";
    final Wei balance = Wei.fromHexString(balanceString);
    final Hash codeHash = Hash.hash(code);

    setupMockTransaction();
    setupMockAccount();
    setupMockBlock();

    Mockito.when(account.getCode()).thenReturn(code);
    Mockito.when(account.getNonce()).thenReturn(nonce);
    Mockito.when(account.getBalance()).thenReturn(balance);
    Mockito.when(account.getCodeHash()).thenReturn(codeHash);

    final Object[] params =
        new Object[] {Hash.ZERO.getBytes().toHexString(), 0, Address.ZERO.getBytes().toHexString()};
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", "debug_accountAt", params));
    final JsonRpcSuccessResponse response =
        (JsonRpcSuccessResponse) debugAccountAt.response(request);

    final DebugAccountAtResult result = (DebugAccountAtResult) response.getResult();
    Assertions.assertThat(result.getCode()).isEqualTo(codeString);
    Assertions.assertThat(result.getNonce()).isEqualTo("0x" + Long.toHexString(nonce));
    Assertions.assertThat(result.getBalance()).isEqualTo(balanceString);
    Assertions.assertThat(result.getCodehash()).isEqualTo(codeHash.getBytes().toHexString());
  }

  private void setupMockAccount() {
    Mockito.when(transactionTrace.getTraceFrames())
        .thenReturn(Collections.singletonList(traceFrame));
    Mockito.when(traceFrame.getWorldUpdater()).thenReturn(worldUpdater);
    Mockito.when(worldUpdater.get(any())).thenReturn(account);
    Mockito.when(account.getAddress()).thenReturn(Address.ZERO);
  }

  private void setupMockTransaction() {
    Mockito.when(blockchainQueries.blockByHash(any())).thenReturn(Optional.of(blockWithMetadata));
    Mockito.when(blockWithMetadata.getTransactions())
        .thenReturn(Collections.singletonList(transactionWithMetadata));
    Mockito.when(blockTracer.trace(any(Tracer.TraceableState.class), any(Hash.class), any()))
        .thenReturn(Optional.of(blockTrace));
    Mockito.when(blockTrace.getTransactionTraces())
        .thenReturn(Collections.singletonList(transactionTrace));
    Mockito.when(transactionTrace.getTransaction()).thenReturn(transaction);
    Mockito.when(transactionWithMetadata.getTransaction()).thenReturn(transaction);
    Mockito.when(transaction.getHash()).thenReturn(Hash.ZERO);
  }

  private void setupMockBlock() {
    Mockito.when(blockchainQueries.blockByHash(any())).thenReturn(Optional.of(blockWithMetadata));
    Mockito.when(blockchainQueries.getBlockHeaderByHash(any()))
        .thenReturn(Optional.of(blockHeader));
  }
}
