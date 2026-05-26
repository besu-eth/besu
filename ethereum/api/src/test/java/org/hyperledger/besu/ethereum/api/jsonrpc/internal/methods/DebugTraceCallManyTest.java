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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.DebugTraceTransactionResult;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.MiningBeneficiaryCalculator;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulatorResult;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

public class DebugTraceCallManyTest {

  private final BlockchainQueries blockchainQueries =
      mock(BlockchainQueries.class, Answers.RETURNS_DEEP_STUBS);
  private final ProtocolSchedule protocolSchedule = mock(ProtocolSchedule.class);
  private final ProtocolSpec protocolSpec = mock(ProtocolSpec.class);
  private final MiningBeneficiaryCalculator miningBeneficiaryCalculator =
      mock(MiningBeneficiaryCalculator.class);
  private final TransactionSimulator transactionSimulator = mock(TransactionSimulator.class);
  private final BlockHeader blockHeader = mock(BlockHeader.class);
  private final MutableWorldState worldState = mock(MutableWorldState.class);
  private final WorldUpdater rootUpdater = mock(WorldUpdater.class);
  private final WorldUpdater childUpdater = mock(WorldUpdater.class);

  private final Hash blockHash =
      Hash.fromHexString("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

  private DebugTraceCallMany method;

  @BeforeEach
  public void setUp() {
    method = new DebugTraceCallMany(blockchainQueries, protocolSchedule, transactionSimulator);

    when(blockchainQueries.getBlockHeaderByNumber(0L)).thenReturn(Optional.of(blockHeader));
    when(blockchainQueries.headBlockNumber()).thenReturn(0L);
    when(blockHeader.getBlockHash()).thenReturn(blockHash);
    when(protocolSchedule.getByBlockHeader(blockHeader)).thenReturn(protocolSpec);
    when(protocolSpec.getMiningBeneficiaryCalculator()).thenReturn(miningBeneficiaryCalculator);
    when(miningBeneficiaryCalculator.calculateBeneficiary(blockHeader)).thenReturn(Address.ZERO);

    when(transactionSimulator.getEffectiveWorldStateUpdater(any())).thenReturn(rootUpdater);
    when(rootUpdater.updater()).thenReturn(childUpdater);

    doAnswer(
            invocation ->
                invocation
                    .<Function<MutableWorldState, Optional<?>>>getArgument(1)
                    .apply(worldState))
        .when(blockchainQueries)
        .getAndMapWorldState(any(Hash.class), any());
  }

  @Test
  public void nameShouldBeDebugTraceCallMany() {
    assertThat(method.getName()).isEqualTo("debug_traceCallMany");
  }

  @Test
  public void shouldReturnInvalidParamsWhenCallArrayMissing() {
    final JsonRpcRequestContext request = requestWithRawParams(new Object[] {});

    final Object response = method.response(request);

    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) response).getErrorType())
        .isEqualTo(RpcErrorType.INVALID_TRACE_CALL_MANY_PARAMS);
  }

  @Test
  public void shouldReturnBlockNotFoundWhenBlockMissing() {
    when(blockchainQueries.getBlockHeaderByNumber(0L)).thenReturn(Optional.empty());
    final JsonRpcRequestContext request =
        requestWithRawParams(new Object[] {arrayOfOneSimpleCall()});

    final Object response = method.response(request);

    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) response).getErrorType())
        .isEqualTo(RpcErrorType.BLOCK_NOT_FOUND);
  }

  @Test
  public void shouldChainStateAcrossCallsAndReturnPerCallResults() {
    when(transactionSimulator.processWithWorldUpdater(
            any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(Optional.of(successfulSimulatorResult()))
        .thenReturn(Optional.of(successfulSimulatorResult()));

    final JsonRpcRequestContext request =
        requestWithRawParams(new Object[] {arrayOfTwoSimpleCalls()});

    final Object response = method.response(request);

    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    @SuppressWarnings("unchecked")
    final List<Object> results = (List<Object>) ((JsonRpcSuccessResponse) response).getResult();
    assertThat(results).hasSize(2);
    assertThat(results.get(0)).isInstanceOf(DebugTraceTransactionResult.class);
    assertThat(results.get(1)).isInstanceOf(DebugTraceTransactionResult.class);

    verify(transactionSimulator, times(2))
        .processWithWorldUpdater(any(), any(), any(), any(), any(), any(), any(), any());
    verify(rootUpdater, times(2)).updater();
    verify(childUpdater, times(2)).commit();
  }

  @Test
  public void shouldInlineErrorForInvalidCallAndContinueBatch() {
    when(transactionSimulator.processWithWorldUpdater(
            any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(Optional.of(invalidSimulatorResult("nonce too low")))
        .thenReturn(Optional.of(successfulSimulatorResult()));

    final JsonRpcRequestContext request =
        requestWithRawParams(new Object[] {arrayOfTwoSimpleCalls()});

    final Object response = method.response(request);

    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    @SuppressWarnings("unchecked")
    final List<Object> results = (List<Object>) ((JsonRpcSuccessResponse) response).getResult();
    assertThat(results).hasSize(2);
    assertThat(results.get(0)).isInstanceOf(JsonRpcError.class);
    assertThat(results.get(1)).isInstanceOf(DebugTraceTransactionResult.class);

    verify(childUpdater, times(2)).commit();
  }

  @Test
  public void shouldInlineErrorWhenSimulatorReturnsEmpty() {
    when(transactionSimulator.processWithWorldUpdater(
            any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(Optional.empty());

    final JsonRpcRequestContext request =
        requestWithRawParams(new Object[] {arrayOfOneSimpleCall()});

    final Object response = method.response(request);
    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    @SuppressWarnings("unchecked")
    final List<Object> results = (List<Object>) ((JsonRpcSuccessResponse) response).getResult();
    assertThat(results).hasSize(1);
    assertThat(results.get(0)).isInstanceOf(JsonRpcError.class);
  }

  private TransactionSimulatorResult successfulSimulatorResult() {
    final Transaction tx = mock(Transaction.class);
    when(tx.getHash())
        .thenReturn(
            Hash.fromHexString(
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
    when(tx.getGasLimit()).thenReturn(21000L);

    final TransactionProcessingResult procResult = mock(TransactionProcessingResult.class);
    when(procResult.isSuccessful()).thenReturn(true);
    when(procResult.isInvalid()).thenReturn(false);
    when(procResult.getOutput()).thenReturn(Bytes.EMPTY);
    when(procResult.getGasRemaining()).thenReturn(0L);
    when(procResult.getRevertReason()).thenReturn(Optional.empty());
    when(procResult.getLogs()).thenReturn(Collections.emptyList());
    return new TransactionSimulatorResult(tx, procResult);
  }

  private TransactionSimulatorResult invalidSimulatorResult(final String message) {
    final Transaction tx = mock(Transaction.class);
    final TransactionProcessingResult procResult = mock(TransactionProcessingResult.class);
    when(procResult.isInvalid()).thenReturn(true);
    when(procResult.getValidationResult())
        .thenReturn(ValidationResult.invalid(TransactionInvalidReason.NONCE_TOO_LOW, message));
    return new TransactionSimulatorResult(tx, procResult);
  }

  private Object[] arrayOfOneSimpleCall() {
    return new Object[] {new Object[] {simpleCallJson(), null}};
  }

  private Object[] arrayOfTwoSimpleCalls() {
    return new Object[] {
      new Object[] {simpleCallJson(), null}, new Object[] {simpleCallJson(), null}
    };
  }

  private static Map<String, String> simpleCallJson() {
    return Map.of(
        "from",
        "0xfe3b557e8fb62b89f4916b721be55ceb828dbd73",
        "to",
        "0x0010000000000000000000000000000000000000",
        "gas",
        "0xffff",
        "value",
        "0x0");
  }

  private JsonRpcRequestContext requestWithRawParams(final Object[] params) {
    return new JsonRpcRequestContext(new JsonRpcRequest("2.0", "debug_traceCallMany", params));
  }
}
