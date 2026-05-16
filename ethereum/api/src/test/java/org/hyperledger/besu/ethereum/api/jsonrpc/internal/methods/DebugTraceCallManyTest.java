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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.DebugTraceCallManyParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.DebugTraceCallManyParameter.DebugCallParameterTuple;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.MiningBeneficiaryCalculator;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.transaction.CallParameter;
import org.hyperledger.besu.ethereum.transaction.ImmutableCallParameter;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulatorResult;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

public class DebugTraceCallManyTest {

  private final BlockchainQueries blockchainQueries = mock(BlockchainQueries.class);
  private final ProtocolSchedule protocolSchedule = mock(ProtocolSchedule.class);
  private final ProtocolSpec protocolSpec = mock(ProtocolSpec.class, Answers.RETURNS_DEEP_STUBS);
  private final TransactionSimulator transactionSimulator = mock(TransactionSimulator.class);
  private final MutableWorldState worldState = mock(MutableWorldState.class);
  private final WorldUpdater worldUpdater = mock(WorldUpdater.class);
  private final WorldUpdater localUpdater = mock(WorldUpdater.class);
  private final BlockHeader blockHeader = mock(BlockHeader.class);
  private final MiningBeneficiaryCalculator miningBeneficiaryCalculator =
      mock(MiningBeneficiaryCalculator.class);

  private DebugTraceCallMany method;

  private static final Hash BLOCK_HASH =
      Hash.fromHexString("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
  private static final long BLOCK_NUMBER = 42L;

  @BeforeEach
  @SuppressWarnings("unchecked")
  public void setUp() {
    method = new DebugTraceCallMany(blockchainQueries, protocolSchedule, transactionSimulator);

    when(blockHeader.getNumber()).thenReturn(BLOCK_NUMBER);
    when(blockHeader.getHash()).thenReturn(BLOCK_HASH);
    when(blockHeader.getBlockHash()).thenReturn(BLOCK_HASH);
    when(blockchainQueries.getBlockHeaderByNumber(BLOCK_NUMBER))
        .thenReturn(Optional.of(blockHeader));
    when(protocolSchedule.getByBlockHeader(blockHeader)).thenReturn(protocolSpec);
    when(protocolSpec.getMiningBeneficiaryCalculator()).thenReturn(miningBeneficiaryCalculator);
    when(miningBeneficiaryCalculator.calculateBeneficiary(any())).thenReturn(Address.ZERO);
    when(transactionSimulator.getEffectiveWorldStateUpdater(worldState)).thenReturn(worldUpdater);
    when(worldUpdater.updater()).thenReturn(localUpdater);
    doAnswer(
            inv -> {
              Function<MutableWorldState, Optional<?>> fn = inv.getArgument(1);
              return fn.apply(worldState);
            })
        .when(blockchainQueries)
        .getAndMapWorldState(any(), any());
  }

  @Test
  public void methodNameIsDebugTraceCallMany() {
    assertThat(method.getName()).isEqualTo(RpcMethod.DEBUG_TRACE_CALL_MANY.getMethodName());
    assertThat(method.getName()).isEqualTo("debug_traceCallMany");
  }

  @Test
  public void returnsErrorForWrongParamCount() {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", "debug_traceCallMany", new Object[] {}));

    final JsonRpcResponse response = method.response(request);

    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) response).getError().getCode())
        .isEqualTo(RpcErrorType.INVALID_PARAM_COUNT.getCode());
  }

  @Test
  public void returnsErrorWhenBlockNotFound() {
    when(blockchainQueries.getBlockHeaderByNumber(BLOCK_NUMBER)).thenReturn(Optional.empty());

    final JsonRpcResponse response = method.response(buildRequest(BLOCK_NUMBER));

    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) response).getError().getCode())
        .isEqualTo(RpcErrorType.BLOCK_NOT_FOUND.getCode());
  }

  @Test
  public void returnsSingleCallResult() {
    mockSuccessfulSimulation();

    final JsonRpcRequestContext request =
        buildRequestWithParams(
            new Object[] {
              new DebugTraceCallManyParameter[] {
                parameter(callParameter()),
              },
              "0x" + Long.toHexString(BLOCK_NUMBER),
            });

    final JsonRpcResponse response = method.response(request);

    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    final Object result = ((JsonRpcSuccessResponse) response).getResult();
    assertThat(result).isInstanceOf(List.class);
    assertThat((List<?>) result).hasSize(1);
  }

  @Test
  public void returnsResultPerCallInOrder() {
    mockSuccessfulSimulation();

    final JsonRpcRequestContext request =
        buildRequestWithParams(
            new Object[] {
              new DebugTraceCallManyParameter[] {
                parameter(callParameter()), parameter(callParameter()),
              },
              "0x" + Long.toHexString(BLOCK_NUMBER),
            });

    final JsonRpcResponse response = method.response(request);

    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    assertThat((List<?>) ((JsonRpcSuccessResponse) response).getResult()).hasSize(2);
  }

  @Test
  public void returnsInternalErrorWhenSimulatorResultIsInvalid() {
    mockInvalidSimulation();

    final JsonRpcRequestContext request =
        buildRequestWithParams(
            new Object[] {
              new DebugTraceCallManyParameter[] {
                parameter(callParameter()),
              },
              "0x" + Long.toHexString(BLOCK_NUMBER),
            });

    final JsonRpcResponse response = method.response(request);

    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) response).getError().getCode())
        .isEqualTo(RpcErrorType.INTERNAL_ERROR.getCode());
  }

  @Test
  public void defaultsToLatestBlockWhenBlockParamOmitted() {
    final long headBlock = 99L;
    when(blockchainQueries.headBlockNumber()).thenReturn(headBlock);
    when(blockchainQueries.getBlockHeaderByNumber(headBlock)).thenReturn(Optional.of(blockHeader));
    when(blockHeader.getNumber()).thenReturn(headBlock);
    mockSuccessfulSimulation();

    final JsonRpcRequestContext request =
        buildRequestWithParams(
            new Object[] {
              new DebugTraceCallManyParameter[] {
                parameter(callParameter()),
              },
            });

    final JsonRpcResponse response = method.response(request);

    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
  }

  // --- helpers ---

  private void mockSuccessfulSimulation() {
    final TransactionSimulatorResult result = mock(TransactionSimulatorResult.class);
    final TransactionProcessingResult processingResult =
        mock(TransactionProcessingResult.class, Answers.RETURNS_DEEP_STUBS);

    when(result.isInvalid()).thenReturn(false);
    when(result.isSuccessful()).thenReturn(true);
    when(result.result()).thenReturn(processingResult);
    when(result.transaction())
        .thenReturn(
            mock(org.hyperledger.besu.ethereum.core.Transaction.class, Answers.RETURNS_DEEP_STUBS));
    when(processingResult.getOutput()).thenReturn(org.apache.tuweni.bytes.Bytes.EMPTY);
    when(processingResult.getRevertReason()).thenReturn(Optional.empty());

    when(transactionSimulator.processWithWorldUpdater(
            any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(Optional.of(result));
  }

  private void mockInvalidSimulation() {
    final TransactionSimulatorResult result = mock(TransactionSimulatorResult.class);
    when(result.isInvalid()).thenReturn(true);

    when(transactionSimulator.processWithWorldUpdater(
            any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(Optional.of(result));
  }

  private CallParameter callParameter() {
    return ImmutableCallParameter.builder()
        .sender(Address.fromHexString("0x1111111111111111111111111111111111111111"))
        .to(Address.fromHexString("0x2222222222222222222222222222222222222222"))
        .build();
  }

  private DebugTraceCallManyParameter parameter(final CallParameter callParams) {
    return new DebugTraceCallManyParameter(new DebugCallParameterTuple(callParams, null));
  }

  private JsonRpcRequestContext buildRequest(final long blockNumber) {
    return buildRequestWithParams(
        new Object[] {
          new DebugTraceCallManyParameter[] {}, "0x" + Long.toHexString(blockNumber),
        });
  }

  private JsonRpcRequestContext buildRequestWithParams(final Object[] params) {
    return new JsonRpcRequestContext(new JsonRpcRequest("2.0", "debug_traceCallMany", params));
  }
}
