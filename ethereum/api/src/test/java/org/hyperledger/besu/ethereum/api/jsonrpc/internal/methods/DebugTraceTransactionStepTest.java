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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.CallTracerResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.DebugTraceTransactionResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.FourByteTracerResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.OpCodeLoggerTracerResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.calltrace.FlatCallTracerResult;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.debug.TraceOptions;
import org.hyperledger.besu.ethereum.debug.TracerType;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.precompile.PrecompileContractRegistry;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@DisplayName("DebugTraceTransactionStep")
class DebugTraceTransactionStepTest {

  private TransactionTrace mockTransactionTrace;
  private Transaction mockTransaction;
  private TransactionProcessingResult mockResult;
  private ProtocolSpec mockProtocolSpec;

  private static final String EXPECTED_HASH =
      "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";

  @BeforeEach
  void setUp() {
    mockTransactionTrace = mock(TransactionTrace.class);
    mockTransaction = mock(Transaction.class);
    Hash mockHash = mock(Hash.class);
    mockResult = mock(TransactionProcessingResult.class);
    mockProtocolSpec = mock(ProtocolSpec.class);

    PrecompileContractRegistry mockRegistry = mock(PrecompileContractRegistry.class);
    when(mockProtocolSpec.getPrecompileContractRegistry()).thenReturn(mockRegistry);
    when(mockRegistry.get(org.mockito.ArgumentMatchers.any(Address.class))).thenReturn(null);
    when(mockRegistry.getPrecompileAddresses()).thenReturn(Set.of());
    when(mockTransactionTrace.getTransaction()).thenReturn(mockTransaction);
    when(mockTransaction.getHash()).thenReturn(mockHash);
    when(mockTransaction.getSender()).thenReturn(Address.fromHexString("0x00"));
    when(mockTransaction.getValue()).thenReturn(Wei.ZERO);
    when(mockTransaction.getPayload()).thenReturn(Bytes.EMPTY);
    Bytes hashBytes = Bytes.fromHexString(EXPECTED_HASH);
    when(mockHash.getBytes()).thenReturn(hashBytes);

    when(mockTransactionTrace.getGas()).thenReturn(0L);
    when(mockTransactionTrace.getResult()).thenReturn(mockResult);
    when(mockResult.getOutput()).thenReturn(Bytes.EMPTY);
    when(mockResult.isSuccessful()).thenReturn(true);
    when(mockTransactionTrace.getTraceFrames()).thenReturn(Collections.emptyList());
    when(mockTransactionTrace.getBlock()).thenReturn(Optional.empty());
  }

  @Test
  @DisplayName("should create step for OPCODE_TRACER that returns OpCodeLoggerTracerResult")
  void shouldCreateFunctionForOpcodeTracer() {
    TraceOptions traceOptions = new TraceOptions(TracerType.OPCODE_TRACER, null, null);
    DebugTraceTransactionStep step = DebugTraceTransactionStep.of(traceOptions, mockProtocolSpec);

    DebugTraceTransactionResult result = step.buildResult(mockTransactionTrace);

    assertThat(result).isNotNull();
    assertThat(result.getTxHash()).isEqualTo(EXPECTED_HASH);
    assertThat(result.getResult()).isInstanceOf(OpCodeLoggerTracerResult.class);
  }

  @Test
  @DisplayName("should create step for FOUR_BYTE_TRACER that returns FourByteTracerResult")
  void shouldCreateFunctionForFourByteTracer() {
    TraceOptions traceOptions = new TraceOptions(TracerType.FOUR_BYTE_TRACER, null, null);
    DebugTraceTransactionStep step = DebugTraceTransactionStep.of(traceOptions, mockProtocolSpec);

    DebugTraceTransactionResult result = step.buildResult(mockTransactionTrace);

    assertThat(result).isNotNull();
    assertThat(result.getTxHash()).isEqualTo(EXPECTED_HASH);
    assertThat(result.getResult()).isInstanceOf(FourByteTracerResult.class);
  }

  @Test
  @DisplayName("should create step for FLAT_CALL_TRACER that returns list of FlatCallTracerResult")
  void shouldCreateFunctionForFlatCallTracer() {
    TraceOptions traceOptions = new TraceOptions(TracerType.FLAT_CALL_TRACER, null, null);
    DebugTraceTransactionStep step = DebugTraceTransactionStep.of(traceOptions, mockProtocolSpec);

    DebugTraceTransactionResult result = step.buildResult(mockTransactionTrace);

    assertThat(result).isNotNull();
    assertThat(result.getTxHash()).isEqualTo(EXPECTED_HASH);
    assertThat(result.getResult()).isInstanceOf(List.class);
    @SuppressWarnings("unchecked")
    List<FlatCallTracerResult> flatResults = (List<FlatCallTracerResult>) result.getResult();
    assertThat(flatResults).hasSize(1);
    FlatCallTracerResult frame = flatResults.get(0);
    assertThat(frame.type()).isEqualTo("call");
    assertThat(frame.blockHash()).isNull();
    assertThat(frame.transactionPosition()).isZero();
  }

  @ParameterizedTest
  @EnumSource(TracerType.class)
  @DisplayName("should create non-null step and tracer for all tracer types")
  void shouldCreateNonNullStepAndTracerForAllTracerTypes(final TracerType tracerType) {
    TraceOptions traceOptions = new TraceOptions(tracerType, null, null);
    DebugTraceTransactionStep step = DebugTraceTransactionStep.of(traceOptions, mockProtocolSpec);

    assertThat(step).isNotNull();
    assertThat(step.getOperationTracer()).isNotNull();
  }

  @ParameterizedTest
  @EnumSource(TracerType.class)
  @DisplayName("should return non-null result with correct transaction hash for all tracer types")
  void shouldReturnNonNullResultWithCorrectTransactionHashForAllTracerTypes(
      final TracerType tracerType) {
    TraceOptions traceOptions = new TraceOptions(tracerType, null, null);
    DebugTraceTransactionStep step = DebugTraceTransactionStep.of(traceOptions, mockProtocolSpec);

    DebugTraceTransactionResult result = step.buildResult(mockTransactionTrace);

    assertThat(result).isNotNull();
    assertThat(result.getTxHash()).isEqualTo(EXPECTED_HASH);
    assertThat(result.getResult()).isNotNull();
  }

  @Test
  @DisplayName("CALL_TRACER with onlyTopCall reports only the root frame and omits nested calls")
  void callTracerWithOnlyTopCallOmitsNestedCalls() {
    final TraceOptions traceOptions =
        new TraceOptions(TracerType.CALL_TRACER, null, Map.of("onlyTopCall", true));
    final DebugTraceTransactionStep step =
        DebugTraceTransactionStep.of(traceOptions, mockProtocolSpec);
    final OperationTracer tracer = step.getOperationTracer();

    final org.hyperledger.besu.datatypes.Transaction tx =
        mock(org.hyperledger.besu.datatypes.Transaction.class);
    when(tx.isContractCreation()).thenReturn(false);
    when(tx.getGasLimit()).thenReturn(21000L);
    tracer.traceStartTransaction(null, tx);

    final MessageFrame rootFrame = mock(MessageFrame.class);
    when(rootFrame.getDepth()).thenReturn(0);
    when(rootFrame.getSenderAddress()).thenReturn(Address.fromHexString("0x00"));
    when(rootFrame.getContractAddress()).thenReturn(Address.fromHexString("0x01"));
    when(rootFrame.getValue()).thenReturn(Wei.ZERO);
    when(rootFrame.getInputData()).thenReturn(Bytes.EMPTY);
    when(rootFrame.getOutputData()).thenReturn(Bytes.EMPTY);
    when(rootFrame.getRemainingGas()).thenReturn(21000L);
    when(rootFrame.getState()).thenReturn(MessageFrame.State.COMPLETED_SUCCESS);
    when(rootFrame.getExceptionalHaltReason()).thenReturn(Optional.empty());
    when(rootFrame.getRevertReason()).thenReturn(Optional.empty());
    when(rootFrame.getType()).thenReturn(MessageFrame.Type.MESSAGE_CALL);
    tracer.traceContextEnter(rootFrame);

    final MessageFrame nestedFrame = mock(MessageFrame.class);
    when(nestedFrame.getDepth()).thenReturn(1);
    tracer.traceContextEnter(nestedFrame);
    tracer.tracePrecompileCall(nestedFrame, 0L, Bytes.EMPTY);
    tracer.traceContextExit(nestedFrame);

    tracer.traceContextExit(rootFrame);

    when(mockTransaction.isContractCreation()).thenReturn(false);
    when(mockTransaction.getGasLimit()).thenReturn(21000L);
    when(mockResult.getGasRemaining()).thenReturn(0L);

    final DebugTraceTransactionResult result = step.buildResult(mockTransactionTrace);

    assertThat(result.getResult()).isInstanceOf(CallTracerResult.class);
    final CallTracerResult callResult = (CallTracerResult) result.getResult();
    assertThat(callResult.getType()).isNotNull();
    assertThat(callResult.getFrom()).isNotNull();
    assertThat(callResult.getCalls()).isNullOrEmpty();
    org.mockito.Mockito.verify(nestedFrame, org.mockito.Mockito.atLeastOnce()).getDepth();
    org.mockito.Mockito.verifyNoMoreInteractions(nestedFrame);
  }
}
