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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.calltrace.CallTracer;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.debug.TraceOptions;
import org.hyperledger.besu.ethereum.debug.TracerType;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.vm.DebugOperationTracer;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.precompile.PrecompileContractRegistry;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@DisplayName("DebugTraceTransactionStepFactory")
class DebugTraceTransactionStepFactoryTest {

  private TransactionTrace mockTransactionTrace;
  private Transaction mockTransaction;
  private Hash mockHash;
  private TransactionProcessingResult mockResult;
  private ProtocolSpec mockProtocolSpec;

  private static final String EXPECTED_HASH =
      "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";

  @BeforeEach
  void setUp() {
    // Create mocks
    mockTransactionTrace = mock(TransactionTrace.class);
    mockTransaction = mock(Transaction.class);
    mockHash = mock(Hash.class);
    mockResult = mock(TransactionProcessingResult.class);
    mockProtocolSpec = mock(ProtocolSpec.class);

    // Setup PrecompileContractRegistry for FourByteTracer
    PrecompileContractRegistry mockRegistry = mock(PrecompileContractRegistry.class);
    when(mockProtocolSpec.getPrecompileContractRegistry()).thenReturn(mockRegistry);
    when(mockRegistry.get(org.mockito.ArgumentMatchers.any(Address.class))).thenReturn(null);

    // Set up transaction hash chain
    when(mockTransactionTrace.getTransaction()).thenReturn(mockTransaction);
    when(mockTransaction.getHash()).thenReturn(mockHash);
    when(mockTransaction.getSender()).thenReturn(Address.fromHexString("0x00"));
    when(mockTransaction.getValue()).thenReturn(Wei.ZERO);
    when(mockTransaction.getPayload()).thenReturn(Bytes.EMPTY);
    Bytes hashBytes = Bytes.fromHexString(EXPECTED_HASH);
    when(mockHash.getBytes()).thenReturn(hashBytes);

    // Minimal setup for DebugStructLoggerTracerResult - just enough to avoid NPE
    when(mockTransactionTrace.getGas()).thenReturn(0L);
    when(mockTransactionTrace.getResult()).thenReturn(mockResult);
    when(mockResult.getOutput()).thenReturn(Bytes.EMPTY);
    when(mockResult.isSuccessful()).thenReturn(true);
    when(mockTransactionTrace.getTraceFrames()).thenReturn(Collections.emptyList());
  }

  /**
   * Builds a {@link CallTracer} that has already been driven through a single, trivial, successful
   * top-level call - mirroring how production call sites always drive the tracer through the real
   * transaction before handing it to the step factory. The step factory itself later calls {@link
   * CallTracer#buildResult} using the {@code mockTransaction}/{@code mockResult} pair configured in
   * {@link #setUp()}.
   */
  private OperationTracer tracerFor(final TracerType tracerType) {
    if (tracerType == TracerType.OPCODE_TRACER) {
      final DebugOperationTracer tracer = mock(DebugOperationTracer.class);
      when(tracer.isLimitReached()).thenReturn(false);
      return tracer;
    }
    if (tracerType != TracerType.CALL_TRACER) {
      return null;
    }
    final CallTracer tracer = new CallTracer(false);
    final MessageFrame frame = mock(MessageFrame.class);
    when(frame.getDepth()).thenReturn(0);
    when(frame.getSenderAddress()).thenReturn(Address.fromHexString("0x00"));
    when(frame.getContractAddress()).thenReturn(Address.fromHexString("0x01"));
    when(frame.getValue()).thenReturn(Wei.ZERO);
    when(frame.getInputData()).thenReturn(Bytes.EMPTY);
    when(frame.getOutputData()).thenReturn(Bytes.EMPTY);
    when(frame.getRemainingGas()).thenReturn(21000L);
    when(frame.getState()).thenReturn(MessageFrame.State.COMPLETED_SUCCESS);
    when(frame.getExceptionalHaltReason()).thenReturn(Optional.<ExceptionalHaltReason>empty());
    when(frame.getRevertReason()).thenReturn(Optional.<Bytes>empty());
    when(frame.getType()).thenReturn(MessageFrame.Type.MESSAGE_CALL);

    final org.hyperledger.besu.datatypes.Transaction tx =
        mock(org.hyperledger.besu.datatypes.Transaction.class);
    when(tx.isContractCreation()).thenReturn(false);
    when(tx.getGasLimit()).thenReturn(21000L);

    tracer.traceStartTransaction(null, tx);
    tracer.traceContextEnter(frame);
    tracer.traceContextExit(frame);
    return tracer;
  }

  @Test
  @DisplayName("should create function for OPCODE_TRACER that returns OpCodeLoggerTracerResult")
  void shouldCreateFunctionForOpcodeTracer() {
    // Given
    TracerType tracerType = TracerType.OPCODE_TRACER;
    TraceOptions traceOptions = new TraceOptions(tracerType, null, null);
    Function<TransactionTrace, DebugTraceTransactionResult> function =
        DebugTraceTransactionStepFactory.create(
            traceOptions, mockProtocolSpec, tracerFor(tracerType));

    // When
    DebugTraceTransactionResult result = function.apply(mockTransactionTrace);

    // Then
    assertThat(result).isNotNull();
    assertThat(result.getTxHash()).isEqualTo(EXPECTED_HASH);
    assertThat(result.getResult()).isInstanceOf(OpCodeLoggerTracerResult.class);
  }

  @Test
  @DisplayName("should create function for FOUR_BYTE_TRACER that returns FourByteTracerResult")
  void shouldCreateFunctionForFourByteTracer() {
    // Given
    TracerType tracerType = TracerType.FOUR_BYTE_TRACER;
    TraceOptions traceOptions = new TraceOptions(tracerType, null, null);
    Function<TransactionTrace, DebugTraceTransactionResult> function =
        DebugTraceTransactionStepFactory.create(
            traceOptions, mockProtocolSpec, tracerFor(tracerType));

    // When
    DebugTraceTransactionResult result = function.apply(mockTransactionTrace);

    // Then
    assertThat(result).isNotNull();
    assertThat(result.getTxHash()).isEqualTo(EXPECTED_HASH);
    assertThat(result.getResult()).isInstanceOf(FourByteTracerResult.class);
  }

  @ParameterizedTest
  @EnumSource(
      value = TracerType.class,
      names = {"FLAT_CALL_TRACER"})
  @DisplayName("should create function for unimplemented tracers")
  void shouldCreateFunctionForNotYetImplementedTracers(final TracerType tracerType) {
    // Given
    TraceOptions traceOptions = new TraceOptions(tracerType, null, null);
    Function<TransactionTrace, DebugTraceTransactionResult> function =
        DebugTraceTransactionStepFactory.create(
            traceOptions, mockProtocolSpec, tracerFor(tracerType));

    // When
    DebugTraceTransactionResult result = function.apply(mockTransactionTrace);

    // Then
    assertThat(result).isNotNull();
    assertThat(result.getTxHash()).isEqualTo(EXPECTED_HASH);
    assertThat(result.getResult())
        .isInstanceOf(DebugTraceTransactionStepFactory.UnimplementedTracerResult.class);
  }

  @ParameterizedTest
  @EnumSource(TracerType.class)
  @DisplayName("should create non-null function for all tracer types")
  void shouldCreateNonNullFunctionForAllTracerTypes(final TracerType tracerType) {
    // When
    TraceOptions traceOptions = new TraceOptions(tracerType, null, null);
    Function<TransactionTrace, DebugTraceTransactionResult> function =
        DebugTraceTransactionStepFactory.create(
            traceOptions, mockProtocolSpec, tracerFor(tracerType));

    // Then
    assertThat(function).isNotNull();
  }

  @ParameterizedTest
  @EnumSource(TracerType.class)
  @DisplayName("should return non-null result with correct transaction hash for all tracer types")
  void shouldReturnNonNullResultWithCorrectTransactionHashForAllTracerTypes(
      final TracerType tracerType) {
    // Given
    TraceOptions traceOptions = new TraceOptions(tracerType, null, null);
    Function<TransactionTrace, DebugTraceTransactionResult> function =
        DebugTraceTransactionStepFactory.create(
            traceOptions, mockProtocolSpec, tracerFor(tracerType));

    // When
    DebugTraceTransactionResult result = function.apply(mockTransactionTrace);

    // Then
    assertThat(result).isNotNull();
    assertThat(result.getTxHash()).isEqualTo(EXPECTED_HASH);
    assertThat(result.getResult()).isNotNull();
  }

  @Test
  @DisplayName("requires CallTracer for CALL_TRACER")
  void requiresCallTracerForCallTracer() {
    final TraceOptions traceOptions = new TraceOptions(TracerType.CALL_TRACER, null, null);

    assertThatThrownBy(
            () -> DebugTraceTransactionStepFactory.create(traceOptions, mockProtocolSpec, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("CALL_TRACER requires CallTracer");
  }

  @Test
  @DisplayName("requires DebugOperationTracer for OPCODE_TRACER")
  void requiresDebugOperationTracerForOpcodeTracer() {
    final TraceOptions traceOptions = new TraceOptions(TracerType.OPCODE_TRACER, null, null);

    assertThatThrownBy(
            () -> DebugTraceTransactionStepFactory.create(traceOptions, mockProtocolSpec, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("OPCODE_TRACER requires DebugOperationTracer");
  }

  @Test
  @DisplayName("CALL_TRACER with onlyTopCall reports only the root frame and omits nested calls")
  void callTracerWithOnlyTopCallOmitsNestedCalls() {
    final CallTracer tracer = new CallTracer(true);

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
    when(rootFrame.getExceptionalHaltReason()).thenReturn(Optional.<ExceptionalHaltReason>empty());
    when(rootFrame.getRevertReason()).thenReturn(Optional.<Bytes>empty());
    when(rootFrame.getType()).thenReturn(MessageFrame.Type.MESSAGE_CALL);
    tracer.traceContextEnter(rootFrame);

    // A nested call frame at depth 1: with onlyTopCall the tracer must never inspect it.
    final MessageFrame nestedFrame = mock(MessageFrame.class);
    when(nestedFrame.getDepth()).thenReturn(1);
    tracer.traceContextEnter(nestedFrame);
    tracer.tracePrecompileCall(nestedFrame, 0L, Bytes.EMPTY);
    tracer.traceContextExit(nestedFrame);

    tracer.traceContextExit(rootFrame);

    // buildResult() (invoked by the step factory) reads the transaction/result pair carried by
    // mockTransactionTrace, not the local `tx` used to drive the tracer above.
    when(mockTransaction.isContractCreation()).thenReturn(false);
    when(mockTransaction.getGasLimit()).thenReturn(21000L);
    when(mockResult.getGasRemaining()).thenReturn(0L);

    final TraceOptions traceOptions =
        new TraceOptions(TracerType.CALL_TRACER, null, Map.of("onlyTopCall", true));
    final DebugTraceTransactionResult result =
        DebugTraceTransactionStepFactory.create(traceOptions, mockProtocolSpec, tracer)
            .apply(mockTransactionTrace);

    assertThat(result.getResult()).isInstanceOf(CallTracerResult.class);
    final CallTracerResult callResult = (CallTracerResult) result.getResult();
    // Root frame is populated from the transaction...
    assertThat(callResult.getType()).isNotNull();
    assertThat(callResult.getFrom()).isNotNull();
    // ...but no nested calls are reported: the guard only ever checks the nested frame's depth,
    // never any of its call-tree data (address/value/input/output/gas).
    assertThat(callResult.getCalls()).isNullOrEmpty();
    org.mockito.Mockito.verify(nestedFrame, org.mockito.Mockito.atLeastOnce()).getDepth();
    org.mockito.Mockito.verifyNoMoreInteractions(nestedFrame);
  }
}
