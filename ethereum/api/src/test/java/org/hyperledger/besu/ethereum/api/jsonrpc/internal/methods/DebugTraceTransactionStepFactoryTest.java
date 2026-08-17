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
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.CallTracerResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.DebugTraceTransactionResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.FourByteTracerResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.OpCodeLoggerTracerResult;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.debug.TraceOptions;
import org.hyperledger.besu.ethereum.debug.TracerType;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.evm.precompile.PrecompileContractRegistry;
import org.hyperledger.besu.evm.tracing.TraceFrame;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
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

  @Test
  @DisplayName("should create function for OPCODE_TRACER that returns OpCodeLoggerTracerResult")
  void shouldCreateFunctionForOpcodeTracer() {
    // Given
    TracerType tracerType = TracerType.OPCODE_TRACER;
    TraceOptions traceOptions = new TraceOptions(tracerType, null, null);
    Function<TransactionTrace, DebugTraceTransactionResult> function =
        DebugTraceTransactionStepFactory.create(traceOptions, mockProtocolSpec);

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
        DebugTraceTransactionStepFactory.create(traceOptions, mockProtocolSpec);

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
        DebugTraceTransactionStepFactory.create(traceOptions, mockProtocolSpec);

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
        DebugTraceTransactionStepFactory.create(traceOptions, mockProtocolSpec);

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
        DebugTraceTransactionStepFactory.create(traceOptions, mockProtocolSpec);

    // When
    DebugTraceTransactionResult result = function.apply(mockTransactionTrace);

    // Then
    assertThat(result).isNotNull();
    assertThat(result.getTxHash()).isEqualTo(EXPECTED_HASH);
    assertThat(result.getResult()).isNotNull();
  }

  @Test
  @DisplayName("should create async function for OPCODE_TRACER")
  void shouldCreateAsyncFunctionForOpcodeTracer() throws Exception {
    // Given
    Function<TransactionTrace, CompletableFuture<DebugTraceTransactionResult>> asyncFunction =
        DebugTraceTransactionStepFactory.createAsync(
            new TraceOptions(TracerType.OPCODE_TRACER, null, null), mockProtocolSpec);

    // When
    CompletableFuture<DebugTraceTransactionResult> future =
        asyncFunction.apply(mockTransactionTrace);
    DebugTraceTransactionResult result = future.get();

    // Then
    assertThat(future).isNotNull();
    assertThat(future.isDone()).isTrue();
    assertThat(result).isNotNull();
    assertThat(result.getTxHash()).isEqualTo(EXPECTED_HASH);
    assertThat(result.getResult()).isInstanceOf(OpCodeLoggerTracerResult.class);
  }

  @ParameterizedTest
  @EnumSource(TracerType.class)
  @DisplayName("should create non-null async function for all tracer types")
  void shouldCreateNonNullAsyncFunctionForAllTracerTypes(final TracerType tracerType) {
    // When
    TraceOptions traceOptions = new TraceOptions(tracerType, null, null);
    Function<TransactionTrace, CompletableFuture<DebugTraceTransactionResult>> asyncFunction =
        DebugTraceTransactionStepFactory.createAsync(traceOptions, mockProtocolSpec);

    // Then
    assertThat(asyncFunction).isNotNull();
  }

  @ParameterizedTest
  @EnumSource(TracerType.class)
  @DisplayName("should return completed future with non-null result for all tracer types")
  void shouldReturnCompletedFutureWithNonNullResultForAllTracerTypes(final TracerType tracerType)
      throws Exception {
    // Given
    TraceOptions traceOptions = new TraceOptions(tracerType, null, null);
    Function<TransactionTrace, CompletableFuture<DebugTraceTransactionResult>> asyncFunction =
        DebugTraceTransactionStepFactory.createAsync(traceOptions, mockProtocolSpec);

    // When
    CompletableFuture<DebugTraceTransactionResult> future =
        asyncFunction.apply(mockTransactionTrace);
    DebugTraceTransactionResult result = future.get();

    // Then
    assertThat(future).isNotNull();
    assertThat(future.isDone()).isTrue();
    assertThat(result).isNotNull();
    assertThat(result.getTxHash()).isEqualTo(EXPECTED_HASH);
    assertThat(result.getResult()).isNotNull();
  }

  @Test
  @DisplayName("CALL_TRACER with onlyTopCall reports only the root frame and omits nested calls")
  void callTracerWithOnlyTopCallOmitsNestedCalls() {
    // A non-empty frame list routes through buildCallHierarchyFromFrames; on the onlyTopCall
    // path the frames themselves are never inspected, so a bare mock frame is sufficient.
    final TraceFrame nestedFrame = mock(TraceFrame.class);
    when(mockTransactionTrace.getTraceFrames()).thenReturn(List.of(nestedFrame));
    when(mockTransaction.isContractCreation()).thenReturn(false);
    when(mockTransaction.getTo()).thenReturn(Optional.of(Address.fromHexString("0x01")));
    when(mockTransaction.getGasLimit()).thenReturn(21000L);
    when(mockResult.getGasRemaining()).thenReturn(0L);

    final TraceOptions traceOptions =
        new TraceOptions(TracerType.CALL_TRACER, null, Map.of("onlyTopCall", true));
    final DebugTraceTransactionResult result =
        DebugTraceTransactionStepFactory.create(traceOptions, mockProtocolSpec)
            .apply(mockTransactionTrace);

    assertThat(result.getResult()).isInstanceOf(CallTracerResult.class);
    final CallTracerResult callResult = (CallTracerResult) result.getResult();
    // Root frame is populated from the transaction/result...
    assertThat(callResult.getType()).isNotNull();
    assertThat(callResult.getFrom()).isNotNull();
    // ...but no nested calls are reported, and onlyTopCall collects no subcall frames at all,
    // so the trace frames are never inspected.
    assertThat(callResult.getCalls()).isNullOrEmpty();
    verifyNoInteractions(nestedFrame);
  }

  @Test
  @DisplayName("should produce same result type as synchronous version")
  void shouldProduceSameResultTypeAsSynchronousVersion() throws Exception {
    // Given
    TracerType tracerType = TracerType.OPCODE_TRACER;
    TraceOptions traceOptions = new TraceOptions(tracerType, null, null);
    Function<TransactionTrace, DebugTraceTransactionResult> syncFunction =
        DebugTraceTransactionStepFactory.create(traceOptions, mockProtocolSpec);
    Function<TransactionTrace, CompletableFuture<DebugTraceTransactionResult>> asyncFunction =
        DebugTraceTransactionStepFactory.createAsync(traceOptions, mockProtocolSpec);

    // When
    DebugTraceTransactionResult syncResult = syncFunction.apply(mockTransactionTrace);
    DebugTraceTransactionResult asyncResult = asyncFunction.apply(mockTransactionTrace).get();

    // Then
    assertThat(asyncResult.getTxHash()).isEqualTo(syncResult.getTxHash());
    assertThat(asyncResult.getResult().getClass()).isEqualTo(syncResult.getResult().getClass());
  }
}
