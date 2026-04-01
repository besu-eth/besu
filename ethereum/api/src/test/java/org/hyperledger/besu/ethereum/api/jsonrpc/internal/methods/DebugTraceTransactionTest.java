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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.Tracer;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTracer;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.OpCodeLoggerTracerResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.StructLog;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.TransactionWithMetadata;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.vm.DebugOperationTracer;
import org.hyperledger.besu.evm.precompile.PrecompileContractRegistry;
import org.hyperledger.besu.evm.tracing.TraceFrame;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

public class DebugTraceTransactionTest {

  private final BlockchainQueries blockchainQueries =
      mock(BlockchainQueries.class, Answers.RETURNS_DEEP_STUBS);
  private final BlockHeader blockHeader = mock(BlockHeader.class, Answers.RETURNS_DEEP_STUBS);
  private final MutableWorldState mutableWorldState = mock(MutableWorldState.class);
  private final TransactionTracer transactionTracer = mock(TransactionTracer.class);
  private final ProtocolSchedule protocolSchedule =
      mock(ProtocolSchedule.class, Answers.RETURNS_DEEP_STUBS);
  private final ProtocolSpec protocolSpec = mock(ProtocolSpec.class, Answers.RETURNS_DEEP_STUBS);
  private final PrecompileContractRegistry precompileContractRegistry =
      mock(PrecompileContractRegistry.class);
  private final DebugTraceTransaction debugTraceTransaction =
      new DebugTraceTransaction(blockchainQueries, transactionTracer, protocolSchedule);
  private final Transaction transaction = mock(Transaction.class);

  private final Hash blockHash =
      Hash.fromHexString("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
  private final Hash transactionHash =
      Hash.fromHexString("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

  @BeforeEach
  public void setup() {
    doAnswer(__ -> Optional.of(blockHeader))
        .when(blockchainQueries)
        .getBlockHeaderByHash(any(Hash.class));

    doAnswer(
            invocation ->
                invocation
                    .<Function<MutableWorldState, Optional<? extends JsonRpcResponse>>>getArgument(
                        1)
                    .apply(mutableWorldState))
        .when(blockchainQueries)
        .getAndMapWorldState(any(), any());

    when(blockchainQueries.getBlockchain().getBlockHeader(any(Hash.class)))
        .thenReturn(Optional.of(blockHeader));
    when(protocolSchedule.getByBlockHeader(blockHeader)).thenReturn(protocolSpec);
    when(protocolSpec.getPrecompileContractRegistry()).thenReturn(precompileContractRegistry);
  }

  @Test
  public void nameShouldBeDebugTraceTransaction() {
    assertThat(debugTraceTransaction.getName()).isEqualTo("debug_traceTransaction");
  }

  @Test
  public void shouldTraceTheTransactionUsingTheTransactionTracer() {
    final TransactionWithMetadata transactionWithMetadata =
        new TransactionWithMetadata(transaction, 12L, Optional.empty(), blockHash, 2, 0L);
    final Map<String, Boolean> map = new HashMap<>();
    map.put("disableStorage", true);
    final Object[] params = new Object[] {transactionHash, map};
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", "debug_traceTransaction", params));
    final TransactionProcessingResult result = mock(TransactionProcessingResult.class);

    final Bytes32[] stackBytes =
        new Bytes32[] {
          Bytes32.fromHexString(
              "0x0000000000000000000000000000000000000000000000000000000000000001")
        };
    final Bytes[] memoryBytes =
        new Bytes[] {
          Bytes.fromHexString("0x0000000000000000000000000000000000000000000000000000000000000002")
        };
    final TraceFrame traceFrame =
        TraceFrame.builder()
            .setPc(12)
            .setOpcode("NONE")
            .setOpcodeNumber(Integer.MAX_VALUE)
            .setGasRemaining(45L)
            .setGasCost(OptionalLong.of(56L))
            .setGasRefund(0L)
            .setDepth(2)
            .setRecipient(null)
            .setValue(Wei.ZERO)
            .setInputData(Bytes.EMPTY)
            .setOutputData(Bytes.EMPTY)
            .setStack(Optional.of(stackBytes))
            .setMemory(Optional.of(memoryBytes))
            .setWorldUpdater(null)
            .setRevertReason(Optional.of(Bytes.fromHexString("0x1122334455667788")))
            .setStackItemsProduced(0)
            .setVirtualOperation(false)
            .build();
    final List<TraceFrame> traceFrames = Collections.singletonList(traceFrame);
    final TransactionTrace transactionTrace =
        new TransactionTrace(transaction, result, traceFrames);
    when(transaction.getHash()).thenReturn(transactionHash);
    when(transaction.getGasLimit()).thenReturn(100L);
    when(result.getGasRemaining()).thenReturn(27L);
    when(result.getOutput()).thenReturn(Bytes.fromHexString("1234"));
    when(blockchainQueries.headBlockNumber()).thenReturn(12L);
    when(blockchainQueries.transactionByHash(transactionHash))
        .thenReturn(Optional.of(transactionWithMetadata));
    when(transactionTracer.traceTransaction(
            any(Tracer.TraceableState.class),
            eq(blockHash),
            eq(transactionHash),
            any(DebugOperationTracer.class)))
        .thenReturn(Optional.of(transactionTrace));
    final JsonRpcSuccessResponse response =
        (JsonRpcSuccessResponse) debugTraceTransaction.response(request);
    final OpCodeLoggerTracerResult transactionResult =
        (OpCodeLoggerTracerResult) response.getResult();

    assertThat(transactionResult.getGas()).isEqualTo(73);
    assertThat(transactionResult.getReturnValue()).isEqualTo("0x1234");
    final List<StructLog> expectedStructLogs = Collections.singletonList(new StructLog(traceFrame));
    assertThat(transactionResult.getStructLogs()).isEqualTo(expectedStructLogs);
    assertThat(transactionResult.getStructLogs().size()).isEqualTo(1);
    assertThat(transactionResult.getStructLogs().get(0).stack().length).isEqualTo(1);
    assertThat(transactionResult.getStructLogs().get(0).stack()[0])
        .isEqualTo(StructLog.toCompactHex(stackBytes[0], true));
    assertThat(transactionResult.getStructLogs().get(0).memory().length).isEqualTo(1);
    assertThat(transactionResult.getStructLogs().get(0).memory()[0])
        .isEqualTo(StructLog.toBytes32Hex(memoryBytes[0]));
  }

  @Test
  public void shouldNotTraceTheTransactionIfNotFound() {
    final Map<String, Boolean> map = new HashMap<>();
    map.put("disableStorage", true);
    final Object[] params = new Object[] {transactionHash, map};
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", "debug_traceTransaction", params));
    final TransactionProcessingResult result = mock(TransactionProcessingResult.class);

    final TraceFrame traceFrame =
        TraceFrame.builder()
            .setPc(12)
            .setOpcode("NONE")
            .setOpcodeNumber(Integer.MAX_VALUE)
            .setGasRemaining(45L)
            .setGasCost(OptionalLong.of(56L))
            .setGasRefund(0L)
            .setDepth(2)
            .setRecipient(null)
            .setValue(Wei.ZERO)
            .setInputData(Bytes.EMPTY)
            .setOutputData(Bytes.EMPTY)
            .setWorldUpdater(null)
            .setRevertReason(Optional.of(Bytes.fromHexString("0x1122334455667788")))
            .setStackItemsProduced(0)
            .setVirtualOperation(false)
            .build();
    final List<TraceFrame> traceFrames = Collections.singletonList(traceFrame);
    final TransactionTrace transactionTrace =
        new TransactionTrace(transaction, result, traceFrames);
    when(transaction.getGasLimit()).thenReturn(100L);
    when(result.getGasRemaining()).thenReturn(27L);
    when(result.getOutput()).thenReturn(Bytes.fromHexString("1234"));
    when(blockchainQueries.headBlockNumber()).thenReturn(12L);
    when(blockchainQueries.transactionByHash(transactionHash)).thenReturn(Optional.empty());
    when(transactionTracer.traceTransaction(
            any(Tracer.TraceableState.class),
            eq(blockHash),
            eq(transactionHash),
            any(DebugOperationTracer.class)))
        .thenReturn(Optional.of(transactionTrace));
    final JsonRpcSuccessResponse response =
        (JsonRpcSuccessResponse) debugTraceTransaction.response(request);

    assertThat(response.getResult()).isNull();
  }
}
