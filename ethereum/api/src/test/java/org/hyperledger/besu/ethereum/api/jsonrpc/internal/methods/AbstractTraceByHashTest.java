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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.BlockTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.BlockTracer;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.Tracer;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.flat.FlatTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.flat.FlatTraceGenerator;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.TransactionWithMetadata;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.blockhash.PreExecutionProcessor;
import org.hyperledger.besu.ethereum.mainnet.systemcall.BlockProcessingContext;
import org.hyperledger.besu.ethereum.vm.DebugOperationTracer;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AbstractTraceByHashTest {

  @Mock private Supplier<BlockTracer> blockTracerSupplier;
  @Mock private BlockTracer blockTracer;
  @Mock private BlockchainQueries blockchainQueries;
  @Mock private Blockchain blockchain;
  @Mock private ProtocolSchedule protocolSchedule;
  @Mock private ProtocolSpec protocolSpec;
  @Mock private PreExecutionProcessor preExecutionProcessor;
  @Mock private BlockHashLookup blockHashLookup;
  @Mock private MutableWorldState mutableWorldState;
  @Mock private WorldUpdater worldUpdater;
  @Mock private TransactionWithMetadata transactionWithMetadata;
  @Mock private Block block;
  @Mock private BlockHeader blockHeader;
  @Mock private BlockBody blockBody;
  @Mock private Transaction transaction;
  @Mock private TransactionTrace transactionTrace;
  @Mock private FlatTrace flatTrace;

  private AbstractTraceByHash traceByHash;

  @BeforeEach
  void setUp() {
    traceByHash = new TraceTransaction(blockTracerSupplier, protocolSchedule, blockchainQueries);
  }

  @Test
  void reusesPreExecutedWorldStateForBlockTracing() {
    final Hash transactionHash = Hash.fromHexStringLenient("0x01");
    final Hash parentHash = Hash.fromHexStringLenient("0x02");
    final long blockNumber = 3L;

    when(blockchainQueries.transactionByHash(transactionHash))
        .thenReturn(Optional.of(transactionWithMetadata));
    when(transactionWithMetadata.getBlockNumber()).thenReturn(Optional.of(blockNumber));
    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getBlockByNumber(blockNumber)).thenReturn(Optional.of(block));
    when(block.getBody()).thenReturn(blockBody);
    when(blockBody.getTransactions()).thenReturn(List.of(transaction));
    when(block.getHeader()).thenReturn(blockHeader);
    when(blockHeader.getParentHash()).thenReturn(parentHash);
    when(blockchainQueries.getProtocolSpec(blockHeader)).thenReturn(protocolSpec);
    when(protocolSpec.getPreExecutionProcessor()).thenReturn(preExecutionProcessor);
    when(preExecutionProcessor.createBlockHashLookup(blockchain, blockHeader))
        .thenReturn(blockHashLookup);
    when(blockchainQueries.getAndMapWorldState(eq(parentHash), any()))
        .thenAnswer(
            invocation -> {
              final Function<MutableWorldState, ? extends Optional<Stream<FlatTrace>>> mapper =
                  invocation.getArgument(1);
              return mapper.apply(mutableWorldState);
            });
    when(blockTracerSupplier.get()).thenReturn(blockTracer);
    when(blockTracer.trace(
            any(Tracer.TraceableState.class), same(block), any(DebugOperationTracer.class)))
        .thenReturn(Optional.of(new BlockTrace(List.of(transactionTrace))));
    when(transactionTrace.getTransaction()).thenReturn(transaction);
    when(transaction.getHash()).thenReturn(transactionHash);
    when(mutableWorldState.updater()).thenReturn(worldUpdater);

    try (final MockedStatic<FlatTraceGenerator> flatTraceGenerator =
        mockStatic(FlatTraceGenerator.class)) {
      flatTraceGenerator
          .when(
              () ->
                  FlatTraceGenerator.generateFromTransactionTraceAndBlock(
                      protocolSchedule, transactionTrace, block))
          .thenReturn(Stream.of(flatTrace));

      assertThat(traceByHash.resultByTransactionHash(transactionHash)).containsExactly(flatTrace);

      flatTraceGenerator.verify(
          () ->
              FlatTraceGenerator.generateFromTransactionTraceAndBlock(
                  protocolSchedule, transactionTrace, block));
    }

    verify(blockchainQueries, times(1)).getAndMapWorldState(eq(parentHash), any());

    final ArgumentCaptor<BlockProcessingContext> contextCaptor =
        ArgumentCaptor.forClass(BlockProcessingContext.class);
    verify(preExecutionProcessor, times(1)).process(contextCaptor.capture(), eq(Optional.empty()));
    assertThat(contextCaptor.getValue().getWorldState()).isSameAs(mutableWorldState);

    final ArgumentCaptor<Tracer.TraceableState> traceableStateCaptor =
        ArgumentCaptor.forClass(Tracer.TraceableState.class);
    verify(blockTracer)
        .trace(traceableStateCaptor.capture(), same(block), any(DebugOperationTracer.class));
    assertThat(traceableStateCaptor.getValue().updater()).isSameAs(worldUpdater);
  }
}
