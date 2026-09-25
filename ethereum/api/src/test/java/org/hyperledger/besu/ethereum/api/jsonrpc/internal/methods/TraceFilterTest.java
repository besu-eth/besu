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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.FilterParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.BlockTracer;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.mainnet.BlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.MiningBeneficiaryCalculator;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.blockhash.PreExecutionProcessor;
import org.hyperledger.besu.ethereum.mainnet.systemcall.BlockProcessingContext;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TraceFilterTest {

  private TraceFilter method;

  @Mock Supplier<BlockTracer> blockTracerSupplier;
  @Mock ProtocolSchedule protocolSchedule;
  @Mock BlockchainQueries blockchainQueries;

  @Test
  public void shouldApplyPreExecutionForEveryBlockInRange() throws InterruptedException {
    final BlockDataGenerator blockGenerator = new BlockDataGenerator();
    final Block genesis = blockGenerator.genesisBlock();
    final Block firstBlock =
        blockGenerator.block(
            new BlockDataGenerator.BlockOptions()
                .setBlockNumber(1)
                .setParentHash(genesis.getHash())
                .hasTransactions(false)
                .hasOmmers(false));
    final Block secondBlock =
        blockGenerator.block(
            new BlockDataGenerator.BlockOptions()
                .setBlockNumber(2)
                .setParentHash(firstBlock.getHash())
                .hasTransactions(false)
                .hasOmmers(false));
    final Blockchain blockchain = mock(Blockchain.class);
    final ProtocolSpec firstProtocolSpec = mock(ProtocolSpec.class);
    final ProtocolSpec secondProtocolSpec = mock(ProtocolSpec.class);
    final PreExecutionProcessor firstPreExecutionProcessor = mock(PreExecutionProcessor.class);
    final PreExecutionProcessor secondPreExecutionProcessor = mock(PreExecutionProcessor.class);
    final BlockHashLookup firstBlockHashLookup = mock(BlockHashLookup.class);
    final BlockHashLookup secondBlockHashLookup = mock(BlockHashLookup.class);
    final MutableWorldState firstWorldState = mock(MutableWorldState.class);
    final MutableWorldState secondWorldState = mock(MutableWorldState.class);

    when(blockchainQueries.headBlockNumber()).thenReturn(2L);
    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getBlockByNumber(1L)).thenReturn(Optional.of(firstBlock));
    when(blockchain.getBlockByNumber(2L)).thenReturn(Optional.of(secondBlock));
    setupProtocolSpec(
        blockchain,
        firstBlock,
        firstProtocolSpec,
        firstPreExecutionProcessor,
        firstBlockHashLookup);
    setupProtocolSpec(
        blockchain,
        secondBlock,
        secondProtocolSpec,
        secondPreExecutionProcessor,
        secondBlockHashLookup);

    doAnswer(
            invocation -> {
              final Function<MutableWorldState, ?> mapper = invocation.getArgument(1);
              return mapper.apply(
                  invocation.getArgument(0).equals(firstBlock.getHeader().getParentHash())
                      ? firstWorldState
                      : secondWorldState);
            })
        .when(blockchainQueries)
        .getAndMapWorldState(any(), any());

    final NoOpMetricsSystem metricsSystem = new NoOpMetricsSystem();
    final EthScheduler ethScheduler = new EthScheduler(1, 1, 1, metricsSystem);
    method =
        new TraceFilter(protocolSchedule, blockchainQueries, 100L, metricsSystem, ethScheduler);
    final FilterParameter filterParameter =
        new FilterParameter(
            new BlockParameter(1L), new BlockParameter(2L), null, null, null, null, null, 1, 1);
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", "trace_filter", new Object[] {filterParameter}));

    try {
      final JsonRpcResponse response = method.response(request);

      assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
      assertThat((ArrayNode) ((JsonRpcSuccessResponse) response).getResult()).hasSize(1);
      assertPreExecutionContext(
          firstPreExecutionProcessor,
          firstBlock,
          firstProtocolSpec,
          firstBlockHashLookup,
          firstWorldState);
      assertPreExecutionContext(
          secondPreExecutionProcessor,
          secondBlock,
          secondProtocolSpec,
          secondBlockHashLookup,
          secondWorldState);
      verify(blockchainQueries)
          .getAndMapWorldState(eq(firstBlock.getHeader().getParentHash()), any());
      verify(blockchainQueries)
          .getAndMapWorldState(eq(secondBlock.getHeader().getParentHash()), any());
    } finally {
      ethScheduler.stop();
      ethScheduler.awaitStop();
    }
  }

  @ParameterizedTest
  @CsvSource({"0, 1001, 1000", "1, 6002, 1000", "1000, 3000, 500"})
  public void shouldFailIfParamsExceedMaxRange(
      final long fromBlock, final long toBlock, final long maxFilterRange) {
    final FilterParameter filterParameter =
        new FilterParameter(
            new BlockParameter(fromBlock),
            new BlockParameter(toBlock),
            null,
            null,
            null,
            null,
            null,
            null,
            null);

    JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", "trace_filter", new Object[] {filterParameter}));

    // Mock headBlockNumber for validation with a value higher than toBlock
    when(blockchainQueries.headBlockNumber()).thenReturn(Math.max(toBlock + 1000, 10000L));

    method =
        new TraceFilter(
            protocolSchedule,
            blockchainQueries,
            maxFilterRange,
            new NoOpMetricsSystem(),
            new DeterministicEthScheduler());

    final JsonRpcResponse response = method.response(request);
    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);

    final JsonRpcErrorResponse errorResponse = (JsonRpcErrorResponse) response;
    assertThat(errorResponse.getErrorType()).isEqualTo(RpcErrorType.EXCEEDS_RPC_MAX_BLOCK_RANGE);
  }

  private void setupProtocolSpec(
      final Blockchain blockchain,
      final Block block,
      final ProtocolSpec protocolSpec,
      final PreExecutionProcessor preExecutionProcessor,
      final BlockHashLookup blockHashLookup) {
    final BlockProcessor blockProcessor = mock(BlockProcessor.class);
    final MiningBeneficiaryCalculator miningBeneficiaryCalculator =
        mock(MiningBeneficiaryCalculator.class);
    when(blockchainQueries.getProtocolSpec(block.getHeader())).thenReturn(protocolSpec);
    when(protocolSchedule.getByBlockHeader(block.getHeader())).thenReturn(protocolSpec);
    when(protocolSpec.getPreExecutionProcessor()).thenReturn(preExecutionProcessor);
    when(preExecutionProcessor.createBlockHashLookup(blockchain, block.getHeader()))
        .thenReturn(blockHashLookup);
    when(protocolSpec.getBlockReward()).thenReturn(Wei.ZERO);
    when(protocolSpec.getBlockProcessor()).thenReturn(blockProcessor);
    when(blockProcessor.getCoinbaseReward(Wei.ZERO, block.getHeader().getNumber(), 0))
        .thenReturn(Wei.ZERO);
    when(protocolSpec.getMiningBeneficiaryCalculator()).thenReturn(miningBeneficiaryCalculator);
    when(miningBeneficiaryCalculator.calculateBeneficiary(block.getHeader()))
        .thenReturn(Address.ZERO);
  }

  private void assertPreExecutionContext(
      final PreExecutionProcessor preExecutionProcessor,
      final Block block,
      final ProtocolSpec protocolSpec,
      final BlockHashLookup blockHashLookup,
      final MutableWorldState worldState) {
    final ArgumentCaptor<BlockProcessingContext> contextCaptor =
        ArgumentCaptor.forClass(BlockProcessingContext.class);
    verify(preExecutionProcessor).process(contextCaptor.capture(), eq(Optional.empty()));
    assertThat(contextCaptor.getValue().getBlockHeader()).isEqualTo(block.getHeader());
    assertThat(contextCaptor.getValue().getProtocolSpec()).isSameAs(protocolSpec);
    assertThat(contextCaptor.getValue().getBlockHashLookup()).isSameAs(blockHashLookup);
    assertThat(contextCaptor.getValue().getWorldState()).isSameAs(worldState);
    assertThat(contextCaptor.getValue().getOperationTracer()).isSameAs(OperationTracer.NO_TRACING);
    assertThat(contextCaptor.getValue().getBlockAccessListBuilder()).isEmpty();
  }
}
