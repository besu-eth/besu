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
package org.hyperledger.besu.ethereum.transaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.AbstractBlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.BlockGasAccountingStrategy;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.gascalculator.StateGasCostCalculator;
import org.hyperledger.besu.evm.tracing.EthTransferLogOperationTracer;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

public class BlockStateCallSimulationResultTest {
  private static final long TX_EXECUTION_GAS_LIMIT = 1L << 24;

  @Test
  public void amsterdamBlockGasUsedIsTheLargerDimension() {
    BlockStateCallSimulationResult result =
        resultWith(BlockGasAccountingStrategy.AMSTERDAM, 100_000_000L);

    result.add(
        call(3_900_000L, 1_000_000L, 3_000_000L),
        mock(MutableWorldState.class),
        OperationTracer.NO_TRACING);

    assertThat(result.getBlockGasUsed()).isEqualTo(3_000_000L);
    assertThat(result.getCumulativeGasUsed()).isEqualTo(3_900_000L);
  }

  @Test
  public void amsterdamRemainingGasIsRemainingStateGasWhenExecutionGasFitsTheCap() {
    BlockStateCallSimulationResult result =
        resultWith(BlockGasAccountingStrategy.AMSTERDAM, 100_000_000L);

    result.add(
        call(10_000_000L, 10_000_000L, 0L),
        mock(MutableWorldState.class),
        OperationTracer.NO_TRACING);

    assertThat(result.getRemainingGas()).isEqualTo(100_000_000L);
  }

  @Test
  public void amsterdamRemainingGasIsTheSmallerDimensionWithoutTheExecutionGasCap() {
    BlockStateCallSimulationResult result =
        resultWith(BlockGasAccountingStrategy.AMSTERDAM, 100_000_000L, Long.MAX_VALUE, true);

    result.add(
        call(10_000_000L, 10_000_000L, 0L),
        mock(MutableWorldState.class),
        OperationTracer.NO_TRACING);

    assertThat(result.getRemainingGas()).isEqualTo(90_000_000L);
  }

  @Test
  public void amsterdamRemainingGasIsTheSmallerDimensionBelowTheCap() {
    BlockStateCallSimulationResult result =
        resultWith(BlockGasAccountingStrategy.AMSTERDAM, 20_000_000L);

    result.add(
        call(11_000_000L, 10_000_000L, 1_000_000L),
        mock(MutableWorldState.class),
        OperationTracer.NO_TRACING);

    assertThat(result.getRemainingGas()).isEqualTo(10_000_000L);
  }

  @Test
  public void remainingGasStopsAtTheGasBudgetInReceiptGas() {
    BlockStateCallSimulationResult result =
        resultWith(BlockGasAccountingStrategy.AMSTERDAM, 100_000_000L, 20_000_000L);

    result.add(
        call(20_000_000L, 10_000_000L, 10_000_000L),
        mock(MutableWorldState.class),
        OperationTracer.NO_TRACING);

    assertThat(result.getRemainingGas()).isZero();
  }

  @Test
  public void frontierUsesReceiptGasForBothValues() {
    BlockStateCallSimulationResult result =
        resultWith(BlockGasAccountingStrategy.FRONTIER, 30_000_000L);

    result.add(
        call(21_000L, 21_000L, 0L), mock(MutableWorldState.class), OperationTracer.NO_TRACING);

    assertThat(result.getBlockGasUsed()).isEqualTo(21_000L);
    assertThat(result.getRemainingGas()).isEqualTo(30_000_000L - 21_000L);
  }

  @Test
  public void transferTracerLogsArePreferredOverReceiptLogs() {
    final Log receiptOnlyLog = new Log(CONTRACT, Bytes.fromHexString("0xbb"), List.of());
    final BlockStateCallSimulationResult result =
        resultWith(BlockGasAccountingStrategy.FRONTIER, 30_000_000L, List.of(receiptOnlyLog));

    final EthTransferLogOperationTracer tracer = new EthTransferLogOperationTracer();
    final MessageFrame frame = mock(MessageFrame.class);
    when(frame.getValue()).thenReturn(Wei.of(5));
    when(frame.getSenderAddress()).thenReturn(SENDER);
    when(frame.getRecipientAddress()).thenReturn(CONTRACT);
    tracer.traceContextEnter(frame);

    result.add(call(21_000L, 21_000L, 0L), mock(MutableWorldState.class), tracer);

    final List<Log> logs = result.getTransactionSimulatorResults().getFirst().logs();
    assertThat(logs).hasSize(1);
    assertThat(logs.getFirst().getLogger())
        .isEqualTo(EthTransferLogOperationTracer.SIMULATION_TRANSFER_ADDRESS);
  }

  @Test
  public void withoutTransferTracerUsesReceiptLogsOnly() {
    final Log contractLog = new Log(CONTRACT, Bytes.fromHexString("0xaa"), List.of());
    final BlockStateCallSimulationResult result =
        resultWith(BlockGasAccountingStrategy.FRONTIER, 30_000_000L, List.of(contractLog));

    result.add(
        call(21_000L, 21_000L, 0L), mock(MutableWorldState.class), OperationTracer.NO_TRACING);

    assertThat(result.getTransactionSimulatorResults().getFirst().logs())
        .containsExactly(contractLog);
  }

  private static final Address SENDER =
      Address.fromHexString("0x1111111111111111111111111111111111111111");
  private static final Address CONTRACT =
      Address.fromHexString("0x4444444444444444444444444444444444444444");

  private static BlockStateCallSimulationResult resultWith(
      final BlockGasAccountingStrategy strategy, final long blockGasLimit) {
    return resultWith(strategy, blockGasLimit, Long.MAX_VALUE);
  }

  private static BlockStateCallSimulationResult resultWith(
      final BlockGasAccountingStrategy strategy, final long blockGasLimit, final long gasBudget) {
    return resultWith(strategy, blockGasLimit, gasBudget, false, List.of());
  }

  private static BlockStateCallSimulationResult resultWith(
      final BlockGasAccountingStrategy strategy,
      final long blockGasLimit,
      final List<Log> receiptLogs) {
    return resultWith(strategy, blockGasLimit, Long.MAX_VALUE, false, receiptLogs);
  }

  private static BlockStateCallSimulationResult resultWith(
      final BlockGasAccountingStrategy strategy,
      final long blockGasLimit,
      final long gasBudget,
      final boolean allowExceedingExecutionGasLimit) {
    return resultWith(
        strategy, blockGasLimit, gasBudget, allowExceedingExecutionGasLimit, List.of());
  }

  private static BlockStateCallSimulationResult resultWith(
      final BlockGasAccountingStrategy strategy,
      final long blockGasLimit,
      final long gasBudget,
      final boolean allowExceedingExecutionGasLimit,
      final List<Log> receiptLogs) {
    ProtocolSpec protocolSpec = mock(ProtocolSpec.class);
    AbstractBlockProcessor.TransactionReceiptFactory receiptFactory =
        mock(AbstractBlockProcessor.TransactionReceiptFactory.class);
    TransactionReceipt receipt = mock(TransactionReceipt.class);
    when(receipt.getLogsList()).thenReturn(receiptLogs);
    when(receiptFactory.create(any(), any(), any(), anyLong())).thenReturn(receipt);
    StateGasCostCalculator stateGasCostCalculator = mock(StateGasCostCalculator.class);
    when(stateGasCostCalculator.transactionExecutionGasLimit()).thenReturn(TX_EXECUTION_GAS_LIMIT);
    GasCalculator gasCalculator = mock(GasCalculator.class);
    when(gasCalculator.stateGasCostCalculator()).thenReturn(stateGasCostCalculator);
    when(protocolSpec.getTransactionReceiptFactory()).thenReturn(receiptFactory);
    when(protocolSpec.getBlockGasAccountingStrategy()).thenReturn(strategy);
    when(protocolSpec.getGasCalculator()).thenReturn(gasCalculator);
    return new BlockStateCallSimulationResult(
        protocolSpec, blockGasLimit, gasBudget, allowExceedingExecutionGasLimit);
  }

  private static TransactionSimulatorResult call(
      final long receiptGas, final long executionGasForBlock, final long stateGas) {
    final long gasLimit = 30_000_000L;
    Transaction transaction = mock(Transaction.class);
    when(transaction.getGasLimit()).thenReturn(gasLimit);
    when(transaction.getType()).thenReturn(TransactionType.EIP1559);
    TransactionProcessingResult processingResult = mock(TransactionProcessingResult.class);
    when(processingResult.getGasRemaining()).thenReturn(gasLimit - receiptGas);
    when(processingResult.getExecutionGasUsedForBlock()).thenReturn(executionGasForBlock);
    when(processingResult.getStateGasUsed()).thenReturn(stateGas);
    return new TransactionSimulatorResult(transaction, processingResult);
  }
}
