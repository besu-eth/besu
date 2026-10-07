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

import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.AbstractBlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.BlockGasAccountingStrategy;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.gascalculator.StateGasCostCalculator;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;

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

  private static BlockStateCallSimulationResult resultWith(
      final BlockGasAccountingStrategy strategy, final long blockGasLimit) {
    return resultWith(strategy, blockGasLimit, Long.MAX_VALUE);
  }

  private static BlockStateCallSimulationResult resultWith(
      final BlockGasAccountingStrategy strategy, final long blockGasLimit, final long gasBudget) {
    ProtocolSpec protocolSpec = mock(ProtocolSpec.class);
    AbstractBlockProcessor.TransactionReceiptFactory receiptFactory =
        mock(AbstractBlockProcessor.TransactionReceiptFactory.class);
    TransactionReceipt receipt = mock(TransactionReceipt.class);
    when(receipt.getLogsList()).thenReturn(List.of());
    when(receiptFactory.create(any(), any(), any(), anyLong())).thenReturn(receipt);
    StateGasCostCalculator stateGasCostCalculator = mock(StateGasCostCalculator.class);
    when(stateGasCostCalculator.transactionExecutionGasLimit()).thenReturn(TX_EXECUTION_GAS_LIMIT);
    GasCalculator gasCalculator = mock(GasCalculator.class);
    when(gasCalculator.stateGasCostCalculator()).thenReturn(stateGasCostCalculator);
    when(protocolSpec.getTransactionReceiptFactory()).thenReturn(receiptFactory);
    when(protocolSpec.getBlockGasAccountingStrategy()).thenReturn(strategy);
    when(protocolSpec.getGasCalculator()).thenReturn(gasCalculator);
    return new BlockStateCallSimulationResult(protocolSpec, blockGasLimit, gasBudget);
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
