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

import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.AbstractBlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.BlockGasAccountingStrategy;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.tracing.EthTransferLogOperationTracer;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Manages the results of simulating block calls, including a list of simulation results and
 * tracking cumulative gas used.
 */
public class BlockStateCallSimulationResult {
  private final List<TransactionSimulatorResultWithMetadata> transactionSimulatorResults =
      new ArrayList<>();
  private long cumulativeGasUsed = 0;
  private long cumulativeExecutionGasUsed = 0;
  private long cumulativeStateGasUsed = 0;
  private Optional<BlockAccessList> blockAccessList = Optional.empty();
  private final AbstractBlockProcessor.TransactionReceiptFactory transactionReceiptFactory;
  private final long blockGasLimit;
  private final long gasBudget;
  private long blobCount = 0;
  private final GasCalculator gasCalculator;
  private final BlockGasAccountingStrategy blockGasAccountingStrategy;
  private final long transactionExecutionGasLimit;

  /**
   * Creates a result for one simulated block.
   *
   * @param protocolSpec the protocol spec of the block
   * @param blockGasLimit the gas limit of the block header
   * @param gasBudget the gas that the calls of the block may use in total, as receipt gas
   */
  public BlockStateCallSimulationResult(
      final ProtocolSpec protocolSpec, final long blockGasLimit, final long gasBudget) {
    this.transactionReceiptFactory = protocolSpec.getTransactionReceiptFactory();
    this.blockGasLimit = blockGasLimit;
    this.gasBudget = gasBudget;
    this.gasCalculator = protocolSpec.getGasCalculator();
    this.blockGasAccountingStrategy = protocolSpec.getBlockGasAccountingStrategy();
    this.transactionExecutionGasLimit =
        gasCalculator.stateGasCostCalculator().transactionExecutionGasLimit();
  }

  /**
   * Returns the largest gas limit that the block can still include, as the fork's block gas
   * accounting checks it, and that the remaining gas budget allows.
   *
   * @return the remaining gas of the block
   */
  public long getRemainingGas() {
    final long remainingExecutionGas = Math.max(blockGasLimit - cumulativeExecutionGasUsed, 0);
    final long remainingStateGas = Math.max(blockGasLimit - cumulativeStateGasUsed, 0);
    final long remainingBlockGas =
        blockGasAccountingStrategy.hasBlockCapacity(
                remainingStateGas,
                transactionExecutionGasLimit,
                cumulativeExecutionGasUsed,
                cumulativeStateGasUsed,
                blockGasLimit)
            ? remainingStateGas
            : Math.min(remainingExecutionGas, remainingStateGas);
    return Math.min(remainingBlockGas, Math.max(gasBudget - cumulativeGasUsed, 0));
  }

  public long getCumulativeGasUsed() {
    return cumulativeGasUsed;
  }

  /**
   * Returns the block header gas used, computed with the block gas accounting of the fork.
   *
   * @return the block gas used
   */
  public long getBlockGasUsed() {
    return blockGasAccountingStrategy.effectiveGasUsed(
        cumulativeExecutionGasUsed, cumulativeStateGasUsed);
  }

  public long getCumulativeBlobGasUsed() {
    return gasCalculator.blobGasCost(blobCount);
  }

  /**
   * Adds a new transaction simulation result, updating the cumulative gas used.
   *
   * @param result the transaction simulation result
   * @param worldState the world state after the transaction
   * @param operationTracer the tracer used for the transaction; if it is an {@link
   *     EthTransferLogOperationTracer}, its logs are used in place of the receipt logs
   */
  public void add(
      final TransactionSimulatorResult result,
      final MutableWorldState worldState,
      final OperationTracer operationTracer) {
    Objects.requireNonNull(result, "TransactionSimulatorResult cannot be null");
    Objects.requireNonNull(worldState, "WorldState cannot be null");

    long gasUsedByTransaction = result.getGasEstimate();
    cumulativeGasUsed += gasUsedByTransaction;
    cumulativeExecutionGasUsed +=
        blockGasAccountingStrategy.calculateTransactionExecutionGas(
            result.transaction(), result.result());
    cumulativeStateGasUsed += result.result().getStateGasUsed();

    if (result.transaction().getType().supportsBlob()) {
      blobCount += result.transaction().getBlobCount();
    }

    TransactionReceipt transactionReceipt =
        transactionReceiptFactory.create(
            result.transaction().getType(), result.result(), worldState, cumulativeGasUsed);

    List<Log> logs =
        (operationTracer instanceof EthTransferLogOperationTracer tracer)
            ? tracer.getLogs()
            : transactionReceipt.getLogsList();

    transactionSimulatorResults.add(
        new TransactionSimulatorResultWithMetadata(
            result, transactionReceipt, cumulativeGasUsed, logs));
  }

  public void set(final BlockAccessList blockAccessList) {
    this.blockAccessList = Optional.of(blockAccessList);
  }

  public List<Transaction> getTransactions() {
    return transactionSimulatorResults.stream()
        .map(result -> result.result().transaction())
        .collect(Collectors.toList());
  }

  public List<TransactionReceipt> getReceipts() {
    return transactionSimulatorResults.stream()
        .map(TransactionSimulatorResultWithMetadata::receipt)
        .collect(Collectors.toList());
  }

  public List<TransactionSimulatorResult> getTransactionSimulationResults() {
    return transactionSimulatorResults.stream()
        .map(TransactionSimulatorResultWithMetadata::result)
        .collect(Collectors.toList());
  }

  public List<TransactionSimulatorResultWithMetadata> getTransactionSimulatorResults() {
    return transactionSimulatorResults;
  }

  public Optional<BlockAccessList> getBlockAccessList() {
    return blockAccessList;
  }

  /** Represents a single block call simulation result with metadata. */
  public record TransactionSimulatorResultWithMetadata(
      TransactionSimulatorResult result,
      TransactionReceipt receipt,
      long cumulativeGasUsed,
      List<Log> logs) {}
}
