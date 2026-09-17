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
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Manages the results of simulating block calls, including a list of simulation results and
 * tracking cumulative gas used.
 */
public class BlockCallSimulationResult {
  private final List<TransactionSimulatorResultWithMetadata> transactionSimulatorResults =
      new ArrayList<>();
  private long cumulativeGasUsed = 0;
  private final AbstractBlockProcessor.TransactionReceiptFactory transactionReceiptFactory;
  private final long blockGasLimit;

  public BlockCallSimulationResult(final ProtocolSpec protocolSpec, final long blockGasLimit) {
    this.transactionReceiptFactory = protocolSpec.getTransactionReceiptFactory();
    this.blockGasLimit = blockGasLimit;
  }

  public long getRemainingGas() {
    return Math.max(blockGasLimit - cumulativeGasUsed, 0);
  }

  public long getCumulativeGasUsed() {
    return cumulativeGasUsed;
  }

  /**
   * Adds a new transaction simulation result, updating the cumulative gas used.
   *
   * @param result the transaction simulation result
   * @param worldState the world state after the transaction
   */
  public void add(final TransactionSimulatorResult result, final MutableWorldState worldState) {
    Objects.requireNonNull(result, "TransactionSimulatorResult cannot be null");
    Objects.requireNonNull(worldState, "WorldState cannot be null");

    long gasUsedByTransaction = result.result().getEstimateGasUsedByTransaction();
    cumulativeGasUsed += gasUsedByTransaction;

    TransactionReceipt transactionReceipt =
        transactionReceiptFactory.create(
            result.transaction().getType(), result.result(), worldState, cumulativeGasUsed);

    List<Log> logs = transactionReceipt.getLogsList();
    transactionSimulatorResults.add(
        new TransactionSimulatorResultWithMetadata(
            result, transactionReceipt, cumulativeGasUsed, logs));
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

  /** Represents a single block call simulation result with metadata. */
  public record TransactionSimulatorResultWithMetadata(
      TransactionSimulatorResult result,
      TransactionReceipt receipt,
      long cumulativeGasUsed,
      List<Log> logs) {}
}
