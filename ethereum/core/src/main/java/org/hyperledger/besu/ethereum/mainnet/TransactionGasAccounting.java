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
package org.hyperledger.besu.ethereum.mainnet;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates the gas accounting logic for transaction processing, including EIP-8037
 * multidimensional gas.
 *
 * <p>This extracts the complex gas computation from {@link MainnetTransactionProcessor} into a
 * testable, stateless helper. Uses a generated builder (via Immutables) to prevent parameter
 * ordering mistakes — the many long fields are easily confused without named setters.
 *
 * <p>Usage: {@code TransactionGasAccounting.builder().txGasLimit(...).remainingGas(...)...
 * .build().calculate()}
 */
@Value.Immutable
public abstract class TransactionGasAccounting {

  private static final Logger LOG = LoggerFactory.getLogger(TransactionGasAccounting.class);

  /**
   * Result of the gas accounting calculation.
   *
   * @param effectiveStateGas the state gas dimension
   * @param gasUsedByTransaction floored 2D gas (max(regular, floor) + state) for
   *     estimation/receipts
   * @param usedGas post-refund gas the sender pays
   * @param regularGas the unfloored regular gas dimension (execution - state) for block accounting
   */
  public record GasResult(
      long effectiveStateGas, long gasUsedByTransaction, long usedGas, long regularGas) {}

  /** The transaction gas limit. */
  public abstract long txGasLimit();

  /** Gas remaining in the initial frame after execution. */
  public abstract long remainingGas();

  /** Leftover state gas reservoir in the initial frame. */
  public abstract long stateGasReservoir();

  /** State gas consumed by the initial frame. */
  public abstract long stateGasUsed();

  /** Gas refunded to the sender. */
  public abstract long refundedGas();

  /** Transaction floor cost (EIP-7623), 0 for pre-Prague. */
  public abstract long floorCost();

  /** Whether the regular gas limit was exceeded (EIP-8037). */
  public abstract boolean regularGasLimitExceeded();

  /** Creates a new builder. */
  public static ImmutableTransactionGasAccounting.Builder builder() {
    return ImmutableTransactionGasAccounting.builder();
  }

  /**
   * Calculate gas accounting for a completed transaction.
   *
   * @return the gas result containing effectiveStateGas, gasUsedByTransaction, and usedGas
   */
  public GasResult calculate() {
    if (regularGasLimitExceeded()) {
      return new GasResult(
          stateGasUsed(), txGasLimit(), txGasLimit(), Math.max(0L, txGasLimit() - stateGasUsed()));
    }

    final long executionGas = txGasLimit() - remainingGas() - stateGasReservoir();
    final long stateGas = stateGasUsed();
    final long regularGas = executionGas - stateGas;
    if (regularGas < 0) {
      LOG.error(
          "Negative regularGas={} (executionGas={}, stateGas={})",
          regularGas,
          executionGas,
          stateGas);
    }
    final long gasUsedByTransaction = Math.max(regularGas, floorCost()) + stateGas;
    final long usedGas = txGasLimit() - refundedGas();
    return new GasResult(stateGas, gasUsedByTransaction, usedGas, Math.max(0L, regularGas));
  }
}
