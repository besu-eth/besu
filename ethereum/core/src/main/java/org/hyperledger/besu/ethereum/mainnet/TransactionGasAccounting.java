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
 * multidimensional gas under the frame-end accounting model from PR 11573.
 *
 * <p>This extracts the gas computation from {@link MainnetTransactionProcessor} into a testable,
 * stateless helper. Uses a generated builder (via Immutables) to prevent parameter ordering
 * mistakes — the many long fields are easily confused without named setters.
 *
 * <p>Usage: {@code TransactionGasAccounting.builder().txGasLimit(...).remainingGas(...)...
 * .build().calculate()}
 */
@Value.Immutable
public abstract class TransactionGasAccounting {

  private static final Logger LOG = LoggerFactory.getLogger(TransactionGasAccounting.class);

  /** Result of the gas accounting calculation. */
  public record GasResult(long effectiveStateGas, long gasUsedByTransaction, long usedGas) {}

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
   * <p>Two paths:
   *
   * <ul>
   *   <li><b>regularGasLimitExceeded=true:</b> All gas consumed. effectiveStateGas = stateGasUsed.
   *   <li><b>regularGasLimitExceeded=false:</b> Computes executionGas, stateGas, regularGas via the
   *       frame-end accounting invariant: regularGas = executionGas - stateGas (no spill
   *       bookkeeping under PR 11573 since opcodes never charge state gas inline). Floor cost
   *       applies to regularGas only.
   * </ul>
   *
   * @return the gas result containing effectiveStateGas, gasUsedByTransaction, and usedGas
   */
  public GasResult calculate() {
    if (regularGasLimitExceeded()) {
      final long effectiveStateGas = stateGasUsed();
      return new GasResult(effectiveStateGas, txGasLimit(), txGasLimit());
    }

    // EIP-8037: Include leftover reservoir in remaining gas for execution gas calculation.
    final long executionGas = txGasLimit() - remainingGas() - stateGasReservoir();
    // Under PR 11573 frame-end accounting: stateGasUsed captures the entire state-gas tally
    // (intrinsic + frame-end aggregates). regularGas is what's left.
    final long stateGas = stateGasUsed();
    final long regularGas = executionGas - stateGas;
    if (regularGas < 0) {
      // This should not happen under normal circumstances. A negative regularGas indicates a
      // bug in gas accounting — log at error level to ensure visibility.
      LOG.error(
          "Negative regularGas={} (executionGas={}, stateGas={})",
          regularGas,
          executionGas,
          stateGas);
    }
    final long gasUsedByTransaction = Math.max(regularGas, floorCost()) + stateGas;
    final long usedGas = txGasLimit() - refundedGas();
    return new GasResult(stateGas, gasUsedByTransaction, usedGas);
  }
}
