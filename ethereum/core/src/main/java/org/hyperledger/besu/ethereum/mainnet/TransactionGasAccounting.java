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

  /** State gas spilled from the initial frame's own revert/halt. */
  public abstract long initialFrameStateGasSpill();

  /** Total state gas spilled into gasRemaining from reverted frames. */
  public abstract long stateGasSpillBurned();

  /**
   * Gas that was sitting unused in the initial frame's gasRemaining at the moment of an exceptional
   * halt (EIP-7778/EIP-8037). Paid by the sender (receipts) but must be excluded from block regular
   * gas since no operation consumed it.
   */
  @Value.Default
  public long initialFrameRegularHaltBurn() {
    return 0L;
  }

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
   *   <li><b>regularGasLimitExceeded=true:</b> All gas consumed. effectiveStateGas = stateGasUsed +
   *       initialFrameStateGasSpill.
   *   <li><b>regularGasLimitExceeded=false:</b> Computes executionGas, stateGas, regularGas with
   *       double-counting avoidance. Floor cost applies to regularGas only.
   * </ul>
   *
   * @return the gas result containing effectiveStateGas, gasUsedByTransaction, and usedGas
   */
  public GasResult calculate() {
    if (regularGasLimitExceeded()) {
      final long effectiveStateGas = stateGasUsed() + initialFrameStateGasSpill();
      return new GasResult(effectiveStateGas, txGasLimit(), txGasLimit());
    }

    // EIP-8037: Include leftover reservoir in remaining gas for execution gas calculation
    final long executionGas = txGasLimit() - remainingGas() - stateGasReservoir();
    // EIP-8037: Floor applies to regular gas only, not total gas.
    // Pre-Amsterdam: stateGasUsed=0, spillBurned=0 — identical.
    // stateGasSpillBurned: state gas that spilled into gasRemaining from reverted frames.
    // For child frame reverts: not metered as regular or state gas (invisible to block).
    // For initial frame revert/halt: counts as state gas for block accounting, because the
    // transaction's state gas consumption is final regardless of execution outcome.
    final long stateGas = stateGasUsed() + initialFrameStateGasSpill();
    // initialFrameStateGasSpill is already included in spillBurned AND stateGas,
    // so subtract it from spillBurned to avoid double-counting.
    final long regularGas =
        executionGas
            - stateGas
            - (stateGasSpillBurned() - initialFrameStateGasSpill())
            - initialFrameRegularHaltBurn();
    if (regularGas < 0) {
      // This should not happen under normal circumstances. A negative regularGas indicates a
      // bug in gas accounting — log at error level to ensure visibility.
      LOG.error(
          "Negative regularGas={} (executionGas={}, stateGas={}, spillBurned={}, initialSpill={})",
          regularGas,
          executionGas,
          stateGas,
          stateGasSpillBurned(),
          initialFrameStateGasSpill());
    }
    final long gasUsedByTransaction = Math.max(regularGas, floorCost()) + stateGas;
    final long usedGas = txGasLimit() - refundedGas();
    return new GasResult(stateGas, gasUsedByTransaction, usedGas);
  }
}
