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
      final long effectiveStateGas = stateGasUsed();
      return new GasResult(effectiveStateGas, txGasLimit(), txGasLimit());
    }

    // EIP-8037: gasUsedByTransaction (used for block-regular accounting via
    // BlockGasAccountingStrategy.AMSTERDAM = gasUsedByTransaction - stateGasUsed) reflects ONLY
    // legitimate consumption: actual state growth + actual regular execution. Burned gas-left
    // spill (charge drained from gasRemaining in a reverted frame, cancelled by an in-scope
    // no-growth refund) is sender-paid (visible in receipt cumulative via the refund
    // calculation) but excluded from block-regular — revert spillover never increments
    // regular_gas_used. The reservoir burn (drain that stays paid because the matching refund
    // was eaten by a reverted ancestor's incorporate-on-error) is similarly excluded from block
    // dimensions: it belongs to neither block_regular nor block_state, only to the receipt-level
    // cumulative via state_gas_left = 0.
    final long executionGas = txGasLimit() - remainingGas() - stateGasReservoir();
    final long stateGas = stateGasUsed();
    final long regularGas =
        executionGas - stateGas - stateGasSpillBurned() - initialFrameRegularHaltBurn();
    if (regularGas < 0) {
      LOG.error(
          "Negative regularGas={} (executionGas={}, stateGas={}, spillBurned={})",
          regularGas,
          executionGas,
          stateGas,
          stateGasSpillBurned());
    }
    final long gasUsedByTransaction = Math.max(regularGas, floorCost()) + stateGas;
    final long usedGas = txGasLimit() - refundedGas();
    return new GasResult(stateGas, gasUsedByTransaction, usedGas);
  }
}
