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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/** Tests for {@link TransactionGasAccounting}. */
public class TransactionGasAccountingTest {

  /** Returns a builder with all fields set to 0/false — a valid baseline. */
  private static ImmutableTransactionGasAccounting.Builder baseBuilder() {
    return TransactionGasAccounting.builder()
        .txGasLimit(0L)
        .remainingGas(0L)
        .stateGasReservoir(0L)
        .stateGasUsed(0L)
        .initialFrameStateGasSpill(0L)
        .stateGasSpillBurned(0L)
        .refundedGas(0L)
        .floorCost(0L)
        .regularGasLimitExceeded(false);
  }

  @Test
  public void normalPath_regularGasComputedCorrectly() {
    // Simple execution: 100k gas limit, 30k remaining, no reservoir, no state gas
    final var result =
        baseBuilder()
            .txGasLimit(100_000L)
            .remainingGas(30_000L)
            .refundedGas(5_000L)
            .build()
            .calculate();

    // executionGas = 100k - 30k - 0 = 70k
    // stateGas = 0, regularGas = 70k - 0 - 0 - 0 = 70k
    // gasUsedByTransaction = max(70k, 0) + 0 = 70k
    // usedGas = 100k - 5k = 95k
    assertThat(result.effectiveStateGas()).isEqualTo(0L);
    assertThat(result.gasUsedByTransaction()).isEqualTo(70_000L);
    assertThat(result.usedGas()).isEqualTo(95_000L);
  }

  @Test
  public void normalPath_withStateGas() {
    // Execution with state gas: 100k limit, 20k remaining, 10k reservoir, 10k state gas used
    final var result =
        baseBuilder()
            .txGasLimit(100_000L)
            .remainingGas(20_000L)
            .stateGasReservoir(10_000L)
            .stateGasUsed(10_000L)
            .refundedGas(5_000L)
            .build()
            .calculate();

    // executionGas = 100k - 20k - 10k = 70k
    // stateGas = 10k + 0 = 10k, regularGas = 70k - 10k - 0 - 0 = 60k
    // gasUsedByTransaction = max(60k, 0) + 10k = 70k
    // usedGas = 100k - 5k = 95k
    assertThat(result.effectiveStateGas()).isEqualTo(10_000L);
    assertThat(result.gasUsedByTransaction()).isEqualTo(70_000L);
    assertThat(result.usedGas()).isEqualTo(95_000L);
  }

  @Test
  public void floorCostOverridesRegularGas() {
    // Floor cost higher than actual regular gas
    final var result =
        baseBuilder()
            .txGasLimit(100_000L)
            .remainingGas(60_000L)
            .floorCost(50_000L)
            .build()
            .calculate();

    // executionGas = 100k - 60k - 0 = 40k
    // regularGas = 40k, floorCost = 50k -> max(40k, 50k) = 50k
    // gasUsedByTransaction = 50k + 0 = 50k
    assertThat(result.gasUsedByTransaction()).isEqualTo(50_000L);
    assertThat(result.usedGas()).isEqualTo(100_000L);
  }

  @Test
  public void regularGasLimitExceeded_allGasConsumed() {
    final var result =
        baseBuilder()
            .txGasLimit(100_000L)
            .remainingGas(20_000L)
            .stateGasReservoir(5_000L)
            .stateGasUsed(30_000L)
            .initialFrameStateGasSpill(2_000L)
            .stateGasSpillBurned(5_000L)
            .regularGasLimitExceeded(true)
            .build()
            .calculate();

    // All gas consumed when regular gas limit exceeded
    assertThat(result.effectiveStateGas()).isEqualTo(32_000L); // 30k + 2k spill
    assertThat(result.gasUsedByTransaction()).isEqualTo(100_000L);
    assertThat(result.usedGas()).isEqualTo(100_000L);
  }

  @Test
  public void stateGasSpill_doubleCountingAvoided() {
    // initialFrameStateGasSpill=3000 is included in both stateGas AND spillBurned.
    // The calculation must subtract it from spillBurned to avoid double-counting.
    final var result =
        baseBuilder()
            .txGasLimit(100_000L)
            .remainingGas(10_000L)
            .stateGasUsed(20_000L)
            .initialFrameStateGasSpill(3_000L)
            .stateGasSpillBurned(8_000L)
            .build()
            .calculate();

    // executionGas = 100k - 10k - 0 = 90k
    // stateGas = 20k + 3k = 23k
    // spillBurned correction = 8k - 3k = 5k (avoid double-counting initialFrameStateGasSpill)
    // regularGas = 90k - 23k - 5k - 0 = 62k
    // gasUsedByTransaction = max(62k, 0) + 23k = 85k
    assertThat(result.effectiveStateGas()).isEqualTo(23_000L);
    assertThat(result.gasUsedByTransaction()).isEqualTo(85_000L);
    assertThat(result.usedGas()).isEqualTo(100_000L);
  }

  @Test
  public void zeroStateGas_preAmsterdamEquivalent() {
    // Pre-Amsterdam: stateGasUsed=0, spillBurned=0
    // Should behave identically to pre-8037 gas accounting
    final var result =
        baseBuilder()
            .txGasLimit(100_000L)
            .remainingGas(40_000L)
            .refundedGas(10_000L)
            .build()
            .calculate();

    // executionGas = 100k - 40k = 60k
    // regularGas = 60k, gasUsedByTransaction = 60k, usedGas = 100k - 10k = 90k
    assertThat(result.effectiveStateGas()).isEqualTo(0L);
    assertThat(result.gasUsedByTransaction()).isEqualTo(60_000L);
    assertThat(result.usedGas()).isEqualTo(90_000L);
  }

  @Test
  public void initialFrameRegularHaltBurn_excludedFromRegularGas() {
    // EIP-3607 collision scenario: CREATE tx with gas_limit=600k halts at
    // ContractCreationProcessor.start(). chargeCreateStateGas charged 131488 state gas
    // (spilled into gasRemaining). At halt, gasRemaining=438012 was cleared by
    // clearGasRemaining() and captured into initialFrameRegularHaltBurn.
    // The sender still pays the full 600k via receipts, but block regular gas must
    // only reflect intrinsic regular (i.e. 0 executionGas attributable to the frame
    // beyond state gas and halt burn).
    final var result =
        baseBuilder()
            .txGasLimit(600_000L)
            .remainingGas(0L)
            .stateGasReservoir(0L)
            .stateGasUsed(131_488L)
            .initialFrameRegularHaltBurn(438_012L)
            .build()
            .calculate();

    // executionGas = 600k - 0 - 0 = 600000
    // stateGas = 131_488 + 0 = 131_488
    // regularGas = 600_000 - 131_488 - 0 - 438_012 = 30_500
    // gasUsedByTransaction = max(30_500, 0) + 131_488 = 161_988
    // usedGas = 600_000 - 0 = 600_000 (sender pays full gas_limit)
    assertThat(result.effectiveStateGas()).isEqualTo(131_488L);
    assertThat(result.gasUsedByTransaction()).isEqualTo(161_988L);
    assertThat(result.usedGas()).isEqualTo(600_000L);
  }

  @Test
  public void initialFrameRegularHaltBurn_defaultsToZero() {
    // When not set (pre-Amsterdam or non-halt paths), the field should default to 0
    // and have no effect on the calculation.
    final var result = baseBuilder().txGasLimit(100_000L).remainingGas(30_000L).build().calculate();

    // Same as normalPath_regularGasComputedCorrectly (without refund)
    assertThat(result.gasUsedByTransaction()).isEqualTo(70_000L);
  }

  @Test
  public void build_failsWhenFieldMissing() {
    assertThatThrownBy(() -> TransactionGasAccounting.builder().txGasLimit(100_000L).build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("remainingGas");
  }
}
