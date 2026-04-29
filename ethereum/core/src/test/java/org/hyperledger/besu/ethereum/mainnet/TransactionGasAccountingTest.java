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
    // stateGas = 0, regularGas = 70k - 0 = 70k
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
    // stateGas = 10k, regularGas = 70k - 10k = 60k
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
            .regularGasLimitExceeded(true)
            .build()
            .calculate();

    // EIP-8037: when the regular-gas cap is exceeded, all gas is consumed and effectiveStateGas
    // == stateGasUsed.
    assertThat(result.effectiveStateGas()).isEqualTo(30_000L);
    assertThat(result.gasUsedByTransaction()).isEqualTo(100_000L);
    assertThat(result.usedGas()).isEqualTo(100_000L);
  }

  @Test
  public void zeroStateGas_preAmsterdamEquivalent() {
    // Pre-Amsterdam: stateGasUsed=0
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
  public void build_failsWhenFieldMissing() {
    assertThatThrownBy(() -> TransactionGasAccounting.builder().txGasLimit(100_000L).build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("remainingGas");
  }
}
