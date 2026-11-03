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
        .executionGasLimitExceeded(false);
  }

  @Test
  public void normalPath_executionGasComputedCorrectly() {
    // Simple execution: 100k gas limit, 30k remaining, no reservoir, no state gas
    final var result =
        baseBuilder()
            .txGasLimit(100_000L)
            .remainingGas(30_000L)
            .refundedGas(5_000L)
            .build()
            .calculate();

    // consumedGas = 100k - 30k - 0 = 70k
    // stateGas = 0, executionGas = 70k
    // totalGasBudget = max(consumedGas 70k, floor 0) = 70k
    // usedGas = 100k - 5k = 95k
    assertThat(result.totalGasBudget()).isEqualTo(70_000L);
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

    // consumedGas = 100k - 20k - 10k = 70k
    // stateGas = 10k, executionGas = 70k - 10k = 60k
    // totalGasBudget = max(consumedGas 70k, floor 0) = 70k
    // usedGas = 100k - 5k = 95k
    assertThat(result.totalGasBudget()).isEqualTo(70_000L);
    assertThat(result.usedGas()).isEqualTo(95_000L);
  }

  @Test
  public void floorCostOverridesExecutionGas() {
    // Floor cost higher than actual execution gas
    final var result =
        baseBuilder()
            .txGasLimit(100_000L)
            .remainingGas(60_000L)
            .floorCost(50_000L)
            .build()
            .calculate();

    // consumedGas = 100k - 60k - 0 = 40k
    // floorCost = 50k
    // totalGasBudget = max(consumedGas 40k, floor 50k) = 50k
    assertThat(result.totalGasBudget()).isEqualTo(50_000L);
    assertThat(result.usedGas()).isEqualTo(100_000L);
  }

  @Test
  public void floorCostAppliesToTotalGasNotToExecutionGasPlusStateGas() {
    final var result =
        baseBuilder()
            .txGasLimit(300_000L)
            .remainingGas(43_295L)
            .stateGasUsed(232_560L)
            .floorCost(24_576L)
            .build()
            .calculate();

    // consumedGas = 256,705; executionGas = 24,145 < floorCost 24,576 < consumedGas
    assertThat(result.totalGasBudget()).isEqualTo(256_705L);
    assertThat(result.executionGas()).isEqualTo(24_576L);
    assertThat(result.effectiveStateGas()).isEqualTo(232_560L);
  }

  @Test
  public void zeroStateGas_preAmsterdamEquivalent() {
    final var result =
        baseBuilder()
            .txGasLimit(100_000L)
            .remainingGas(40_000L)
            .refundedGas(10_000L)
            .build()
            .calculate();

    // consumedGas = 100k - 40k = 60k
    // executionGas = 60k, totalGasBudget = 60k, usedGas = 100k - 10k = 90k
    assertThat(result.totalGasBudget()).isEqualTo(60_000L);
    assertThat(result.usedGas()).isEqualTo(90_000L);
  }

  @Test
  public void build_failsWhenFieldMissing() {
    assertThatThrownBy(() -> TransactionGasAccounting.builder().txGasLimit(100_000L).build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("remainingGas");
  }
}
