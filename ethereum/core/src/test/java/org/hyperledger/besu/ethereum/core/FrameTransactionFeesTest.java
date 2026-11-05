/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Wei;

import java.math.BigInteger;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * The EIP-8141 fee budget arithmetic, against values worked out by hand from EELS
 * calculate_frame_transaction_fees and disburse_frame_gas_fees. Every case has max_gas 100,000, a
 * base fee of 7 and, where it has blob gas, 131,072 of it at a blob base fee of 3: a required fee
 * of 700,000 without blobs and 1,093,216 with one.
 */
class FrameTransactionFeesTest {

  private static final long MAX_GAS = 100_000L;
  private static final Wei BASE_FEE = Wei.of(7);
  private static final Wei BLOB_BASE_FEE = Wei.of(3);

  @ParameterizedTest(name = "max fee {0}, tip {1}, blob gas {2}")
  @CsvSource({
    // surplus budget: the full tip of 10 per gas
    "2000000, 10, 0, 1700000, 17",
    // partial tip: 300,000 left after the required fee, 3 per gas
    "1000000, 10, 0, 1000000, 10",
    // the tip per gas rounds down: 350,000 left is 3.5 per gas
    "1050000, 10, 0, 1050000, 10",
    // no headroom: the escrow is the required fee and GASPRICE the base fee
    "700000, 10, 0, 700000, 7",
    // the blob fee comes out of the budget before the tip
    "1393216, 10, 131072, 1393216, 10",
    "1093216, 10, 131072, 1093216, 7"
  })
  void escrowCoversTheRequiredFeeAndTheTipTheBudgetAllows(
      final long maxFee,
      final long tip,
      final long blobGas,
      final long expectedMaxCost,
      final long expectedGasPrice) {
    final FrameTransactionFees.Escrow escrow =
        FrameTransactionFees.escrow(
            Wei.of(maxFee), Wei.of(tip), MAX_GAS, BASE_FEE, blobGas, BLOB_BASE_FEE);

    assertThat(escrow.maxCost()).isEqualTo(Wei.of(expectedMaxCost));
    assertThat(escrow.gasPrice()).isEqualTo(Wei.of(expectedGasPrice));
  }

  @Test
  void requiredMaxFeeIsMaxGasAtTheBaseFeePlusTheBlobFee() {
    assertThat(FrameTransactionFees.requiredMaxFee(MAX_GAS, BASE_FEE, 0L, BLOB_BASE_FEE))
        .isEqualTo(BigInteger.valueOf(700_000L));
    assertThat(FrameTransactionFees.requiredMaxFee(MAX_GAS, BASE_FEE, 131_072L, BLOB_BASE_FEE))
        .isEqualTo(BigInteger.valueOf(1_093_216L));
  }

  @Test
  void aTipOf2To256Minus1IsCappedByTheBudget() {
    final Wei maxFee = Wei.of(700_000L + 250_000L);
    final FrameTransactionFees.Escrow escrow =
        FrameTransactionFees.escrow(maxFee, Wei.MAX_WEI, MAX_GAS, BASE_FEE, 0L, BLOB_BASE_FEE);

    assertThat(escrow.maxCost()).isEqualTo(maxFee);
    assertThat(escrow.gasPrice()).isEqualTo(Wei.of(9));

    // Settlement pays the coinbase everything the escrow holds after the burn.
    final FrameTransactionFees.Disbursement disbursement =
        FrameTransactionFees.disburse(
            escrow.maxCost(), Wei.MAX_WEI, 40_000L, BASE_FEE, 0L, BLOB_BASE_FEE);
    assertThat(disbursement.priorityFeePaid()).isEqualTo(Wei.of(950_000L - 280_000L));
    assertThat(disbursement.payerRefund()).isEqualTo(Wei.ZERO);
  }

  @ParameterizedTest(name = "max cost {0}, gas used {1}")
  @CsvSource({
    // the full tip on the gas used, the unused escrow refunded
    "1700000, 40000, 0, 400000, 1020000",
    "700000, 40000, 0, 400000, 20000",
    // the tip is capped by the 280,000 the escrow holds after burning 420,000
    "700000, 60000, 0, 280000, 0",
    // all of max_gas used: the whole escrow is burned
    "700000, 100000, 0, 0, 0",
    // the blob fee is burned in full whatever gas is used
    "1393216, 40000, 131072, 400000, 320000"
  })
  void disbursementBurnsTheBaseFeeAndCapsTheTip(
      final long maxCost,
      final long gasUsed,
      final long blobGas,
      final long expectedPriorityFee,
      final long expectedRefund) {
    final FrameTransactionFees.Disbursement disbursement =
        FrameTransactionFees.disburse(
            Wei.of(maxCost), Wei.of(10), gasUsed, BASE_FEE, blobGas, BLOB_BASE_FEE);

    assertThat(disbursement.priorityFeePaid()).isEqualTo(Wei.of(expectedPriorityFee));
    assertThat(disbursement.payerRefund()).isEqualTo(Wei.of(expectedRefund));
  }

  @Test
  void effectivePriorityFeeOfAFrameTransactionIsTheTipItsBudgetAllows() {
    final Transaction transaction = KeyedNonceGasCapVectors.atCapStandard();
    final long maxGas = transaction.getGasLimit();
    final Transaction tipping =
        Transaction.builder()
            .copiedFrom(transaction)
            .maxPriorityFeePerGas(Wei.of(10))
            .maxFee(Wei.of(7).multiply(maxGas).add(Wei.of(3).multiply(maxGas)))
            .build();

    assertThat(tipping.getEffectivePriorityFeePerGas(Optional.of(Wei.of(7)))).isEqualTo(Wei.of(3));
    assertThat(tipping.getEffectiveGasPrice(Optional.of(Wei.of(7)))).isEqualTo(Wei.of(10));
    // a base fee the budget cannot cover leaves no tip
    assertThat(tipping.getEffectivePriorityFeePerGas(Optional.of(Wei.of(11)))).isEqualTo(Wei.ZERO);
  }
}
