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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.hyperledger.besu.config.BlobSchedule;
import org.hyperledger.besu.ethereum.GasLimitCalculator;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.evm.gascalculator.AmsterdamGasCalculator;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AmsterdamTargetingGasLimitCalculatorTest {

  // Amsterdam: target=14, max=21, baseFeeUpdateFraction=11684671 (EIP-7918)
  private static final long TARGET_BLOB_GAS_PER_BLOCK = 0x1C0000; // 14 * 131072
  private static final int TARGET_BLOBS = 14;
  private static final int MAX_BLOBS = 21;
  private static final AmsterdamGasCalculator gasCalculator = new AmsterdamGasCalculator();
  private final AmsterdamTargetingGasLimitCalculator gasLimitCalculator =
      new AmsterdamTargetingGasLimitCalculator(
          0L,
          FeeMarket.cancun(0L, Optional.empty(), BlobSchedule.BPO2_DEFAULT),
          gasCalculator,
          MAX_BLOBS,
          TARGET_BLOBS,
          OptionalInt.empty(),
          OptionalInt.empty());

  /**
   * Test Amsterdam excessBlobGas calculation using the standard Cancun formula branch. With
   * base_fee_per_gas=7 and Amsterdam's baseFeeUpdateFraction=11684671, the blob gas price is 1 at
   * low excess values, so base_blob_tx_price (7*8192=57344) < target_blob_gas_price
   * (131072*1=131072) and the standard formula applies: excess = parentExcess + parentBlobGasUsed -
   * targetBlobGasPerBlock.
   *
   * <p>Test values derived from hive test: test_invalid_blob_gas_used_in_header
   * [fork_Amsterdam-new_blobs_1-header_blob_gas_used_1703936-blockchain_test_engine-parent_blobs_0]
   * where parent_excess_blobs=18 (=(21+14)//2+1), parent_blobs=0, block_base_fee_per_gas=7
   */
  @ParameterizedTest(name = "{index} - parent excess {0}, used gas {1}, base fee {2}")
  @MethodSource("standardBranchBlobGases")
  public void shouldCalculateExcessBlobGasCorrectly_standardBranch(
      final long parentExcess,
      final long parentBlobGasUsed,
      final long baseFee,
      final long expected) {
    assertThat(gasLimitCalculator.computeExcessBlobGas(parentExcess, parentBlobGasUsed, baseFee))
        .isEqualTo(expected);
  }

  Iterable<Arguments> standardBranchBlobGases() {
    return List.of(
        // zero + zero: below target → 0
        Arguments.of(0L, 0L, 7L, 0L),
        // at target + 0: at target → 0
        Arguments.of(TARGET_BLOB_GAS_PER_BLOCK, 0L, 7L, 0L),
        // 0 + target gas = target → 0
        Arguments.of(0L, TARGET_BLOB_GAS_PER_BLOCK, 7L, 0L),
        // just above target → 1
        Arguments.of(1L, TARGET_BLOB_GAS_PER_BLOCK, 7L, 1L),
        // hive default: parent_excess_blobs=18, parent_blobs=0
        // excess = 18*131072 - 14*131072 = 4*131072 = 524288
        Arguments.of(2359296L, 0L, 7L, 524288L),
        // 18 excess blobs + 1 blob
        // excess = 18*131072 + 131072 - 14*131072 = 5*131072 = 655360
        Arguments.of(2359296L, 131072L, 7L, 655360L),
        // 18 excess blobs + target blobs: excess stays same as parent
        // excess = 18*131072 + 14*131072 - 14*131072 = 18*131072 = 2359296
        Arguments.of(2359296L, TARGET_BLOB_GAS_PER_BLOCK, 7L, 2359296L));
  }

  /**
   * Test Amsterdam excessBlobGas calculation using the EIP-7918 reserve price branch. When
   * base_fee_per_gas * BLOB_BASE_COST (8192) > blob_gas_price * GAS_PER_BLOB, the formula changes
   * to: excess = parentExcess + parentBlobGasUsed * (max - target) / max.
   *
   * <p>With base_fee=17: base_blob_tx_price = 17*8192 = 139264 > target_blob_gas_price = 131072*1 =
   * 131072 (at low excess where blob gas price = 1), so the EIP-7918 branch triggers. delta = 21 -
   * 14 = 7, so: excess = parentExcess + parentBlobGasUsed * 7 / 21
   */
  @ParameterizedTest(name = "{index} - parent excess {0}, used gas {1}, base fee {2}")
  @MethodSource("eip7918BranchBlobGases")
  public void shouldCalculateExcessBlobGasCorrectly_eip7918Branch(
      final long parentExcess,
      final long parentBlobGasUsed,
      final long baseFee,
      final long expected) {
    assertThat(gasLimitCalculator.computeExcessBlobGas(parentExcess, parentBlobGasUsed, baseFee))
        .isEqualTo(expected);
  }

  Iterable<Arguments> eip7918BranchBlobGases() {
    // parentExcess = TARGET_GAS + 1 = 1835009, just above target so not clamped to 0
    // With base_fee=17: 17*8192=139264 > 1*131072=131072 → EIP-7918 branch
    // excess = 1835009 + parentBlobGasUsed * 7 / 21
    return List.of(
        // 1 blob: 1835009 + 131072*7/21 = 1835009 + 43690 = 1878699
        Arguments.of(1835009L, 131072L, 17L, 1878699L),
        // 3 blobs: 1835009 + 393216*7/21 = 1835009 + 131072 = 1966081
        Arguments.of(1835009L, 393216L, 17L, 1966081L),
        // target (14) blobs: 1835009 + 1835008*7/21 = 1835009 + 611669 = 2446678
        Arguments.of(1835009L, 1835008L, 17L, 2446678L),
        // max (21) blobs: 1835009 + 2752512*7/21 = 1835009 + 917504 = 2752513
        Arguments.of(1835009L, 2752512L, 17L, 2752513L));
  }

  @Test
  void defaultGasLimit() {
    GasLimitCalculator calculator =
        new AmsterdamTargetingGasLimitCalculator(
            0L,
            FeeMarket.cancun(0L, Optional.empty(), BlobSchedule.BPO2_DEFAULT),
            new AmsterdamGasCalculator(),
            BlobSchedule.BPO2_DEFAULT.getMax(),
            BlobSchedule.BPO2_DEFAULT.getTarget(),
            OptionalInt.empty(),
            OptionalInt.empty());
    // 21 * 131072 = 2752512 = 0x2A0000
    assertThat(calculator.currentBlobGasLimit()).isEqualTo(0x2A0000);
    // per-tx cap: DEFAULT_MAX_BLOBS_PER_TRANSACTION (6) * 131072 = 0xC0000
    assertThat(calculator.transactionBlobGasLimitCap()).isEqualTo(0xC0000);
  }
}
