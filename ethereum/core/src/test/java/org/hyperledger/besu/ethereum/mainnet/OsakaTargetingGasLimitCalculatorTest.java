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

import org.hyperledger.besu.ethereum.mainnet.feemarket.BaseFeeMarket;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.evm.gascalculator.OsakaGasCalculator;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OsakaTargetingGasLimitCalculatorTest {
  private static final long TARGET_BLOB_GAS_PER_BLOCK_OSAKA = 0x120000;
  private final OsakaGasCalculator osakaGasCalculator = new OsakaGasCalculator();
  private final BaseFeeMarket feeMarket = FeeMarket.cancunDefault(0L, Optional.empty());

  @ParameterizedTest(
      name =
          "{index} - parent excess blob gas {0}, used gas {1}, parent base fee per gas {2}, new excess {3}")
  @MethodSource("osakaExcessBlobGasTestCases")
  public void shouldCalculateOsakaExcessBlobGasCorrectly(
      final long parentExcess,
      final long used,
      final long parentBaseFeePerGas,
      final long expected) {
    int maxBlobs = 10;
    int targetBlobs = 9;
    final OsakaTargetingGasLimitCalculator osakaTargetingGasLimitCalculator =
        new OsakaTargetingGasLimitCalculator(
            0L,
            feeMarket,
            osakaGasCalculator,
            maxBlobs,
            targetBlobs,
            OptionalInt.empty(),
            OptionalInt.empty());

    final long usedBlobGas = osakaGasCalculator.blobGasCost(used);
    assertThat(
            osakaTargetingGasLimitCalculator.computeExcessBlobGas(
                parentExcess, usedBlobGas, parentBaseFeePerGas))
        .isEqualTo(expected);
  }

  Iterable<Arguments> osakaExcessBlobGasTestCases() {
    long targetGasPerBlock = TARGET_BLOB_GAS_PER_BLOCK_OSAKA;
    return List.of(
        // Case 1: Below target, should return 0
        Arguments.of(0L, 0L, 0L, 0L),
        Arguments.of(targetGasPerBlock - 1, 0L, 0L, 0L),
        Arguments.of(0L, 8, 0L, 0L), // 8 blobs is below target (9)
        Arguments.of(0L, 9, 0L, 0L), // 9 blobs == target (9)

        // Case 2: Above target, BLOB_BASE_COST * baseFee <= GAS_PER_BLOB * blobFee
        // This should use the formula: parentExcess + parentBlobGasUsed - target
        Arguments.of(targetGasPerBlock * 2, 10, 17L, 2490368L),

        // Case 3: Above target, BLOB_BASE_COST * baseFee > GAS_PER_BLOB * blobFee
        // This should use the formula: parentExcess + parentBlobGasUsed * (max - target) / max
        Arguments.of(
            0L, 10L, 0L, osakaGasCalculator.getBlobGasPerBlob()), // 10 blobs is above target (9)
        Arguments.of(targetGasPerBlock, 1, 0L, osakaGasCalculator.getBlobGasPerBlob()),
        Arguments.of(targetGasPerBlock, 10, 1L, 1310720L),
        Arguments.of(targetGasPerBlock, 10, 16L, 1310720L));
  }

  @Test
  void shouldUseCorrectBlobGasPerBlob() {
    // should use OsakaGasCalculator's blob gas per blob to calculate the gas limit
    final long blobGasPerBlob = osakaGasCalculator.getBlobGasPerBlob();
    assertThat(blobGasPerBlob).isEqualTo(131072); // same as Cancun

    int maxBlobs = 10;
    int targetBlobs = 9;
    var osakaTargetingGasLimitCalculator =
        new OsakaTargetingGasLimitCalculator(
            0L,
            feeMarket,
            osakaGasCalculator,
            maxBlobs,
            targetBlobs,
            OptionalInt.empty(),
            OptionalInt.empty());

    // if maxBlobs = 10, then the gas limit would be 131072 * 10 = 1310720
    assertThat(osakaTargetingGasLimitCalculator.currentBlobGasLimit())
        .isEqualTo(blobGasPerBlob * maxBlobs);
    assertThat(osakaTargetingGasLimitCalculator.currentBlobGasLimit()).isEqualTo(1310720);
    assertThat(osakaTargetingGasLimitCalculator.getTargetBlobGasPerBlock())
        .isEqualTo(blobGasPerBlob * targetBlobs);
  }

  @Test
  void testComputeExcessBlobGasWithDifferentConditions() {
    // Create a test instance with specific parameters
    int maxBlobs = 10;
    int targetBlobs = 9;
    var calculator =
        new OsakaTargetingGasLimitCalculator(
            0L,
            feeMarket,
            osakaGasCalculator,
            maxBlobs,
            targetBlobs,
            OptionalInt.empty(),
            OptionalInt.empty());
    assertThat(calculator.maxBlobsPerBlock).isEqualTo(maxBlobs);
    assertThat(calculator.targetBlobsPerBlock).isEqualTo(targetBlobs);

    long parentExcessBlobGas = calculator.getTargetBlobGasPerBlock();
    long parentBlobGasUsed = osakaGasCalculator.getBlobGasPerBlob() * 2;

    // Test the condition where BLOB_BASE_COST * baseFee > GAS_PER_BLOB * blobFee
    // This should use the first formula
    long result1 = calculator.computeExcessBlobGas(parentExcessBlobGas, parentBlobGasUsed, 0L);
    // Expected value based on the test case
    assertThat(result1).isEqualTo(262144L);

    // Test with very high excess blob gas to trigger the second formula
    // When excess is high, blob fee will be high, potentially making BLOB_BASE_COST * baseFee <=
    // GAS_PER_BLOB * blobFee
    long highExcess = calculator.getTargetBlobGasPerBlock() * 10;
    long result2 = calculator.computeExcessBlobGas(highExcess, parentBlobGasUsed, 0L);
    // Expected value based on the test case
    assertThat(result2).isEqualTo(10878976L);
  }

  @Test
  void osakaBlobGasLimitPerTransaction() {
    int maxBlobs = 10;
    int targetBlobs = 9;
    var calculator =
        new OsakaTargetingGasLimitCalculator(
            0L,
            feeMarket,
            osakaGasCalculator,
            maxBlobs,
            targetBlobs,
            OptionalInt.empty(),
            OptionalInt.empty());
    assertThat(calculator.transactionBlobGasLimitCap()).isEqualTo(0xC0000); // 6 * 131072
  }

  @Test
  void osakaBlobGasLimitPerTransactionWithCustomValue() {
    int maxBlobs = 10;
    int targetBlobs = 9;
    int customMaxBlobsPerTx = 3;
    var calculator =
        new OsakaTargetingGasLimitCalculator(
            0L,
            feeMarket,
            osakaGasCalculator,
            maxBlobs,
            targetBlobs,
            OptionalInt.of(customMaxBlobsPerTx),
            OptionalInt.empty());
    // 3 * 131072 = 393216
    assertThat(calculator.transactionBlobGasLimitCap())
        .isEqualTo(osakaGasCalculator.getBlobGasPerBlob() * customMaxBlobsPerTx);
  }

  @Test
  void maxBlobsPerTransactionClampedToMaxBlobsPerBlock() {
    int maxBlobsPerBlock = 10;
    int targetBlobs = 9;
    int tooManyBlobsPerTx = 15; // exceeds maxBlobsPerBlock
    var calculator =
        new OsakaTargetingGasLimitCalculator(
            0L,
            feeMarket,
            osakaGasCalculator,
            maxBlobsPerBlock,
            targetBlobs,
            OptionalInt.of(tooManyBlobsPerTx),
            OptionalInt.empty());
    // Should be clamped to maxBlobsPerBlock (10), not the user value (15)
    assertThat(calculator.transactionBlobGasLimitCap())
        .isEqualTo(osakaGasCalculator.getBlobGasPerBlob() * maxBlobsPerBlock);
  }

  @Test
  void blockBuilderBlobGasLimitDefaultsToProtocolMax() {
    int maxBlobs = 9;
    int targetBlobs = 6;
    var calculator =
        new OsakaTargetingGasLimitCalculator(
            0L,
            feeMarket,
            osakaGasCalculator,
            maxBlobs,
            targetBlobs,
            OptionalInt.empty(),
            OptionalInt.empty());
    assertThat(calculator.blockBuilderBlobGasLimit())
        .isEqualTo(osakaGasCalculator.getBlobGasPerBlob() * maxBlobs);
  }

  @Test
  void blockBuilderBlobGasLimitRespectedWhenUserConfigured() {
    int maxBlobs = 9;
    int targetBlobs = 6;
    int userMaxBlobs = 3;
    var calculator =
        new OsakaTargetingGasLimitCalculator(
            0L,
            feeMarket,
            osakaGasCalculator,
            maxBlobs,
            targetBlobs,
            OptionalInt.empty(),
            OptionalInt.of(userMaxBlobs));
    assertThat(calculator.blockBuilderBlobGasLimit())
        .isEqualTo(osakaGasCalculator.getBlobGasPerBlob() * userMaxBlobs);
  }

  @Test
  void blockBuilderBlobGasLimitClampedToProtocolMax() {
    int maxBlobs = 9;
    int targetBlobs = 6;
    int userMaxBlobs = 20; // exceeds protocol max
    var calculator =
        new OsakaTargetingGasLimitCalculator(
            0L,
            feeMarket,
            osakaGasCalculator,
            maxBlobs,
            targetBlobs,
            OptionalInt.empty(),
            OptionalInt.of(userMaxBlobs));
    assertThat(calculator.blockBuilderBlobGasLimit())
        .isEqualTo(osakaGasCalculator.getBlobGasPerBlob() * maxBlobs);
  }
}
