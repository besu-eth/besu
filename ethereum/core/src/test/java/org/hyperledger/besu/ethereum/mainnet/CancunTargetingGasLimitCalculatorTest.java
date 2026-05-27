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
import org.hyperledger.besu.evm.gascalculator.CancunGasCalculator;
import org.hyperledger.besu.evm.gascalculator.PragueGasCalculator;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CancunTargetingGasLimitCalculatorTest {
  private static final int MAX_BLOBS_PER_BLOCK = 6;
  private static final int TARGET_BLOBS_PER_BLOCK = 3;
  private static final long TARGET_BLOB_GAS_PER_BLOCK_CANCUN = 0x60000;
  private final CancunTargetingGasLimitCalculator cancunTargetingGasLimitCalculator =
      new CancunTargetingGasLimitCalculator(
          0L,
          FeeMarket.cancunDefault(0L, Optional.empty()),
          new CancunGasCalculator(),
          MAX_BLOBS_PER_BLOCK,
          TARGET_BLOBS_PER_BLOCK);

  @ParameterizedTest(name = "{index} - parent gas {0}, used gas {1}, new excess {2}")
  @MethodSource("cancunBlobGasses")
  public void shouldCalculateCancunExcessBlobGasCorrectly(
      final long parentExcess, final long used, final long expected) {
    final long usedBlobGas = new CancunGasCalculator().blobGasCost(used);
    assertThat(
            cancunTargetingGasLimitCalculator.computeExcessBlobGas(parentExcess, usedBlobGas, 0L))
        .isEqualTo(expected);
  }

  Iterable<Arguments> cancunBlobGasses() {
    long targetGasPerBlock = TARGET_BLOB_GAS_PER_BLOCK_CANCUN;
    return List.of(
        Arguments.of(0L, 0L, 0L),
        Arguments.of(targetGasPerBlock, 0L, 0L),
        Arguments.of(0L, 3, 0L),
        Arguments.of(1, 3, 1),
        Arguments.of(targetGasPerBlock, 1, cancunTargetingGasLimitCalculator.getBlobGasPerBlob()),
        Arguments.of(targetGasPerBlock, 3, targetGasPerBlock));
  }

  @Test
  void shouldUseCancunCalculatorBlobGasPerBlob() {
    // should use CancunGasCalculator's blob gas per blob to calculate the gas limit
    final long blobGasPerBlob = new CancunGasCalculator().getBlobGasPerBlob();
    assertThat(blobGasPerBlob).isEqualTo(131072);
    int maxBlobs = 10;
    int targetBlobs = 3;
    var cancunTargetingGasLimitCalculator =
        new CancunTargetingGasLimitCalculator(
            0L,
            FeeMarket.cancunDefault(0L, Optional.empty()),
            new CancunGasCalculator(),
            maxBlobs,
            targetBlobs);
    // if maxBlobs = 10, then the gas limit would be 131072 * 10 = 1310720
    assertThat(cancunTargetingGasLimitCalculator.currentBlobGasLimit())
        .isEqualTo(blobGasPerBlob * maxBlobs);
    assertThat(cancunTargetingGasLimitCalculator.currentBlobGasLimit()).isEqualTo(1310720);
  }

  private final PragueGasCalculator pragueGasCalculator = new PragueGasCalculator();
  // CancunTargetingGasLimitCalculator with Prague numbers
  private final CancunTargetingGasLimitCalculator pragueGasLimitCalculator =
      new CancunTargetingGasLimitCalculator(
          0L,
          FeeMarket.cancunDefault(0L, Optional.empty()),
          pragueGasCalculator,
          BlobSchedule.PRAGUE_DEFAULT.getMax(),
          BlobSchedule.PRAGUE_DEFAULT.getTarget());

  private static final long TARGET_BLOB_GAS_PER_BLOCK_PRAGUE = 0xC0000;

  @ParameterizedTest(name = "{index} - parent gas {0}, used gas {1}, blob target {2}")
  @MethodSource("pragueBlobGasses")
  public void shouldCalculatePragueExcessBlobGasCorrectly(
      final long parentExcess, final long used, final long expected) {
    final long usedBlobGas = pragueGasCalculator.blobGasCost(used);
    assertThat(pragueGasLimitCalculator.computeExcessBlobGas(parentExcess, usedBlobGas, 0L))
        .isEqualTo(expected);
  }

  Iterable<Arguments> pragueBlobGasses() {
    long sixBlobTargetGas = TARGET_BLOB_GAS_PER_BLOCK_PRAGUE;
    long newTargetCount = 6;

    return List.of(
        // New target count
        Arguments.of(0L, 0L, 0L),
        Arguments.of(sixBlobTargetGas, 0L, 0L),
        Arguments.of(newTargetCount, 0L, 0L),
        Arguments.of(0L, newTargetCount, 0L),
        Arguments.of(1L, newTargetCount, 1L),
        Arguments.of(
            pragueGasCalculator.blobGasCost(newTargetCount),
            1L,
            pragueGasLimitCalculator.getBlobGasPerBlob()),
        Arguments.of(sixBlobTargetGas, newTargetCount, sixBlobTargetGas));
  }

  @Test
  void shouldCalculateCorrectlyPragueBlobGasPerBlob() {
    // should use PragueGasCalculator's blob gas per blob to calculate the gas limit
    final long blobGasPerBlob = new PragueGasCalculator().getBlobGasPerBlob();
    assertThat(blobGasPerBlob).isEqualTo(131072); // same as Cancun
    int maxBlobs = 10;
    int targetBlobs = 3;
    var pragueTargetingGasLimitCalculator =
        new CancunTargetingGasLimitCalculator(
            0L,
            FeeMarket.cancun(0L, Optional.empty(), BlobSchedule.PRAGUE_DEFAULT),
            new PragueGasCalculator(),
            maxBlobs,
            targetBlobs);
    // if maxBlobs = 10, then the gas limit would be 131072 * 10 = 1310720
    assertThat(pragueTargetingGasLimitCalculator.currentBlobGasLimit())
        .isEqualTo(blobGasPerBlob * maxBlobs);
    assertThat(pragueTargetingGasLimitCalculator.currentBlobGasLimit()).isEqualTo(1310720);
  }

  @Test
  void cancunDefaultGasLimit() {
    GasLimitCalculator gasLimitCalculator =
        new CancunTargetingGasLimitCalculator(
            0L,
            FeeMarket.cancunDefault(0L, Optional.empty()),
            new CancunGasCalculator(),
            BlobSchedule.CANCUN_DEFAULT.getMax(),
            BlobSchedule.CANCUN_DEFAULT.getTarget());
    assertThat(gasLimitCalculator.currentBlobGasLimit()).isEqualTo(0xC0000); // 6 * 131072
    assertThat(gasLimitCalculator.transactionBlobGasLimitCap()).isEqualTo(0xC0000); // 6 * 131072
  }

  @Test
  void pragueDefaultGasLimit() {
    GasLimitCalculator gasLimitCalculator =
        new CancunTargetingGasLimitCalculator(
            0L,
            FeeMarket.cancunDefault(0L, Optional.empty()),
            new CancunGasCalculator(),
            BlobSchedule.PRAGUE_DEFAULT.getMax(),
            BlobSchedule.PRAGUE_DEFAULT.getTarget());
    assertThat(gasLimitCalculator.currentBlobGasLimit()).isEqualTo(0x120000); // 9 * 131072
    assertThat(gasLimitCalculator.transactionBlobGasLimitCap()).isEqualTo(0x120000); // 9 * 131072
  }
}
