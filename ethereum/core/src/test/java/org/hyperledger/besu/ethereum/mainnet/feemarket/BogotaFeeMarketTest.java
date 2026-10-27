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
package org.hyperledger.besu.ethereum.mainnet.feemarket;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import org.hyperledger.besu.config.BlobSchedule;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;

import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class BogotaFeeMarketTest {

  private static final long LONDON_FORK_BLOCK = 0L;
  private static final long PARENT_NUMBER = 100L;
  private static final long PARENT_TIMESTAMP = 1_000L;
  private static final long PARENT_SLOT = 50L;
  private static final long GAS_LIMIT = 30_000_000L;
  private static final long GAS_TARGET = GAS_LIMIT / 2;
  // a multiple of 8 * 12, so that the max changes are exact
  private static final Wei PARENT_BASE_FEE = Wei.of(960_000L);

  private final BaseFeeMarket bogotaFeeMarket =
      FeeMarket.bogota(LONDON_FORK_BLOCK, Optional.empty(), BlobSchedule.CANCUN_DEFAULT);
  private final BaseFeeMarket cancunFeeMarket =
      FeeMarket.cancun(LONDON_FORK_BLOCK, Optional.empty(), BlobSchedule.CANCUN_DEFAULT);

  @ParameterizedTest
  @ValueSource(
      longs = {
        0L,
        1L,
        7_499_999L,
        GAS_TARGET - 1,
        GAS_TARGET,
        GAS_TARGET + 1,
        22_222_222L,
        GAS_LIMIT
      })
  void maxChangeIsLondonOneWithTwelveSecondSlots(final long parentGasUsed) {
    for (final Wei parentBaseFee :
        new Wei[] {Wei.of(7L), Wei.of(8L), Wei.of(1_000_000_007L), Wei.of(123_456_789_012L)}) {
      final BlockHeader parent = parent(parentBaseFee, parentGasUsed);

      assertThat(computeBaseFee(parent, 12L, 1L))
          .isEqualTo(cancunFeeMarket.computeBaseFee(PARENT_NUMBER + 1, 0L, 0L, parent));
    }
  }

  @Test
  void maxChangeIsFiveFortyEighthsWithTenSecondSlots() {
    // 960_000 * 5 / 48 = 100_000
    assertThat(computeBaseFee(parent(PARENT_BASE_FEE, GAS_LIMIT), 10L, 1L))
        .isEqualTo(Wei.of(1_060_000L));
    assertThat(computeBaseFee(parent(PARENT_BASE_FEE, 0L), 10L, 1L)).isEqualTo(Wei.of(860_000L));
  }

  @Test
  void changeIsProportionalToTheGasDelta() {
    // a quarter of the max change: 960_000 / 4 * 10 / 96 = 25_000
    assertThat(computeBaseFee(parent(PARENT_BASE_FEE, GAS_TARGET + GAS_TARGET / 4), 10L, 1L))
        .isEqualTo(Wei.of(985_000L));
  }

  @Test
  void baseFeeIsUnchangedAtTheGasTarget() {
    assertThat(computeBaseFee(parent(PARENT_BASE_FEE, GAS_TARGET), 10L, 1L))
        .isEqualTo(PARENT_BASE_FEE);
  }

  @Test
  void missedSlotsDoNotChangeTheMaxChange() {
    final BlockHeader parent = parent(PARENT_BASE_FEE, GAS_LIMIT);

    assertThat(computeBaseFee(parent, 30L, 3L)).isEqualTo(computeBaseFee(parent, 10L, 1L));
  }

  @Test
  void maxChangeIsCappedAtOneEighth() {
    // 960_000 / 8 = 120_000
    assertThat(computeBaseFee(parent(PARENT_BASE_FEE, GAS_LIMIT), 30L, 1L))
        .isEqualTo(Wei.of(1_080_000L));
    assertThat(computeBaseFee(parent(PARENT_BASE_FEE, 0L), 3_600L, 2L)).isEqualTo(Wei.of(840_000L));
  }

  @Test
  void maxChangeIsLowerWithShorterSlots() {
    // 960_000 * 6 / 96 = 60_000
    assertThat(computeBaseFee(parent(PARENT_BASE_FEE, GAS_LIMIT), 6L, 1L))
        .isEqualTo(Wei.of(1_020_000L));
  }

  @Test
  void increaseIsAtLeastOne() {
    assertThat(computeBaseFee(parent(Wei.of(7L), GAS_TARGET + 1), 10L, 1L)).isEqualTo(Wei.of(8L));
  }

  @Test
  void slotNumbersAreUnsigned() {
    final BlockHeader parent =
        new BlockHeaderTestFixture()
            .number(PARENT_NUMBER)
            .timestamp(PARENT_TIMESTAMP)
            .slotNumber(-2L) // 2^64 - 2
            .baseFeePerGas(PARENT_BASE_FEE)
            .gasUsed(GAS_LIMIT)
            .gasLimit(GAS_LIMIT)
            .buildHeader();

    assertThat(
            bogotaFeeMarket.computeBaseFee(PARENT_NUMBER + 1, PARENT_TIMESTAMP + 10, -1L, parent))
        .isEqualTo(Wei.of(1_060_000L));
  }

  @Test
  void largestSlotGapDoesNotOverflow() {
    final BlockHeader fullParent =
        new BlockHeaderTestFixture()
            .number(PARENT_NUMBER)
            .timestamp(PARENT_TIMESTAMP)
            .slotNumber(0L)
            .baseFeePerGas(PARENT_BASE_FEE)
            .gasUsed(GAS_LIMIT)
            .gasLimit(GAS_LIMIT)
            .buildHeader();

    // 2^64 - 1 slots in 10 seconds: the change rounds down to zero, and to one for an increase
    assertThat(
            bogotaFeeMarket.computeBaseFee(
                PARENT_NUMBER + 1, PARENT_TIMESTAMP + 10, -1L, fullParent))
        .isEqualTo(PARENT_BASE_FEE.add(Wei.ONE));
  }

  @Test
  void failsWhenTheSlotNumberIsNotGreaterThanTheParentOne() {
    final BlockHeader parent = parent(PARENT_BASE_FEE, GAS_LIMIT);

    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                bogotaFeeMarket.computeBaseFee(
                    PARENT_NUMBER + 1, PARENT_TIMESTAMP + 10, PARENT_SLOT, parent))
        .withMessageContaining("slot number");
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                bogotaFeeMarket.computeBaseFee(
                    PARENT_NUMBER + 1, PARENT_TIMESTAMP + 10, PARENT_SLOT - 1, parent));
  }

  @Test
  void failsWhenTheTimestampIsNotGreaterThanTheParentOne() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                bogotaFeeMarket.computeBaseFee(
                    PARENT_NUMBER + 1,
                    PARENT_TIMESTAMP,
                    PARENT_SLOT + 1,
                    parent(PARENT_BASE_FEE, GAS_LIMIT)))
        .withMessageContaining("timestamp");
  }

  @Test
  void londonForkBlockHasTheInitialBaseFee() {
    final BaseFeeMarket feeMarket =
        FeeMarket.bogota(PARENT_NUMBER + 1, Optional.empty(), BlobSchedule.CANCUN_DEFAULT);

    assertThat(
            feeMarket.computeBaseFee(
                PARENT_NUMBER + 1,
                PARENT_TIMESTAMP + 10,
                PARENT_SLOT + 1,
                parent(PARENT_BASE_FEE, GAS_LIMIT)))
        .isEqualTo(feeMarket.getInitialBasefee());
  }

  @Test
  void predictionAssumesTheSpacingPerSlotOfTheHead() {
    final BlockHeader headParent = parent(PARENT_BASE_FEE, GAS_TARGET);
    // the head follows its parent after two slots of 10 seconds
    final BlockHeader head =
        new BlockHeaderTestFixture()
            .number(PARENT_NUMBER + 1)
            .timestamp(PARENT_TIMESTAMP + 20)
            .slotNumber(PARENT_SLOT + 2)
            .baseFeePerGas(PARENT_BASE_FEE)
            .gasUsed(GAS_LIMIT)
            .gasLimit(GAS_LIMIT)
            .buildHeader();

    assertThat(bogotaFeeMarket.predictNextBaseFee(head, () -> Optional.of(headParent)))
        .isEqualTo(
            bogotaFeeMarket.computeBaseFee(
                PARENT_NUMBER + 2, head.getTimestamp() + 10, head.getSlotNumber() + 1, head))
        .isEqualTo(Wei.of(1_060_000L));
  }

  @Test
  void predictionAssumesTwelveSecondSlotsWithoutTheParentOfTheHead() {
    final BlockHeader head = parent(PARENT_BASE_FEE, GAS_LIMIT);

    assertThat(bogotaFeeMarket.predictNextBaseFee(head, Optional::empty))
        .isEqualTo(Wei.of(1_080_000L));
  }

  @Test
  void predictionAssumesTwelveSecondSlotsWhenTheHeadDoesNotFollowItsParent() {
    final BlockHeader head = parent(PARENT_BASE_FEE, GAS_LIMIT);
    final BlockHeader sameSlotParent =
        new BlockHeaderTestFixture()
            .number(PARENT_NUMBER - 1)
            .timestamp(PARENT_TIMESTAMP - 10)
            .slotNumber(PARENT_SLOT)
            .buildHeader();

    assertThat(bogotaFeeMarket.predictNextBaseFee(head, () -> Optional.of(sameSlotParent)))
        .isEqualTo(Wei.of(1_080_000L));
  }

  private Wei computeBaseFee(
      final BlockHeader parent, final long secondsElapsed, final long slotsElapsed) {
    return bogotaFeeMarket.computeBaseFee(
        PARENT_NUMBER + 1, PARENT_TIMESTAMP + secondsElapsed, PARENT_SLOT + slotsElapsed, parent);
  }

  private static BlockHeader parent(final Wei baseFee, final long gasUsed) {
    return new BlockHeaderTestFixture()
        .number(PARENT_NUMBER)
        .timestamp(PARENT_TIMESTAMP)
        .slotNumber(PARENT_SLOT)
        .baseFeePerGas(baseFee)
        .gasUsed(gasUsed)
        .gasLimit(GAS_LIMIT)
        .buildHeader();
  }
}
