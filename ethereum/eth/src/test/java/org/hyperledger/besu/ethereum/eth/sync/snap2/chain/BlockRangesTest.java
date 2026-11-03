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
package org.hyperledger.besu.ethereum.eth.sync.snap2.chain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.BlockRanges.Gap;

import org.junit.jupiter.api.Test;

class BlockRangesTest {

  @Test
  void mergesAdjacentAndOverlappingRanges() {
    final BlockRanges ranges = new BlockRanges();
    ranges.add(10, 19);
    ranges.add(30, 39);
    ranges.add(20, 29);

    assertThat(ranges.startOf(39)).isEqualTo(10);

    ranges.add(5, 12);
    assertThat(ranges.startOf(39)).isEqualTo(5);
    assertThat(ranges.startOf(4)).isEqualTo(-1);
  }

  @Test
  void removesInsideAndAcrossRanges() {
    final BlockRanges ranges = new BlockRanges();
    ranges.add(1, 100);
    ranges.remove(40, 49);

    assertThat(ranges.startOf(100)).isEqualTo(50);
    assertThat(ranges.endBelow(50)).isEqualTo(39);
    assertThat(ranges.startOf(45)).isEqualTo(-1);

    ranges.remove(30, 100);
    assertThat(ranges.endBelow(101)).isEqualTo(29);
    ranges.remove(1, 29);
    assertThat(ranges.isEmpty()).isTrue();
  }

  @Test
  void findsTheHighestGapOfBothSets() {
    final BlockRanges stored = new BlockRanges();
    final BlockRanges fetching = new BlockRanges();
    stored.add(91, 100);
    fetching.add(81, 90);
    stored.add(61, 70);

    assertThat(BlockRanges.highestGap(stored, fetching, 1, 100, 32)).contains(new Gap(71, 80));
    fetching.add(71, 80);
    assertThat(BlockRanges.highestGap(stored, fetching, 1, 100, 32)).contains(new Gap(29, 60));
    assertThat(BlockRanges.highestGap(stored, fetching, 65, 100, 32)).isEmpty();
    assertThat(BlockRanges.highestGap(stored, fetching, 1, 100, 8)).contains(new Gap(53, 60));
    // the lowest block cuts the gap short
    assertThat(BlockRanges.highestGap(stored, fetching, 50, 100, 32)).contains(new Gap(50, 60));
  }

  @Test
  void rejectsALimitBelowOneAndANegativeLowest() {
    final BlockRanges ranges = new BlockRanges();

    assertThatThrownBy(() -> BlockRanges.highestGap(ranges, ranges, 1, 100, 0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> BlockRanges.highestGap(ranges, ranges, -1, 100, 32))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
