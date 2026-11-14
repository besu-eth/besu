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

import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.BlockRanges.Gap;

import org.junit.jupiter.api.Test;

class BackfillRangesTest {

  private final BackfillRanges ranges = new BackfillRanges(51);

  @Test
  void claimsTheHighestFreeRunsDownToTheFloor() {
    assertThat(ranges.claim(43, 5)).contains(new Gap(46, 50));
    assertThat(ranges.claim(43, 5)).contains(new Gap(43, 45));
    assertThat(ranges.claim(43, 5)).isEmpty();
  }

  @Test
  void movesTheLowestBlockOnlyOverRunsStoredRightBelowIt() {
    final Gap upper = ranges.claim(1, 5).orElseThrow();
    final Gap lower = ranges.claim(1, 5).orElseThrow();

    // the lower run stored while the upper one is still in flight
    ranges.store(lower.from(), lower.to());
    ranges.release(lower);
    assertThat(ranges.lowest()).isEqualTo(51);

    ranges.store(upper.from(), upper.to());
    ranges.release(upper);
    assertThat(ranges.lowest()).isEqualTo(41);
  }

  @Test
  void claimsARangeReleasedWithoutItsBlocksAgain() {
    final Gap range = ranges.claim(1, 5).orElseThrow();

    ranges.release(range);

    assertThat(ranges.claim(1, 5)).contains(range);
    assertThat(ranges.lowest()).isEqualTo(51);
  }
}
