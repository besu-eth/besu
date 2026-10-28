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

import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.BlockRanges.Gap;

import java.util.Optional;

/**
 * The ranges of a body back-fill below its lowest block: claimed by a worker, or stored until the
 * blocks above them are. Not thread-safe.
 */
final class BackfillRanges {
  private final BlockRanges claimed = new BlockRanges();
  private final BlockRanges stored = new BlockRanges();
  private long lowest;

  BackfillRanges(final long lowest) {
    this.lowest = lowest;
  }

  /** Returns the lowest block stored with every block above it. */
  long lowest() {
    return lowest;
  }

  /** Claims the highest run of blocks below the lowest one, down to the floor, still free. */
  Optional<Gap> claim(final long floor, final int limit) {
    final Optional<Gap> range = BlockRanges.highestGap(stored, claimed, floor, lowest - 1, limit);
    range.ifPresent(gap -> claimed.add(gap.from(), gap.to()));
    return range;
  }

  /** Gives a claimed range back, stored or not. */
  void release(final Gap range) {
    claimed.remove(range.from(), range.to());
  }

  /** Records stored blocks: the lowest block moves down over the run stored right below it. */
  void store(final long from, final long to) {
    stored.add(from, to);
    final long below = lowest - 1;
    final long runStart = stored.startOf(below);
    if (runStart < 0) {
      // the block right below the lowest one is still missing
      return;
    }
    stored.remove(runStart, below);
    lowest = runStart;
  }
}
