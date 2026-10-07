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
package org.hyperledger.besu.ethereum.slothashing;

import java.util.function.Supplier;

/** The keccak256 computed per block by a benchmark, printed at the end of the trial. */
final class KeccakPerBlock {

  private long blocks;
  private long all;
  private long slots;
  private int distinctSlots;

  /** Runs a block, counting its keccak256. */
  <T> T count(final Supplier<T> block) {
    final long allBefore = KeccakCounter.ALL.sum();
    final long slotsBefore = KeccakCounter.WORD_INPUTS.sum();
    final T result = block.get();
    all += KeccakCounter.ALL.sum() - allBefore;
    slots += KeccakCounter.WORD_INPUTS.sum() - slotsBefore;
    blocks++;
    return result;
  }

  /** Runs a block once, outside the measurement, to find how many different slots it hashes. */
  void findDistinctSlots(final Runnable block) {
    distinctSlots = KeccakCounter.distinctWordInputs(block);
  }

  void print(final Object scenario) {
    System.out.printf(
        "%n%s: %,d keccak256 per block, %,d on storage slots (32-byte inputs)"
            + " for %,d different slots%n",
        scenario, all / blocks, slots / blocks, distinctSlots);
  }
}
