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
package org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator;

/**
 * Observes logical state reads made through a {@link PathBasedWorldStateUpdateAccumulator}.
 *
 * <p>Every read is reported, whether it is served from the accumulator's own maps, from a Block
 * Access List overlay, or by falling through to the underlying world state — the observer counts
 * total logical loads, not misses. Time spent in reads that do fall through is reported separately
 * so callers can attribute state-read latency without double counting.
 *
 * <p>The accumulator holds a nullable reference to an observer, so tracing costs a single null
 * check when disabled. Implementations must tolerate concurrent calls: the accumulator's {@code
 * commit()} can resolve original storage values from a parallel stream.
 */
public interface AccumulatorReadObserver {

  /** Records one logical account load. */
  void onAccountRead();

  /** Records one logical storage-slot load. */
  void onStorageRead();

  /**
   * Records one logical code load.
   *
   * @param codeBytes the size of the code that was read, zero when the account has no code
   */
  void onCodeRead(int codeBytes);

  /**
   * Records time spent in a read that fell through to the underlying world state.
   *
   * @param nanos elapsed nanoseconds
   */
  void addFallThroughReadNanos(long nanos);
}
