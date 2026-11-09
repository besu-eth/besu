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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache;

import java.util.Optional;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;

/**
 * Replay benchmarks: the flat reads a thread sent to storage because the cache did not hold them,
 * and the time they took, so that a transaction's run can be split into waiting and executing.
 */
public final class BenchReads {

  /** Whether reads are counted, with the per-transaction timings of the replay benchmarks. */
  public static final boolean ENABLED = Boolean.getBoolean("besu.bench.logTxTimes");

  private static final ThreadLocal<long[]> READS = ThreadLocal.withInitial(() -> new long[2]);

  private BenchReads() {}

  static Optional<Bytes> timed(final Supplier<Optional<Bytes>> storageGetter) {
    if (!ENABLED) {
      return storageGetter.get();
    }
    final long start = System.nanoTime();
    try {
      return storageGetter.get();
    } finally {
      final long[] reads = READS.get();
      reads[0]++;
      reads[1] += System.nanoTime() - start;
    }
  }

  /**
   * The reads of this thread since the last call, which starts counting anew.
   *
   * @return the number of reads and the microseconds they took
   */
  public static long[] take() {
    final long[] reads = READS.get();
    final long[] taken = {reads[0], reads[1] / 1000};
    reads[0] = 0;
    reads[1] = 0;
    return taken;
  }
}
