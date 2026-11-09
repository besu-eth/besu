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
package org.hyperledger.besu.ethereum.eth.transactions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration.Unstable.DEFAULT_MAX_TRACKED_SEEN_TXS;

import org.hyperledger.besu.ethereum.eth.transactions.PeerTransactionTracker.FixedCapacityLRUMap;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FixedCapacityLRUMapTest {

  // 209_715 / 0.8 is just below 2^18 and 209_716 / 0.8 just above it; 262_143 to 262_145 are
  // entry counts just below, at and above 2^18
  @ParameterizedTest
  @ValueSource(
      ints = {
        1,
        2,
        1_000,
        209_715,
        209_716,
        262_143,
        262_144,
        262_145,
        DEFAULT_MAX_TRACKED_SEEN_TXS
      })
  void tableIsAPowerOfTwoSizedForTheLoadFactorAndNeverResized(final int maxEntries) {
    final BucketRecordingMap map = new BucketRecordingMap(maxEntries);
    final Random random = new Random(maxEntries);

    // overfill so that the map evicts as well as adds
    for (int i = 0; i < 3 * maxEntries; i++) {
      map.put(random.nextInt(), i);
    }

    assertThat(map.size()).isEqualTo(maxEntries);
    assertThat(map.tableSizes).as("table sizes seen by hashIndex").hasSize(1);
    final int buckets = map.tableSizes.iterator().next();
    // maxEntries / 0.8 rounded up, in exact integer arithmetic
    final int minBuckets = (int) ((5L * maxEntries + 3) / 4);
    assertThat(Integer.bitCount(buckets)).as("%d buckets is a power of two", buckets).isOne();
    assertThat(buckets).isGreaterThanOrEqualTo(minBuckets).isLessThan(2 * minBuckets);
  }

  @Test
  void entriesSpreadOverTheBuckets() {
    final int maxEntries = DEFAULT_MAX_TRACKED_SEEN_TXS;
    final BucketRecordingMap map = new BucketRecordingMap(maxEntries);
    final Random random = new Random(1);

    for (int i = 0; i < maxEntries; i++) {
      map.put(random.nextInt(), i);
    }

    // random keys fill 1 - e^-load of the buckets, about 228k of 524288 here; a bucket mask that
    // is not all ones reaches only 2^bitCount(mask) of them, 4096 for a 375000 bucket table
    assertThat(map.usedBuckets).hasSizeGreaterThan(maxEntries / 2);
  }

  /** Records the table size and bucket index of every lookup the map makes. */
  private static final class BucketRecordingMap extends FixedCapacityLRUMap<Integer, Integer> {
    private final Set<Integer> tableSizes = new HashSet<>();
    private final Set<Integer> usedBuckets = new HashSet<>();

    BucketRecordingMap(final int maxEntries) {
      super(maxEntries);
    }

    @Override
    protected int hashIndex(final int hashCode, final int dataSize) {
      final int index = super.hashIndex(hashCode, dataSize);
      tableSizes.add(dataSize);
      usedBuckets.add(index);
      return index;
    }
  }
}
