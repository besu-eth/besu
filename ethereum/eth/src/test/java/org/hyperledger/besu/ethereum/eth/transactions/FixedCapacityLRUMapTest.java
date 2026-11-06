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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration.Unstable.DEFAULT_MAX_TRACKED_SEEN_TXS_BUCKETS_LOG2;
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration.Unstable.MAX_MAX_TRACKED_SEEN_TXS_BUCKETS_LOG2;
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration.Unstable.MIN_MAX_TRACKED_SEEN_TXS_BUCKETS_LOG2;

import org.hyperledger.besu.ethereum.eth.transactions.PeerTransactionTracker.FixedCapacityLRUMap;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FixedCapacityLRUMapTest {

  @ParameterizedTest
  @ValueSource(ints = {MIN_MAX_TRACKED_SEEN_TXS_BUCKETS_LOG2, 3, 10, 18, 19, 20})
  void tableHasTwoToTheBucketsLog2BucketsAndIsNeverResized(final int bucketsLog2) {
    final int buckets = 1 << bucketsLog2;
    final BucketRecordingMap map = new BucketRecordingMap(bucketsLog2);
    final Random random = new Random(bucketsLog2);

    assertThat(map.maxSize()).isEqualTo((int) (buckets * 4L / 5));

    // overfill so that the map evicts as well as adds
    for (int i = 0; i < 3 * map.maxSize(); i++) {
      map.put(random.nextInt(), i);
    }

    assertThat(map.size()).isEqualTo(map.maxSize());
    assertThat(map.tableSizes).as("table sizes seen by hashIndex").containsExactly(buckets);
  }

  @Test
  void defaultHoldsFourFifthsOfItsBuckets() {
    assertThat(FixedCapacityLRUMap.maxEntries(DEFAULT_MAX_TRACKED_SEEN_TXS_BUCKETS_LOG2))
        .isEqualTo(419_430);
  }

  @ParameterizedTest
  @ValueSource(
      ints = {
        Integer.MIN_VALUE,
        0,
        MIN_MAX_TRACKED_SEEN_TXS_BUCKETS_LOG2 - 1,
        MAX_MAX_TRACKED_SEEN_TXS_BUCKETS_LOG2 + 1,
        Integer.MAX_VALUE
      })
  void rejectsBucketsLog2OutOfRange(final int bucketsLog2) {
    assertThatThrownBy(() -> new FixedCapacityLRUMap<Integer, Integer>(bucketsLog2))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("bucketsLog2 must be between");
  }

  @Test
  void entriesSpreadOverTheBuckets() {
    final BucketRecordingMap map =
        new BucketRecordingMap(DEFAULT_MAX_TRACKED_SEEN_TXS_BUCKETS_LOG2);
    final Random random = new Random(1);

    for (int i = 0; i < map.maxSize(); i++) {
      map.put(random.nextInt(), i);
    }

    // random keys fill 1 - e^-0.8 of the buckets, about 288k of 524288 here; a bucket mask that
    // is not all ones reaches only 2^bitCount(mask) of them
    assertThat(map.usedBuckets).hasSizeGreaterThan(map.maxSize() / 2);
  }

  /** Records the table size and bucket index of every lookup the map makes. */
  private static final class BucketRecordingMap extends FixedCapacityLRUMap<Integer, Integer> {
    private final Set<Integer> tableSizes = new HashSet<>();
    private final Set<Integer> usedBuckets = new HashSet<>();

    BucketRecordingMap(final int bucketsLog2) {
      super(bucketsLog2);
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
