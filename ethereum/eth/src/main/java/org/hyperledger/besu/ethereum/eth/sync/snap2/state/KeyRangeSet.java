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
package org.hyperledger.besu.ethereum.eth.sync.snap2.state;

import org.hyperledger.besu.ethereum.trie.RangeManager;

import java.math.BigInteger;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes32;

/** Thread-safe set of disjoint, inclusive key ranges; adjacent ranges are merged. */
final class KeyRangeSet {

  private static final double KEY_SPACE = Math.pow(2, 256);

  private final TreeMap<Bytes32, Bytes32> ranges = new TreeMap<>();

  /**
   * Returns the key right after {@code key}, as a plain {@link Bytes32}, or empty after the last.
   */
  static Optional<Bytes32> next(final Bytes32 key) {
    final byte[] next = key.toArray();
    for (int i = next.length - 1; i >= 0; i--) {
      if (++next[i] != 0) {
        return Optional.of(Bytes32.wrap(next));
      }
    }
    return Optional.empty();
  }

  /** Returns the share of the key space covered by {@code [from, to]}. */
  static double fraction(final Bytes32 from, final Bytes32 to) {
    if (from.compareTo(to) > 0) {
      throw new IllegalArgumentException("from after to");
    }
    final BigInteger size =
        to.toUnsignedBigInteger().subtract(from.toUnsignedBigInteger()).add(BigInteger.ONE);
    return size.doubleValue() / KEY_SPACE;
  }

  /** Adds {@code [start, end]}. */
  public synchronized void add(final Bytes32 start, final Bytes32 end) {
    if (start.compareTo(end) > 0) {
      throw new IllegalArgumentException("start after end");
    }
    // bounds often slice a whole network message, keep a compact copy instead
    Bytes32 from = start.copy();
    Bytes32 to = end.copy();
    final Map.Entry<Bytes32, Bytes32> floor = ranges.floorEntry(start);
    if (floor != null && touches(floor.getValue(), start)) {
      from = floor.getKey();
    }
    // the ranges starting from there up to the key after the end merge, the last ends furthest
    final NavigableMap<Bytes32, Bytes32> merged =
        ranges.subMap(from, true, next(to).orElse(to), true);
    if (!merged.isEmpty() && merged.lastEntry().getValue().compareTo(to) > 0) {
      to = merged.lastEntry().getValue();
    }
    merged.clear();
    ranges.put(from, to);
  }

  /** Returns whether {@code key} is in the set. */
  public synchronized boolean contains(final Bytes32 key) {
    return encloses(key, key);
  }

  /** Returns whether every key of {@code [start, end]} is in the set. */
  public synchronized boolean encloses(final Bytes32 start, final Bytes32 end) {
    final Map.Entry<Bytes32, Bytes32> floor = ranges.floorEntry(start);
    return floor != null && floor.getValue().compareTo(end) >= 0;
  }

  /** Returns whether the whole key space is covered. */
  public synchronized boolean isFull() {
    return ranges.size() == 1
        && ranges.firstKey().equals(RangeManager.MIN_RANGE)
        && ranges.firstEntry().getValue().equals(RangeManager.MAX_RANGE);
  }

  /** Returns the share of the key space covered. */
  public synchronized double coverage() {
    double total = 0;
    for (final Map.Entry<Bytes32, Bytes32> range : ranges.entrySet()) {
      total += fraction(range.getKey(), range.getValue());
    }
    // each share is rounded, so their sum can pass one
    return Math.min(1.0, total);
  }

  /** Returns a copy of the ranges, keyed by start. */
  @VisibleForTesting
  synchronized NavigableMap<Bytes32, Bytes32> snapshot() {
    return new TreeMap<>(ranges);
  }

  // whether a range ending at end touches or overlaps a range starting at start
  private static boolean touches(final Bytes32 end, final Bytes32 start) {
    return end.compareTo(start) >= 0 || next(end).map(start::equals).orElse(true);
  }
}
