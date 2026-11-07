/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.services.kvstore;

import java.util.Arrays;
import java.util.Comparator;

import org.apache.tuweni.bytes.Bytes;

/**
 * This class is a comparator that allows comparing two byte arrays from left to right.
 *
 * <p>For example:
 *
 * <p>>0x01 is smaller than 0x0101 or 0x01 is smaller than 0x02.
 */
public class KeyComparator {

  /**
   * Unsigned byte-wise key order, the same order RocksDB uses. Unlike {@link Bytes#compareTo},
   * which compares keys as numbers, it orders keys of different lengths by their bytes and keeps
   * all-zero keys of different lengths (such as the empty key and 0x00) distinct. Every in-memory
   * segment map, and every stream merged with RocksDB data, must use this order.
   */
  public static final Comparator<Bytes> UNSIGNED_KEY_ORDER =
      Comparator.comparing(Bytes::toArrayUnsafe, Arrays::compareUnsigned);

  /** Instantiates a new KeyComparator */
  public KeyComparator() {}

  /**
   * Compares two keys from left to right.
   *
   * <p>This method performs a byte-by-byte comparison between two keys, starting from the left
   * (most significant byte). It is designed to compare keys in a way that reflects their
   * hierarchical or sequential order.
   *
   * <p>The method returns: - A negative integer if {@code key1} is lexicographically less than
   * key2. - Zero if key1 and key2 are equal. - A positive integer if key1 is lexicographically
   * greater than key2.
   *
   * <p>If the keys are of unequal length but identical for the length of the shorter key (prefix),
   * the shorter key is considered to be lexicographically less than the longer key. This is
   * consistent with the lexicographic ordering used by rocksdb.
   *
   * @param key1 the first key compare.
   * @param key2 the second key to compare with.
   * @return the value 0 if key1 is equal to key2; a value less than 0 if key1 is lexicographically
   *     less than key2; and a value greater than 0 if key1 is lexicographically greater than key2.
   */
  public static int compareKeyLeftToRight(final Bytes key1, final Bytes key2) {
    int minLength = Math.min(key1.size(), key2.size());
    for (int i = 0; i < minLength; i++) {
      int compare = Byte.compareUnsigned(key1.get(i), key2.get(i));
      if (compare != 0) {
        return compare;
      }
    }
    return Integer.compare(key1.size(), key2.size());
  }
}
