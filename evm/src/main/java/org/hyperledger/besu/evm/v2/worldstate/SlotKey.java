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
package org.hyperledger.besu.evm.v2.worldstate;

import org.hyperledger.besu.crypto.SecureRandomProvider;

import java.security.SecureRandom;

import org.apache.tuweni.units.bigints.UInt256;

/**
 * A storage slot as the key of the journal's maps.
 *
 * <p>Whoever sends the transaction picks the slots, and {@link UInt256#hashCode()} is a polynomial
 * in which colliding keys are cheap to make, so the hash is pair-multiply-shift with seeds drawn
 * once per process. The class implements {@link Comparable} directly, so that a hash map bucket
 * which fills up anyway is turned into a tree and stays fast to search.
 */
final class SlotKey implements Comparable<SlotKey> {

  private static final long LOW_32_BITS = 0xffffffffL;

  private static final long[] HASH_SEEDS = new long[9];

  static {
    final SecureRandom random = SecureRandomProvider.createSecureRandom();
    for (int i = 0; i < HASH_SEEDS.length; i++) {
      HASH_SEEDS[i] = random.nextLong();
    }
  }

  final UInt256 value;
  private final long word0;
  private final long word1;
  private final long word2;
  private final long word3;
  private final int hash;

  SlotKey(final UInt256 value) {
    this.value = value;
    word0 = value.getLong(0);
    word1 = value.getLong(8);
    word2 = value.getLong(16);
    word3 = value.getLong(24);
    // pairing the 32-bit halves of a word keeps keys that differ only in the top bits of two
    // words apart, which summing one product per word would collide whatever the seeds
    final long sum =
        HASH_SEEDS[8]
            + pair(word0, HASH_SEEDS[0], HASH_SEEDS[1])
            + pair(word1, HASH_SEEDS[2], HASH_SEEDS[3])
            + pair(word2, HASH_SEEDS[4], HASH_SEEDS[5])
            + pair(word3, HASH_SEEDS[6], HASH_SEEDS[7]);
    hash = (int) (sum >>> 32);
  }

  private static long pair(final long word, final long highSeed, final long lowSeed) {
    return (highSeed + (word >>> 32)) * (lowSeed + (word & LOW_32_BITS));
  }

  @Override
  public int hashCode() {
    return hash;
  }

  @Override
  public boolean equals(final Object other) {
    return other instanceof SlotKey key
        && word0 == key.word0
        && word1 == key.word1
        && word2 == key.word2
        && word3 == key.word3;
  }

  @Override
  public int compareTo(final SlotKey other) {
    int compare = Long.compareUnsigned(word0, other.word0);
    if (compare == 0) {
      compare = Long.compareUnsigned(word1, other.word1);
    }
    if (compare == 0) {
      compare = Long.compareUnsigned(word2, other.word2);
    }
    if (compare == 0) {
      compare = Long.compareUnsigned(word3, other.word3);
    }
    return compare;
  }
}
