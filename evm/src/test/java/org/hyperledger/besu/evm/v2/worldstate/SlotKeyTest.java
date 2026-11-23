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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class SlotKeyTest {

  /** A slot with {@code word} in the 64-bit word at {@code index} and every other word zero. */
  private static UInt256 slotWithWord(final int index, final long word) {
    final long[] words = new long[4];
    words[index] = word;
    return UInt256.fromBytes(
        Bytes32.fromHexString(
            String.format("0x%016x%016x%016x%016x", words[0], words[1], words[2], words[3])));
  }

  @Test
  void ordersKeysAsTheirUnsignedValues() {
    final List<UInt256> values = new ArrayList<>();
    for (int index = 0; index < 4; index++) {
      for (final long word : new long[] {1L, 0x7fffffffffffffffL, 0x8000000000000000L, -1L}) {
        values.add(slotWithWord(index, word));
      }
    }
    values.add(UInt256.ZERO);
    values.add(UInt256.MAX_VALUE);

    for (final UInt256 a : values) {
      for (final UInt256 b : values) {
        final SlotKey keyA = new SlotKey(a);
        final SlotKey keyB = new SlotKey(b);
        assertThat(Integer.signum(keyA.compareTo(keyB)))
            .as("%s vs %s", a, b)
            .isEqualTo(Integer.signum(a.compareTo(b)));
        assertThat(keyA.compareTo(keyB) == 0).isEqualTo(keyA.equals(keyB));
        if (keyA.equals(keyB)) {
          assertThat(keyA.hashCode()).isEqualTo(keyB.hashCode());
        }
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"0, 32", "0, 0", "1, 32", "1, 0", "2, 32", "2, 0", "3, 32", "3, 0"})
  void everyHalfOfEveryWordChangesTheHash(final int index, final int shift) {
    // keys that differ in one 32-bit half only, which a hash leaving that half out would collide
    final Set<Integer> hashes = new HashSet<>();
    for (long half = 1; half <= 64; half++) {
      hashes.add(new SlotKey(slotWithWord(index, half << shift)).hashCode());
    }
    assertThat(hashes).hasSizeGreaterThan(1);
  }
}
