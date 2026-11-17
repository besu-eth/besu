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
import java.util.List;

import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SlotKeyTest {

  private static SlotKey key(final String hex) {
    return new SlotKey(UInt256.fromBytes(Bytes32.fromHexString(hex)));
  }

  /** A key with {@code word} set in the 64-bit word at {@code index} and every other word zero. */
  private static SlotKey keyWithWord(final int index, final long word) {
    final long[] words = new long[4];
    words[index] = word;
    return key(String.format("0x%016x%016x%016x%016x", words[0], words[1], words[2], words[3]));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3})
  void comparesEachWordAsUnsigned(final int index) {
    final SlotKey highBit = keyWithWord(index, 0x8000000000000000L);
    final SlotKey belowHighBit = keyWithWord(index, 0x7fffffffffffffffL);
    final SlotKey allOnes = keyWithWord(index, -1L);

    assertThat(highBit.compareTo(belowHighBit)).isPositive();
    assertThat(belowHighBit.compareTo(highBit)).isNegative();
    assertThat(allOnes.compareTo(highBit)).isPositive();
    assertThat(highBit.compareTo(keyWithWord(index, 0x8000000000000000L))).isZero();
  }

  @Test
  void anEarlierWordDecidesTheOrder() {
    final SlotKey highFirstWord = key("0x80" + "00".repeat(31));
    final SlotKey highLaterWords = key("0x" + "7f" + "ff".repeat(31));

    assertThat(highFirstWord.compareTo(highLaterWords)).isPositive();
    assertThat(highLaterWords.compareTo(highFirstWord)).isNegative();
  }

  @Test
  void ordersKeysAsTheirUnsignedValues() {
    final List<UInt256> values = new ArrayList<>();
    for (int index = 0; index < 4; index++) {
      for (final long word : new long[] {1L, 0x7fffffffffffffffL, 0x8000000000000000L, -1L}) {
        final long[] words = new long[4];
        words[index] = word;
        values.add(
            UInt256.fromBytes(
                Bytes32.fromHexString(
                    String.format(
                        "0x%016x%016x%016x%016x", words[0], words[1], words[2], words[3]))));
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
}
