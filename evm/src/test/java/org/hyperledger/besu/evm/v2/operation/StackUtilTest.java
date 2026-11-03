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
package org.hyperledger.besu.evm.v2.operation;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;
import java.util.Random;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class StackUtilTest {

  @ParameterizedTest(name = "{0} clamps to {1}")
  @CsvSource({
    "0x00, 0",
    "0x7fffffffffffffff, 9223372036854775807",
    "0x8000000000000000, 9223372036854775807",
    "0xffffffffffffffff, 9223372036854775807",
    "0x010000000000000000, 9223372036854775807",
    "0x0100000000000000000000000000000000000000000000000000000000000000, 9223372036854775807",
    "0x0000000000000000000000000000000100000000000000000000000000000005, 9223372036854775807"
  })
  void clampsToLong(final String word, final long expected) {
    assertThat(StackUtil.readClampedLongAt(stackOf(word), 1, 0)).isEqualTo(expected);
  }

  @ParameterizedTest(name = "{0} clamps to {1}")
  @CsvSource({
    "0x00, 0",
    "0x7fffffff, 2147483647",
    "0x80000000, 2147483647",
    "0x0100000000, 2147483647",
    "0x0000000000000000000000000000000100000000000000000000000000000005, 2147483647"
  })
  void clampsToInt(final String word, final int expected) {
    assertThat(StackUtil.readClampedIntAt(stackOf(word), 1, 0)).isEqualTo(expected);
  }

  @Test
  void comparesLikeBigInteger() {
    final Random random = new Random(7);
    for (int i = 0; i < 2_000; i++) {
      final byte[] a = randomWord(random);
      final byte[] b = randomWord(random);
      // share 0 to 4 leading limbs, so that the comparison has to look further down
      System.arraycopy(a, 0, b, 0, 8 * random.nextInt(5));
      final long[] stack = new long[8];
      StackUtil.setBytes32(Bytes32.wrap(a), stack, 0);
      StackUtil.setBytes32(Bytes32.wrap(b), stack, 1);

      assertThat(Integer.signum(StackUtil.compareUnsigned(stack, 0, 4)))
          .isEqualTo(new BigInteger(1, a).compareTo(new BigInteger(1, b)));
      assertThat(Integer.signum(StackUtil.compareSigned(stack, 0, 4)))
          .isEqualTo(new BigInteger(a).compareTo(new BigInteger(b)));
    }
  }

  @Test
  void readsBackWhatItPushes() {
    final Bytes32 word =
        Bytes32.fromHexString("0x0123456789abcdeffedcba98765432100f1e2d3c4b5a69788796a5b4c3d2e1f0");
    final long[] stack = new long[8];
    StackUtil.setBytes32(word, stack, 1);

    assertThat(StackUtil.readBytes32At(stack, 2, 0)).isEqualTo(word);
    assertThat(StackUtil.readAddressAt(stack, 2, 0).getBytes()).isEqualTo(word.slice(12));
  }

  @Test
  void comparesAWordWithItsBytes() {
    final Bytes32 word =
        Bytes32.fromHexString("0x0123456789abcdeffedcba98765432100f1e2d3c4b5a69788796a5b4c3d2e1f0");
    final long[] stack = new long[8];
    StackUtil.setBytes32(word, stack, 1);

    assertThat(StackUtil.wordEquals(word, stack, 2, 0)).isTrue();
    assertThat(StackUtil.wordEquals(word, stack, 2, 1)).isFalse();
    for (int i = 0; i < 32; i += 8) {
      final byte[] changed = word.toArray();
      changed[i] ^= 1;
      assertThat(StackUtil.wordEquals(Bytes32.wrap(changed), stack, 2, 0)).isFalse();
    }
  }

  @Test
  void pushesShortAndMissingValuesRightAligned() {
    final long[] stack = new long[8];
    StackUtil.setBytes(Bytes.of(0x12, 0x34), stack, 0);
    StackUtil.setBytes(null, stack, 1);

    assertThat(StackUtil.readBytes32At(stack, 2, 1))
        .isEqualTo(Bytes32.leftPad(Bytes.of(0x12, 0x34)));
    assertThat(StackUtil.isZeroAt(stack, 2, 0)).isTrue();
  }

  private static long[] stackOf(final String word) {
    final long[] stack = new long[4];
    StackUtil.setBytes32(Bytes32.fromHexString(word), stack, 0);
    return stack;
  }

  private static byte[] randomWord(final Random random) {
    final byte[] word = new byte[32];
    random.nextBytes(word);
    return word;
  }
}
