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
import static org.junit.jupiter.params.provider.Arguments.arguments;

import org.hyperledger.besu.evm.UInt256;

import java.math.BigInteger;
import java.util.Random;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

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
    assertThat(StackUtil.clampedToLong(stackOf(word), 1, 0)).isEqualTo(expected);
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
    assertThat(StackUtil.clampedToInt(stackOf(word), 1, 0)).isEqualTo(expected);
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
      StackUtil.pushBytes32(Bytes32.wrap(a), stack, 0);
      StackUtil.pushBytes32(Bytes32.wrap(b), stack, 1);

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
    StackUtil.pushBytes32(word, stack, 1);

    assertThat(StackUtil.readBytes32At(stack, 2, 0)).isEqualTo(word);
    assertThat(StackUtil.readAddressAt(stack, 2, 0).getBytes()).isEqualTo(word.slice(12));
  }

  @Test
  void pushesShortAndMissingValuesRightAligned() {
    final long[] stack = new long[8];
    StackUtil.pushBytes(Bytes.of(0x12, 0x34), stack, 0);
    StackUtil.pushBytes(null, stack, 1);

    assertThat(StackUtil.readBytes32At(stack, 2, 1))
        .isEqualTo(Bytes32.leftPad(Bytes.of(0x12, 0x34)));
    assertThat(StackUtil.isZeroAt(stack, 2, 0)).isTrue();
  }

  private static Stream<Arguments> fromBytesBERangeCases() {
    return Stream.of(
        // ---- zero length ----------------------------------------------
        arguments(new byte[0], new int[] {0, 0}, UInt256.ZERO),
        arguments(new byte[0], new int[] {0, 2}, UInt256.ZERO),
        arguments(hex("0x55ab55"), new int[] {0, 0}, UInt256.ZERO),
        arguments(hex("0x55ab55"), new int[] {2, 0}, UInt256.ZERO),
        arguments(hex("0x010203040506070809"), new int[] {0, -1}, UInt256.ZERO),
        arguments(hex("0x010203040506070809"), new int[] {4, 0}, UInt256.ZERO),

        // ---- single-limb path (length < 8) ----------------------------------------------
        // 0x55 bytes sit outside the converted range and must never leak into the result
        arguments(hex("0x55ab55"), new int[] {1, 1}, new UInt256(0, 0, 0, 0xabL)),
        arguments(hex("0x55ffff55"), new int[] {1, 2}, new UInt256(0, 0, 0, 0xffffL)),
        // pins big-endian byte order inside the limb
        arguments(hex("0x550102030455"), new int[] {1, 4}, new UInt256(0, 0, 0, 0x01020304L)),
        // widest single-limb range
        arguments(
            hex("0x55ffffffffffffff55"),
            new int[] {1, 7},
            new UInt256(0, 0, 0, 0x00ffffffffffffffL)),
        // high bit set in the most significant byte of the range
        arguments(
            hex("0x8000000000000055"), new int[] {0, 7}, new UInt256(0, 0, 0, 0x80000000000000L)),
        // range starting on the last byte of the array
        arguments(hex("0x0102030405060708090a"), new int[] {9, 1}, new UInt256(0, 0, 0, 0x0aL)),

        // ---- exactly one limb (length == 8), the multi-limb path boundary ---------------
        // pins big-endian byte order across the whole limb
        arguments(
            hex("0x55010203040506070855"),
            new int[] {1, 8},
            new UInt256(0, 0, 0, 0x0102030405060708L)),
        arguments(hex("0x55ffffffffffffffff55"), new int[] {1, 8}, new UInt256(0, 0, 0, -1L)),
        // range ends exactly at bytes.length, so the 8-byte VarHandle read must not overrun
        arguments(hex("0x5555ffffffffffffffff"), new int[] {2, 8}, new UInt256(0, 0, 0, -1L)),
        // all-zero range inside a non-zero array
        arguments(hex("0x55000000000000000055"), new int[] {1, 8}, UInt256.ZERO),

        // ---- length 9, one byte spills into u1 -----------------------------------------
        arguments(hex("0x5501ffffffffffffffff55"), new int[] {1, 9}, new UInt256(0, 0, 1, -1L)),
        // same range, but ending exactly at bytes.length
        arguments(hex("0x5501ffffffffffffffff"), new int[] {1, 9}, new UInt256(0, 0, 1, -1L)),

        // ---- two and three whole limbs, and the partial limb above each --------------
        arguments(
            hex("0x550102030405060708090a0b0c0d0e0f1055"),
            new int[] {1, 16},
            new UInt256(0, 0, 0x0102030405060708L, 0x090a0b0c0d0e0f10L)),
        arguments(
            hex("0x55ff0102030405060708090a0b0c0d0e0f1055"),
            new int[] {1, 17},
            new UInt256(0, 0xff, 0x0102030405060708L, 0x090a0b0c0d0e0f10L)),
        arguments(
            hex("0x550102030405060708090a0b0c0d0e0f10111213141516171855"),
            new int[] {1, 24},
            new UInt256(0, 0x0102030405060708L, 0x090a0b0c0d0e0f10L, 0x1112131415161718L)),
        arguments(
            hex("0x55ff0102030405060708090a0b0c0d0e0f10111213141516171855"),
            new int[] {1, 25},
            new UInt256(0xff, 0x0102030405060708L, 0x090a0b0c0d0e0f10L, 0x1112131415161718L)),

        // ---- full width (length == 32) ------------------------------------------------
        // four distinct limbs, pins limb ordering end to end
        arguments(
            hex("0x55550102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f205555"),
            new int[] {2, 32},
            new UInt256(
                0x0102030405060708L,
                0x090a0b0c0d0e0f10L,
                0x1112131415161718L,
                0x191a1b1c1d1e1f20L)),
        // whole array is the range: offset 0 and end == bytes.length
        arguments(
            hex("0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"),
            new int[] {0, 32},
            new UInt256(
                0x0102030405060708L,
                0x090a0b0c0d0e0f10L,
                0x1112131415161718L,
                0x191a1b1c1d1e1f20L)),
        arguments(
            hex("0x55ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff55"),
            new int[] {1, 32},
            UInt256.MAX),
        // 32-byte range ending exactly at bytes.length
        arguments(
            hex("0x55ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
            new int[] {1, 32},
            UInt256.MAX),
        // high bit set in every limb, catches sign extension between limbs
        arguments(
            hex("0x55800000000000000080000000000000008000000000000000800000000000000055"),
            new int[] {1, 32},
            new UInt256(Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE)),

        // ---- length > 32 truncates the most significant bytes -------------------------
        // the leading 0xaa is dropped, leaving 32 x 0xff
        arguments(
            hex("0xaaffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
            new int[] {0, 33},
            UInt256.MAX),
        // same, with a non-zero offset
        arguments(
            hex("0x55aaffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff55"),
            new int[] {1, 33},
            UInt256.MAX),
        // 40-byte range: the leading 8 x 0xaa are dropped, leaving 32 x 0x01
        arguments(
            hex(
                "0xaaaaaaaaaaaaaaaa0101010101010101010101010101010101010101010101010101010101010101"),
            new int[] {0, 40},
            new UInt256(
                0x0101010101010101L,
                0x0101010101010101L,
                0x0101010101010101L,
                0x0101010101010101L)),
        // 36-byte range: the kept low 32 bytes straddle the 0x01 / 0xbb boundary
        arguments(
            hex("0x0101010101010101010101010101010101010101010101010101010101010101bbbbbbbb"),
            new int[] {0, 36},
            new UInt256(
                0x0101010101010101L,
                0x0101010101010101L,
                0x0101010101010101L,
                0x01010101bbbbbbbbL)));
  }

  @ParameterizedTest
  @MethodSource("fromBytesBERangeCases")
  void fromBytesBERange(final byte[] bytes, final int[] bounds, final UInt256 expected) {
    assertThat(StackUtil.fromBytesBE(bytes, bounds[0], bounds[1])).isEqualTo(expected);
  }

  private static Stream<Arguments> getLongBECases() {
    return Stream.of(
        // ── empty array → 0 regardless of from/to ───────────────────────────────
        arguments(new byte[0], 0, 0, 0L),

        // ── empty range (from == to) → 0 ────────────────────────────────────────
        // slow path (bytes.length < 8)
        arguments(hex("0x0102"), 0, 0, 0L),
        arguments(hex("0x0102"), 1, 1, 0L),
        // from == to == bytes.length (OOB-pc edge case that must not crash)
        arguments(hex("0x010203"), 3, 3, 0L),
        // fast path (bytes.length >= 8)
        arguments(hex("0x0102030405060708"), 4, 4, 0L),
        arguments(hex("0x0102030405060708"), 8, 8, 0L),

        // ── span 1, slow path (bytes.length < 8) ────────────────────────────────
        arguments(hex("0x42"), 0, 1, 0x42L),
        // high bit must NOT sign-extend
        arguments(hex("0x80"), 0, 1, 0x80L),
        // all-ones byte
        arguments(hex("0xff"), 0, 1, 0xffL),
        // byte not at index 0; flanking 0xff bytes must not leak into result
        arguments(hex("0xff42ff"), 1, 2, 0x42L),
        arguments(hex("0xff80ff"), 1, 2, 0x80L),

        // ── span 1, fast path (bytes.length >= 8) ───────────────────────────────
        // embedded mid-array; 0xff neighbours must not contaminate
        arguments(hex("0xffff42ffffffffff"), 2, 3, 0x42L),
        arguments(hex("0xffff80ffffffffff"), 2, 3, 0x80L),
        // to == bytes.length: boundary read, no overrun
        arguments(hex("0xffffffffffffff42"), 7, 8, 0x42L),
        arguments(hex("0xffffffffffffff80"), 7, 8, 0x80L),

        // ── span 2 ───────────────────────────────────────────────────────────────
        // slow path, full array
        arguments(hex("0x0102"), 0, 2, 0x0102L),
        // slow path, 0xff isolation
        arguments(hex("0xff0102ff"), 1, 3, 0x0102L),
        // fast path, mid-array
        arguments(hex("0xff0102ffffffffff"), 1, 3, 0x0102L),
        // fast path, to == bytes.length
        arguments(hex("0xffffffffffffff0102"), 7, 9, 0x0102L),

        // ── span 4 ───────────────────────────────────────────────────────────────
        // slow path
        arguments(hex("0x01020304"), 0, 4, 0x01020304L),
        // fast path, mid-array
        arguments(hex("0xff01020304ffffffff"), 1, 5, 0x01020304L),
        // fast path, to == bytes.length
        arguments(hex("0xffffffff01020304"), 4, 8, 0x01020304L),

        // ── span 7 ───────────────────────────────────────────────────────────────
        // slow path, full array
        arguments(hex("0x01020304050607"), 0, 7, 0x01020304050607L),
        // fast path, 0xff isolation
        arguments(hex("0xff01020304050607ff"), 1, 8, 0x01020304050607L),
        // fast path, to == bytes.length (common truncation boundary)
        arguments(hex("0xff01020304050607"), 1, 8, 0x01020304050607L),
        // high bit in MSB of range: result must be positive (unsigned)
        arguments(hex("0xff80000000000000ff"), 1, 8, 0x80000000000000L),

        // ── span 8, fast path only ───────────────────────────────────────────────
        // standard full-array read
        arguments(hex("0x0102030405060708"), 0, 8, 0x0102030405060708L),
        // to == bytes.length
        arguments(hex("0xff0102030405060708"), 1, 9, 0x0102030405060708L),
        // all-ones: must produce -1L
        arguments(hex("0xffffffffffffffff"), 0, 8, -1L),
        // high bit in MSB → Long.MIN_VALUE (unsigned semantics)
        arguments(hex("0x8000000000000000"), 0, 8, 0x8000000000000000L),
        // 0xff guards around the range
        arguments(hex("0xff0102030405060708ff"), 1, 9, 0x0102030405060708L),

        // ── all-zeros ────────────────────────────────────────────────────────────
        arguments(hex("0x000000"), 0, 3, 0L),
        arguments(hex("0x0000000000000000"), 0, 8, 0L),

        // ── cross-path consistency ───────────────────────────────────────────────
        // Same logical range must produce the same result on both paths.
        // span 3, slow (length 3)
        arguments(hex("0x010203"), 0, 3, 0x010203L),
        // span 3, fast (same bytes at offset 0)
        arguments(hex("0x0102035555555555"), 0, 3, 0x010203L),
        // span 5, slow
        arguments(hex("0x0102030405"), 0, 5, 0x0102030405L),
        // span 5, fast (offset 1, flanked by 0x55)
        arguments(hex("0x5501020304055555"), 1, 6, 0x0102030405L),
        // span 7, slow
        arguments(hex("0x01020304050607"), 0, 7, 0x01020304050607L),
        // span 7, fast (same bytes at offset 1)
        arguments(hex("0x550102030405060755"), 1, 8, 0x01020304050607L));
  }

  @ParameterizedTest
  @MethodSource("getLongBECases")
  void getLongBE(final byte[] bytes, final int from, final int to, final long expected) {
    assertThat(StackUtil.getLongBE(bytes, from, to))
        .withFailMessage(
            "getLongBE(bytes[%d], from=%d, to=%d): expected 0x%016x",
            bytes.length, from, to, expected)
        .isEqualTo(expected);
  }

  private static long[] stackOf(final String word) {
    final long[] stack = new long[4];
    StackUtil.pushBytes32(Bytes32.fromHexString(word), stack, 0);
    return stack;
  }

  private static byte[] randomWord(final Random random) {
    final byte[] word = new byte[32];
    random.nextBytes(word);
    return word;
  }

  private static byte[] hex(final String s) {
    return Bytes.fromHexString(s).toArray();
  }
}
