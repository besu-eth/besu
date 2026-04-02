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
package org.hyperledger.besu.evm.v2;

import org.hyperledger.besu.evm.UInt256;

/**
 * Static utility operating directly on the flat {@code long[]} operand stack. Each slot occupies 4
 * consecutive longs in big-endian limb order: {@code [u3, u2, u1, u0]} where u3 is the most
 * significant limb.
 *
 * <p>All methods take {@code (long[] s, int top)} and return the new {@code top}. The caller
 * (operation) is responsible for underflow/overflow checks before calling.
 */
public class StackArithmetic {

  /** Utility class — not instantiable. */
  private StackArithmetic() {}

  // region SHL (Shift Left)
  // ---------------------------------------------------------------------------

  /**
   * Performs EVM SHL (shift left) on the two top stack items.
   *
   * <p>Pops the shift amount (unsigned) and the value, pushes {@code value << shift}. Shifts >= 256
   * or a zero value produce 0.
   *
   * @param stack the flat limb array
   * @param top current stack-top (item count)
   * @return the new stack-top after consuming one item
   */
  public static int shl(final long[] stack, final int top) {
    final int shiftOffset = (top - 1) << 2;
    final int valueOffset = (top - 2) << 2;
    // If shift amount > 255 or value is zero, result is zero
    if (stack[shiftOffset] != 0
        || stack[shiftOffset + 1] != 0
        || stack[shiftOffset + 2] != 0
        || Long.compareUnsigned(stack[shiftOffset + 3], 256) >= 0
        || (stack[valueOffset] == 0
            && stack[valueOffset + 1] == 0
            && stack[valueOffset + 2] == 0
            && stack[valueOffset + 3] == 0)) {
      stack[valueOffset] = 0;
      stack[valueOffset + 1] = 0;
      stack[valueOffset + 2] = 0;
      stack[valueOffset + 3] = 0;
      return top - 1;
    }
    int shift = (int) stack[shiftOffset + 3];
    shiftLeftInPlace(stack, valueOffset, shift);
    return top - 1;
  }

  /**
   * Left-shifts a 256-bit value in place by 1..255 bits, zero-filling from the right.
   *
   * @param stack the flat limb array
   * @param valueOffset index of the value's most-significant limb
   * @param shift number of bits to shift (must be in [1, 255])
   */
  private static void shiftLeftInPlace(final long[] stack, final int valueOffset, final int shift) {
    if (shift == 0) return;
    long w0 = stack[valueOffset],
        w1 = stack[valueOffset + 1],
        w2 = stack[valueOffset + 2],
        w3 = stack[valueOffset + 3];
    final int wordShift = shift >>> 6;
    final int bitShift = shift & 63;
    switch (wordShift) {
      case 0:
        w0 = shiftLeftWord(w0, w1, bitShift);
        w1 = shiftLeftWord(w1, w2, bitShift);
        w2 = shiftLeftWord(w2, w3, bitShift);
        w3 = shiftLeftWord(w3, 0, bitShift);
        break;
      case 1:
        w0 = shiftLeftWord(w1, w2, bitShift);
        w1 = shiftLeftWord(w2, w3, bitShift);
        w2 = shiftLeftWord(w3, 0, bitShift);
        w3 = 0;
        break;
      case 2:
        w0 = shiftLeftWord(w2, w3, bitShift);
        w1 = shiftLeftWord(w3, 0, bitShift);
        w2 = 0;
        w3 = 0;
        break;
      case 3:
        w0 = shiftLeftWord(w3, 0, bitShift);
        w1 = 0;
        w2 = 0;
        w3 = 0;
        break;
    }
    stack[valueOffset] = w0;
    stack[valueOffset + 1] = w1;
    stack[valueOffset + 2] = w2;
    stack[valueOffset + 3] = w3;
  }

  /**
   * Shifts a 64-bit word left and carries in bits from the next less-significant word.
   *
   * @param value the current word
   * @param nextValue the next less-significant word (bits carry in from its top)
   * @param bitShift the intra-word shift amount in [0, 63]; 0 returns {@code value} unchanged to
   *     avoid Java's mod-64 shift semantics on {@code nextValue >>> 64}
   * @return the shifted word
   */
  private static long shiftLeftWord(final long value, final long nextValue, final int bitShift) {
    if (bitShift == 0) return value;
    return (value << bitShift) | (nextValue >>> (64 - bitShift));
  }

  // endregion

  // region SHR (Shift Right)
  // ---------------------------------------------------------------------------

  /**
   * Performs EVM SHR (logical shift right) on the two top stack items.
   *
   * <p>Pops the shift amount (unsigned) and the value, pushes {@code value >>> shift}. Shifts >=
   * 256 or a zero value produce 0.
   *
   * @param stack the flat limb array
   * @param top current stack-top (item count)
   * @return the new stack-top after consuming one item
   */
  public static int shr(final long[] stack, final int top) {
    final int shiftOffset = (top - 1) << 2;
    final int valueOffset = (top - 2) << 2;
    if (stack[shiftOffset] != 0
        || stack[shiftOffset + 1] != 0
        || stack[shiftOffset + 2] != 0
        || Long.compareUnsigned(stack[shiftOffset + 3], 256) >= 0
        || (stack[valueOffset] == 0
            && stack[valueOffset + 1] == 0
            && stack[valueOffset + 2] == 0
            && stack[valueOffset + 3] == 0)) {
      stack[valueOffset] = 0;
      stack[valueOffset + 1] = 0;
      stack[valueOffset + 2] = 0;
      stack[valueOffset + 3] = 0;
      return top - 1;
    }
    int shift = (int) stack[shiftOffset + 3];
    shiftRightInPlace(stack, valueOffset, shift);
    return top - 1;
  }

  /**
   * Logically right-shifts a 256-bit value in place by 1..255 bits, zero-filling from the left.
   *
   * @param s the flat limb array
   * @param valueOffset index of the value's most-significant limb
   * @param shift number of bits to shift (must be in [1, 255])
   */
  private static void shiftRightInPlace(final long[] s, final int valueOffset, final int shift) {
    if (shift == 0) return;
    long w0 = s[valueOffset],
        w1 = s[valueOffset + 1],
        w2 = s[valueOffset + 2],
        w3 = s[valueOffset + 3];
    final int wordShift = shift >>> 6;
    final int bitShift = shift & 63;
    switch (wordShift) {
      case 0:
        w3 = shiftRightWord(w3, w2, bitShift);
        w2 = shiftRightWord(w2, w1, bitShift);
        w1 = shiftRightWord(w1, w0, bitShift);
        w0 = shiftRightWord(w0, 0, bitShift);
        break;
      case 1:
        w3 = shiftRightWord(w2, w1, bitShift);
        w2 = shiftRightWord(w1, w0, bitShift);
        w1 = shiftRightWord(w0, 0, bitShift);
        w0 = 0;
        break;
      case 2:
        w3 = shiftRightWord(w1, w0, bitShift);
        w2 = shiftRightWord(w0, 0, bitShift);
        w1 = 0;
        w0 = 0;
        break;
      case 3:
        w3 = shiftRightWord(w0, 0, bitShift);
        w2 = 0;
        w1 = 0;
        w0 = 0;
        break;
    }
    s[valueOffset] = w0;
    s[valueOffset + 1] = w1;
    s[valueOffset + 2] = w2;
    s[valueOffset + 3] = w3;
  }

  // endregion

  // region SAR (Shift Arithmetic Right)
  // ---------------------------------------------------------------------------

  /**
   * Performs EVM SAR (arithmetic shift right) on the two top stack items.
   *
   * <p>Pops the shift amount (unsigned) and the value (signed), pushes {@code value >> shift}.
   * Shifts >= 256 produce 0 for positive values and -1 for negative values.
   *
   * @param stack the flat limb array
   * @param top current stack-top (item count)
   * @return the new stack-top after consuming one item
   */
  public static int sar(final long[] stack, final int top) {
    final int shiftOffset = (top - 1) << 2;
    final int valueOffset = (top - 2) << 2;
    boolean negative = stack[valueOffset] < 0;
    if (stack[shiftOffset] != 0
        || stack[shiftOffset + 1] != 0
        || stack[shiftOffset + 2] != 0
        || Long.compareUnsigned(stack[shiftOffset + 3], 256) >= 0) {
      long fill = negative ? -1L : 0L;
      stack[valueOffset] = fill;
      stack[valueOffset + 1] = fill;
      stack[valueOffset + 2] = fill;
      stack[valueOffset + 3] = fill;
      return top - 1;
    }
    int shift = (int) stack[shiftOffset + 3];
    sarInPlace(stack, valueOffset, shift, negative);
    return top - 1;
  }

  /**
   * Arithmetic right-shifts a 256-bit value in place by 0..255 bits, sign-extending with {@code
   * fill}.
   *
   * @param stack the flat limb array
   * @param valueOffset index of the value's most-significant limb
   * @param shift number of bits to shift (must be in [0, 255])
   * @param negative true if the original value is negative (fill = -1)
   */
  private static void sarInPlace(
      final long[] stack, final int valueOffset, final int shift, final boolean negative) {
    if (shift == 0) return;
    long w0 = stack[valueOffset],
        w1 = stack[valueOffset + 1],
        w2 = stack[valueOffset + 2],
        w3 = stack[valueOffset + 3];
    final long fill = negative ? -1L : 0L;
    final int wordShift = shift >>> 6;
    final int bitShift = shift & 63;
    switch (wordShift) {
      case 0:
        w3 = shiftRightWord(w3, w2, bitShift);
        w2 = shiftRightWord(w2, w1, bitShift);
        w1 = shiftRightWord(w1, w0, bitShift);
        w0 = shiftRightWord(w0, fill, bitShift);
        break;
      case 1:
        w3 = shiftRightWord(w2, w1, bitShift);
        w2 = shiftRightWord(w1, w0, bitShift);
        w1 = shiftRightWord(w0, fill, bitShift);
        w0 = fill;
        break;
      case 2:
        w3 = shiftRightWord(w1, w0, bitShift);
        w2 = shiftRightWord(w0, fill, bitShift);
        w1 = fill;
        w0 = fill;
        break;
      case 3:
        w3 = shiftRightWord(w0, fill, bitShift);
        w2 = fill;
        w1 = fill;
        w0 = fill;
        break;
    }
    stack[valueOffset] = w0;
    stack[valueOffset + 1] = w1;
    stack[valueOffset + 2] = w2;
    stack[valueOffset + 3] = w3;
  }

  // endregion

  // region Private Helpers
  // ---------------------------------------------------------------------------

  /**
   * Shifts a 64-bit word right and carries in bits from the previous more-significant word.
   *
   * <p>The {@code bitShift == 0} fast path avoids Java long-shift masking, where a shift by 64 is
   * treated as a shift by 0.
   *
   * @param value the current word
   * @param prevValue the previous more-significant word
   * @param bitShift the intra-word shift amount in the range {@code [0..63]}
   * @return the shifted word
   */
  private static long shiftRightWord(final long value, final long prevValue, final int bitShift) {
    if (bitShift == 0) return value;
    return (value >>> bitShift) | (prevValue << (64 - bitShift));
  }

  // region Arithmetic Operations
  // --------------------------------------------------------------------------

  /** MULMOD: s[top-3] = (s[top-1] * s[top-2]) mod s[top-3], return top-2. */
  public static int mulMod(final long[] s, final int top) {
    final int a = (top - 1) << 2;
    final int b = (top - 2) << 2;
    final int c = (top - 3) << 2;
    UInt256 va = new UInt256(s[a], s[a + 1], s[a + 2], s[a + 3]);
    UInt256 vb = new UInt256(s[b], s[b + 1], s[b + 2], s[b + 3]);
    UInt256 vc = new UInt256(s[c], s[c + 1], s[c + 2], s[c + 3]);
    UInt256 r = vc.isZero() ? UInt256.ZERO : va.mulMod(vb, vc);
    s[c] = r.u3();
    s[c + 1] = r.u2();
    s[c + 2] = r.u1();
    s[c + 3] = r.u0();
    return top - 2;
  }

  // --------------------------------------------------------------------------
  // end region
}
