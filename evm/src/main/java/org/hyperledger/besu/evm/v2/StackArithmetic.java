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

  // region SHR (Shift Right)
  // ---------------------------------------------------------------------------

  /**
   * Performs EVM SHR (logical shift right) on the two top stack items.
   *
   * <p>Reads the shift amount (unsigned) and the value from the top two stack slots, writes {@code
   * value >>> shift} back into the value slot and decrements the top. Shifts >= 256 or a zero value
   * produce 0.
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
   * @param stack the flat limb array
   * @param valueOffset index of the value's most-significant limb
   * @param shift number of bits to shift (must be in [1, 255])
   */
  private static void shiftRightInPlace(
      final long[] stack, final int valueOffset, final int shift) {
    if (shift == 0) return;
    long w0 = stack[valueOffset],
        w1 = stack[valueOffset + 1],
        w2 = stack[valueOffset + 2],
        w3 = stack[valueOffset + 3];
    // Number of whole 64-bit words to shift (shift / 64)
    final int wordShift = shift >>> 6;
    // Remaining intra-word bit shift (shift % 64)
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
    stack[valueOffset] = w0;
    stack[valueOffset + 1] = w1;
    stack[valueOffset + 2] = w2;
    stack[valueOffset + 3] = w3;
  }

  private static long shiftRightWord(final long value, final long prevValue, final int bitShift) {
    if (bitShift == 0) return value;
    return (value >>> bitShift) | (prevValue << (64 - bitShift));
  }
  // endregion

}
