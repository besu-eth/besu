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

/**
 * Static utility operating directly on the flat {@code long[]} operand stack. Each slot occupies 4
 * consecutive longs in big-endian limb order: {@code [u3, u2, u1, u0]} where u3 is the most
 * significant limb.
 *
 * <p>All methods take {@code (long[] s, int top)} and return the new {@code top}. The caller
 * (operation) is responsible for underflow/overflow checks before calling.
 */
public class StackArithmetic {

  /** SHL: s[top-2] = s[top-2] << s[top-1], return top-1. */
  public static int shl(final long[] s, final int top) {
    final int a = (top - 1) << 2; // shift amount
    final int b = (top - 2) << 2; // value
    // If shift amount > 255 or value is zero, result is zero
    if (s[a] != 0
        || s[a + 1] != 0
        || s[a + 2] != 0
        || Long.compareUnsigned(s[a + 3], 256) >= 0
        || (s[b] == 0 && s[b + 1] == 0 && s[b + 2] == 0 && s[b + 3] == 0)) {
      s[b] = 0;
      s[b + 1] = 0;
      s[b + 2] = 0;
      s[b + 3] = 0;
      return top - 1;
    }
    int shift = (int) s[a + 3];
    shiftLeftInPlace(s, b, shift);
    return top - 1;
  }

  /** Shift left in place. shift must be 0..255. */
  private static void shiftLeftInPlace(final long[] s, final int off, final int shift) {
    if (shift == 0) return;
    int limbShift = shift >>> 6;
    int bitShift = shift & 63;

    // Move limbs (stored as [u3, u2, u1, u0] at [off, off+1, off+2, off+3])
    // u3=off, u2=off+1, u1=off+2, u0=off+3
    long u0 = s[off + 3], u1 = s[off + 2], u2 = s[off + 1], u3 = s[off];
    long a0, a1, a2, a3;
    switch (limbShift) {
      case 0:
        a0 = u0;
        a1 = u1;
        a2 = u2;
        a3 = u3;
        break;
      case 1:
        a0 = 0;
        a1 = u0;
        a2 = u1;
        a3 = u2;
        break;
      case 2:
        a0 = 0;
        a1 = 0;
        a2 = u0;
        a3 = u1;
        break;
      case 3:
        a0 = 0;
        a1 = 0;
        a2 = 0;
        a3 = u0;
        break;
      default:
        s[off] = 0;
        s[off + 1] = 0;
        s[off + 2] = 0;
        s[off + 3] = 0;
        return;
    }

    if (bitShift == 0) {
      s[off] = a3;
      s[off + 1] = a2;
      s[off + 2] = a1;
      s[off + 3] = a0;
    } else {
      int inv = 64 - bitShift;
      s[off + 3] = a0 << bitShift;
      s[off + 2] = (a1 << bitShift) | (a0 >>> inv);
      s[off + 1] = (a2 << bitShift) | (a1 >>> inv);
      s[off] = (a3 << bitShift) | (a2 >>> inv);
    }
  }

  /** SHR: s[top-2] = s[top-2] >>> s[top-1], return top-1. */
  public static int shr(final long[] s, final int top) {
    final int a = (top - 1) << 2; // shift amount
    final int b = (top - 2) << 2; // value
    if (s[a] != 0
        || s[a + 1] != 0
        || s[a + 2] != 0
        || Long.compareUnsigned(s[a + 3], 256) >= 0
        || (s[b] == 0 && s[b + 1] == 0 && s[b + 2] == 0 && s[b + 3] == 0)) {
      s[b] = 0;
      s[b + 1] = 0;
      s[b + 2] = 0;
      s[b + 3] = 0;
      return top - 1;
    }
    int shift = (int) s[a + 3];
    shiftRightInPlace(s, b, shift);
    return top - 1;
  }

  /** Logical shift right in place. shift must be 0..255. */
  private static void shiftRightInPlace(final long[] s, final int off, final int shift) {
    if (shift == 0) return;
    int limbShift = shift >>> 6;
    int bitShift = shift & 63;

    long u0 = s[off + 3], u1 = s[off + 2], u2 = s[off + 1], u3 = s[off];
    long a0, a1, a2, a3;
    switch (limbShift) {
      case 0:
        a0 = u0;
        a1 = u1;
        a2 = u2;
        a3 = u3;
        break;
      case 1:
        a0 = u1;
        a1 = u2;
        a2 = u3;
        a3 = 0;
        break;
      case 2:
        a0 = u2;
        a1 = u3;
        a2 = 0;
        a3 = 0;
        break;
      case 3:
        a0 = u3;
        a1 = 0;
        a2 = 0;
        a3 = 0;
        break;
      default:
        s[off] = 0;
        s[off + 1] = 0;
        s[off + 2] = 0;
        s[off + 3] = 0;
        return;
    }

    if (bitShift == 0) {
      s[off] = a3;
      s[off + 1] = a2;
      s[off + 2] = a1;
      s[off + 3] = a0;
    } else {
      int inv = 64 - bitShift;
      s[off] = a3 >>> bitShift;
      s[off + 1] = (a2 >>> bitShift) | (a3 << inv);
      s[off + 2] = (a1 >>> bitShift) | (a2 << inv);
      s[off + 3] = (a0 >>> bitShift) | (a1 << inv);
    }
  }

  /** SAR: s[top-2] = s[top-2] >> s[top-1] (arithmetic), return top-1. */
  public static int sar(final long[] s, final int top) {
    final int a = (top - 1) << 2; // shift amount
    final int b = (top - 2) << 2; // value
    boolean negative = s[b] < 0; // MSB of u3

    if (s[a] != 0 || s[a + 1] != 0 || s[a + 2] != 0 || Long.compareUnsigned(s[a + 3], 256) >= 0) {
      long fill = negative ? -1L : 0L;
      s[b] = fill;
      s[b + 1] = fill;
      s[b + 2] = fill;
      s[b + 3] = fill;
      return top - 1;
    }
    int shift = (int) s[a + 3];
    sarInPlace(s, b, shift, negative);
    return top - 1;
  }

  /** Arithmetic shift right in place. shift must be 0..255. */
  private static void sarInPlace(
      final long[] s, final int off, final int shift, final boolean negative) {
    if (shift == 0) return;
    int limbShift = shift >>> 6;
    int bitShift = shift & 63;
    long fill = negative ? -1L : 0L;

    long u0 = s[off + 3], u1 = s[off + 2], u2 = s[off + 1], u3 = s[off];
    long a0, a1, a2, a3;
    switch (limbShift) {
      case 0:
        a0 = u0;
        a1 = u1;
        a2 = u2;
        a3 = u3;
        break;
      case 1:
        a0 = u1;
        a1 = u2;
        a2 = u3;
        a3 = fill;
        break;
      case 2:
        a0 = u2;
        a1 = u3;
        a2 = fill;
        a3 = fill;
        break;
      case 3:
        a0 = u3;
        a1 = fill;
        a2 = fill;
        a3 = fill;
        break;
      default:
        s[off] = fill;
        s[off + 1] = fill;
        s[off + 2] = fill;
        s[off + 3] = fill;
        return;
    }

    if (bitShift == 0) {
      s[off] = a3;
      s[off + 1] = a2;
      s[off + 2] = a1;
      s[off + 3] = a0;
    } else {
      int inv = 64 - bitShift;
      s[off] = a3 >> bitShift; // arithmetic shift for MSB
      s[off + 1] = (a2 >>> bitShift) | (a3 << inv);
      s[off + 2] = (a1 >>> bitShift) | (a2 << inv);
      s[off + 3] = (a0 >>> bitShift) | (a1 << inv);
    }
  }
}
