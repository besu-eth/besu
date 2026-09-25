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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.UInt256;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.math.BigInteger;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Static utility operating directly on the flat {@code long[]} operand stack. Each slot occupies 4
 * consecutive longs in big-endian limb order: {@code [u3, u2, u1, u0]} where u3 is the most
 * significant limb.
 *
 * <p>All methods take {@code (long[] s, int top)} and return the new {@code top}. The caller
 * (operation) is responsible for underflow/overflow checks before calling.
 */
public class StackArithmetic {

  private static final VarHandle LONG_BE =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);
  private static final VarHandle INT_BE =
      MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);

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

  // region Binary Arithmetic (pop 2, push 1, return top-1)
  // ---------------------------------------------------------------------------

  /**
   * ADD: s[top-2] = s[top-1] + s[top-2], return top-1.
   *
   * @param s the stack array
   * @param top the current top index
   * @return the new top index
   */
  public static int add(final long[] s, final int top) {
    final int a = (top - 1) << 2;
    final int b = (top - 2) << 2;
    // Fast path: both values fit in a single limb (common case)
    if ((s[a] | s[a + 1] | s[a + 2] | s[b] | s[b + 1] | s[b + 2]) == 0) {
      long z = s[a + 3] + s[b + 3];
      s[b + 2] = ((s[a + 3] & s[b + 3]) | ((s[a + 3] | s[b + 3]) & ~z)) >>> 63;
      s[b + 3] = z;
      return top - 1;
    }
    long a0 = s[a + 3], a1 = s[a + 2], a2 = s[a + 1], a3 = s[a];
    long b0 = s[b + 3], b1 = s[b + 2], b2 = s[b + 1], b3 = s[b];
    long z0 = a0 + b0;
    long c = ((a0 & b0) | ((a0 | b0) & ~z0)) >>> 63;
    long t1 = a1 + b1;
    long c1 = ((a1 & b1) | ((a1 | b1) & ~t1)) >>> 63;
    long z1 = t1 + c;
    c = c1 | (((t1 & c) | ((t1 | c) & ~z1)) >>> 63);
    long t2 = a2 + b2;
    long c2 = ((a2 & b2) | ((a2 | b2) & ~t2)) >>> 63;
    long z2 = t2 + c;
    c = c2 | (((t2 & c) | ((t2 | c) & ~z2)) >>> 63);
    long z3 = a3 + b3 + c;
    s[b] = z3;
    s[b + 1] = z2;
    s[b + 2] = z1;
    s[b + 3] = z0;
    return top - 1;
  }

  /**
   * SUB: s[top-2] = s[top-1] - s[top-2], return top-1.
   *
   * @param s the stack array
   * @param top the current top index
   * @return the new top index
   */
  public static int sub(final long[] s, final int top) {
    final int a = (top - 1) << 2; // first operand (top)
    final int b = (top - 2) << 2; // second operand
    // Fast path: both values fit in a single limb (common case)
    if ((s[a] | s[a + 1] | s[a + 2] | s[b] | s[b + 1] | s[b + 2]) == 0) {
      long z = s[a + 3] - s[b + 3];
      // borrow is 0 or 1; negate to get 0 or -1L (0xFFFFFFFFFFFFFFFF) for upper limbs
      long fill = -(((~s[a + 3] & s[b + 3]) | (~(s[a + 3] ^ s[b + 3]) & z)) >>> 63);
      s[b] = fill;
      s[b + 1] = fill;
      s[b + 2] = fill;
      s[b + 3] = z;
      return top - 1;
    }
    long a0 = s[a + 3], a1 = s[a + 2], a2 = s[a + 1], a3 = s[a];
    long b0 = s[b + 3], b1 = s[b + 2], b2 = s[b + 1], b3 = s[b];
    // a - b with branchless borrow chain
    long z0 = a0 - b0;
    long w = ((~a0 & b0) | (~(a0 ^ b0) & z0)) >>> 63;
    long t1 = a1 - b1;
    long w1 = ((~a1 & b1) | (~(a1 ^ b1) & t1)) >>> 63;
    long z1 = t1 - w;
    w = w1 | (((~t1 & w) | (~(t1 ^ w) & z1)) >>> 63);
    long t2 = a2 - b2;
    long w2 = ((~a2 & b2) | (~(a2 ^ b2) & t2)) >>> 63;
    long z2 = t2 - w;
    w = w2 | (((~t2 & w) | (~(t2 ^ w) & z2)) >>> 63);
    long z3 = a3 - b3 - w;
    s[b] = z3;
    s[b + 1] = z2;
    s[b + 2] = z1;
    s[b + 3] = z0;
    return top - 1;
  }

  /**
   * MUL: s[top-2] = s[top-1] * s[top-2], return top-1. Delegates to UInt256 for now.
   *
   * @param s the stack array
   * @param top the current top index
   * @return the new top index
   */
  public static int mul(final long[] s, final int top) {
    final int a = (top - 1) << 2;
    final int b = (top - 2) << 2;
    UInt256 va = new UInt256(s[a], s[a + 1], s[a + 2], s[a + 3]);
    UInt256 vb = new UInt256(s[b], s[b + 1], s[b + 2], s[b + 3]);
    UInt256 r = va.mul(vb);
    s[b] = r.u3();
    s[b + 1] = r.u2();
    s[b + 2] = r.u1();
    s[b + 3] = r.u0();
    return top - 1;
  }

  /**
   * DIV: s[top-2] = s[top-1] / s[top-2], return top-1. Delegates to UInt256.
   *
   * @param s the stack array
   * @param top the current top index
   * @return the new top index
   */
  public static int div(final long[] s, final int top) {
    final int a = (top - 1) << 2;
    final int b = (top - 2) << 2;
    UInt256 va = new UInt256(s[a], s[a + 1], s[a + 2], s[a + 3]);
    UInt256 vb = new UInt256(s[b], s[b + 1], s[b + 2], s[b + 3]);
    UInt256 r = va.div(vb);
    s[b] = r.u3();
    s[b + 1] = r.u2();
    s[b + 2] = r.u1();
    s[b + 3] = r.u0();
    return top - 1;
  }

  /**
   * SDIV: s[top-2] = s[top-1] sdiv s[top-2], return top-1. Delegates to UInt256.
   *
   * @param s the stack array
   * @param top the current top index
   * @return the new top index
   */
  public static int signedDiv(final long[] s, final int top) {
    final int a = (top - 1) << 2;
    final int b = (top - 2) << 2;
    UInt256 va = new UInt256(s[a], s[a + 1], s[a + 2], s[a + 3]);
    UInt256 vb = new UInt256(s[b], s[b + 1], s[b + 2], s[b + 3]);
    UInt256 r = va.signedDiv(vb);
    s[b] = r.u3();
    s[b + 1] = r.u2();
    s[b + 2] = r.u1();
    s[b + 3] = r.u0();
    return top - 1;
  }

  /**
   * MOD: s[top-2] = s[top-1] mod s[top-2], return top-1. Delegates to UInt256.
   *
   * @param s the stack array
   * @param top the current top index
   * @return the new top index
   */
  public static int mod(final long[] s, final int top) {
    final int a = (top - 1) << 2;
    final int b = (top - 2) << 2;
    UInt256 va = new UInt256(s[a], s[a + 1], s[a + 2], s[a + 3]);
    UInt256 vb = new UInt256(s[b], s[b + 1], s[b + 2], s[b + 3]);
    UInt256 r = va.mod(vb);
    s[b] = r.u3();
    s[b + 1] = r.u2();
    s[b + 2] = r.u1();
    s[b + 3] = r.u0();
    return top - 1;
  }

  /**
   * SMOD: s[top-2] = s[top-1] smod s[top-2], return top-1. Delegates to UInt256.
   *
   * @param s the stack array
   * @param top the current top index
   * @return the new top index
   */
  public static int signedMod(final long[] s, final int top) {
    final int a = (top - 1) << 2;
    final int b = (top - 2) << 2;
    UInt256 va = new UInt256(s[a], s[a + 1], s[a + 2], s[a + 3]);
    UInt256 vb = new UInt256(s[b], s[b + 1], s[b + 2], s[b + 3]);
    UInt256 r = va.signedMod(vb);
    s[b] = r.u3();
    s[b + 1] = r.u2();
    s[b + 2] = r.u1();
    s[b + 3] = r.u0();
    return top - 1;
  }

  /**
   * EXP: s[top-2] = s[top-1] ** s[top-2] mod 2^256, return top-1.
   *
   * @param s the stack array
   * @param top the current top index
   * @return the new top index
   */
  public static int exp(final long[] s, final int top) {
    final int a = (top - 1) << 2; // base
    final int b = (top - 2) << 2; // exponent
    UInt256 base = new UInt256(s[a], s[a + 1], s[a + 2], s[a + 3]);
    UInt256 power = new UInt256(s[b], s[b + 1], s[b + 2], s[b + 3]);
    BigInteger result = base.toBigInteger().modPow(power.toBigInteger(), BigInteger.TWO.pow(256));
    byte[] rBytes = result.toByteArray();
    if (rBytes.length > 32) {
      rBytes = Arrays.copyOfRange(rBytes, rBytes.length - 32, rBytes.length);
    } else if (rBytes.length < 32) {
      byte[] padded = new byte[32];
      System.arraycopy(rBytes, 0, padded, 32 - rBytes.length, rBytes.length);
      rBytes = padded;
    }
    UInt256 r = UInt256.fromBytesBE(rBytes);
    s[b] = r.u3();
    s[b + 1] = r.u2();
    s[b + 2] = r.u1();
    s[b + 3] = r.u0();
    return top - 1;
  }

  // endregion

  // region Bitwise Binary (pop 2, push 1, return top-1)
  // ---------------------------------------------------------------------------

  /**
   * AND: s[top-2] = s[top-1] &amp; s[top-2], return top-1.
   *
   * @param s the stack array
   * @param top the current top index
   * @return the new top index
   */
  public static int and(final long[] s, final int top) {
    final int a = (top - 1) << 2;
    final int b = (top - 2) << 2;
    s[b] = s[a] & s[b];
    s[b + 1] = s[a + 1] & s[b + 1];
    s[b + 2] = s[a + 2] & s[b + 2];
    s[b + 3] = s[a + 3] & s[b + 3];
    return top - 1;
  }

  /**
   * OR: s[top-2] = s[top-1] | s[top-2], return top-1.
   *
   * @param s the stack array
   * @param top the current top index
   * @return the new top index
   */
  public static int or(final long[] s, final int top) {
    final int a = (top - 1) << 2;
    final int b = (top - 2) << 2;
    s[b] = s[a] | s[b];
    s[b + 1] = s[a + 1] | s[b + 1];
    s[b + 2] = s[a + 2] | s[b + 2];
    s[b + 3] = s[a + 3] | s[b + 3];
    return top - 1;
  }

  /**
   * XOR: s[top-2] = s[top-1] ^ s[top-2], return top-1.
   *
   * @param s the stack array
   * @param top the current top index
   * @return the new top index
   */
  public static int xor(final long[] s, final int top) {
    final int a = (top - 1) << 2;
    final int b = (top - 2) << 2;
    s[b] = s[a] ^ s[b];
    s[b + 1] = s[a + 1] ^ s[b + 1];
    s[b + 2] = s[a + 2] ^ s[b + 2];
    s[b + 3] = s[a + 3] ^ s[b + 3];
    return top - 1;
  }

  /**
   * BYTE: s[top-2] = byte at offset s[top-1] of s[top-2], return top-1.
   *
   * @param s the stack array
   * @param top the current top index
   * @return the new top index
   */
  public static int byte_(final long[] s, final int top) {
    final int a = (top - 1) << 2; // offset
    final int b = (top - 2) << 2; // value
    // offset must be 0..31
    if (s[a] != 0 || s[a + 1] != 0 || s[a + 2] != 0 || s[a + 3] < 0 || s[a + 3] >= 32) {
      s[b] = 0;
      s[b + 1] = 0;
      s[b + 2] = 0;
      s[b + 3] = 0;
      return top - 1;
    }
    int idx = (int) s[a + 3]; // 0..31, big-endian byte index
    // Determine which limb and bit position
    // byte 0 is the MSB of u3, byte 31 is the LSB of u0
    int limbIdx = idx >> 3; // which limb offset from base (0=u3, 3=u0)
    int byteInLimb = 7 - (idx & 7); // byte position within limb (7=MSB, 0=LSB)
    long limb = s[b + limbIdx];
    long result = (limb >>> (byteInLimb << 3)) & 0xFFL;
    s[b] = 0;
    s[b + 1] = 0;
    s[b + 2] = 0;
    s[b + 3] = result;
    return top - 1;
  }

  /**
   * SIGNEXTEND: sign-extend s[top-2] from byte s[top-1], return top-1.
   *
   * @param s the stack array
   * @param top the current top index
   * @return the new top index
   */
  public static int signExtend(final long[] s, final int top) {
    final int a = (top - 1) << 2; // byte index b
    final int b = (top - 2) << 2; // value
    // If b >= 31, no extension needed
    if (s[a] != 0 || s[a + 1] != 0 || s[a + 2] != 0 || s[a + 3] >= 31) {
      // result is just the value unchanged
      return top - 1;
    }
    int byteIdx = (int) s[a + 3]; // 0..30
    // The sign bit is at bit (byteIdx * 8 + 7) from LSB
    int signBit = byteIdx * 8 + 7;
    int limbIdx = signBit >> 6; // which limb (0=u0/LSB)
    int bitInLimb = signBit & 63;
    // Read from the value slot - limbs are stored [u3, u2, u1, u0] at [b, b+1, b+2, b+3]
    long limb = s[b + 3 - limbIdx];
    boolean isNeg = ((limb >>> bitInLimb) & 1L) != 0;
    if (isNeg) {
      // Set all bits above signBit to 1
      s[b + 3 - limbIdx] = limb | (-1L << bitInLimb);
      for (int i = limbIdx + 1; i < 4; i++) {
        s[b + 3 - i] = -1L;
      }
    } else {
      // Clear all bits above signBit to 0
      if (bitInLimb < 63) {
        s[b + 3 - limbIdx] = limb & ((1L << (bitInLimb + 1)) - 1);
      }
      for (int i = limbIdx + 1; i < 4; i++) {
        s[b + 3 - i] = 0;
      }
    }
    return top - 1;
  }

  // endregion

  // region Unary Operations (pop 1, push 1, return top)
  // ---------------------------------------------------------------------------

  /**
   * NOT: s[top-1] = ~s[top-1], return top.
   *
   * @param s the stack array
   * @param top the current top index
   * @return the new top index
   */
  public static int not(final long[] s, final int top) {
    final int a = (top - 1) << 2;
    s[a] = ~s[a];
    s[a + 1] = ~s[a + 1];
    s[a + 2] = ~s[a + 2];
    s[a + 3] = ~s[a + 3];
    return top;
  }

  /**
   * ISZERO: s[top-1] = (s[top-1] == 0) ? 1 : 0, return top.
   *
   * @param s the stack array
   * @param top the current top index
   * @return the new top index
   */
  public static int isZero(final long[] s, final int top) {
    final int a = (top - 1) << 2;
    boolean zero = (s[a] | s[a + 1] | s[a + 2] | s[a + 3]) == 0;
    s[a] = 0;
    s[a + 1] = 0;
    s[a + 2] = 0;
    s[a + 3] = zero ? 1L : 0L;
    return top;
  }

  /**
   * CLZ: s[top-1] = count leading zeros of s[top-1], return top.
   *
   * @param s the stack array
   * @param top the current top index
   * @return the new top index
   */
  public static int clz(final long[] s, final int top) {
    final int a = (top - 1) << 2;
    int result;
    if (s[a] != 0) {
      result = Long.numberOfLeadingZeros(s[a]);
    } else if (s[a + 1] != 0) {
      result = 64 + Long.numberOfLeadingZeros(s[a + 1]);
    } else if (s[a + 2] != 0) {
      result = 128 + Long.numberOfLeadingZeros(s[a + 2]);
    } else {
      result = 192 + Long.numberOfLeadingZeros(s[a + 3]);
    }
    s[a] = 0;
    s[a + 1] = 0;
    s[a + 2] = 0;
    s[a + 3] = result;
    return top;
  }

  // endregion

  // region Comparison Operations (pop 2, push 1, return top-1)
  // ---------------------------------------------------------------------------

  /**
   * LT: s[top-2] = (s[top-1] &lt; s[top-2]) ? 1 : 0, return top-1. Unsigned.
   *
   * @param s the stack array
   * @param top the current top index
   * @return the new top index
   */
  public static int lt(final long[] s, final int top) {
    final int a = (top - 1) << 2;
    final int b = (top - 2) << 2;
    boolean less = unsignedLt(s, a, s, b);
    s[b] = 0;
    s[b + 1] = 0;
    s[b + 2] = 0;
    s[b + 3] = less ? 1L : 0L;
    return top - 1;
  }

  /**
   * GT: s[top-2] = (s[top-1] &gt; s[top-2]) ? 1 : 0, return top-1. Unsigned.
   *
   * @param s the stack array
   * @param top the current top index
   * @return the new top index
   */
  public static int gt(final long[] s, final int top) {
    final int a = (top - 1) << 2;
    final int b = (top - 2) << 2;
    boolean greater = unsignedLt(s, b, s, a);
    s[b] = 0;
    s[b + 1] = 0;
    s[b + 2] = 0;
    s[b + 3] = greater ? 1L : 0L;
    return top - 1;
  }

  /**
   * SLT: s[top-2] = (s[top-1] &lt;s s[top-2]) ? 1 : 0, return top-1. Signed.
   *
   * @param s the stack array
   * @param top the current top index
   * @return the new top index
   */
  public static int slt(final long[] s, final int top) {
    final int a = (top - 1) << 2;
    final int b = (top - 2) << 2;
    int cmp = signedCompare(s, a, s, b);
    s[b] = 0;
    s[b + 1] = 0;
    s[b + 2] = 0;
    s[b + 3] = cmp < 0 ? 1L : 0L;
    return top - 1;
  }

  /**
   * SGT: s[top-2] = (s[top-1] &gt;s s[top-2]) ? 1 : 0, return top-1. Signed.
   *
   * @param s the stack array
   * @param top the current top index
   * @return the new top index
   */
  public static int sgt(final long[] s, final int top) {
    final int a = (top - 1) << 2;
    final int b = (top - 2) << 2;
    int cmp = signedCompare(s, a, s, b);
    s[b] = 0;
    s[b + 1] = 0;
    s[b + 2] = 0;
    s[b + 3] = cmp > 0 ? 1L : 0L;
    return top - 1;
  }

  /**
   * EQ: s[top-2] = (s[top-1] == s[top-2]) ? 1 : 0, return top-1.
   *
   * @param s the stack array
   * @param top the current top index
   * @return the new top index
   */
  public static int eq(final long[] s, final int top) {
    final int a = (top - 1) << 2;
    final int b = (top - 2) << 2;
    boolean equal =
        s[a] == s[b] && s[a + 1] == s[b + 1] && s[a + 2] == s[b + 2] && s[a + 3] == s[b + 3];
    s[b] = 0;
    s[b + 1] = 0;
    s[b + 2] = 0;
    s[b + 3] = equal ? 1L : 0L;
    return top - 1;
  }

  // endregion

  // region Ternary Operations (pop 3, push 1, return top-2)
  // ---------------------------------------------------------------------------

  /**
   * ADDMOD: s[top-3] = (s[top-1] + s[top-2]) mod s[top-3], return top-2.
   *
   * @param s the stack array
   * @param top the current top index
   * @return the new top index
   */
  public static int addMod(final long[] s, final int top) {
    final int a = (top - 1) << 2;
    final int b = (top - 2) << 2;
    final int c = (top - 3) << 2;
    UInt256 va = new UInt256(s[a], s[a + 1], s[a + 2], s[a + 3]);
    UInt256 vb = new UInt256(s[b], s[b + 1], s[b + 2], s[b + 3]);
    UInt256 vc = new UInt256(s[c], s[c + 1], s[c + 2], s[c + 3]);
    UInt256 r = vc.isZero() ? UInt256.ZERO : va.addMod(vb, vc);
    s[c] = r.u3();
    s[c + 1] = r.u2();
    s[c + 2] = r.u1();
    s[c + 3] = r.u0();
    return top - 2;
  }

  /**
   * MULMOD: s[top-3] = (s[top-1] * s[top-2]) mod s[top-3], return top-2.
   *
   * @param s the stack array
   * @param top the current top index
   * @return the new top index
   */
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

  // endregion

  // region Stack Manipulation
  // ---------------------------------------------------------------------------

  /**
   * DUP: copy slot at depth to new top, return top+1. depth is 1-based (DUP1 = depth 1).
   *
   * @param s the stack array
   * @param top the current top index
   * @param depth the 1-based depth of the slot to duplicate
   * @return the new top index
   */
  public static int dup(final long[] s, final int top, final int depth) {
    final int src = (top - depth) << 2;
    final int dst = top << 2;
    s[dst] = s[src];
    s[dst + 1] = s[src + 1];
    s[dst + 2] = s[src + 2];
    s[dst + 3] = s[src + 3];
    return top + 1;
  }

  /**
   * SWAP: swap top with slot at depth, return top. depth is 1-based (SWAP1 = depth 1).
   *
   * @param s the stack array
   * @param top the current top index
   * @param depth the 1-based depth of the slot to swap with the top
   * @return the new top index
   */
  public static int swap(final long[] s, final int top, final int depth) {
    final int a = (top - 1) << 2;
    final int b = (top - 1 - depth) << 2;
    long t;
    t = s[a];
    s[a] = s[b];
    s[b] = t;
    t = s[a + 1];
    s[a + 1] = s[b + 1];
    s[b + 1] = t;
    t = s[a + 2];
    s[a + 2] = s[b + 2];
    s[b + 2] = t;
    t = s[a + 3];
    s[a + 3] = s[b + 3];
    s[b + 3] = t;
    return top;
  }

  /**
   * EXCHANGE: swap slot at n with slot at m (both 0-indexed from top), return top.
   *
   * @param s the stack array
   * @param top the current top index
   * @param n the 0-based index of the first slot (from top)
   * @param m the 0-based index of the second slot (from top)
   * @return the new top index
   */
  public static int exchange(final long[] s, final int top, final int n, final int m) {
    final int a = (top - 1 - n) << 2;
    final int b = (top - 1 - m) << 2;
    long t;
    t = s[a];
    s[a] = s[b];
    s[b] = t;
    t = s[a + 1];
    s[a + 1] = s[b + 1];
    s[b + 1] = t;
    t = s[a + 2];
    s[a + 2] = s[b + 2];
    s[b + 2] = t;
    t = s[a + 3];
    s[a + 3] = s[b + 3];
    s[b + 3] = t;
    return top;
  }

  /**
   * PUSH0: push zero, return top+1.
   *
   * @param s the stack array
   * @param top the current top index
   * @return the new top index
   */
  public static int pushZero(final long[] s, final int top) {
    final int dst = top << 2;
    s[dst] = 0;
    s[dst + 1] = 0;
    s[dst + 2] = 0;
    s[dst + 3] = 0;
    return top + 1;
  }

  /**
   * PUSH1..PUSH32: decode bytes from code into a new top slot, return top+1.
   *
   * @param s the stack array
   * @param top the current top index
   * @param code the bytecode array
   * @param start the start offset within the code array
   * @param len the number of bytes to push (1-32)
   * @return the new top index
   */
  public static int pushFromBytes(
      final long[] s, final int top, final byte[] code, final int start, final int len) {
    final int dst = top << 2;

    if (start >= code.length) {
      s[dst] = 0;
      s[dst + 1] = 0;
      s[dst + 2] = 0;
      s[dst + 3] = 0;
      return top + 1;
    }
    final int copyLen = Math.min(len, code.length - start);

    s[dst] = 0;
    s[dst + 1] = 0;
    s[dst + 2] = 0;
    s[dst + 3] = 0;

    if (copyLen == len) {
      // Fast path: all bytes available (common case — not near end of code)
      if (len <= 8) {
        s[dst + 3] = buildLong(code, start, len);
      } else if (len <= 16) {
        final int hiLen = len - 8;
        s[dst + 2] = buildLong(code, start, hiLen);
        s[dst + 3] = bytesToLong(code, start + hiLen);
      } else if (len <= 24) {
        final int hiLen = len - 16;
        s[dst + 1] = buildLong(code, start, hiLen);
        s[dst + 2] = bytesToLong(code, start + hiLen);
        s[dst + 3] = bytesToLong(code, start + hiLen + 8);
      } else {
        final int hiLen = len - 24;
        s[dst] = buildLong(code, start, hiLen);
        s[dst + 1] = bytesToLong(code, start + hiLen);
        s[dst + 2] = bytesToLong(code, start + hiLen + 8);
        s[dst + 3] = bytesToLong(code, start + hiLen + 16);
      }
    } else {
      // Truncated push (rare: near end of code). Right-pad with zeros.
      int bytePos = len - 1;
      for (int i = 0; i < copyLen; i++) {
        int limbOffset = 3 - (bytePos >> 3);
        int shift = (bytePos & 7) << 3;
        s[dst + limbOffset] |= (code[start + i] & 0xFFL) << shift;
        bytePos--;
      }
    }
    return top + 1;
  }

  /**
   * Push a long value (GAS, NUMBER, etc.), return top+1.
   *
   * @param s the stack array
   * @param top the current top index
   * @param value the long value to push
   * @return the new top index
   */
  public static int pushLong(final long[] s, final int top, final long value) {
    final int dst = top << 2;
    s[dst] = 0;
    s[dst + 1] = 0;
    s[dst + 2] = 0;
    s[dst + 3] = value;
    return top + 1;
  }

  /**
   * Push a Wei value onto the stack, return top+1.
   *
   * @param s the stack array
   * @param top the current top index
   * @param value the Wei value to push
   * @return the new top index
   */
  public static int pushWei(final long[] s, final int top, final Wei value) {
    final int dst = top << 2;
    final byte[] bytes = value.toArrayUnsafe();
    s[dst] = getLong(bytes, 0);
    s[dst + 1] = getLong(bytes, 8);
    s[dst + 2] = getLong(bytes, 16);
    s[dst + 3] = getLong(bytes, 24);
    return top + 1;
  }

  /**
   * Push an Address (20 bytes), return top+1.
   *
   * @param s the stack array
   * @param top the current top index
   * @param addr the address to push
   * @return the new top index
   */
  public static int pushAddress(final long[] s, final int top, final Address addr) {
    final int dst = top << 2;
    byte[] bytes = addr.getBytes().toArrayUnsafe();
    // Address is 20 bytes: fits in u2(4 bytes) + u1(8 bytes) + u0(8 bytes)
    s[dst] = 0; // u3
    s[dst + 1] = getInt(bytes, 0) & 0xFFFFFFFFL; // u2 (top 4 bytes)
    s[dst + 2] = getLong(bytes, 4); // u1
    s[dst + 3] = getLong(bytes, 12); // u0
    return top + 1;
  }

  // endregion

  // region Boundary Helpers (read/write slots without changing top)
  // ---------------------------------------------------------------------------

  /**
   * Extract u0 (LSB limb) of slot at depth from top.
   *
   * @param s the stack array
   * @param top the current top index
   * @param depth the 0-based depth from the top
   * @return the least-significant limb of the slot
   */
  public static long longAt(final long[] s, final int top, final int depth) {
    return s[((top - 1 - depth) << 2) + 3];
  }

  /**
   * Check if slot at depth is zero.
   *
   * @param s the stack array
   * @param top the current top index
   * @param depth the 0-based depth from the top
   * @return true if the slot is zero
   */
  public static boolean isZeroAt(final long[] s, final int top, final int depth) {
    final int off = (top - 1 - depth) << 2;
    return (s[off] | s[off + 1] | s[off + 2] | s[off + 3]) == 0;
  }

  /**
   * Check if slot at depth fits in a non-negative int.
   *
   * @param s the stack array
   * @param top the current top index
   * @param depth the 0-based depth from the top
   * @return true if the slot value fits in a non-negative int
   */
  public static boolean fitsInInt(final long[] s, final int top, final int depth) {
    final int off = (top - 1 - depth) << 2;
    return s[off] == 0
        && s[off + 1] == 0
        && s[off + 2] == 0
        && s[off + 3] >= 0
        && s[off + 3] <= Integer.MAX_VALUE;
  }

  /**
   * Check if slot at depth fits in a non-negative long.
   *
   * @param s the stack array
   * @param top the current top index
   * @param depth the 0-based depth from the top
   * @return true if the slot value fits in a non-negative long
   */
  public static boolean fitsInLong(final long[] s, final int top, final int depth) {
    final int off = (top - 1 - depth) << 2;
    return s[off] == 0 && s[off + 1] == 0 && s[off + 2] == 0 && s[off + 3] >= 0;
  }

  /**
   * Clamp slot at depth to long, returning Long.MAX_VALUE if it doesn't fit in a non-negative long.
   *
   * @param s the stack array
   * @param top the current top index
   * @param depth the 0-based depth from the top
   * @return the slot value as a long, or Long.MAX_VALUE if it does not fit
   */
  public static long clampedToLong(final long[] s, final int top, final int depth) {
    final int off = (top - 1 - depth) << 2;
    if (s[off] != 0 || s[off + 1] != 0 || s[off + 2] != 0 || s[off + 3] < 0) {
      return Long.MAX_VALUE;
    }
    return s[off + 3];
  }

  /**
   * Clamp slot at depth to int, returning Integer.MAX_VALUE if it doesn't fit in a non-negative
   * int.
   *
   * @param s the stack array
   * @param top the current top index
   * @param depth the 0-based depth from the top
   * @return the slot value as an int, or Integer.MAX_VALUE if it does not fit
   */
  public static int clampedToInt(final long[] s, final int top, final int depth) {
    final int off = (top - 1 - depth) << 2;
    if (s[off] != 0
        || s[off + 1] != 0
        || s[off + 2] != 0
        || s[off + 3] < 0
        || s[off + 3] > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return (int) s[off + 3];
  }

  /**
   * Write 32 big-endian bytes from slot at depth into dst.
   *
   * @param s the stack array
   * @param top the current top index
   * @param depth the 0-based depth from the top
   * @param dst the destination byte array (must have at least 32 bytes)
   */
  public static void toBytesAt(final long[] s, final int top, final int depth, final byte[] dst) {
    final int off = (top - 1 - depth) << 2;
    longIntoBytes(dst, 0, s[off]);
    longIntoBytes(dst, 8, s[off + 1]);
    longIntoBytes(dst, 16, s[off + 2]);
    longIntoBytes(dst, 24, s[off + 3]);
  }

  /**
   * Read bytes into slot at depth from src[srcOff..srcOff+len). Pads with zeros.
   *
   * @param s the stack array
   * @param top the current top index
   * @param depth the 0-based depth from the top
   * @param src the source byte array
   * @param srcOff the start offset within the source array
   * @param len the number of bytes to read (up to 32)
   */
  public static void fromBytesAt(
      final long[] s,
      final int top,
      final int depth,
      final byte[] src,
      final int srcOff,
      final int len) {
    final int off = (top - 1 - depth) << 2;
    // Fast path: full 32-byte read — decode 8 bytes per limb
    if (len >= 32 && srcOff + 32 <= src.length) {
      s[off] = bytesToLong(src, srcOff);
      s[off + 1] = bytesToLong(src, srcOff + 8);
      s[off + 2] = bytesToLong(src, srcOff + 16);
      s[off + 3] = bytesToLong(src, srcOff + 24);
      return;
    }
    // Slow path: variable-length, byte-by-byte
    s[off] = 0;
    s[off + 1] = 0;
    s[off + 2] = 0;
    s[off + 3] = 0;
    int end = srcOff + Math.min(len, 32);
    if (end > src.length) end = src.length;
    int pos = 0;
    for (int i = srcOff; i < end; i++, pos++) {
      int limbIdx = pos >> 3;
      int shift = (7 - (pos & 7)) << 3;
      s[off + limbIdx] |= (src[i] & 0xFFL) << shift;
    }
  }

  /**
   * Extract 20-byte Address from slot at depth.
   *
   * @param s the stack array
   * @param top the current top index
   * @param depth the 0-based depth from the top
   * @return the address stored in the slot
   */
  public static Address toAddressAt(final long[] s, final int top, final int depth) {
    final int off = (top - 1 - depth) << 2;
    byte[] bytes = new byte[20];
    // u2 has top 4 bytes, u1 has next 8, u0 has last 8
    putInt(bytes, 0, (int) s[off + 1]);
    putLong(bytes, 4, s[off + 2]);
    putLong(bytes, 12, s[off + 3]);
    return Address.wrap(org.apache.tuweni.bytes.Bytes.wrap(bytes));
  }

  /**
   * Materialize UInt256 record from slot at depth (boundary only).
   *
   * @param s the stack array
   * @param top the current top index
   * @param depth the 0-based depth from the top
   * @return the UInt256 value at the slot
   */
  public static UInt256 getAt(final long[] s, final int top, final int depth) {
    final int off = (top - 1 - depth) << 2;
    return new UInt256(s[off], s[off + 1], s[off + 2], s[off + 3]);
  }

  /**
   * Write a Wei value into slot at depth.
   *
   * @param s the stack array
   * @param top the current top index
   * @param depth the 0-based depth from the top
   * @param value the Wei value to write
   */
  public static void putWeiAt(final long[] s, final int top, final int depth, final Wei value) {
    final int off = (top - 1 - depth) << 2;
    final byte[] bytes = value.toArrayUnsafe();
    s[off] = getLong(bytes, 0);
    s[off + 1] = getLong(bytes, 8);
    s[off + 2] = getLong(bytes, 16);
    s[off + 3] = getLong(bytes, 24);
  }

  /**
   * Write UInt256 record into slot at depth.
   *
   * @param s the stack array
   * @param top the current top index
   * @param depth the 0-based depth from the top
   * @param val the UInt256 value to write
   */
  public static void putAt(final long[] s, final int top, final int depth, final UInt256 val) {
    final int off = (top - 1 - depth) << 2;
    s[off] = val.u3();
    s[off + 1] = val.u2();
    s[off + 2] = val.u1();
    s[off + 3] = val.u0();
  }

  /**
   * Write raw limbs into slot at depth.
   *
   * @param s the stack array
   * @param top the current top index
   * @param depth the 0-based depth from the top
   * @param u3 the most-significant limb
   * @param u2 the second limb
   * @param u1 the third limb
   * @param u0 the least-significant limb
   */
  public static void putAt(
      final long[] s,
      final int top,
      final int depth,
      final long u3,
      final long u2,
      final long u1,
      final long u0) {
    final int off = (top - 1 - depth) << 2;
    s[off] = u3;
    s[off + 1] = u2;
    s[off + 2] = u1;
    s[off + 3] = u0;
  }

  /**
   * Number of significant bytes in slot at depth. Used by EXP gas calculation.
   *
   * @param s the stack array
   * @param top the current top index
   * @param depth the 0-based depth from the top
   * @return the number of significant bytes in the slot
   */
  public static int byteLengthAt(final long[] s, final int top, final int depth) {
    final int off = (top - 1 - depth) << 2;
    if (s[off] != 0) return 24 + byteLen(s[off]);
    if (s[off + 1] != 0) return 16 + byteLen(s[off + 1]);
    if (s[off + 2] != 0) return 8 + byteLen(s[off + 2]);
    if (s[off + 3] != 0) return byteLen(s[off + 3]);
    return 0;
  }

  // endregion

  // region Private Helpers
  // ---------------------------------------------------------------------------

  private static long getLong(final byte[] b, final int off) {
    return (long) LONG_BE.get(b, off);
  }

  private static void putLong(final byte[] b, final int off, final long v) {
    LONG_BE.set(b, off, v);
  }

  private static int getInt(final byte[] b, final int off) {
    return (int) INT_BE.get(b, off);
  }

  private static void putInt(final byte[] b, final int off, final int v) {
    INT_BE.set(b, off, v);
  }

  /** Build a long from 1-8 big-endian bytes. */
  private static long buildLong(final byte[] src, final int off, final int len) {
    long v = 0;
    for (int i = off, end = off + len; i < end; i++) {
      v = (v << 8) | (src[i] & 0xFFL);
    }
    return v;
  }

  /** Decode 8 big-endian bytes from src[off] into a long. */
  private static long bytesToLong(final byte[] src, final int off) {
    return getLong(src, off);
  }

  private static void longIntoBytes(final byte[] bytes, final int offset, final long value) {
    putLong(bytes, offset, value);
  }

  private static int byteLen(final long v) {
    return (64 - Long.numberOfLeadingZeros(v) + 7) / 8;
  }

  /** Unsigned less-than comparison of two slots. */
  private static boolean unsignedLt(
      final long[] s1, final int off1, final long[] s2, final int off2) {
    // Compare u3 first (MSB)
    if (s1[off1] != s2[off2]) return Long.compareUnsigned(s1[off1], s2[off2]) < 0;
    if (s1[off1 + 1] != s2[off2 + 1]) return Long.compareUnsigned(s1[off1 + 1], s2[off2 + 1]) < 0;
    if (s1[off1 + 2] != s2[off2 + 2]) return Long.compareUnsigned(s1[off1 + 2], s2[off2 + 2]) < 0;
    return Long.compareUnsigned(s1[off1 + 3], s2[off2 + 3]) < 0;
  }

  /** Signed comparison of two slots. */
  private static int signedCompare(
      final long[] s1, final int off1, final long[] s2, final int off2) {
    boolean aNeg = s1[off1] < 0;
    boolean bNeg = s2[off2] < 0;
    if (aNeg && !bNeg) return -1;
    if (!aNeg && bNeg) return 1;
    // Same sign: unsigned compare gives correct result
    if (s1[off1] != s2[off2]) return Long.compareUnsigned(s1[off1], s2[off2]);
    if (s1[off1 + 1] != s2[off2 + 1]) return Long.compareUnsigned(s1[off1 + 1], s2[off2 + 1]);
    if (s1[off1 + 2] != s2[off2 + 2]) return Long.compareUnsigned(s1[off1 + 2], s2[off2 + 2]);
    return Long.compareUnsigned(s1[off1 + 3], s2[off2 + 3]);
  }

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

  // endregion
}
