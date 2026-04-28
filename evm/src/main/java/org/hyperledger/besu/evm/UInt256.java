/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.evm;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.math.BigInteger;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * 256-bits wide unsigned integer class.
 *
 * <p>This class is an optimised version of BigInteger for fixed width 256-bits integers.
 *
 * @param u3 4th digit = most significant limb
 * @param u2 3rd digit
 * @param u1 2nd digit
 * @param u0 1st digit = least significant limb
 */
public record UInt256(long u3, long u2, long u1, long u0) {

  // region Values
  // --------------------------------------------------------------------------
  // UInt256 represents a big-endian 256-bits integer.
  // As opposed to Java int, operations are by default unsigned,
  // and signed version are interpreted in two-complements as usual.
  // --------------------------------------------------------------------------

  /** Fixed size in bytes. */
  public static final int BYTESIZE = 32;

  /** The constant 0. */
  public static final UInt256 ZERO = new UInt256(0, 0, 0, 0);

  public static final UInt256 ONE = new UInt256(0, 0, 0, 1);

  private static final byte[] ZERO_BYTES = new byte[BYTESIZE];

  /** The constant All ones */
  public static final UInt256 MAX =
      new UInt256(
          0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL);

  // --------------------------------------------------------------------------
  // endregion

  // region (private) Internal Values
  // --------------------------------------------------------------------------

  // Fixed number of limbs or digits
  // private static final int N_LIMBS = 4;
  // Fixed number of bits per limb.
  private static final int N_BITS_PER_LIMB = 64;

  private static final int N_BYTES_PER_LIMB = 8;

  private static final VarHandle LONG_BE =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

  // --------------------------------------------------------------------------
  // endregion

  // region Alternative Constructors
  // --------------------------------------------------------------------------

  /**
   * Instantiates a new UInt256 from byte array.
   *
   * @param bytes raw bytes in BigEndian order.
   * @return Big-endian UInt256 represented by the bytes.
   */
  public static UInt256 fromBytesBE(final byte[] bytes) {
    if (bytes.length == 0) {
      return ZERO;
    }
    if (bytes.length < 8) {
      return fromBytesSingleLimb(bytes);
    }
    int prevIndex = bytes.length;
    int nextIndex = prevIndex - 8;
    final long u0 = getLong(bytes, nextIndex, prevIndex);
    prevIndex = nextIndex;

    nextIndex = Math.max(0, prevIndex - 8);
    final long u1 = getLong(bytes, nextIndex, prevIndex);
    prevIndex = nextIndex;

    nextIndex = Math.max(0, prevIndex - 8);
    final long u2 = getLong(bytes, nextIndex, prevIndex);
    prevIndex = nextIndex;

    nextIndex = Math.max(0, bytes.length - BYTESIZE);
    final long u3 = getLong(bytes, nextIndex, prevIndex);

    return new UInt256(u3, u2, u1, u0);
  }

  private static long getLong(final byte[] bytes, final int from, final int to) {
    int shift = (N_BYTES_PER_LIMB + from - to) * 8;
    final long value = (long) LONG_BE.get(bytes, from);
    return shift == N_BITS_PER_LIMB ? 0L : value >>> shift;
  }

  private static UInt256 fromBytesSingleLimb(final byte[] bytes) {
    long value = 0;
    for (int i = bytes.length - 1, shift = 0; i >= 0; i--, shift += 8) {
      value |= ((bytes[i] & 0xFFL) << shift);
    }
    return new UInt256(0, 0, 0, value);
  }

  /**
   * Instantiates a new UInt256 from an int.
   *
   * @param value int value to convert to UInt256.
   * @return The UInt256 equivalent of value.
   */
  public static UInt256 fromInt(final int value) {
    return new UInt256(0, 0, 0, value & 0xFFFFFFFFL);
  }

  /**
   * Instantiates a new UInt256 from a long.
   *
   * @param value long value to convert to UInt256.
   * @return The UInt256 equivalent of value.
   */
  public static UInt256 fromLong(final long value) {
    return new UInt256(0, 0, 0, value);
  }

  /**
   * Instantiates a new UInt256 from an array.
   *
   * <p>Read digits from an array starting from the end. The array must have at least N_LIMBS
   * elements.
   *
   * @param limbs The array holding the digits.
   * @return The UInt256 from the array
   */
  public static UInt256 fromArray(final long[] limbs) {
    int i = limbs.length;
    long z0 = limbs[--i];
    long z1 = limbs[--i];
    long z2 = limbs[--i];
    long z3 = limbs[--i];
    return new UInt256(z3, z2, z1, z0);
  }

  // --------------------------------------------------------------------------
  // endregion

  // region Conversions
  // --------------------------------------------------------------------------
  /**
   * Convert to int.
   *
   * @return Value truncated to an int, possibly lossy.
   */
  public int intValue() {
    return (int) u0;
  }

  /**
   * Convert to long.
   *
   * @return Value truncated to a long, possibly lossy.
   */
  public long longValue() {
    return u0;
  }

  /**
   * Convert to BigEndian byte array.
   *
   * @return Big-endian ordered bytes for this UInt256 value.
   */
  public byte[] toBytesBE() {
    byte[] result = new byte[BYTESIZE];
    LONG_BE.set(result, 0, u3);
    LONG_BE.set(result, 8, u2);
    LONG_BE.set(result, 16, u1);
    LONG_BE.set(result, 24, u0);
    return result;
  }

  /**
   * Convert to BigInteger.
   *
   * @return BigInteger representing the integer.
   */
  public BigInteger toBigInteger() {
    return new BigInteger(1, toBytesBE());
  }

  /**
   * Convert to hexstring.
   *
   * <p>Convert this integer into big-endian hexstring representation.
   *
   * @return The hexstring representing the integer.
   */
  public String toHexString() {
    StringBuilder sb = new StringBuilder("0x");
    for (byte b : toBytesBE()) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }

  private UInt320 UInt320Value() {
    return new UInt320(0, u3, u2, u1, u0);
  }

  private UInt64 asUInt64() {
    return new UInt64(u0);
  }

  private UInt128 asUInt128() {
    return new UInt128(u1, u0);
  }

  private UInt192 asUInt192() {
    return new UInt192(u2, u1, u0);
  }

  // --------------------------------------------------------------------------
  // endregion

  // region Comparisons
  // --------------------------------------------------------------------------

  /**
   * Is the value 0 ?
   *
   * @return true if this UInt256 value is 0.
   */
  public boolean isZero() {
    return (u0 | u1 | u2 | u3) == 0;
  }

  /**
   * Is the value 1 ?
   *
   * @return true if this UInt256 value is 1.
   */
  public boolean isOne() {
    return ((u0 ^ 1L) | u1 | u2 | u3) == 0;
  }

  /**
   * Is the value 0 or 1 ?
   *
   * @return true if this UInt256 value is 1.
   */
  public boolean isZeroOrOne() {
    return ((u0 & -2L) | u1 | u2 | u3) == 0;
  }

  /**
   * Is the two complements signed representation of this integer negative.
   *
   * @return True if the two complements representation of this integer is negative.
   */
  public boolean isNegative() {
    return u3 < 0;
  }

  /**
   * Does the value fit a long.
   *
   * @return true if it has at most 1 effective digit.
   */
  public boolean isUInt64() {
    return (u1 | u2 | u3) == 0;
  }

  /**
   * Does the value fit 2 longs.
   *
   * @return true if it has at most 2 effective digits.
   */
  public boolean isUInt128() {
    return (u2 | u3) == 0;
  }

  /**
   * Does the value fit 3 longs.
   *
   * @return true if it has at most 3 effective digits.
   */
  public boolean isUInt192() {
    return u3 == 0;
  }

  /**
   * Compares two UInt256. If any of the values is `null` it is considered smaller than the other.
   *
   * @param u1 UInt256 value
   * @param u2 UInt256 value
   * @return 0 if this == that, negative if this &lt; that and positive if this &gt; that.
   */
  public static int compare(final UInt256 u1, final UInt256 u2) {
    if (u1 == null || u2 == null) {
      return Boolean.compare(u2 == null, u1 == null);
    }
    return u1.compareTo(u2);
  }

  private int compareTo(final UInt256 that) {
    if (u3 != that.u3) return Long.compareUnsigned(u3, that.u3);
    if (u2 != that.u2) return Long.compareUnsigned(u2, that.u2);
    if (u1 != that.u1) return Long.compareUnsigned(u1, that.u1);
    return Long.compareUnsigned(u0, that.u0);
  }

  /**
   * Compares a UInt256 value with a UInt512 value.
   *
   * @param that UInt512 value
   * @return 0 if this == that, negative if this &lt; that and positive if this &gt; that.
   */
  private int compareTo(final UInt512 that) {
    if ((that.u7 | that.u6 | that.u5 | that.u4) != 0) return -1;
    if (that.u3 != u3) return Long.compareUnsigned(u3, that.u3);
    if (that.u2 != u2) return Long.compareUnsigned(u2, that.u2);
    if (that.u1 != u1) return Long.compareUnsigned(u1, that.u1);
    return Long.compareUnsigned(u0, that.u0);
  }

  // --------------------------------------------------------------------------
  // endregion

  // region Bitwise Operations
  // --------------------------------------------------------------------------

  /**
   * Bitwise AND operation
   *
   * @param other The UInt256 to AND with this.
   * @return The UInt256 result from the bitwise AND operation
   */
  public UInt256 and(final UInt256 other) {
    return new UInt256(u3 & other.u3, u2 & other.u2, u1 & other.u1, u0 & other.u0);
  }

  /**
   * Bitwise XOR operation
   *
   * @param other The UInt256 to XOR with this.
   * @return The UInt256 result from the bitwise XOR operation
   */
  public UInt256 xor(final UInt256 other) {
    return new UInt256(u3 ^ other.u3, u2 ^ other.u2, u1 ^ other.u1, u0 ^ other.u0);
  }

  /**
   * Bitwise OR operation
   *
   * @param other The UInt256 to OR with this.
   * @return The UInt256 result from the bitwise OR operation
   */
  public UInt256 or(final UInt256 other) {
    return new UInt256(u3 | other.u3, u2 | other.u2, u1 | other.u1, u0 | other.u0);
  }

  /**
   * Bitwise NOT operation
   *
   * @return The UInt256 result from the bitwise NOT operation
   */
  public UInt256 not() {
    return new UInt256(~u3, ~u2, ~u1, ~u0);
  }

  /**
   * Bitwise shift left.
   *
   * @param shift The number of bits to shift left (at most 64).
   * @return The shifted UInt256.
   */
  public UInt256 shiftLeft(final int shift) {
    // Unchecked: 0 <= shift < 64
    if (shift == 0) return this;
    int invShift = (N_BITS_PER_LIMB - shift);
    long z0 = (u0 << shift);
    long z1 = (u1 << shift) | u0 >>> invShift;
    long z2 = (u2 << shift) | u1 >>> invShift;
    long z3 = (u3 << shift) | u2 >>> invShift;
    return new UInt256(z3, z2, z1, z0);
  }

  /**
   * Bitwise shift right.
   *
   * @param shift The number of bits to shift right (at most 64).
   * @return The shifted UInt256.
   */
  public UInt256 shiftRight(final int shift) {
    // Unchecked: 0 <= shift < 64
    if (shift == 0) return this;
    int invShift = (N_BITS_PER_LIMB - shift);
    long z3 = (u3 >>> shift);
    long z2 = (u2 >>> shift) | u3 << invShift;
    long z1 = (u1 >>> shift) | u2 << invShift;
    long z0 = (u0 >>> shift) | u1 << invShift;
    return new UInt256(z3, z2, z1, z0);
  }

  // --------------------------------------------------------------------------
  // endregion

  // region Arithmetic Operations
  // --------------------------------------------------------------------------

  /**
   * Compute the two-complement negative representation of this integer.
   *
   * @return The negative of this integer.
   */
  public UInt256 neg() {
    long z0 = ~u0 + 1;
    long carry = (z0 == 0) ? 1 : 0;
    long z1 = ~u1 + carry;
    carry = (z1 == 0 && carry == 1) ? 1 : 0;
    long z2 = ~u2 + carry;
    carry = (z2 == 0 && carry == 1) ? 1 : 0;
    long z3 = ~u3 + carry;
    return new UInt256(z3, z2, z1, z0);
  }

  /**
   * Compute the absolute value for a two-complement negative representation of this integer.
   *
   * @return The absolute value of this integer.
   */
  public UInt256 abs() {
    return isNegative() ? neg() : this;
  }

  /**
   * Addition
   *
   * <p>Compute the wrapping sum of 2 256-bits integers.
   *
   * @param other Integer to add to this integer.
   * @return The sum.
   */
  public UInt256 add(final UInt256 other) {
    return adc(other).UInt256Value();
  }

  /**
   * Subtraction
   *
   * <p>Compute the wrapping difference of 2 256-bits integers.
   *
   * @param other Integer to subtract from this integer.
   * @return The difference.
   */
  public UInt256 sub(final UInt256 other) {
    // Limb 0: no incoming borrow, so a plain unsigned compare of the inputs suffices.
    long z0 = u0 - other.u0;
    long borrow = Long.compareUnsigned(u0, other.u0) < 0 ? 1 : 0;

    // Limbs 1-3 subtract three values (u_i - other.u_i - borrow), so the borrow check
    // needs a tie-breaker: if u_i equals other.u_i, their difference is 0 and whether
    // we underflow depends entirely on the incoming borrow.
    long z1 = u1 - other.u1 - borrow;
    long underflowed = Long.compareUnsigned(u1, other.u1) < 0 ? 1 : 0;
    long underflowIfBorrowed = Long.compareUnsigned(u1, other.u1) == 0 ? 1 : 0;
    borrow = underflowed | (underflowIfBorrowed & borrow);

    long z2 = u2 - other.u2 - borrow;
    underflowed = Long.compareUnsigned(u2, other.u2) < 0 ? 1 : 0;
    underflowIfBorrowed = Long.compareUnsigned(u2, other.u2) == 0 ? 1 : 0;
    borrow = underflowed | (underflowIfBorrowed & borrow);

    long z3 = u3 - other.u3 - borrow;
    // Final borrow discarded: sub returns the wrapping result mod 2^256.
    return new UInt256(z3, z2, z1, z0);
  }

  /**
   * Multiplication
   *
   * <p>Compute the wrapping product of 2 256-bits integers.
   *
   * @param other Integer to multiply with this integer.
   * @return The product.
   */
  public UInt256 mul(final UInt256 other) {
    if (isZero() || other.isZero()) return ZERO;
    if (other.isOne()) return this;
    if (this.isOne()) return other;
    if (u3 != 0) return mul256(other).UInt256Value();
    if (u2 != 0) return mul192(other).UInt256Value();
    if (u1 != 0) return mul128(other);
    return mul64(other);
  }

  /**
   * Unsigned modulo reduction.
   *
   * <p>Compute dividend (mod modulus) as unsigned big-endian integer.
   *
   * @param modulus The modulus of the reduction.
   * @return The remainder.
   */
  public UInt256 mod(final UInt256 modulus) {
    if (isZero()) return ZERO;
    if (modulus.u3 != 0) return modulus.modReduce(this);
    if (modulus.u2 != 0) return modulus.asUInt192().modReduce(this);
    if (modulus.u1 != 0) return modulus.asUInt128().modReduce(this);
    if ((modulus.u0 == 0) || (modulus.u0 == 1)) return ZERO;
    return modulus.asUInt64().modReduce(this);
  }

  /**
   * Signed modulo reduction.
   *
   * <p>In signed modulo reduction, integers are interpretated as fixed 256 bits width two's
   * complement signed integers.
   *
   * @param modulus The modulus of the reduction
   * @return The remainder modulo {@code modulus}.
   */
  public UInt256 signedMod(final UInt256 modulus) {
    if (isZero() || modulus.isZeroOrOne() || modulus.equals(MAX)) return ZERO;
    UInt256 a = abs();
    UInt256 m = modulus.abs();
    UInt256 r = a.mod(m);
    if (isNegative()) r = r.neg();
    return r;
  }

  /**
   * Unsigned division.
   *
   * <p>Compute this / divisor as unsigned big-endian integer.
   *
   * @param divisor The divisor.
   * @return The quotient.
   */
  public UInt256 div(final UInt256 divisor) {
    if (isZero()) return ZERO;
    if (divisor.u3 != 0) return divisor.divReduce(this);
    if (divisor.u2 != 0) return divisor.asUInt192().divReduce(this);
    if (divisor.u1 != 0) return divisor.asUInt128().divReduce(this);
    if ((divisor.u0 == 0) || (divisor.u0 == 1)) return (divisor.u0 == 1) ? this : ZERO;
    return divisor.asUInt64().divReduce(this);
  }

  /**
   * Signed division.
   *
   * <p>In signed division, integers are interpreted as fixed 256 bits width two's complement signed
   * integers.
   *
   * @param divisor The divisor.
   * @return The quotient.
   */
  public UInt256 signedDiv(final UInt256 divisor) {
    if (isZero() || divisor.isZero()) return ZERO;
    UInt256 a = abs();
    UInt256 d = divisor.abs();
    UInt256 q = a.div(d);
    if (isNegative() != divisor.isNegative()) q = q.neg();
    return q;
  }

  /**
   * Modular addition.
   *
   * @param other The integer to add to this.
   * @param modulus The modulus of the reduction.
   * @return This integer this + other (mod modulus).
   */
  public UInt256 addMod(final UInt256 other, final UInt256 modulus) {
    if (isZero()) return other.mod(modulus);
    if (other.isZero()) return this.mod(modulus);
    if (modulus.isZeroOrOne()) return ZERO;
    if (modulus.u3 != 0) return modulus.sum(this, other);
    if (modulus.u2 != 0) return modulus.asUInt192().sum(this, other);
    if (modulus.u1 != 0) return modulus.asUInt128().sum(this, other);
    return modulus.asUInt64().sum(this, other);
  }

  /**
   * Modular multiplication.
   *
   * @param other The integer to add to this.
   * @param modulus The modulus of the reduction.
   * @return This integer this + other (mod modulus).
   */
  public UInt256 mulMod(final UInt256 other, final UInt256 modulus) {
    if (this.isZero() || other.isZero() || modulus.isZeroOrOne()) return ZERO;
    if (this.isOne()) return other.mod(modulus);
    if (other.isOne()) return this.mod(modulus);
    if (modulus.u3 != 0) return modulus.mul(this, other);
    if (modulus.u2 != 0) return modulus.asUInt192().mul(this, other);
    if (modulus.u1 != 0) return modulus.asUInt128().mul(this, other);
    return modulus.asUInt64().mul(this, other);
  }

  // --------------------------------------------------------------------------
  // endregion

  // region Bytes Arithmetic Operations
  //
  // Addition is faster when done straight in byte[]
  // --------------------------------------------------------------------------

  /**
   * Addition in bytes: x + y.
   *
   * <p>Compute the wrapping sum
   *
   * @param x The left value to add.
   * @param y The right value to add.
   * @return The sum x + y.
   */
  public static byte[] add(final byte[] x, final byte[] y) {
    if (isZero(x)) return y;
    if (isZero(y)) return x;
    return adc(x, y);
  }

  /**
   * Substraction in bytes: x - y.
   *
   * <p>Compute the wrapping difference
   *
   * @param x The left value.
   * @param y The right value to substract.
   * @return The wrapping difference x - y.
   */
  public static byte[] sub(final byte[] x, final byte[] y) {
    if (isZero(y)) return x;
    if (isZero(x)) return neg(y);
    return sbb(x, y);
  }

  /**
   * Performs EVM SAR (arithmetic shift right) on the two top stack items.
   *
   * <p>Reads the shift amount (unsigned) and the value (signed) from the top two stack slots,
   * writes {@code value >> shift} back into the value slot and decrements the top. Shifts >= 256
   * produce 0 for positive values and -1 for negative values.
   *
   * @param shift 256 bit value storing the amount of bits to shift
   * @return the result
   */
  public UInt256 sar(final UInt256 shift) {
    int bitShift;
    if (shift.u3() != 0
        || shift.u2() != 0
        || shift.u1() != 0
        || Long.compareUnsigned(shift.u0(), 256) >= 0) {
      bitShift = 256;
    } else {
      bitShift = (int) shift.u0();
    }
    long fill = (u3 < 0 ? -1L : 0);
    return sar0(bitShift, fill);
  }

  /**
   * Performs EVM SHR (logical shift right) on the two top stack items.
   *
   * <p>Reads the shift amount (unsigned) and the value from the top two stack slots, writes {@code
   * value >>> shift} back into the value slot and decrements the top. Shifts >= 256 or a zero value
   * produce 0.
   *
   * @param shift 256 bit value storing the amount of bits to shift
   * @return the result
   */
  public UInt256 shr(final UInt256 shift) {
    int bitShift;
    if (shift.u3() != 0
        || shift.u2() != 0
        || shift.u1() != 0
        || Long.compareUnsigned(shift.u0(), 256) >= 0) {
      bitShift = 256;
    } else {
      bitShift = (int) shift.u0();
    }
    return sar0(bitShift, 0);
  }

  /**
   * Arithmetic right-shifts a 256-bit value in place by 0..255 bits, sign-extending with {@code
   * fill}.
   *
   * @param shift number of bits to shift
   * @param fill value to prepend while shifting
   * @return the result
   */
  // TODO: check perf - wiring shiftRight callers with this one
  private UInt256 sar0(final int shift, final long fill) {
    long w3 = u3, w2 = u2, w1 = u1, w0 = u0;
    if (shift == 256) {
      w3 = fill;
      w2 = fill;
      w1 = fill;
      w0 = fill;
    } else if (shift != 0) {
      // Number of whole 64-bit words to shift (shift / 64)
      final int wordShift = shift >>> 6;
      // Remaining intra-word bit shift (shift % 64)
      final int bitShift = shift & 63;
      switch (wordShift) {
        case 0:
          w0 = shiftRightWord(w0, w1, bitShift);
          w1 = shiftRightWord(w1, w2, bitShift);
          w2 = shiftRightWord(w2, w3, bitShift);
          w3 = shiftRightWord(w3, fill, bitShift);
          break;
        case 1:
          w0 = shiftRightWord(w1, w2, bitShift);
          w1 = shiftRightWord(w2, w3, bitShift);
          w2 = shiftRightWord(w3, fill, bitShift);
          w3 = fill;
          break;
        case 2:
          w0 = shiftRightWord(w2, w3, bitShift);
          w1 = shiftRightWord(w3, fill, bitShift);
          w2 = fill;
          w3 = fill;
          break;
        case 3:
          w0 = shiftRightWord(w3, fill, bitShift);
          w1 = fill;
          w2 = fill;
          w3 = fill;
          break;
      }
    }
    return new UInt256(w3, w2, w1, w0);
  }

  /**
   * Performs EVM SHL (shift left) on the two top stack items.
   *
   * <p>Reads the shift amount (unsigned) and the value from the top two stack slots, writes {@code
   * value << shift} back into the value slot and decrements the top. Shifts >= 256 or a zero value
   * produce 0.
   *
   * @param shift 256 bit value storing the amount of bits to shift
   * @return the result
   */
  public UInt256 shl(final UInt256 shift) {
    int bitShift;
    if (shift.u3() != 0
        || shift.u2() != 0
        || shift.u1() != 0
        || Long.compareUnsigned(shift.u0(), 256) >= 0) {
      bitShift = 256;
    } else {
      bitShift = (int) shift.u0();
    }
    return shl0(bitShift);
  }

  /**
   * Left-shifts a 256-bit value in place by 1..255 bits, zero-filling from the right.
   *
   * @param shift number of bits to shift
   * @return the result
   */
  // TODO: check perf - wiring shiftLeft callers with this one
  private UInt256 shl0(final int shift) {
    long w3 = u3, w2 = u2, w1 = u1, w0 = u0;
    if (shift == 256) {
      w3 = 0;
      w2 = 0;
      w1 = 0;
      w0 = 0;
    } else if (shift != 0) {
      // Number of whole 64-bit words to shift (shift / 64)
      final int wordShift = shift >>> 6;
      // Remaining intra-word bit shift (shift % 64)
      final int bitShift = shift & 63;
      switch (wordShift) {
        case 0:
          w3 = shiftLeftWord(w3, w2, bitShift);
          w2 = shiftLeftWord(w2, w1, bitShift);
          w1 = shiftLeftWord(w1, w0, bitShift);
          w0 = shiftLeftWord(w0, 0, bitShift);
          break;
        case 1:
          w3 = shiftLeftWord(w2, w1, bitShift);
          w2 = shiftLeftWord(w1, w0, bitShift);
          w1 = shiftLeftWord(w0, 0, bitShift);
          w0 = 0;
          break;
        case 2:
          w3 = shiftLeftWord(w1, w0, bitShift);
          w2 = shiftLeftWord(w0, 0, bitShift);
          w1 = 0;
          w0 = 0;
          break;
        case 3:
          w3 = shiftLeftWord(w0, 0, bitShift);
          w2 = 0;
          w1 = 0;
          w0 = 0;
          break;
      }
    }
    return new UInt256(w3, w2, w1, w0);
  }

  private static long shiftLeftWord(final long value, final long nextValue, final int bitShift) {
    if (bitShift == 0) return value;
    return (value << bitShift) | (nextValue >>> (64 - bitShift));
  }

  private static long shiftRightWord(final long value, final long prevValue, final int bitShift) {
    if (bitShift == 0) return value;
    return (value >>> bitShift) | (prevValue << (64 - bitShift));
  }

  private static boolean isZero(final byte[] arr) {
    int index = Arrays.mismatch(arr, ZERO_BYTES);
    return (index == -1 || index >= arr.length);
  }

  private static byte[] padLeft(final byte[] a) {
    if (a.length == BYTESIZE) return a;
    byte[] res = new byte[BYTESIZE];
    System.arraycopy(a, 0, res, BYTESIZE - a.length, a.length);
    return res;
  }

  private static byte[] adc(final byte[] a, final byte[] b) {
    int res;
    int carry = 0;
    byte[] x = padLeft(a);
    byte[] y = padLeft(b);
    byte[] sum = new byte[BYTESIZE];
    for (int i = 31; i >= 0; i--) {
      res = (x[i] & 0xFF) + (y[i] & 0xFF) + carry;
      sum[i] = (byte) res;
      carry = (res >> 8);
    }
    return sum;
  }

  private static byte[] neg(final byte[] a) {
    int res;
    int carry = 1;
    byte[] x = padLeft(a);
    byte[] out = new byte[BYTESIZE];
    for (int i = 31; i >= 0; i--) {
      res = (~x[i] & 0xFF) + carry;
      out[i] = (byte) res;
      carry = (res >> 8);
    }
    return out;
  }

  private static byte[] sbb(final byte[] a, final byte[] b) {
    int res;
    int borrow = 0;
    byte[] x = padLeft(a);
    byte[] y = padLeft(b);
    byte[] diff = new byte[BYTESIZE];
    for (int i = 31; i >= 0; i--) {
      res = (x[i] & 0xFF) - (y[i] & 0xFF) - borrow;
      diff[i] = (byte) res;
      borrow = (res < 0) ? 1 : 0;
    }
    return diff;
  }

  // --------------------------------------------------------------------------
  // endregion

  // region private basic operations
  //
  // adc (add and carry): carry, a <- a + b
  // mac (multiply accumulate): a <- a + b * c + carryIn
  // --------------------------------------------------------------------------

  private UInt320 shiftLeftWide(final int shift) {
    if (shift == 0) return UInt320Value();
    int invShift = (N_BITS_PER_LIMB - shift);
    long z0 = (u0 << shift);
    long z1 = (u1 << shift) | u0 >>> invShift;
    long z2 = (u2 << shift) | u1 >>> invShift;
    long z3 = (u3 << shift) | u2 >>> invShift;
    long z4 = u3 >>> invShift;
    return new UInt320(z4, z3, z2, z1, z0);
  }

  private UInt256 shiftDigitsRight() {
    return new UInt256(0, u3, u2, u1);
  }

  // Add with carry
  private UInt257 adc(final UInt256 other) {
    // Limb 0: no incoming carry, so a plain unsigned-wrap check suffices.
    long z0 = u0 + other.u0;
    long carry = Long.compareUnsigned(z0, u0) < 0 ? 1 : 0;

    // Limbs 1-3 add three values (u_i + other.u_i + carry), so the wrap check
    // needs a tie-breaker: if the sum equals u_i, other.u_i was 0xFF..FF
    // and the incoming carry alone pushed us over, which is overflow iff carry=1.
    long z1 = u1 + other.u1 + carry;
    long overflowed = Long.compareUnsigned(z1, u1) < 0 ? 1 : 0;
    long overflowIfCarried = Long.compareUnsigned(z1, u1) == 0 ? 1 : 0;
    carry = overflowed | (overflowIfCarried & carry);

    long z2 = u2 + other.u2 + carry;
    overflowed = Long.compareUnsigned(z2, u2) < 0 ? 1 : 0;
    overflowIfCarried = Long.compareUnsigned(z2, u2) == 0 ? 1 : 0;
    carry = overflowed | (overflowIfCarried & carry);

    long z3 = u3 + other.u3 + carry;
    overflowed = Long.compareUnsigned(z3, u3) < 0 ? 1 : 0;
    overflowIfCarried = Long.compareUnsigned(z3, u3) == 0 ? 1 : 0;
    carry = overflowed | (overflowIfCarried & carry);

    return new UInt257(carry != 0, new UInt256(z3, z2, z1, z0));
  }

  private UInt256 mac128(final long multiplier, final UInt256 carryIn) {
    // Multiply accumulate for 128bits integer (this):
    // <p1, p0> = <u1, u0> * multiplier + carryIn
    if (multiplier == 0) return carryIn.shiftDigitsRight();

    long p0 = u0 * multiplier;
    long p1 = Math.unsignedMultiplyHigh(u0, multiplier);
    long z0 = p0 + carryIn.u1;
    long carry = p1 + ((Long.compareUnsigned(z0, p0) < 0) ? 1 : 0);

    p0 = u1 * multiplier;
    p1 = Math.unsignedMultiplyHigh(u1, multiplier);
    long res = carry + carryIn.u2;
    long z1 = res + p0;
    carry = (Long.compareUnsigned(res, carry) < 0) ? 1 : 0;
    carry += p1 + ((Long.compareUnsigned(z1, res) < 0) ? 1 : 0);

    long z2 = carry;

    return new UInt256(0, z2, z1, z0);
  }

  private UInt256 mac192(final long multiplier, final UInt256 carryIn) {
    // Multiply accumulate for 192bits integer (this):
    // Returns this * multiplier + (carryIn >>> 64)
    // <p1, p0> = <u1, u0> * multiplier + carryIn
    if (multiplier == 0) return carryIn.shiftDigitsRight();

    long p0 = u0 * multiplier;
    long p1 = Math.unsignedMultiplyHigh(u0, multiplier);
    long z0 = p0 + carryIn.u1;
    long carry = p1 + ((Long.compareUnsigned(z0, p0) < 0) ? 1 : 0);

    p0 = u1 * multiplier;
    p1 = Math.unsignedMultiplyHigh(u1, multiplier);
    long res = carry + carryIn.u2;
    long z1 = res + p0;
    carry = (Long.compareUnsigned(res, carry) < 0) ? 1 : 0;
    carry += p1 + ((Long.compareUnsigned(z1, res) < 0) ? 1 : 0);

    p0 = u2 * multiplier;
    p1 = Math.unsignedMultiplyHigh(u2, multiplier);
    res = carry + carryIn.u3;
    long z2 = res + p0;
    carry = (Long.compareUnsigned(res, carry) < 0) ? 1 : 0;
    carry += p1 + ((Long.compareUnsigned(z2, res) < 0) ? 1 : 0);

    long z3 = carry;
    return new UInt256(z3, z2, z1, z0);
  }

  private UInt320 mac256(final long multiplier, final UInt320 carryIn) {
    // Multiply accumulate for 192bits integer (this):
    // Returns this * multiplier + carryIn
    // <p1, p0> = <u1, u0> * multiplier + carryIn
    if (multiplier == 0) return carryIn.shiftDigitsRight();

    long p0 = u0 * multiplier;
    long p1 = Math.unsignedMultiplyHigh(u0, multiplier);
    long z0 = p0 + carryIn.u1;
    long carry = p1 + ((Long.compareUnsigned(z0, p0) < 0) ? 1 : 0);

    p0 = u1 * multiplier;
    p1 = Math.unsignedMultiplyHigh(u1, multiplier);
    long res = carry + carryIn.u2;
    long z1 = res + p0;
    carry = (Long.compareUnsigned(res, carry) < 0) ? 1 : 0;
    carry += p1 + ((Long.compareUnsigned(z1, res) < 0) ? 1 : 0);

    p0 = u2 * multiplier;
    p1 = Math.unsignedMultiplyHigh(u2, multiplier);
    res = carry + carryIn.u3;
    long z2 = res + p0;
    carry = (Long.compareUnsigned(res, carry) < 0) ? 1 : 0;
    carry += p1 + ((Long.compareUnsigned(z2, res) < 0) ? 1 : 0);

    p0 = u3 * multiplier;
    p1 = Math.unsignedMultiplyHigh(u3, multiplier);
    res = carry + carryIn.u4;
    long z3 = res + p0;
    carry = (Long.compareUnsigned(res, carry) < 0) ? 1 : 0;
    carry += p1 + ((Long.compareUnsigned(z3, res) < 0) ? 1 : 0);

    long z4 = carry;

    return new UInt320(z4, z3, z2, z1, z0);
  }

  // --------------------------------------------------------------------------
  // endregion

  // region private multiplication
  // --------------------------------------------------------------------------

  private UInt256 mul64(final UInt256 v) {
    // <p1, p0> = <u1, u0> * multiplier
    long p0 = u0 * v.u0;
    long p1 = Math.unsignedMultiplyHigh(u0, v.u0);
    long z0 = p0;
    long carry = p1;

    p0 = u0 * v.u1;
    p1 = Math.unsignedMultiplyHigh(u0, v.u1);
    long z1 = p0 + carry;
    carry = p1 + ((Long.compareUnsigned(z1, p0) < 0) ? 1 : 0);

    p0 = u0 * v.u2;
    p1 = Math.unsignedMultiplyHigh(u0, v.u2);
    long z2 = p0 + carry;
    carry = p1 + ((Long.compareUnsigned(z2, p0) < 0) ? 1 : 0);

    long z3 = u0 * v.u3 + carry;

    return new UInt256(z3, z2, z1, z0);
  }

  private UInt256 mul128(final UInt256 v) {
    UInt256 res;

    res = mac128(v.u0, ZERO);
    long z0 = res.u0;
    res = mac128(v.u1, res);
    long z1 = res.u0;
    res = mac128(v.u2, res);
    long z2 = res.u0;
    res = mac128(v.u3, res);

    return new UInt256(res.u0, z2, z1, z0);
  }

  private UInt448 mul192(final UInt256 v) {
    UInt256 res;
    res = mac192(v.u0, ZERO);
    long z0 = res.u0;
    res = mac192(v.u1, res);
    long z1 = res.u0;
    res = mac192(v.u2, res);
    long z2 = res.u0;
    res = mac192(v.u3, res);

    return new UInt448(res.u3, res.u2, res.u1, res.u0, z2, z1, z0);
  }

  private UInt512 mul256(final UInt256 v) {
    UInt320 res;
    res = mac256(v.u0, UInt320.ZERO);
    long z0 = res.u0;
    res = mac256(v.u1, res);
    long z1 = res.u0;
    res = mac256(v.u2, res);
    long z2 = res.u0;
    res = mac256(v.u3, res);
    return new UInt512(res.u4, res.u3, res.u2, res.u1, res.u0, z2, z1, z0);
  }

  // --------------------------------------------------------------------------
  // endregion

  // region private division
  // --------------------------------------------------------------------------
  private UInt256 modReduce(final UInt256 that) {
    int cmp = compareTo(that);
    if (cmp == 0) return ZERO;
    if (cmp > 0) return that;
    int shift = Long.numberOfLeadingZeros(u3);
    UInt256 m = shiftLeft(shift);
    long inv = reciprocal(m.u3);
    return m.modReduceNormalised(that, shift, inv);
  }

  private UInt256 modReduce(final UInt512 that) {
    int cmp = compareTo(that);
    if (cmp == 0) return ZERO;
    if (cmp > 0) return that.UInt256Value();
    int shift = Long.numberOfLeadingZeros(u3);
    UInt256 m = shiftLeft(shift);
    long inv = reciprocal(m.u3);
    return m.modReduceNormalised(that, shift, inv);
  }

  private UInt256 sum(final UInt256 a, final UInt256 b) {
    UInt257 sum = a.adc(b);
    if (!sum.carry()) {
      int cmp = compareTo(sum.UInt256Value());
      if (cmp == 0) return ZERO;
      if (cmp > 0) return sum.UInt256Value();
    }
    int shift = Long.numberOfLeadingZeros(u3);
    UInt256 m = shiftLeft(shift);
    long inv = reciprocal(m.u3);
    return m.modReduceNormalised(sum, shift, inv);
  }

  private UInt256 mul(final UInt256 a, final UInt256 b) {
    // multiply-reduce
    UInt512 prod = a.mul256(b);
    int cmp = compareTo(prod);
    if (cmp == 0) return ZERO;
    if (cmp > 0) return prod.UInt256Value();
    return modReduce(prod);
  }

  private QR256 addBack(final long v3, final long v2, final long v1, final long v0, final long q) {
    // Add back
    long z0 = v0 + u0;
    long carry = ((v0 & u0) | ((v0 | u0) & ~z0)) >>> 63;

    long z1 = v1 + u1 + carry;
    long overflow1 = (Long.compareUnsigned(z1, v1) < 0) ? 1 : 0;
    long overflow2 = (Long.compareUnsigned(z1, v1) == 0) ? 1 : 0;
    carry = overflow1 | (overflow2 & carry);

    long z2 = v2 + u2 + carry;
    overflow1 = (Long.compareUnsigned(z2, v2) < 0) ? 1 : 0;
    overflow2 = (Long.compareUnsigned(z2, v2) == 0) ? 1 : 0;
    carry = overflow1 | (overflow2 & carry);

    long z3 = v3 + u3 + carry;
    overflow1 = (Long.compareUnsigned(z3, v3) < 0) ? 1 : 0;
    overflow2 = (Long.compareUnsigned(z3, v3) == 0) ? 1 : 0;
    carry = overflow1 | (overflow2 & carry);

    if (carry == 0) { // unlikely: add back again
      // Proper quotient estimation guarantees recursion max-depth <= 2
      // Unbounded recursion only if there's a bug - fail fast is better than give wrong result
      return addBack(z3, z2, z1, z0, q - 1);
    }
    return new QR256(q, new UInt256(z3, z2, z1, z0));
  }

  private QR256 mulSub(final long v3, final long v2, final long v1, final long v0, final long q) {
    // Multiply-subtract: already have highest 1 limbs
    // <z4, z3, z2, z1, z0>  =  <u3, u2, u1, u0> * q
    long p0 = u0 * q;
    long p1 = Math.unsignedMultiplyHigh(u0, q);
    long z0 = v0 - p0;
    long carry = p1 + (((Long.compareUnsigned(v0, z0) < 0) ? 1 : 0));

    p0 = u1 * q;
    p1 = Math.unsignedMultiplyHigh(u1, q);
    long res = v1 - p0;
    long z1 = res - carry;
    long borrow = (Long.compareUnsigned(res, z1) < 0) ? 1 : 0;
    carry = p1 + ((Long.compareUnsigned(v1, res) < 0) ? 1 : 0);

    p0 = u2 * q;
    p1 = Math.unsignedMultiplyHigh(u2, q);
    long t2 = v2 - p0;
    res = t2 - borrow;
    long z2 = res - carry;
    borrow = (Long.compareUnsigned(res, z2) < 0) ? 1 : 0;
    carry =
        p1
            + ((Long.compareUnsigned(v2, t2) < 0) ? 1 : 0)
            + ((Long.compareUnsigned(t2, res) < 0) ? 1 : 0);

    // Propagate overflows (borrows)
    long t3 = v3 - carry;
    long z3 = t3 - borrow;
    borrow =
        ((Long.compareUnsigned(v3, t3) < 0) ? 1 : 0) | ((Long.compareUnsigned(t3, z3) < 0) ? 1 : 0);

    if (borrow != 0) return addBack(z3, z2, z1, z0, q - 1);
    return new QR256(q, new UInt256(z3, z2, z1, z0));
  }

  private QR256 mulSubOverflow(final long v3, final long v2, final long v1, final long v0) {
    // Overflow case: div2by1 quotient would be <1, 0>, but adjusts to <0, -1>
    // <p1, p0> = -1 * u0 = <u0 - 1, -u0>
    long res, borrow;

    long z0 = v0 + u0;
    long carry = u0 - 1 + ((Long.compareUnsigned(v0, z0) <= 0) ? 1 : 0);

    res = v1 - carry;
    long z1 = res + u1;
    borrow = (Long.compareUnsigned(res, z1) <= 0) ? 1 : 0;
    carry = u1 - 1 + ((Long.compareUnsigned(v1, res) < 0) ? 1 : 0);

    res = v2 - carry - borrow;
    long z2 = res + u2;
    borrow = (Long.compareUnsigned(res, z2) <= 0) ? 1 : 0;
    carry = u2 - 1 + ((Long.compareUnsigned(v2, res) < 0) ? 1 : 0);

    long z3 = v3 + u3 - carry - borrow;
    // q = MAX may still be 1 too high - need to pass q - 1 (-2L) to addBack; check if result >=
    // modulus (i.e. negative wrapped)
    if (Long.compareUnsigned(z3, u3) > 0
        || (z3 == u3
            && (Long.compareUnsigned(z2, u2) > 0
                || (z2 == u2
                    && (Long.compareUnsigned(z1, u1) > 0
                        || (z1 == u1 && Long.compareUnsigned(z0, u0) >= 0)))))) {
      return addBack(z3, z2, z1, z0, -2L);
    }
    return new QR256(-1L, new UInt256(z3, z2, z1, z0));
  }

  private QR256 reduceStep(
      final long v4, final long v3, final long v2, final long v1, final long v0, final long inv) {
    if (v4 == u3) return mulSubOverflow(v3, v2, v1, v0);
    QR64 qr = div2by1(v4, v3, u3, inv);
    if (qr.q != 0) return mulSub(qr.r, v2, v1, v0, qr.q);
    return new QR256(0, new UInt256(v3, v2, v1, v0));
  }

  private UInt256 modReduceNormalised(final UInt256 that, final int shift, final long inv) {
    UInt320 v = that.shiftLeftWide(shift);
    QR256 qr;
    if (Long.compareUnsigned(v.u4, u3) >= 0) {
      qr = reduceStep(0, v.u4, v.u3, v.u2, v.u1, inv);
      qr = reduceStep(qr.r.u3, qr.r.u2, qr.r.u1, qr.r.u0, v.u0, inv);
    } else {
      qr = reduceStep(v.u4, v.u3, v.u2, v.u1, v.u0, inv);
    }
    return qr.r.shiftRight(shift);
  }

  private UInt256 modReduceNormalised(final UInt257 that, final int shift, final long inv) {
    UInt320 v = that.shiftLeftWide(shift);
    QR256 qr;
    if (Long.compareUnsigned(v.u4, u3) >= 0) {
      qr = reduceStep(0, v.u4, v.u3, v.u2, v.u1, inv);
      qr = reduceStep(qr.r.u3, qr.r.u2, qr.r.u1, qr.r.u0, v.u0, inv);
    } else {
      qr = reduceStep(v.u4, v.u3, v.u2, v.u1, v.u0, inv);
    }
    return qr.r.shiftRight(shift);
  }

  private UInt256 modReduceNormalised(final UInt512 that, final int shift, final long inv) {
    UInt576 v = that.shiftLeftWide(shift);
    if ((v.u8 | v.u7 | v.u6) == 0 && Long.compareUnsigned(v.u5, u3) < 0) {
      QR256 qr;
      if (v.u5 != 0 || Long.compareUnsigned(v.u4, u3) >= 0) {
        qr = reduceStep(v.u5, v.u4, v.u3, v.u2, v.u1, inv);
        qr = reduceStep(qr.r.u3, qr.r.u2, qr.r.u1, qr.r.u0, v.u0, inv);
      } else {
        qr = reduceStep(v.u4, v.u3, v.u2, v.u1, v.u0, inv);
      }
      return qr.r.shiftRight(shift);
    }
    return modReduceNormalisedSlowPath(v, shift, inv);
  }

  private UInt256 modReduceNormalisedSlowPath(final UInt576 v, final int shift, final long inv) {
    QR256 qr;
    if (v.u8 != 0 || Long.compareUnsigned(v.u7, u3) >= 0) {
      qr = reduceStep(v.u8, v.u7, v.u6, v.u5, v.u4, inv);
      qr = reduceStep(qr.r.u3, qr.r.u2, qr.r.u1, qr.r.u0, v.u3, inv);
      qr = reduceStep(qr.r.u3, qr.r.u2, qr.r.u1, qr.r.u0, v.u2, inv);
      qr = reduceStep(qr.r.u3, qr.r.u2, qr.r.u1, qr.r.u0, v.u1, inv);
      qr = reduceStep(qr.r.u3, qr.r.u2, qr.r.u1, qr.r.u0, v.u0, inv);
    } else if (v.u7 != 0 || Long.compareUnsigned(v.u6, u3) >= 0) {
      qr = reduceStep(v.u7, v.u6, v.u5, v.u4, v.u3, inv);
      qr = reduceStep(qr.r.u3, qr.r.u2, qr.r.u1, qr.r.u0, v.u2, inv);
      qr = reduceStep(qr.r.u3, qr.r.u2, qr.r.u1, qr.r.u0, v.u1, inv);
      qr = reduceStep(qr.r.u3, qr.r.u2, qr.r.u1, qr.r.u0, v.u0, inv);
    } else {
      qr = reduceStep(v.u6, v.u5, v.u4, v.u3, v.u2, inv);
      qr = reduceStep(qr.r.u3, qr.r.u2, qr.r.u1, qr.r.u0, v.u1, inv);
      qr = reduceStep(qr.r.u3, qr.r.u2, qr.r.u1, qr.r.u0, v.u0, inv);
    }
    return qr.r.shiftRight(shift);
  }

  private UInt256 divReduce(final UInt256 that) {
    int cmp = compareTo(that);
    if (cmp == 0) return ONE;
    if (cmp > 0) return ZERO;
    int shift = Long.numberOfLeadingZeros(u3);
    UInt256 m = shiftLeft(shift);
    long inv = reciprocal(m.u3);
    UInt320 v = that.shiftLeftWide(shift);
    return fromLong(m.reduceStep(v.u4, v.u3, v.u2, v.u1, v.u0, inv).q);
  }

  // Lookup table for $\floor{\frac{2^{19} -3 ⋅ 2^8}{d_9 - 256}}$
  private static final short[] LUT =
      new short[] {
        2045, 2037, 2029, 2021, 2013, 2005, 1998, 1990, 1983, 1975, 1968, 1960, 1953, 1946, 1938,
        1931, 1924, 1917, 1910, 1903, 1896, 1889, 1883, 1876, 1869, 1863, 1856, 1849, 1843, 1836,
        1830, 1824, 1817, 1811, 1805, 1799, 1792, 1786, 1780, 1774, 1768, 1762, 1756, 1750, 1745,
        1739, 1733, 1727, 1722, 1716, 1710, 1705, 1699, 1694, 1688, 1683, 1677, 1672, 1667, 1661,
        1656, 1651, 1646, 1641, 1636, 1630, 1625, 1620, 1615, 1610, 1605, 1600, 1596, 1591, 1586,
        1581, 1576, 1572, 1567, 1562, 1558, 1553, 1548, 1544, 1539, 1535, 1530, 1526, 1521, 1517,
        1513, 1508, 1504, 1500, 1495, 1491, 1487, 1483, 1478, 1474, 1470, 1466, 1462, 1458, 1454,
        1450, 1446, 1442, 1438, 1434, 1430, 1426, 1422, 1418, 1414, 1411, 1407, 1403, 1399, 1396,
        1392, 1388, 1384, 1381, 1377, 1374, 1370, 1366, 1363, 1359, 1356, 1352, 1349, 1345, 1342,
        1338, 1335, 1332, 1328, 1325, 1322, 1318, 1315, 1312, 1308, 1305, 1302, 1299, 1295, 1292,
        1289, 1286, 1283, 1280, 1276, 1273, 1270, 1267, 1264, 1261, 1258, 1255, 1252, 1249, 1246,
        1243, 1240, 1237, 1234, 1231, 1228, 1226, 1223, 1220, 1217, 1214, 1211, 1209, 1206, 1203,
        1200, 1197, 1195, 1192, 1189, 1187, 1184, 1181, 1179, 1176, 1173, 1171, 1168, 1165, 1163,
        1160, 1158, 1155, 1153, 1150, 1148, 1145, 1143, 1140, 1138, 1135, 1133, 1130, 1128, 1125,
        1123, 1121, 1118, 1116, 1113, 1111, 1109, 1106, 1104, 1102, 1099, 1097, 1095, 1092, 1090,
        1088, 1086, 1083, 1081, 1079, 1077, 1074, 1072, 1070, 1068, 1066, 1064, 1061, 1059, 1057,
        1055, 1053, 1051, 1049, 1047, 1044, 1042, 1040, 1038, 1036, 1034, 1032, 1030, 1028, 1026,
        1024,
      };

  private static final long TWO_POW_SIXTY = 1L << 60;

  // Taken from https://gmplib.org/~tege/division-paper.pdf taken from III. algorithm 2
  private static long reciprocal(final long x) {
    // Unchecked: x >= (1 << 63)
    long x0 = x & 1L;
    int x9 = (int) (x >>> 55);
    long x40 = 1 + (x >>> 24);
    long x63 = (x + 1) >>> 1;
    long v0 = LUT[x9 - 256] & 0xFFFFL;
    long v1 = (v0 << 11) - ((v0 * v0 * x40) >>> 40) - 1;
    long v2 = (v1 << 13) + ((v1 * (TWO_POW_SIXTY - v1 * x40)) >>> 47);
    long e = ((v2 >>> 1) & (-x0)) - v2 * x63;
    long s = Math.unsignedMultiplyHigh(v2, e);
    long v3 = (s >>> 1) + (v2 << 31);
    long t0 = v3 * x;
    long t1 = Math.unsignedMultiplyHigh(v3, x);
    t0 += x;
    t1 += Long.compareUnsigned(t0, x) < 0 ? 1 : 0;
    t1 += x;
    return v3 - t1;
  }

  private static QR64 div2by1(final long x1, final long x0, final long y, final long yInv) {
    // wrapping umul z1 * yInv
    long q0 = x1 * yInv;
    long q1 = Math.unsignedMultiplyHigh(x1, yInv);

    // wrapping uadd <q1, q0> + <z1, z0> + <1, 0>
    long sum = q0 + x0;
    long carry = ((q0 & x0) | ((q0 | x0) & ~sum)) >>> 63;
    q0 = sum;
    q1 += x1 + carry + 1;

    long r = x0 - q1 * y;

    long adjust = Long.compareUnsigned(q0, r) < 0 ? 1 : 0;
    q1 -= adjust;
    r += adjust * y;

    adjust = Long.compareUnsigned(y, r) <= 0 ? 1 : 0;
    q1 += adjust;
    r -= y * adjust;

    return new QR64(q1, r);
  }

  // --------------------------------------------------------------------------
  // endregion

  // region Quotient / Remainder types
  // These types are used to store the result of division. Due to the nature of the algorithm
  // (div2by1) quotient (q) can't
  // ever exceed 64 bits while remainder (r) can range from 64 to 256 bits.
  private record QR64(long q, long r) {}

  private record QR128(long q, UInt128 r) {}

  private record QR192(long q, UInt192 r) {}

  private record QR256(long q, UInt256 r) {}

  // --------------------------------------------------------------------------
  // endregion

  // region UInt* types
  // --------------------------------------------------------------------------
  record UInt64(long u0) {
    UInt64 shiftLeft(final int shift) {
      return (shift == 0) ? this : new UInt64(u0 << shift);
    }

    UInt256 modReduce(final UInt256 that) {
      if (that.isUInt64()) {
        return UInt256.fromLong(Long.remainderUnsigned(that.u0, u0));
      }
      int shift = Long.numberOfLeadingZeros(u0);
      UInt64 m = shiftLeft(shift);
      long inv = reciprocal(m.u0);
      return m.modReduceNormalised(that, shift, inv);
    }

    UInt256 sum(final UInt256 a, final UInt256 b) {
      UInt257 sum = a.adc(b);
      if (sum.isUInt64()) return UInt256.fromLong(Long.remainderUnsigned(sum.u().u0, u0));
      int shift = Long.numberOfLeadingZeros(u0);
      UInt64 m = shiftLeft(shift);
      long inv = reciprocal(m.u0);
      return m.modReduceNormalised(sum, shift, inv);
    }

    UInt256 mul(final UInt256 a, final UInt256 b) {
      // multiply-reduce
      if (a.isUInt64() && b.isUInt64()) {
        UInt256 prod = a.mul64(b);
        if (prod.isUInt64()) return UInt256.fromLong(Long.remainderUnsigned(prod.u0, u0));
        return modReduce(prod);
      }
      // reduce-multiply-reduce
      int shift = Long.numberOfLeadingZeros(u0);
      UInt64 m = shiftLeft(shift);
      long inv = reciprocal(m.u0);
      UInt256 x = (a.isUInt64()) ? a : m.modReduceNormalised(a, shift, inv);
      UInt256 y = (b.isUInt64()) ? b : m.modReduceNormalised(b, shift, inv);
      UInt256 prod = x.mul64(y);
      return prod.isUInt64()
          ? UInt256.fromLong(Long.remainderUnsigned(prod.u0, u0))
          : m.modReduceNormalised(prod, shift, inv);
    }

    private QR64 reduceStep(final long v1, final long v0, final long inv) {
      return div2by1(v1, v0, u0, inv);
    }

    private UInt256 modReduceNormalised(final UInt256 that, final int shift, final long inv) {
      UInt320 v = that.shiftLeftWide(shift);
      if ((v.u4 | v.u3) == 0 && Long.compareUnsigned(v.u2, u0) < 0) {
        QR64 qr;
        if (v.u2 != 0 || Long.compareUnsigned(v.u1, u0) >= 0) {
          qr = reduceStep(v.u2, v.u1, inv);
          qr = reduceStep(qr.r, v.u0, inv);
        } else {
          qr = reduceStep(v.u1, v.u0, inv);
        }
        return UInt256.fromLong(qr.r >>> shift);
      }
      return reduceNormalisedSlowPath(v, shift, inv);
    }

    private UInt256 modReduceNormalised(final UInt257 that, final int shift, final long inv) {
      UInt320 v = that.shiftLeftWide(shift);
      if ((v.u4 | v.u3) == 0 && Long.compareUnsigned(v.u2, u0) < 0) {
        QR64 qr;
        if (v.u2 != 0 || Long.compareUnsigned(v.u1, u0) >= 0) {
          qr = reduceStep(v.u2, v.u1, inv);
          qr = reduceStep(qr.r, v.u0, inv);
        } else {
          qr = reduceStep(v.u1, v.u0, inv);
        }
        return UInt256.fromLong(qr.r >>> shift);
      }
      return reduceNormalisedSlowPath(v, shift, inv);
    }

    private UInt256 reduceNormalisedSlowPath(final UInt320 v, final int shift, final long inv) {
      QR64 qr;
      if (Long.compareUnsigned(v.u4, u0) >= 0) {
        qr = reduceStep(0, v.u4, inv);
        qr = reduceStep(qr.r, v.u3, inv);
        qr = reduceStep(qr.r, v.u2, inv);
        qr = reduceStep(qr.r, v.u1, inv);
        qr = reduceStep(qr.r, v.u0, inv);
      } else if (v.u4 != 0 || Long.compareUnsigned(v.u3, u0) >= 0) {
        qr = reduceStep(v.u4, v.u3, inv);
        qr = reduceStep(qr.r, v.u2, inv);
        qr = reduceStep(qr.r, v.u1, inv);
        qr = reduceStep(qr.r, v.u0, inv);
      } else {
        qr = reduceStep(v.u3, v.u2, inv);
        qr = reduceStep(qr.r, v.u1, inv);
        qr = reduceStep(qr.r, v.u0, inv);
      }
      return UInt256.fromLong(qr.r >>> shift);
    }

    UInt256 divReduce(final UInt256 that) {
      if (that.isUInt64()) {
        return UInt256.fromLong(Long.divideUnsigned(that.u0, u0));
      }
      int shift = Long.numberOfLeadingZeros(u0);
      UInt64 m = shiftLeft(shift);
      long inv = reciprocal(m.u0);
      return m.divReduceNormalised(that, shift, inv);
    }

    private UInt256 divReduceNormalised(final UInt256 that, final int shift, final long inv) {
      UInt320 v = that.shiftLeftWide(shift);
      if ((v.u4 | v.u3) == 0 && Long.compareUnsigned(v.u2, u0) < 0) {
        if (v.u2 != 0 || Long.compareUnsigned(v.u1, u0) >= 0) {
          QR64 qr1 = reduceStep(v.u2, v.u1, inv);
          QR64 qr0 = reduceStep(qr1.r, v.u0, inv);
          return new UInt256(0, 0, qr1.q, qr0.q);
        } else {
          QR64 qr0 = reduceStep(v.u1, v.u0, inv);
          return new UInt256(0, 0, 0, qr0.q);
        }
      }
      return divReduceNormalisedSlowPath(v, inv);
    }

    private UInt256 divReduceNormalisedSlowPath(final UInt320 v, final long inv) {
      if (v.u4 == 0 && Long.compareUnsigned(v.u3, u0) < 0) {
        QR64 qr2 = reduceStep(v.u3, v.u2, inv);
        QR64 qr1 = reduceStep(qr2.r, v.u1, inv);
        QR64 qr0 = reduceStep(qr1.r, v.u0, inv);
        return new UInt256(0, qr2.q, qr1.q, qr0.q);
      }
      QR64 qr3 = reduceStep(v.u4, v.u3, inv);
      QR64 qr2 = reduceStep(qr3.r, v.u2, inv);
      QR64 qr1 = reduceStep(qr2.r, v.u1, inv);
      QR64 qr0 = reduceStep(qr1.r, v.u0, inv);
      return new UInt256(qr3.q, qr2.q, qr1.q, qr0.q);
    }
  }

  record UInt128(long u1, long u0) {
    UInt128 shiftLeft(final int shift) {
      if (shift == 0) return this;
      int invShift = N_BITS_PER_LIMB - shift;
      return new UInt128((u1 << shift) | (u0 >>> invShift), u0 << shift);
    }

    int compareTo(final UInt256 v) {
      if ((v.u3 | v.u2) != 0) return -1;
      if (v.u1 != u1) return Long.compareUnsigned(u1, v.u1);
      return Long.compareUnsigned(u0, v.u0);
    }

    UInt256 modReduce(final UInt256 that) {
      int cmp = compareTo(that);
      if (cmp == 0) return ZERO;
      if (cmp > 0) return that;
      int shift = Long.numberOfLeadingZeros(u1);
      UInt128 m = shiftLeft(shift);
      long inv = reciprocal(m.u1);
      return m.modReduceNormalised(that, shift, inv);
    }

    UInt256 sum(final UInt256 a, final UInt256 b) {
      UInt257 sum = a.adc(b);
      int cmp = sum.isUInt256() ? compareTo(sum.UInt256Value()) : -1;
      if (cmp == 0) return ZERO;
      if (cmp > 0) return sum.UInt256Value();
      int shift = Long.numberOfLeadingZeros(u1);
      UInt128 m = shiftLeft(shift);
      long inv = reciprocal(m.u1);
      return m.modReduceNormalised(sum, shift, inv);
    }

    UInt256 mul(final UInt256 a, final UInt256 b) {
      // smaller allocation path: multiply-reduce with UInt256 prod
      if (a.isUInt128() && b.isUInt128()) {
        UInt256 prod = a.mul128(b);
        int cmp = compareTo(prod);
        if (cmp == 0) return ZERO;
        if (cmp > 0) return prod;
        return modReduce(prod);
      }
      // At least one input exceeds 128 bits: full multiply then single reduce with UInt512 prod
      UInt512 prod = a.mul256(b);
      int shift = Long.numberOfLeadingZeros(u1);
      UInt128 m = shiftLeft(shift);
      long inv = reciprocal(m.u1);
      return m.modReduceNormalised(prod, shift, inv);
    }

    private QR128 addBack(final long v1, final long v0, final long q) {
      // Quotient estimate could be 0, +1, +2 of real quotient.
      // Add back step in case estimate is off.
      long z0 = v0 + u0;
      long carry = (Long.compareUnsigned(z0, v0) < 0) ? 1 : 0;

      long z1 = v1 + u1 + carry;
      long overflow1 = (Long.compareUnsigned(z1, v1) < 0) ? 1 : 0;
      long overflow2 = (Long.compareUnsigned(z1, v1) == 0) ? 1 : 0;
      carry = overflow1 | (overflow2 & carry);

      if (carry == 0) { // unlikely: add back again
        // Proper quotient estimation guarantees recursion max-depth <= 2
        // Unbounded recursion only if there's a bug - fail fast is better than give wrong result
        return addBack(z1, z0, q - 1);
      }
      return new QR128(q, new UInt128(z1, z0));
    }

    private QR128 mulSub(final long x1, final long x0, final long q) {
      // Multiply-subtract: highest limb is already substracted
      // <v2, v1, v0>  =  <u1, u0> * q
      long p0 = u0 * q;
      long p1 = Math.unsignedMultiplyHigh(u0, q);
      long z0 = x0 - p0;
      long carry = p1 + ((Long.compareUnsigned(x0, z0) < 0) ? 1 : 0);

      // Propagate overflows (borrows)
      long z1 = x1 - carry;
      long borrow = (Long.compareUnsigned(x1, z1) < 0) ? 1 : 0;

      if (borrow != 0) return addBack(z1, z0, q - 1); // less likely
      return new QR128(q, new UInt128(z1, z0));
    }

    private QR128 mulSubOverflow(final long v1, final long v0) {
      // Overflow case: div2by1 quotient would be <1, 0>, but adjusts to <0, MAX>
      // <p1, p0> = -1 * u0 = <u0 - 1, -u0>
      long z0 = v0 + u0;
      long carry = u0 - 1 + ((Long.compareUnsigned(v0, z0) <= 0) ? 1 : 0);

      long z1 = v1 + u1 - carry;
      // q = MAX may still be 1 too high - need to pass q - 1 (-2L) to addBack; check if result >=
      // modulus (i.e. negative wrapped)
      if (Long.compareUnsigned(z1, u1) > 0 || (z1 == u1 && Long.compareUnsigned(z0, u0) >= 0)) {
        return addBack(z1, z0, -2L);
      }
      return new QR128(-1L, new UInt128(z1, z0));
    }

    private QR128 reduceStep(final long v2, final long v1, final long v0, final long inv) {
      if (v2 == u1) return mulSubOverflow(v1, v0);
      QR64 qr = div2by1(v2, v1, u1, inv);
      if (qr.q != 0) return mulSub(qr.r, v0, qr.q);
      return new QR128(0, new UInt128(qr.r, v0));
    }

    private UInt256 modReduceNormalised(final UInt256 that, final int shift, final long inv) {
      UInt320 v = that.shiftLeftWide(shift);
      if (v.u4 == 0 && Long.compareUnsigned(v.u3, u1) < 0) {
        QR128 qr;
        if (v.u3 != 0 || Long.compareUnsigned(v.u2, u1) >= 0) {
          qr = reduceStep(v.u3, v.u2, v.u1, inv);
          qr = reduceStep(qr.r.u1, qr.r.u0, v.u0, inv);
        } else {
          qr = reduceStep(v.u2, v.u1, v.u0, inv);
        }
        return new UInt256(0, 0, qr.r.u1, qr.r.u0).shiftRight(shift);
      }
      return modReduceNormalisedSlowPath(v, shift, inv);
    }

    private UInt256 modReduceNormalised(final UInt257 that, final int shift, final long inv) {
      UInt320 v = that.shiftLeftWide(shift);
      if (v.u4 == 0 && Long.compareUnsigned(v.u3, u1) < 0) {
        QR128 qr;
        if (v.u3 != 0 || Long.compareUnsigned(v.u2, u1) >= 0) {
          qr = reduceStep(v.u3, v.u2, v.u1, inv);
          qr = reduceStep(qr.r.u1, qr.r.u0, v.u0, inv);
        } else {
          qr = reduceStep(v.u2, v.u1, v.u0, inv);
        }
        return new UInt256(0, 0, qr.r.u1, qr.r.u0).shiftRight(shift);
      }
      return modReduceNormalisedSlowPath(v, shift, inv);
    }

    private UInt256 modReduceNormalisedSlowPath(final UInt320 v, final int shift, final long inv) {
      QR128 qr;
      if (Long.compareUnsigned(v.u4, u1) >= 0) {
        qr = reduceStep(0, v.u4, v.u3, inv);
        qr = reduceStep(qr.r.u1, qr.r.u0, v.u2, inv);
        qr = reduceStep(qr.r.u1, qr.r.u0, v.u1, inv);
        qr = reduceStep(qr.r.u1, qr.r.u0, v.u0, inv);
      } else {
        qr = reduceStep(v.u4, v.u3, v.u2, inv);
        qr = reduceStep(qr.r.u1, qr.r.u0, v.u1, inv);
        qr = reduceStep(qr.r.u1, qr.r.u0, v.u0, inv);
      }
      return new UInt256(0, 0, qr.r.u1, qr.r.u0).shiftRight(shift);
    }

    private UInt256 modReduceNormalised(final UInt512 that, final int shift, final long inv) {
      UInt576 v = that.shiftLeftWide(shift);
      QR128 qr;
      if (v.u8 != 0 || Long.compareUnsigned(v.u7, u1) >= 0) {
        qr = reduceStep(v.u8, v.u7, v.u6, inv);
        qr = reduceStep(qr.r.u1, qr.r.u0, v.u5, inv);
        qr = reduceStep(qr.r.u1, qr.r.u0, v.u4, inv);
        qr = reduceStep(qr.r.u1, qr.r.u0, v.u3, inv);
        qr = reduceStep(qr.r.u1, qr.r.u0, v.u2, inv);
        qr = reduceStep(qr.r.u1, qr.r.u0, v.u1, inv);
        qr = reduceStep(qr.r.u1, qr.r.u0, v.u0, inv);
      } else if (v.u7 != 0 || Long.compareUnsigned(v.u6, u1) >= 0) {
        qr = reduceStep(v.u7, v.u6, v.u5, inv);
        qr = reduceStep(qr.r.u1, qr.r.u0, v.u4, inv);
        qr = reduceStep(qr.r.u1, qr.r.u0, v.u3, inv);
        qr = reduceStep(qr.r.u1, qr.r.u0, v.u2, inv);
        qr = reduceStep(qr.r.u1, qr.r.u0, v.u1, inv);
        qr = reduceStep(qr.r.u1, qr.r.u0, v.u0, inv);
      } else if (v.u6 != 0 || Long.compareUnsigned(v.u5, u1) >= 0) {
        qr = reduceStep(v.u6, v.u5, v.u4, inv);
        qr = reduceStep(qr.r.u1, qr.r.u0, v.u3, inv);
        qr = reduceStep(qr.r.u1, qr.r.u0, v.u2, inv);
        qr = reduceStep(qr.r.u1, qr.r.u0, v.u1, inv);
        qr = reduceStep(qr.r.u1, qr.r.u0, v.u0, inv);
      } else if (v.u5 != 0 || Long.compareUnsigned(v.u4, u1) >= 0) {
        qr = reduceStep(v.u5, v.u4, v.u3, inv);
        qr = reduceStep(qr.r.u1, qr.r.u0, v.u2, inv);
        qr = reduceStep(qr.r.u1, qr.r.u0, v.u1, inv);
        qr = reduceStep(qr.r.u1, qr.r.u0, v.u0, inv);
      } else {
        qr = reduceStep(v.u4, v.u3, v.u2, inv);
        qr = reduceStep(qr.r.u1, qr.r.u0, v.u1, inv);
        qr = reduceStep(qr.r.u1, qr.r.u0, v.u0, inv);
      }
      return new UInt256(0, 0, qr.r.u1, qr.r.u0).shiftRight(shift);
    }

    UInt256 divReduce(final UInt256 that) {
      int cmp = compareTo(that);
      if (cmp == 0) return ONE;
      if (cmp > 0) return ZERO;
      int shift = Long.numberOfLeadingZeros(u1);
      UInt128 m = shiftLeft(shift);
      long inv = reciprocal(m.u1);
      return m.divReduceNormalised(that, shift, inv);
    }

    private UInt256 divReduceNormalised(final UInt256 that, final int shift, final long inv) {
      UInt320 v = that.shiftLeftWide(shift);
      if (v.u4 == 0 && Long.compareUnsigned(v.u3, u1) < 0) {
        if (v.u3 != 0 || Long.compareUnsigned(v.u2, u1) >= 0) {
          QR128 qr1 = reduceStep(v.u3, v.u2, v.u1, inv);
          QR128 qr0 = reduceStep(qr1.r.u1, qr1.r.u0, v.u0, inv);
          return new UInt256(0, 0, qr1.q, qr0.q);
        }
        QR128 qr0 = reduceStep(v.u2, v.u1, v.u0, inv);
        return new UInt256(0, 0, 0, qr0.q);
      }
      return divReduceNormalisedSlowPath(v, inv);
    }

    private UInt256 divReduceNormalisedSlowPath(final UInt320 v, final long inv) {
      QR128 qr2 = reduceStep(v.u4, v.u3, v.u2, inv);
      QR128 qr1 = reduceStep(qr2.r.u1, qr2.r.u0, v.u1, inv);
      QR128 qr0 = reduceStep(qr1.r.u1, qr1.r.u0, v.u0, inv);
      return new UInt256(0, qr2.q, qr1.q, qr0.q);
    }
  }

  record UInt192(long u2, long u1, long u0) {
    UInt192 shiftLeft(final int shift) {
      if (shift == 0) return this;
      int invShift = N_BITS_PER_LIMB - shift;
      long z0 = u0 << shift;
      long z1 = (u1 << shift) | (u0 >>> invShift);
      long z2 = (u2 << shift) | (u1 >>> invShift);
      return new UInt192(z2, z1, z0);
    }

    int compareTo(final UInt256 v) {
      if (v.u3 != 0) return -1;
      if (v.u2 != u2) return Long.compareUnsigned(u2, v.u2);
      if (v.u1 != u1) return Long.compareUnsigned(u1, v.u1);
      return Long.compareUnsigned(u0, v.u0);
    }

    int compareTo(final UInt448 v) {
      if ((v.u6 | v.u5 | v.u4 | v.u3) != 0) return -1;
      if (v.u2 != u2) return Long.compareUnsigned(u2, v.u2);
      if (v.u1 != u1) return Long.compareUnsigned(u1, v.u1);
      return Long.compareUnsigned(u0, v.u0);
    }

    UInt256 modReduce(final UInt256 that) {
      int cmp = compareTo(that);
      if (cmp == 0) return ZERO;
      if (cmp > 0) return that;
      int shift = Long.numberOfLeadingZeros(u2);
      UInt192 m = shiftLeft(shift);
      long inv = reciprocal(m.u2);
      return m.modReduceNormalised(that, shift, inv);
    }

    UInt256 modReduce(final UInt448 that) {
      int cmp = compareTo(that);
      if (cmp == 0) return ZERO;
      if (cmp > 0) return that.UInt256Value();
      int shift = Long.numberOfLeadingZeros(u2);
      UInt192 m = shiftLeft(shift);
      long inv = reciprocal(m.u2);
      return m.modReduceNormalised(that, shift, inv);
    }

    UInt256 sum(final UInt256 a, final UInt256 b) {
      UInt257 sum = a.adc(b);
      if (!sum.carry()) {
        int cmp = compareTo(sum.UInt256Value());
        if (cmp == 0) return ZERO;
        if (cmp > 0) return sum.UInt256Value();
      }
      int shift = Long.numberOfLeadingZeros(u2);
      UInt192 m = shiftLeft(shift);
      long inv = reciprocal(m.u2);
      return m.modReduceNormalised(sum, shift, inv);
    }

    UInt256 mul(final UInt256 a, final UInt256 b) {
      // smaller allocation path: multiply-reduce with UInt448 prod
      if (a.isUInt192() && b.isUInt192()) {
        UInt448 prod = a.mul192(b);
        int cmp = compareTo(prod);
        if (cmp == 0) return ZERO;
        if (cmp > 0) return prod.UInt256Value();
        return modReduce(prod);
      }
      // At least one input exceeds 192 bits: full multiply then single reduce with UInt512 prod
      UInt512 prod = a.mul256(b);
      int shift = Long.numberOfLeadingZeros(u2);
      UInt192 m = shiftLeft(shift);
      long inv = reciprocal(m.u2);
      return m.modReduceNormalised(prod, shift, inv);
    }

    private QR192 addBack(final long v2, final long v1, final long v0, final long q) {
      // Add back
      long z0 = v0 + u0;
      long carry = ((v0 & u0) | ((v0 | u0) & ~z0)) >>> 63;

      long z1 = v1 + u1 + carry;
      long overflow1 = (Long.compareUnsigned(z1, v1) < 0) ? 1 : 0;
      long overflow2 = (Long.compareUnsigned(z1, v1) == 0) ? 1 : 0;
      carry = overflow1 | (overflow2 & carry);

      long z2 = v2 + u2 + carry;
      overflow1 = (Long.compareUnsigned(z2, v2) < 0) ? 1 : 0;
      overflow2 = (Long.compareUnsigned(z2, v2) == 0) ? 1 : 0;
      carry = overflow1 | (overflow2 & carry);

      if (carry == 0) { // unlikely: add back again
        // Proper quotient estimation guarantees recursion max-depth <= 2
        // Unbounded recursion only if there's a bug - fail fast is better than give wrong result
        return addBack(z2, z1, z0, q - 1);
      }
      return new QR192(q, new UInt192(z2, z1, z0));
    }

    private QR192 mulSub(final long v2, final long v1, final long v0, final long q) {
      // Multiply-subtract: already have highest 2 limbs
      // <u4, u3, u2, u1>  =  <u2, u1, u0> * q
      long p0 = u0 * q;
      long p1 = Math.unsignedMultiplyHigh(u0, q);
      long z0 = v0 - p0;
      long carry = p1 + (((Long.compareUnsigned(v0, z0) < 0) ? 1 : 0));

      p0 = u1 * q;
      p1 = Math.unsignedMultiplyHigh(u1, q);
      long res = v1 - p0;
      long z1 = res - carry;
      long borrow = (Long.compareUnsigned(res, z1) < 0) ? 1 : 0;
      carry = p1 + ((Long.compareUnsigned(v1, res) < 0) ? 1 : 0);

      // Propagate overflows (borrows)
      long t2 = v2 - carry;
      long z2 = t2 - borrow;
      borrow =
          ((Long.compareUnsigned(v2, t2) < 0) ? 1 : 0)
              | ((Long.compareUnsigned(t2, z2) < 0) ? 1 : 0);

      if (borrow != 0) return addBack(z2, z1, z0, q - 1); // unlikely
      return new QR192(q, new UInt192(z2, z1, z0));
    }

    private QR192 mulSubOverflow(final long v2, final long v1, final long v0) {
      // Overflow case: div2by1 quotient would be <1, 0>, but adjusts to <0, -1>
      // <p1, p0> = -1 * u0 = <u0 - 1, -u0>
      long z0 = v0 + u0;
      long carry = u0 - 1 + ((Long.compareUnsigned(v0, z0) <= 0) ? 1 : 0);

      long res = v1 - carry;
      long z1 = res + u1;
      long borrow = (Long.compareUnsigned(res, z1) <= 0) ? 1 : 0;
      carry = u1 - 1 + ((Long.compareUnsigned(v1, res) < 0) ? 1 : 0);

      long z2 = v2 - carry + u2 - borrow;
      // q = MAX may still be 1 too high - need to pass q - 1 (-2L) to addBack; check if result >=
      // modulus (i.e. negative wrapped)
      if (Long.compareUnsigned(z2, u2) > 0
          || (z2 == u2
              && (Long.compareUnsigned(z1, u1) > 0
                  || (z1 == u1 && Long.compareUnsigned(z0, u0) >= 0)))) {

        return addBack(z2, z1, z0, -2L);
      }
      return new QR192(-1L, new UInt192(z2, z1, z0));
    }

    private QR192 reduceStep(
        final long v3, final long v2, final long v1, final long v0, final long inv) {
      if (v3 == u2) return mulSubOverflow(v2, v1, v0);
      QR64 qr = div2by1(v3, v2, u2, inv);
      if (qr.q != 0) return mulSub(qr.r, v1, v0, qr.q);
      return new QR192(0, new UInt192(v2, v1, v0));
    }

    private UInt256 modReduceNormalised(final UInt256 that, final int shift, final long inv) {
      UInt320 v = that.shiftLeftWide(shift);
      if (Long.compareUnsigned(v.u4, u2) < 0) {
        QR192 qr;
        if (v.u4 != 0 || Long.compareUnsigned(v.u3, u2) >= 0) {
          qr = reduceStep(v.u4, v.u3, v.u2, v.u1, inv);
          qr = reduceStep(qr.r.u2, qr.r.u1, qr.r.u0, v.u0, inv);
        } else {
          qr = reduceStep(v.u3, v.u2, v.u1, v.u0, inv);
        }
        return new UInt256(0, qr.r.u2, qr.r.u1, qr.r.u0).shiftRight(shift);
      }
      return reduceNormalisedSlowPath(v, shift, inv);
    }

    private UInt256 modReduceNormalised(final UInt257 that, final int shift, final long inv) {
      UInt320 v = that.shiftLeftWide(shift);
      if (Long.compareUnsigned(v.u4, u2) < 0) {
        QR192 qr;
        if (v.u4 != 0 || Long.compareUnsigned(v.u3, u2) >= 0) {
          qr = reduceStep(v.u4, v.u3, v.u2, v.u1, inv);
          qr = reduceStep(qr.r.u2, qr.r.u1, qr.r.u0, v.u0, inv);
        } else {
          qr = reduceStep(v.u3, v.u2, v.u1, v.u0, inv);
        }
        return new UInt256(0, qr.r.u2, qr.r.u1, qr.r.u0).shiftRight(shift);
      }
      return reduceNormalisedSlowPath(v, shift, inv);
    }

    private UInt256 reduceNormalisedSlowPath(final UInt320 v, final int shift, final long inv) {
      QR192 qr = reduceStep(0, v.u4, v.u3, v.u2, inv);
      qr = reduceStep(qr.r.u2, qr.r.u1, qr.r.u0, v.u1, inv);
      qr = reduceStep(qr.r.u2, qr.r.u1, qr.r.u0, v.u0, inv);
      return new UInt256(0, qr.r.u2, qr.r.u1, qr.r.u0).shiftRight(shift);
    }

    private UInt256 modReduceNormalised(final UInt448 that, final int shift, final long inv) {
      UInt512 v = that.shiftLeftWide(shift);
      if ((v.u7 | v.u6 | v.u5) == 0 && Long.compareUnsigned(v.u4, u2) < 0) {
        QR192 qr;
        if (v.u4 != 0 || Long.compareUnsigned(v.u3, u2) >= 0) {
          qr = reduceStep(v.u4, v.u3, v.u2, v.u1, inv);
          qr = reduceStep(qr.r.u2, qr.r.u1, qr.r.u0, v.u0, inv);
        } else {
          qr = reduceStep(v.u3, v.u2, v.u1, v.u0, inv);
        }
        return new UInt256(0, qr.r.u2, qr.r.u1, qr.r.u0).shiftRight(shift);
      }
      return modReduceNormalisedSlowPath(v, shift, inv);
    }

    private UInt256 modReduceNormalisedSlowPath(final UInt512 v, final int shift, final long inv) {
      QR192 qr;
      if (v.u7 != 0 || Long.compareUnsigned(v.u6, u2) >= 0) {
        qr = reduceStep(v.u7, v.u6, v.u5, v.u4, inv);
        qr = reduceStep(qr.r.u2, qr.r.u1, qr.r.u0, v.u3, inv);
        qr = reduceStep(qr.r.u2, qr.r.u1, qr.r.u0, v.u2, inv);
        qr = reduceStep(qr.r.u2, qr.r.u1, qr.r.u0, v.u1, inv);
        qr = reduceStep(qr.r.u2, qr.r.u1, qr.r.u0, v.u0, inv);
      } else if (v.u6 != 0 || Long.compareUnsigned(v.u5, u2) >= 0) {
        qr = reduceStep(v.u6, v.u5, v.u4, v.u3, inv);
        qr = reduceStep(qr.r.u2, qr.r.u1, qr.r.u0, v.u2, inv);
        qr = reduceStep(qr.r.u2, qr.r.u1, qr.r.u0, v.u1, inv);
        qr = reduceStep(qr.r.u2, qr.r.u1, qr.r.u0, v.u0, inv);
      } else {
        qr = reduceStep(v.u5, v.u4, v.u3, v.u2, inv);
        qr = reduceStep(qr.r.u2, qr.r.u1, qr.r.u0, v.u1, inv);
        qr = reduceStep(qr.r.u2, qr.r.u1, qr.r.u0, v.u0, inv);
      }
      return new UInt256(0, qr.r.u2, qr.r.u1, qr.r.u0).shiftRight(shift);
    }

    private UInt256 modReduceNormalised(final UInt512 that, final int shift, final long inv) {
      UInt576 v = that.shiftLeftWide(shift);
      QR192 qr;
      if (v.u8 != 0 || Long.compareUnsigned(v.u7, u2) >= 0) {
        qr = reduceStep(v.u8, v.u7, v.u6, v.u5, inv);
        qr = reduceStep(qr.r.u2, qr.r.u1, qr.r.u0, v.u4, inv);
        qr = reduceStep(qr.r.u2, qr.r.u1, qr.r.u0, v.u3, inv);
        qr = reduceStep(qr.r.u2, qr.r.u1, qr.r.u0, v.u2, inv);
        qr = reduceStep(qr.r.u2, qr.r.u1, qr.r.u0, v.u1, inv);
        qr = reduceStep(qr.r.u2, qr.r.u1, qr.r.u0, v.u0, inv);
      } else if (v.u7 != 0 || Long.compareUnsigned(v.u6, u2) >= 0) {
        qr = reduceStep(v.u7, v.u6, v.u5, v.u4, inv);
        qr = reduceStep(qr.r.u2, qr.r.u1, qr.r.u0, v.u3, inv);
        qr = reduceStep(qr.r.u2, qr.r.u1, qr.r.u0, v.u2, inv);
        qr = reduceStep(qr.r.u2, qr.r.u1, qr.r.u0, v.u1, inv);
        qr = reduceStep(qr.r.u2, qr.r.u1, qr.r.u0, v.u0, inv);
      } else if (v.u6 != 0 || Long.compareUnsigned(v.u5, u2) >= 0) {
        qr = reduceStep(v.u6, v.u5, v.u4, v.u3, inv);
        qr = reduceStep(qr.r.u2, qr.r.u1, qr.r.u0, v.u2, inv);
        qr = reduceStep(qr.r.u2, qr.r.u1, qr.r.u0, v.u1, inv);
        qr = reduceStep(qr.r.u2, qr.r.u1, qr.r.u0, v.u0, inv);
      } else {
        qr = reduceStep(v.u5, v.u4, v.u3, v.u2, inv);
        qr = reduceStep(qr.r.u2, qr.r.u1, qr.r.u0, v.u1, inv);
        qr = reduceStep(qr.r.u2, qr.r.u1, qr.r.u0, v.u0, inv);
      }
      return new UInt256(0, qr.r.u2, qr.r.u1, qr.r.u0).shiftRight(shift);
    }

    UInt256 divReduce(final UInt256 that) {
      int cmp = compareTo(that);
      if (cmp == 0) return ONE;
      if (cmp > 0) return ZERO;
      int shift = Long.numberOfLeadingZeros(u2);
      UInt192 m = shiftLeft(shift);
      long inv = reciprocal(m.u2);
      return m.divReduceNormalised(that, shift, inv);
    }

    private UInt256 divReduceNormalised(final UInt256 that, final int shift, final long inv) {
      UInt320 v = that.shiftLeftWide(shift);
      if (v.u4 == 0 && Long.compareUnsigned(v.u3, u2) < 0) {
        QR192 qr0 = reduceStep(v.u3, v.u2, v.u1, v.u0, inv);
        return new UInt256(0, 0, 0, qr0.q);
      }
      return divReduceNormalisedSlowPath(v, inv);
    }

    private UInt256 divReduceNormalisedSlowPath(final UInt320 v, final long inv) {
      QR192 qr1 = reduceStep(v.u4, v.u3, v.u2, v.u1, inv);
      QR192 qr0 = reduceStep(qr1.r.u2, qr1.r.u1, qr1.r.u0, v.u0, inv);
      return new UInt256(0, 0, qr1.q, qr0.q);
    }
  }

  record UInt257(boolean carry, UInt256 u) {
    boolean isUInt64() {
      return !carry && u.isUInt64();
    }

    boolean isUInt256() {
      return !carry;
    }

    UInt256 UInt256Value() {
      return u;
    }

    UInt320 shiftLeftWide(final int shift) {
      long u4 = (carry ? 1L : 0L);
      if (shift == 0) return new UInt320(u4, u.u3, u.u2, u.u1, u.u0);
      int invShift = (N_BITS_PER_LIMB - shift);
      long z0 = (u.u0 << shift);
      long z1 = (u.u1 << shift) | u.u0 >>> invShift;
      long z2 = (u.u2 << shift) | u.u1 >>> invShift;
      long z3 = (u.u3 << shift) | u.u2 >>> invShift;
      long z4 = (u4 << shift) | u.u3 >>> invShift;
      return new UInt320(z4, z3, z2, z1, z0);
    }
  }

  record UInt320(long u4, long u3, long u2, long u1, long u0) {
    static final UInt320 ZERO = new UInt320(0, 0, 0, 0, 0);

    UInt320 shiftDigitsRight() {
      return new UInt320(0, u4, u3, u2, u1);
    }
  }

  record UInt448(long u6, long u5, long u4, long u3, long u2, long u1, long u0) {
    UInt256 UInt256Value() {
      return new UInt256(u3, u2, u1, u0);
    }

    UInt512 shiftLeftWide(final int shift) {
      if (shift == 0) return new UInt512(0, u6, u5, u4, u3, u2, u1, u0);
      int invShift = (N_BITS_PER_LIMB - shift);
      long z0 = (u0 << shift);
      long z1 = (u1 << shift) | u0 >>> invShift;
      long z2 = (u2 << shift) | u1 >>> invShift;
      long z3 = (u3 << shift) | u2 >>> invShift;
      long z4 = (u4 << shift) | u3 >>> invShift;
      long z5 = (u5 << shift) | u4 >>> invShift;
      long z6 = (u6 << shift) | u5 >>> invShift;
      long z7 = u6 >>> invShift;
      return new UInt512(z7, z6, z5, z4, z3, z2, z1, z0);
    }
  }

  private record UInt512(long u7, long u6, long u5, long u4, long u3, long u2, long u1, long u0) {
    UInt256 UInt256Value() {
      return new UInt256(u3, u2, u1, u0);
    }

    UInt576 shiftLeftWide(final int shift) {
      if (shift == 0) return new UInt576(0, u7, u6, u5, u4, u3, u2, u1, u0);
      int invShift = (N_BITS_PER_LIMB - shift);
      long z0 = (u0 << shift);
      long z1 = (u1 << shift) | u0 >>> invShift;
      long z2 = (u2 << shift) | u1 >>> invShift;
      long z3 = (u3 << shift) | u2 >>> invShift;
      long z4 = (u4 << shift) | u3 >>> invShift;
      long z5 = (u5 << shift) | u4 >>> invShift;
      long z6 = (u6 << shift) | u5 >>> invShift;
      long z7 = (u7 << shift) | u6 >>> invShift;
      long z8 = u7 >>> invShift;
      return new UInt576(z8, z7, z6, z5, z4, z3, z2, z1, z0);
    }
  }

  record UInt576(long u8, long u7, long u6, long u5, long u4, long u3, long u2, long u1, long u0) {}

  // --------------------------------------------------------------------------
  // endregion

}
