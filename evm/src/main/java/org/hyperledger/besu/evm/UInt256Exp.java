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
package org.hyperledger.besu.evm;

import java.math.BigInteger;
import java.util.function.IntUnaryOperator;

import com.google.common.annotations.VisibleForTesting;

/**
 * Wrapping exponentiation {@code base^exponent mod 2^256}, with {@code 0^0 = 1} as in the EVM EXP
 * opcode.
 *
 * <p>Short exponents use left-to-right square-and-multiply. A longer exponent of an odd base uses
 * the 2-adic logarithm and exponential, following K. Ventullo, "2-adic logarithms and fast
 * exponentiation" (2020). For {@code u ≡ 1 (mod 4)} the identity {@code u^e = exp(e·log(u))} holds
 * exactly modulo 2^256 and both series are finite there, so a full-width exponent costs about 50
 * multiplications instead of about 255 squarings and 128 multiplications.
 *
 * <ol>
 *   <li>Even base {@code a = 2^t·m}: {@code a^e = 2^(t·e)·m^e}, which is 0 once {@code t·e ≥ 256}.
 *   <li>Odd base {@code a ≡ 3 (mod 4)}: {@code a^e = (-1)^e·u^e} with {@code u = -a ≡ 1 (mod 4)}.
 *   <li>Lift: square {@code u} {@code s} times, consuming the low {@code s} bits of {@code e},
 *       until {@code p = u^(2^s) ≡ 1 (mod 2^LIFT_BITS)}. Then {@code u^e = u^(e mod 2^s)·p^(e >>
 *       s)}.
 *   <li>{@code p^E = exp(E·log(p))}, with {@code log(1 + x) = Σ (-1)^(k+1)·x^k/k} and {@code exp(y)
 *       = Σ y^k/k!}. A term whose 2-adic valuation reaches 256 is 0 modulo 2^256, so 10 terms of
 *       each series are enough.
 * </ol>
 *
 * <p>Precision: with {@code x = 2^R·x'} the k-th log term is {@code 2^(kR - v2(k))·x'^k / odd(k)}:
 * a left shift and a multiplication by the inverse of {@code odd(k)}. Dividing a wrapped product by
 * {@code 2^v2(k)} with a right shift would instead lose its top {@code v2(k)} bits. The exp terms
 * use {@code k!} the same way. Both series are evaluated with Horner's rule, and each product is
 * truncated to the limbs that can still reach the result.
 *
 * <p>An instance holds scratch registers for a single computation.
 */
final class UInt256Exp {

  /** Lift target: the series run on a base {@code p ≡ 1 (mod 2^LIFT_BITS)}. At most 63. */
  private static final int LIFT_BITS = 24;

  /**
   * Odd-base exponents of at most this many bits use square-and-multiply, longer ones the 2-adic
   * series. At least {@code LIFT_BITS}, so the lift never consumes the whole exponent, and at most
   * 64, since the short path only reads the low limb of the exponent.
   */
  private static final int SHORT_EXPONENT_BITS = 48;

  /** Index of the last log term whose valuation is below 256. */
  private static final int LOG_TERMS = lastTerm(UInt256Exp::logValuation);

  /** Index of the last exp term whose valuation is below 256. */
  private static final int EXP_TERMS = lastTerm(UInt256Exp::expValuation);

  /** Limbs of the {@code (256 - LIFT_BITS)}-bit values {@code L' = log(p) / 2^R} and {@code y'}. */
  private static final int TOP_LIMBS = limbsFor(256 - LIFT_BITS);

  // Log Horner scheme: H_K = c_K, H_k = c_k + 2^LOG_SHIFT[k]·(x'·H_{k+1} truncated to
  // LOG_LIMBS[k] limbs), log(p) = 2^R·x'·H_1, with c_k = (-1)^(k+1) / odd(k) mod 2^256.
  private static final int[] LOG_SHIFT = new int[LOG_TERMS];
  private static final int[] LOG_LIMBS = new int[LOG_TERMS];
  private static final long[] LOG_COEFFICIENTS = new long[4 * (LOG_TERMS + 1)];

  // Exp Horner scheme: G_J = d_J, G_k = d_k + 2^EXP_SHIFT[k]·(y'·G_{k+1} truncated to
  // EXP_LIMBS[k] limbs), exp(2^R·y') = G_0, with d_k = 1 / odd(k!) mod 2^256.
  private static final int[] EXP_SHIFT = new int[EXP_TERMS];
  private static final int[] EXP_LIMBS = new int[EXP_TERMS];
  private static final long[] EXP_COEFFICIENTS = new long[4 * (EXP_TERMS + 1)];

  static {
    final BigInteger modulus = BigInteger.ONE.shiftLeft(256);
    BigInteger oddFactorial = BigInteger.ONE;
    for (int k = 0; k <= Math.max(LOG_TERMS, EXP_TERMS); k++) {
      if (k > 0) {
        final BigInteger odd = BigInteger.valueOf(k >> Integer.numberOfTrailingZeros(k));
        oddFactorial = oddFactorial.multiply(odd);
        if (k <= LOG_TERMS) {
          final BigInteger inverse = odd.modInverse(modulus);
          store(LOG_COEFFICIENTS, k, (k & 1) == 1 ? inverse : modulus.subtract(inverse));
        }
      }
      if (k <= EXP_TERMS) {
        store(EXP_COEFFICIENTS, k, oddFactorial.modInverse(modulus));
      }
    }
    for (int k = 1; k < LOG_TERMS; k++) {
      LOG_SHIFT[k] = checkedShift(logValuation(k + 1) - logValuation(k));
      LOG_LIMBS[k] = limbsFor(256 - logValuation(k + 1));
    }
    for (int k = 0; k < EXP_TERMS; k++) {
      EXP_SHIFT[k] = checkedShift(expValuation(k + 1) - expValuation(k));
      EXP_LIMBS[k] = limbsFor(256 - expValuation(k + 1));
    }
  }

  // Scratch registers, least significant limb first.
  private long t0, t1, t2, t3; // result of the last multiply or shift
  private long p0, p1, p2, p3; // lifted base p = u^(2^i)
  private long v0, v1, v2, v3; // power accumulator
  private long h0, h1, h2, h3; // Horner accumulator

  /** The t register: the result of the last multiply or shift. */
  @VisibleForTesting
  UInt256 product() {
    return new UInt256(t3, t2, t1, t0);
  }

  /** The h register: the result of the last {@code shiftLeftAdd}. */
  @VisibleForTesting
  UInt256 horner() {
    return new UInt256(h3, h2, h1, h0);
  }

  @VisibleForTesting
  UInt256Exp() {}

  /**
   * Computes {@code base^exponent mod 2^256}.
   *
   * @param base the base
   * @param exponent the exponent
   * @return the wrapping power, 1 when the exponent is 0
   */
  static UInt256 exp(final UInt256 base, final UInt256 exponent) {
    if (exponent.isZero()) {
      return UInt256.ONE;
    }
    if (base.isZeroOrOne() || exponent.isOne()) {
      return base;
    }
    return new UInt256Exp().compute(base, exponent);
  }

  private UInt256 compute(final UInt256 base, final UInt256 exponent) {
    final long a0 = base.u0(), a1 = base.u1(), a2 = base.u2(), a3 = base.u3();
    final long e0 = exponent.u0(), e1 = exponent.u1(), e2 = exponent.u2(), e3 = exponent.u3();
    final int bits = bitLength(e0, e1, e2, e3);
    if ((a0 & 1) == 0) {
      return evenBase(a0, a1, a2, a3, e0, bits);
    }
    if (bits <= SHORT_EXPONENT_BITS) {
      squareAndMultiply(a0, a1, a2, a3, e0, bits);
      return new UInt256(v3, v2, v1, v0);
    }
    return oddBase(a0, a1, a2, a3, e0, e1, e2, e3);
  }

  /**
   * {@code a^e} for an even {@code a = 2^t·m}: {@code 2^(t·e)·m^e}, or 0 once {@code t·e ≥ 256}.
   */
  private UInt256 evenBase(
      final long a0, final long a1, final long a2, final long a3, final long e, final int bits) {
    if (bits > 8) {
      return UInt256.ZERO; // e ≥ 256 and t ≥ 1
    }
    final int t = numberOfTrailingZeros(a0, a1, a2, a3);
    final int shift = t * (int) e;
    if (shift >= 256) {
      return UInt256.ZERO;
    }
    shiftRight(a0, a1, a2, a3, t);
    if (((t0 - 1) | t1 | t2 | t3) == 0) {
      shiftLeft(1, 0, 0, 0, shift); // a is a power of two
    } else {
      squareAndMultiply(t0, t1, t2, t3, e, bits);
      shiftLeft(v0, v1, v2, v3, shift);
    }
    return new UInt256(t3, t2, t1, t0);
  }

  /** {@code a^e} for an odd {@code a} and an exponent of more than SHORT_EXPONENT_BITS bits. */
  private UInt256 oddBase(
      final long a0,
      final long a1,
      final long a2,
      final long a3,
      final long e0,
      final long e1,
      final long e2,
      final long e3) {
    final boolean threeModFour = (a0 & 2) != 0;
    // u = -a when a ≡ 3 (mod 4). a is odd, so the negation does not borrow past the low limb.
    final long u0 = threeModFour ? -a0 : a0;
    final long u1 = threeModFour ? ~a1 : a1;
    final long u2 = threeModFour ? ~a2 : a2;
    final long u3 = threeModFour ? ~a3 : a3;
    final boolean negate = threeModFour && (e0 & 1) != 0;
    final long x0 = u0 & ~1L; // low limb of u - 1
    if ((x0 | u1 | u2 | u3) == 0) {
      return negate ? UInt256.MAX : UInt256.ONE; // a = 1 or a = -1
    }

    // v2(u^2 - 1) = v2(u - 1) + 1 for u ≡ 1 (mod 4), so s squarings reach the lift target. The low
    // s bits of e are consumed right to left on the way.
    final int s = Math.max(0, LIFT_BITS - numberOfTrailingZeros(x0, u1, u2, u3));
    p0 = u0;
    p1 = u1;
    p2 = u2;
    p3 = u3;
    boolean valueIsOne = true;
    for (int i = 0; i < s; i++) {
      if (((e0 >>> i) & 1) != 0) {
        if (valueIsOne) {
          v0 = p0;
          v1 = p1;
          v2 = p2;
          v3 = p3;
          valueIsOne = false;
        } else {
          multiply(v0, v1, v2, v3, p0, p1, p2, p3);
          v0 = t0;
          v1 = t1;
          v2 = t2;
          v3 = t3;
        }
      }
      square(p0, p1, p2, p3);
      p0 = t0;
      p1 = t1;
      p2 = t2;
      p3 = t3;
    }

    // E = e >> s is not 0: e has more than SHORT_EXPONENT_BITS bits and s < LIFT_BITS.
    if (s == 0) {
      liftedPower(e0, e1, e2, e3);
    } else {
      liftedPower(
          (e0 >>> s) | (e1 << (64 - s)),
          (e1 >>> s) | (e2 << (64 - s)),
          (e2 >>> s) | (e3 << (64 - s)),
          e3 >>> s);
    }
    if (!valueIsOne) {
      multiply(v0, v1, v2, v3, h0, h1, h2, h3);
      h0 = t0;
      h1 = t1;
      h2 = t2;
      h3 = t3;
    }
    return negate ? negated(h0, h1, h2, h3) : new UInt256(h3, h2, h1, h0);
  }

  /**
   * {@code h = p^E = exp(E·log(p))} for the lifted base {@code p ≡ 1 (mod 2^LIFT_BITS)}.
   *
   * <p>With {@code p = 1 + 2^R·x'}, {@code log(p) = 2^R·L'} and the exp argument is {@code y =
   * 2^R·y'} with {@code y' = E·L'}, so both series only see the exact cofactors {@code x'} and
   * {@code y'}.
   */
  private void liftedPower(final long f0, final long f1, final long f2, final long f3) {
    shiftRight(p0 & ~1L, p1, p2, p3, LIFT_BITS);
    final long x0 = t0, x1 = t1, x2 = t2, x3 = t3;

    setHorner(LOG_COEFFICIENTS, LOG_TERMS);
    for (int k = LOG_TERMS - 1; k >= 1; k--) {
      multiplyLimbs(LOG_LIMBS[k], x0, x1, x2, x3, h0, h1, h2, h3);
      shiftLeftAdd(LOG_SHIFT[k], LOG_COEFFICIENTS, k);
    }
    multiplyLimbs(TOP_LIMBS, x0, x1, x2, x3, h0, h1, h2, h3);
    multiplyLimbs(TOP_LIMBS, t0, t1, t2, t3, f0, f1, f2, f3);
    final long y0 = t0, y1 = t1, y2 = t2, y3 = t3;

    setHorner(EXP_COEFFICIENTS, EXP_TERMS);
    for (int k = EXP_TERMS - 1; k >= 0; k--) {
      multiplyLimbs(EXP_LIMBS[k], y0, y1, y2, y3, h0, h1, h2, h3);
      shiftLeftAdd(EXP_SHIFT[k], EXP_COEFFICIENTS, k);
    }
  }

  /** {@code v = b^e} by left-to-right square-and-multiply, where {@code e} has 1 to 64 bits. */
  private void squareAndMultiply(
      final long b0, final long b1, final long b2, final long b3, final long e, final int bits) {
    final boolean singleLimb = (b1 | b2 | b3) == 0;
    v0 = b0;
    v1 = b1;
    v2 = b2;
    v3 = b3;
    for (int i = bits - 2; i >= 0; i--) {
      square(v0, v1, v2, v3);
      if (((e >>> i) & 1) != 0) {
        if (singleLimb) {
          multiplySingleLimb(t0, t1, t2, t3, b0);
        } else {
          multiply(t0, t1, t2, t3, b0, b1, b2, b3);
        }
      }
      v0 = t0;
      v1 = t1;
      v2 = t2;
      v3 = t3;
    }
  }

  // --------------------------------------------------------------------------
  // Limb arithmetic. Results go to t, except setHorner and shiftLeftAdd, which write h.

  /** {@code t = a·b mod 2^(64·limbs)}, with the limbs above {@code limbs} set to 0. */
  @VisibleForTesting
  void multiplyLimbs(
      final int limbs,
      final long a0,
      final long a1,
      final long a2,
      final long a3,
      final long b0,
      final long b1,
      final long b2,
      final long b3) {
    switch (limbs) {
      case 4 -> multiply(a0, a1, a2, a3, b0, b1, b2, b3);
      case 3 -> {
        final long l01 = a0 * b1;
        final long l10 = a1 * b0;
        long sum = Math.unsignedMultiplyHigh(a0, b0) + l01;
        long carry = carry(sum, l01);
        sum += l10;
        carry += carry(sum, l10);
        t0 = a0 * b0;
        t1 = sum;
        t2 =
            carry
                + Math.unsignedMultiplyHigh(a0, b1)
                + Math.unsignedMultiplyHigh(a1, b0)
                + a0 * b2
                + a1 * b1
                + a2 * b0;
        t3 = 0;
      }
      case 2 -> {
        t0 = a0 * b0;
        t1 = Math.unsignedMultiplyHigh(a0, b0) + a0 * b1 + a1 * b0;
        t2 = 0;
        t3 = 0;
      }
      default -> {
        t0 = a0 * b0;
        t1 = 0;
        t2 = 0;
        t3 = 0;
      }
    }
  }

  /** {@code t = a·b mod 2^256}, from the 10 low and 6 high partial products that reach it. */
  @VisibleForTesting
  void multiply(
      final long a0,
      final long a1,
      final long a2,
      final long a3,
      final long b0,
      final long b1,
      final long b2,
      final long b3) {
    final long l01 = a0 * b1;
    final long l10 = a1 * b0;
    long sum = Math.unsignedMultiplyHigh(a0, b0) + l01;
    long carry = carry(sum, l01);
    sum += l10;
    carry += carry(sum, l10);
    final long r1 = sum;

    final long h01 = Math.unsignedMultiplyHigh(a0, b1);
    final long h10 = Math.unsignedMultiplyHigh(a1, b0);
    final long l02 = a0 * b2;
    final long l11 = a1 * b1;
    final long l20 = a2 * b0;
    sum = carry + h01;
    carry = carry(sum, h01);
    sum += h10;
    carry += carry(sum, h10);
    sum += l02;
    carry += carry(sum, l02);
    sum += l11;
    carry += carry(sum, l11);
    sum += l20;
    carry += carry(sum, l20);
    final long r2 = sum;

    t3 =
        carry
            + Math.unsignedMultiplyHigh(a0, b2)
            + Math.unsignedMultiplyHigh(a1, b1)
            + Math.unsignedMultiplyHigh(a2, b0)
            + a0 * b3
            + a1 * b2
            + a2 * b1
            + a3 * b0;
    t0 = a0 * b0;
    t1 = r1;
    t2 = r2;
  }

  /** {@code t = a·b mod 2^256} for a single-limb {@code b}. */
  @VisibleForTesting
  void multiplySingleLimb(
      final long a0, final long a1, final long a2, final long a3, final long b) {
    final long l1 = a1 * b;
    final long r1 = Math.unsignedMultiplyHigh(a0, b) + l1;
    final long carry1 = carry(r1, l1);
    final long l2 = a2 * b;
    final long sum2 = l2 + Math.unsignedMultiplyHigh(a1, b);
    long carry2 = carry(sum2, l2);
    final long r2 = sum2 + carry1;
    carry2 += carry(r2, sum2);
    t3 = a3 * b + Math.unsignedMultiplyHigh(a2, b) + carry2;
    t0 = a0 * b;
    t1 = r1;
    t2 = r2;
  }

  /** {@code t = a^2 mod 2^256}: the 3 cross products are doubled, then the 2 squares added. */
  @VisibleForTesting
  void square(final long a0, final long a1, final long a2, final long a3) {
    final long c1 = a0 * a1;
    final long l02 = a0 * a2;
    final long c2 = Math.unsignedMultiplyHigh(a0, a1) + l02;
    final long c3 = Math.unsignedMultiplyHigh(a0, a2) + a0 * a3 + a1 * a2 + carry(c2, l02);
    final long d1 = c1 << 1;
    final long d2 = (c2 << 1) | (c1 >>> 63);
    final long d3 = (c3 << 1) | (c2 >>> 63);

    final long s1 = Math.unsignedMultiplyHigh(a0, a0);
    final long s2 = a1 * a1;
    final long r1 = d1 + s1;
    final long carry1 = carry(r1, s1);
    final long sum2 = d2 + s2;
    long carry2 = carry(sum2, s2);
    final long r2 = sum2 + carry1;
    carry2 += carry(r2, sum2);
    t3 = d3 + Math.unsignedMultiplyHigh(a1, a1) + carry2;
    t0 = a0 * a0;
    t1 = r1;
    t2 = r2;
  }

  /** {@code h = coefficients[k]}. */
  private void setHorner(final long[] coefficients, final int k) {
    final int i = 4 * k;
    h0 = coefficients[i];
    h1 = coefficients[i + 1];
    h2 = coefficients[i + 2];
    h3 = coefficients[i + 3];
  }

  /** {@code h = (t << shift) + coefficients[k] mod 2^256}, for {@code 1 ≤ shift ≤ 63}. */
  @VisibleForTesting
  void shiftLeftAdd(final int shift, final long[] coefficients, final int k) {
    final int back = 64 - shift;
    final long x0 = t0 << shift;
    final long x1 = (t1 << shift) | (t0 >>> back);
    final long x2 = (t2 << shift) | (t1 >>> back);
    final long x3 = (t3 << shift) | (t2 >>> back);
    final int i = 4 * k;
    final long c0 = coefficients[i];
    final long c1 = coefficients[i + 1];
    final long c2 = coefficients[i + 2];
    final long r0 = x0 + c0;
    long carry = carry(r0, c0);
    long sum = x1 + c1;
    long nextCarry = carry(sum, c1);
    final long r1 = sum + carry;
    carry = nextCarry | carry(r1, sum);
    sum = x2 + c2;
    nextCarry = carry(sum, c2);
    final long r2 = sum + carry;
    carry = nextCarry | carry(r2, sum);
    h0 = r0;
    h1 = r1;
    h2 = r2;
    h3 = x3 + coefficients[i + 3] + carry;
  }

  /** {@code t = x << n mod 2^256}, for {@code 0 ≤ n ≤ 255}. */
  @VisibleForTesting
  void shiftLeft(final long x0, final long x1, final long x2, final long x3, final int n) {
    final int limbs = n >>> 6;
    final int bits = n & 63;
    long r0 = x0, r1 = x1, r2 = x2, r3 = x3;
    if (limbs == 1) {
      r3 = x2;
      r2 = x1;
      r1 = x0;
      r0 = 0;
    } else if (limbs == 2) {
      r3 = x1;
      r2 = x0;
      r1 = 0;
      r0 = 0;
    } else if (limbs == 3) {
      r3 = x0;
      r2 = 0;
      r1 = 0;
      r0 = 0;
    }
    if (bits != 0) {
      final int back = 64 - bits;
      r3 = (r3 << bits) | (r2 >>> back);
      r2 = (r2 << bits) | (r1 >>> back);
      r1 = (r1 << bits) | (r0 >>> back);
      r0 <<= bits;
    }
    t0 = r0;
    t1 = r1;
    t2 = r2;
    t3 = r3;
  }

  /** {@code t = x >>> n}, for {@code 0 ≤ n ≤ 255}. */
  @VisibleForTesting
  void shiftRight(final long x0, final long x1, final long x2, final long x3, final int n) {
    final int limbs = n >>> 6;
    final int bits = n & 63;
    long r0 = x0, r1 = x1, r2 = x2, r3 = x3;
    if (limbs == 1) {
      r0 = x1;
      r1 = x2;
      r2 = x3;
      r3 = 0;
    } else if (limbs == 2) {
      r0 = x2;
      r1 = x3;
      r2 = 0;
      r3 = 0;
    } else if (limbs == 3) {
      r0 = x3;
      r1 = 0;
      r2 = 0;
      r3 = 0;
    }
    if (bits != 0) {
      final int back = 64 - bits;
      r0 = (r0 >>> bits) | (r1 << back);
      r1 = (r1 >>> bits) | (r2 << back);
      r2 = (r2 >>> bits) | (r3 << back);
      r3 >>>= bits;
    }
    t0 = r0;
    t1 = r1;
    t2 = r2;
    t3 = r3;
  }

  /** {@code -x mod 2^256 = ~x + 1}: the +1 carries past a limb only while the limbs are 0. */
  @VisibleForTesting
  static UInt256 negated(final long x0, final long x1, final long x2, final long x3) {
    long carry = x0 == 0 ? 1 : 0;
    final long r1 = ~x1 + carry;
    carry &= x1 == 0 ? 1 : 0;
    final long r2 = ~x2 + carry;
    carry &= x2 == 0 ? 1 : 0;
    return new UInt256(~x3 + carry, r2, r1, -x0);
  }

  private static long carry(final long sum, final long addend) {
    return Long.compareUnsigned(sum, addend) < 0 ? 1L : 0L;
  }

  private static int numberOfTrailingZeros(
      final long x0, final long x1, final long x2, final long x3) {
    if (x0 != 0) {
      return Long.numberOfTrailingZeros(x0);
    }
    if (x1 != 0) {
      return 64 + Long.numberOfTrailingZeros(x1);
    }
    if (x2 != 0) {
      return 128 + Long.numberOfTrailingZeros(x2);
    }
    return 192 + Long.numberOfTrailingZeros(x3);
  }

  private static int bitLength(final long x0, final long x1, final long x2, final long x3) {
    if (x3 != 0) {
      return 256 - Long.numberOfLeadingZeros(x3);
    }
    if (x2 != 0) {
      return 192 - Long.numberOfLeadingZeros(x2);
    }
    if (x1 != 0) {
      return 128 - Long.numberOfLeadingZeros(x1);
    }
    return 64 - Long.numberOfLeadingZeros(x0);
  }

  // --------------------------------------------------------------------------
  // Series schedule, computed once.

  /** 2-adic valuation of the log term {@code x^k/k} when {@code v2(x) = LIFT_BITS}. */
  private static int logValuation(final int k) {
    return k * LIFT_BITS - Integer.numberOfTrailingZeros(k);
  }

  /** 2-adic valuation of the exp term {@code y^k/k!} when {@code v2(y) = LIFT_BITS}. */
  private static int expValuation(final int k) {
    return k * LIFT_BITS - (k - Integer.bitCount(k)); // v2(k!) = k - popcount(k)
  }

  /** The last k whose term valuation is below 256: every later term is 0 modulo 2^256. */
  private static int lastTerm(final IntUnaryOperator valuation) {
    int last = 0;
    for (int k = 1; k <= 256; k++) {
      if (valuation.applyAsInt(k) < 256) {
        last = k;
      }
    }
    return last;
  }

  private static int limbsFor(final int bits) {
    return Math.max(1, Math.min(4, (bits + 63) >>> 6));
  }

  private static int checkedShift(final int shift) {
    if (shift < 1 || shift > 63) {
      throw new IllegalStateException("Horner shift out of range: " + shift);
    }
    return shift;
  }

  private static void store(final long[] table, final int k, final BigInteger value) {
    for (int i = 0; i < 4; i++) {
      table[4 * k + i] = value.shiftRight(64 * i).longValue();
    }
  }
}
