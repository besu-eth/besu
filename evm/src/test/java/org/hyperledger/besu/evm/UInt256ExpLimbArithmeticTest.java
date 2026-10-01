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

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;
import java.util.Random;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

/**
 * Checks the limb arithmetic of {@link UInt256Exp} against BigInteger.
 *
 * <p>Several carries in these helpers only reach the result for rare limb values, so random EXP
 * inputs would not notice if one were dropped. The limbs here are drawn from values that force
 * carries (0, 1, 2^63, 2^64 - 1, ...).
 */
class UInt256ExpLimbArithmeticTest {

  private static final BigInteger TWO_256 = BigInteger.ONE.shiftLeft(256);
  private static final int ROUNDS = 100_000;
  private static final long[] EDGE_LIMBS = {
    0L, 1L, 2L, -1L, -2L, Long.MIN_VALUE, Long.MAX_VALUE, 0xFFFFFFFFL, 0xFFFFFFFF00000000L
  };

  private final Random random = new Random(0x11AB_5EEDL);
  private final UInt256Exp exp = new UInt256Exp();

  @Test
  void multiply() {
    for (int i = 0; i < ROUNDS; i++) {
      final UInt256 a = randomValue();
      final UInt256 b = randomValue();
      exp.multiply(a.u0(), a.u1(), a.u2(), a.u3(), b.u0(), b.u1(), b.u2(), b.u3());
      assertThat(exp.product().toBigInteger())
          .as("%s * %s", a.toHexString(), b.toHexString())
          .isEqualTo(a.toBigInteger().multiply(b.toBigInteger()).mod(TWO_256));
    }
  }

  @Test
  void multiplyTruncatedToLimbs() {
    for (int limbs = 1; limbs <= 4; limbs++) {
      final BigInteger modulus = BigInteger.ONE.shiftLeft(64 * limbs);
      for (int i = 0; i < ROUNDS; i++) {
        final UInt256 a = randomValue();
        final UInt256 b = randomValue();
        exp.multiplyLimbs(limbs, a.u0(), a.u1(), a.u2(), a.u3(), b.u0(), b.u1(), b.u2(), b.u3());
        assertThat(exp.product().toBigInteger())
            .as("%s * %s on %d limbs", a.toHexString(), b.toHexString(), limbs)
            .isEqualTo(
                a.toBigInteger().mod(modulus).multiply(b.toBigInteger().mod(modulus)).mod(modulus));
      }
    }
  }

  @Test
  void multiplySingleLimb() {
    for (int i = 0; i < ROUNDS; i++) {
      final UInt256 a = randomValue();
      final long b = randomLimb();
      exp.multiplySingleLimb(a.u0(), a.u1(), a.u2(), a.u3(), b);
      assertThat(exp.product().toBigInteger())
          .as("%s * %x", a.toHexString(), b)
          .isEqualTo(a.toBigInteger().multiply(unsigned(b)).mod(TWO_256));
    }
  }

  @Test
  void multiplySingleLimbCarryIntoTopLimb() {
    final UInt256 a = hex("fffffffffffffffb00010000000000000001000000000000fffffffffffeffff");
    exp.multiplySingleLimb(a.u0(), a.u1(), a.u2(), a.u3(), -1L);
    assertThat(exp.product())
        .isEqualTo(hex("00010000000000050000000000000000fffefffffffefffe0000000000010001"));
  }

  @Test
  void square() {
    for (int i = 0; i < ROUNDS; i++) {
      final UInt256 a = randomValue();
      exp.square(a.u0(), a.u1(), a.u2(), a.u3());
      assertThat(exp.product().toBigInteger())
          .as("%s ^ 2", a.toHexString())
          .isEqualTo(a.toBigInteger().pow(2).mod(TWO_256));
    }
  }

  @Test
  void shiftLeftAdd() {
    for (int i = 0; i < ROUNDS; i++) {
      final UInt256 t = randomValue();
      final UInt256 c = randomValue();
      final int shift = 1 + random.nextInt(63);
      assertShiftLeftAdd(t, shift, c);
    }
  }

  @Test
  void shiftLeftAddCarriesThroughMiddleLimbs() {
    assertShiftLeftAdd(
        hex("937f761aa89f4cde0000000000fffffffefffffffffffffffffffffffffffffe"),
        21,
        hex("ffffffff00000000fd9f6d84dc55f7fc00000000000000008aab53b8c3d4d7ce"));
  }

  @Test
  void shifts() {
    for (int n = 0; n < 256; n++) {
      for (int i = 0; i < 200; i++) {
        final UInt256 x = randomValue();
        exp.shiftLeft(x.u0(), x.u1(), x.u2(), x.u3(), n);
        assertThat(exp.product().toBigInteger())
            .as("%s << %d", x.toHexString(), n)
            .isEqualTo(x.toBigInteger().shiftLeft(n).mod(TWO_256));
        exp.shiftRight(x.u0(), x.u1(), x.u2(), x.u3(), n);
        assertThat(exp.product().toBigInteger())
            .as("%s >>> %d", x.toHexString(), n)
            .isEqualTo(x.toBigInteger().shiftRight(n));
      }
    }
  }

  @Test
  void negated() {
    for (int i = 0; i < ROUNDS; i++) {
      final UInt256 x = randomValue();
      assertThat(UInt256Exp.negated(x.u0(), x.u1(), x.u2(), x.u3()).toBigInteger())
          .as("-%s", x.toHexString())
          .isEqualTo(x.toBigInteger().negate().mod(TWO_256));
    }
  }

  private void assertShiftLeftAdd(final UInt256 t, final int shift, final UInt256 c) {
    exp.shiftLeft(t.u0(), t.u1(), t.u2(), t.u3(), 0); // loads t
    exp.shiftLeftAdd(shift, new long[] {c.u0(), c.u1(), c.u2(), c.u3()}, 0);
    assertThat(exp.horner().toBigInteger())
        .as("(%s << %d) + %s", t.toHexString(), shift, c.toHexString())
        .isEqualTo(t.toBigInteger().shiftLeft(shift).add(c.toBigInteger()).mod(TWO_256));
  }

  private UInt256 randomValue() {
    return new UInt256(randomLimb(), randomLimb(), randomLimb(), randomLimb());
  }

  private long randomLimb() {
    return switch (random.nextInt(4)) {
      case 0 -> EDGE_LIMBS[random.nextInt(EDGE_LIMBS.length)];
      case 1 -> random.nextLong() | 0xFFFFFFFFL; // low half all ones
      case 2 -> random.nextLong() & 0xFFFFFFFF00000000L; // low half zero
      default -> random.nextLong();
    };
  }

  private static BigInteger unsigned(final long limb) {
    return new BigInteger(Long.toUnsignedString(limb));
  }

  private static UInt256 hex(final String digits) {
    return UInt256.fromBytesBE(Bytes.fromHexString(digits).toArrayUnsafe());
  }
}
