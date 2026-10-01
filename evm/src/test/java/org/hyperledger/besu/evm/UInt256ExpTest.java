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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Checks {@link UInt256#exp(UInt256)} against {@code BigInteger.modPow(exponent, 2^256)}. */
class UInt256ExpTest {

  private static final BigInteger TWO_256 = BigInteger.ONE.shiftLeft(256);
  private static final BigInteger MAX = TWO_256.subtract(BigInteger.ONE);

  /** Bases of tests/benchmark/compute/instruction/test_arithmetic.py::test_exp_bench_arithmetic. */
  private static final long[] BENCHMARK_VALUES = {3, 5, 7, 11, 13, 136279841};

  private final Random random = new Random(0x5EED_E4B0L);

  @Test
  void zeroExponentGivesOne() {
    for (final BigInteger base :
        new BigInteger[] {BigInteger.ZERO, BigInteger.ONE, BigInteger.TWO, BigInteger.TEN, MAX}) {
      assertThat(exp(base, BigInteger.ZERO)).isEqualTo(BigInteger.ONE);
    }
  }

  @Test
  void zeroAndOneBases() {
    for (final BigInteger exponent : exponentsOfEveryLength()) {
      assertExp(BigInteger.ZERO, exponent);
      assertExp(BigInteger.ONE, exponent);
    }
  }

  @Test
  void minusOne() {
    assertThat(exp(MAX, MAX)).isEqualTo(MAX);
    assertThat(exp(MAX, MAX.subtract(BigInteger.ONE))).isEqualTo(BigInteger.ONE);
    for (final BigInteger exponent : exponentsOfEveryLength()) {
      assertExp(MAX, exponent);
    }
  }

  @Test
  void powersOfTwo() {
    for (int t = 1; t < 256; t++) {
      final BigInteger base = BigInteger.ONE.shiftLeft(t);
      for (int e = 0; e <= 300; e++) {
        assertExp(base, BigInteger.valueOf(e));
      }
      assertExp(base, MAX);
    }
  }

  @Test
  void evenBasesOfEveryValuation() {
    for (int t = 1; t < 256; t++) {
      final BigInteger odd = new BigInteger(256 - t, random).setBit(0);
      for (final BigInteger m : new BigInteger[] {BigInteger.valueOf(3), odd}) {
        final BigInteger base = m.shiftLeft(t).mod(TWO_256);
        for (int e = 2; e <= 9; e++) {
          assertExp(base, BigInteger.valueOf(e));
        }
        assertExp(base, BigInteger.valueOf(255 / t));
        assertExp(base, BigInteger.valueOf(255 / t + 1));
        assertExp(base, BigInteger.valueOf(256));
        assertExp(base, MAX);
      }
    }
  }

  /**
   * Odd bases {@code ±1 + 2^k·m}: below the lift target the base is squared before the series,
   * above it the series run on the base directly.
   */
  @Test
  void oddBasesCloseToPlusOrMinusOne() {
    for (int k = 1; k < 256; k++) {
      final BigInteger offset = new BigInteger(256 - k, random).setBit(0).shiftLeft(k);
      for (final BigInteger base :
          new BigInteger[] {
            BigInteger.ONE.add(offset).mod(TWO_256),
            MAX.add(offset).mod(TWO_256),
            BigInteger.ONE.add(BigInteger.ONE.shiftLeft(k)).mod(TWO_256),
            MAX.add(BigInteger.ONE.shiftLeft(k)).mod(TWO_256)
          }) {
        for (int bits = 1; bits <= 256; bits += 5) {
          assertExp(base, randomOfLength(bits));
        }
        assertExp(base, MAX);
      }
    }
  }

  @Test
  void everyExponentLength() {
    final BigInteger[] bases = {
      BigInteger.valueOf(3),
      BigInteger.valueOf(5),
      BigInteger.TEN,
      new BigInteger(256, random).setBit(0),
      new BigInteger(256, random).clearBit(0),
      MAX.subtract(BigInteger.TWO)
    };
    for (final BigInteger base : bases) {
      for (final BigInteger exponent : exponentsOfEveryLength()) {
        assertExp(base, exponent);
      }
    }
  }

  /**
   * The execution-specs EXP benchmark loops on {@code DUP2 EXP}: each EXP raises the same base to
   * the previous result, so after one or two steps the exponent is a full 256-bit odd number.
   */
  @Test
  void executionSpecsBenchmarkChains() {
    for (final long base : BENCHMARK_VALUES) {
      for (final long start : BENCHMARK_VALUES) {
        final UInt256 b = UInt256.fromLong(base);
        UInt256 exponent = UInt256.fromLong(start);
        BigInteger expected = BigInteger.valueOf(start);
        for (int step = 0; step < 64; step++) {
          exponent = b.exp(exponent);
          expected = BigInteger.valueOf(base).modPow(expected, TWO_256);
          assertThat(exponent.toBigInteger())
              .as("base %d, step %d", base, step)
              .isEqualTo(expected);
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 8, 24, 25, 64, 128, 192, 256})
  void randomInputs(final int exponentBits) {
    for (int i = 0; i < 2_000; i++) {
      final BigInteger base = new BigInteger(1 + random.nextInt(256), random);
      assertExp(base, randomOfLength(exponentBits));
      assertExp(base.setBit(0), randomOfLength(exponentBits));
    }
  }

  private static BigInteger exp(final BigInteger base, final BigInteger exponent) {
    return toUInt256(base).exp(toUInt256(exponent)).toBigInteger();
  }

  private static void assertExp(final BigInteger base, final BigInteger exponent) {
    assertThat(exp(base, exponent))
        .as("0x%s ^ 0x%s", base.toString(16), exponent.toString(16))
        .isEqualTo(base.modPow(exponent, TWO_256));
  }

  private static UInt256 toUInt256(final BigInteger value) {
    final byte[] bytes = value.toByteArray();
    final int length = Math.min(bytes.length, 32);
    final byte[] be = new byte[length];
    System.arraycopy(bytes, bytes.length - length, be, 0, length);
    return UInt256.fromBytesBE(be);
  }

  /** A random exponent of exactly {@code bits} bits. */
  private BigInteger randomOfLength(final int bits) {
    return new BigInteger(bits - 1, random).setBit(bits - 1);
  }

  /** For every bit length: the smallest, the largest and a random exponent of that length. */
  private BigInteger[] exponentsOfEveryLength() {
    final BigInteger[] exponents = new BigInteger[3 * 256];
    for (int bits = 1; bits <= 256; bits++) {
      exponents[3 * (bits - 1)] = BigInteger.ONE.shiftLeft(bits - 1);
      exponents[3 * (bits - 1) + 1] = BigInteger.ONE.shiftLeft(bits).subtract(BigInteger.ONE);
      exponents[3 * (bits - 1) + 2] = randomOfLength(bits);
    }
    return exponents;
  }
}
