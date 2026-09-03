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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class UInt256Test {
  static final int SAMPLE_SIZE = 1_000;

  private Bytes32 bigIntTo32B(final BigInteger y) {
    byte[] a = y.toByteArray();
    if (a.length > 32) return Bytes32.wrap(a, a.length - 32);
    return Bytes32.leftPad(Bytes.wrap(a));
  }

  private Bytes32 bigIntTo32B(final BigInteger x, final int sign) {
    if (sign >= 0) return bigIntTo32B(x);
    byte[] a = new byte[32];
    Arrays.fill(a, (byte) 0xFF);
    byte[] b = x.toByteArray();
    final int length = Math.min(32, b.length);
    System.arraycopy(b, 0, a, 32 - length, length);
    return Bytes32.leftPad(Bytes.wrap(a));
  }

  @Test
  public void fromInts() {
    UInt256 result;

    result = UInt256.fromInt(0);
    assertThat(result.isZero()).as("Int 0, isZero").isTrue();

    int[] testInts = new int[] {130, -128, 32500};
    for (int i : testInts) {
      result = UInt256.fromInt(i);
      assertThat(result.intValue()).as(String.format("Int %s value", i)).isEqualTo(i);
    }
  }

  @Test
  public void fromBytesBE() {
    byte[] input;
    UInt256 result;
    UInt256 expected;

    input = new byte[] {-128, 0, 0, 0};
    result = UInt256.fromBytesBE(input);
    expected = new UInt256(0, 0, 0, 2147483648L);
    assertThat(result).as("4b-neg-limbs").isEqualTo(expected);

    input = new byte[] {0, 0, 1, 1, 1};
    result = UInt256.fromBytesBE(input);
    expected = new UInt256(0, 0, 0, 1 + 256 + 65536);
    assertThat(result).as("3b-limbs").isEqualTo(expected);

    input = new byte[] {1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1};
    result = UInt256.fromBytesBE(input);
    expected = new UInt256(0, 0, 16777216, 1 + 256 + 65536);
    assertThat(result).as("8b-limbs").isEqualTo(expected);

    input =
        new byte[] {
          1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        };
    result = UInt256.fromBytesBE(input);
    expected = new UInt256(72057594037927936L, 0, 0, 0);
    assertThat(result).as("32b-limbs").isEqualTo(expected);

    input =
        new byte[] {
          0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0,
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        };
    result = UInt256.fromBytesBE(input);
    expected = new UInt256(257, 0, 0, 0);
    assertThat(result).as("32b-padded-limbs").isEqualTo(expected);

    Bytes inputBytes =
        Bytes.fromHexString("0x000000000000000000000000ffffffffffffffffffffffffffffffffffffffff");
    input = inputBytes.toArrayUnsafe();
    result = UInt256.fromBytesBE(input);
    expected = new UInt256(0, 4294967295L, -1L, -1L);
    assertThat(result).as("32b-case2-limbs").isEqualTo(expected);
  }

  private static Stream<Arguments> fromBytesBERangeCases() {
    return Stream.of(
        // ---- zero length ----------------------------------------------
        arguments(new byte[0], new int[] {0, 0}, UInt256.ZERO),
        arguments(new byte[0], new int[] {0, 2}, UInt256.ZERO),
        arguments(Bytes.fromHexString("0x55ab55").toArray(), new int[] {0, 0}, UInt256.ZERO),
        arguments(Bytes.fromHexString("0x55ab55").toArray(), new int[] {2, 0}, UInt256.ZERO),
        arguments(
            Bytes.fromHexString("0x010203040506070809").toArray(), new int[] {0, -1}, UInt256.ZERO),
        arguments(
            Bytes.fromHexString("0x010203040506070809").toArray(), new int[] {4, 0}, UInt256.ZERO),

        // ---- single-limb path (length < 8) ----------------------------------------------
        // 0x55 bytes sit outside the converted range and must never leak into the result
        arguments(
            Bytes.fromHexString("0x55ab55").toArray(),
            new int[] {1, 1},
            new UInt256(0, 0, 0, 0xabL)),
        arguments(
            Bytes.fromHexString("0x55ffff55").toArray(),
            new int[] {1, 2},
            new UInt256(0, 0, 0, 0xffffL)),
        // pins big-endian byte order inside the limb
        arguments(
            Bytes.fromHexString("0x550102030455").toArray(),
            new int[] {1, 4},
            new UInt256(0, 0, 0, 0x01020304L)),
        // widest single-limb range
        arguments(
            Bytes.fromHexString("0x55ffffffffffffff55").toArray(),
            new int[] {1, 7},
            new UInt256(0, 0, 0, 0x00ffffffffffffffL)),
        // high bit set in the most significant byte of the range
        arguments(
            Bytes.fromHexString("0x8000000000000055").toArray(),
            new int[] {0, 7},
            new UInt256(0, 0, 0, 0x80000000000000L)),
        // range starting on the last byte of the array
        arguments(
            Bytes.fromHexString("0x0102030405060708090a").toArray(),
            new int[] {9, 1},
            new UInt256(0, 0, 0, 0x0aL)),

        // ---- exactly one limb (length == 8), the multi-limb path boundary ---------------
        // pins big-endian byte order across the whole limb
        arguments(
            Bytes.fromHexString("0x55010203040506070855").toArray(),
            new int[] {1, 8},
            new UInt256(0, 0, 0, 0x0102030405060708L)),
        arguments(
            Bytes.fromHexString("0x55ffffffffffffffff55").toArray(),
            new int[] {1, 8},
            new UInt256(0, 0, 0, -1L)),
        // range ends exactly at bytes.length, so the 8-byte VarHandle read must not overrun
        arguments(
            Bytes.fromHexString("0x5555ffffffffffffffff").toArray(),
            new int[] {2, 8},
            new UInt256(0, 0, 0, -1L)),
        // all-zero range inside a non-zero array
        arguments(
            Bytes.fromHexString("0x55000000000000000055").toArray(),
            new int[] {1, 8},
            UInt256.ZERO),

        // ---- length 9, one byte spills into u1 -----------------------------------------
        arguments(
            Bytes.fromHexString("0x5501ffffffffffffffff55").toArray(),
            new int[] {1, 9},
            new UInt256(0, 0, 1, -1L)),
        // same range, but ending exactly at bytes.length
        arguments(
            Bytes.fromHexString("0x5501ffffffffffffffff").toArray(),
            new int[] {1, 9},
            new UInt256(0, 0, 1, -1L)),

        // ---- two and three whole limbs, and the partial limb above each --------------
        arguments(
            Bytes.fromHexString("0x550102030405060708090a0b0c0d0e0f1055").toArray(),
            new int[] {1, 16},
            new UInt256(0, 0, 0x0102030405060708L, 0x090a0b0c0d0e0f10L)),
        arguments(
            Bytes.fromHexString("0x55ff0102030405060708090a0b0c0d0e0f1055").toArray(),
            new int[] {1, 17},
            new UInt256(0, 0xff, 0x0102030405060708L, 0x090a0b0c0d0e0f10L)),
        arguments(
            Bytes.fromHexString("0x550102030405060708090a0b0c0d0e0f10111213141516171855").toArray(),
            new int[] {1, 24},
            new UInt256(0, 0x0102030405060708L, 0x090a0b0c0d0e0f10L, 0x1112131415161718L)),
        arguments(
            Bytes.fromHexString("0x55ff0102030405060708090a0b0c0d0e0f10111213141516171855")
                .toArray(),
            new int[] {1, 25},
            new UInt256(0xff, 0x0102030405060708L, 0x090a0b0c0d0e0f10L, 0x1112131415161718L)),

        // ---- full width (length == 32) ------------------------------------------------
        // four distinct limbs, pins limb ordering end to end
        arguments(
            Bytes.fromHexString(
                    "0x55550102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f205555")
                .toArray(),
            new int[] {2, 32},
            new UInt256(
                0x0102030405060708L,
                0x090a0b0c0d0e0f10L,
                0x1112131415161718L,
                0x191a1b1c1d1e1f20L)),
        // whole array is the range: offset 0 and end == bytes.length
        arguments(
            Bytes.fromHexString(
                    "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20")
                .toArray(),
            new int[] {0, 32},
            new UInt256(
                0x0102030405060708L,
                0x090a0b0c0d0e0f10L,
                0x1112131415161718L,
                0x191a1b1c1d1e1f20L)),
        arguments(
            Bytes.fromHexString(
                    "0x55ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff55")
                .toArray(),
            new int[] {1, 32},
            UInt256.MAX),
        // 32-byte range ending exactly at bytes.length
        arguments(
            Bytes.fromHexString(
                    "0x55ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
                .toArray(),
            new int[] {1, 32},
            UInt256.MAX),
        // high bit set in every limb, catches sign extension between limbs
        arguments(
            Bytes.fromHexString(
                    "0x55800000000000000080000000000000008000000000000000800000000000000055")
                .toArray(),
            new int[] {1, 32},
            new UInt256(Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE)),

        // ---- length > 32 truncates the most significant bytes -------------------------
        // the leading 0xaa is dropped, leaving 32 x 0xff
        arguments(
            Bytes.fromHexString(
                    "0xaaffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
                .toArray(),
            new int[] {0, 33},
            UInt256.MAX),
        // same, with a non-zero offset
        arguments(
            Bytes.fromHexString(
                    "0x55aaffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff55")
                .toArray(),
            new int[] {1, 33},
            UInt256.MAX),
        // 40-byte range: the leading 8 x 0xaa are dropped, leaving 32 x 0x01
        arguments(
            Bytes.fromHexString(
                    "0xaaaaaaaaaaaaaaaa0101010101010101010101010101010101010101010101010101010101010101")
                .toArray(),
            new int[] {0, 40},
            new UInt256(
                0x0101010101010101L,
                0x0101010101010101L,
                0x0101010101010101L,
                0x0101010101010101L)),
        // 36-byte range: the kept low 32 bytes straddle the 0x01 / 0xbb boundary
        arguments(
            Bytes.fromHexString(
                    "0x0101010101010101010101010101010101010101010101010101010101010101bbbbbbbb")
                .toArray(),
            new int[] {0, 36},
            new UInt256(
                0x0101010101010101L,
                0x0101010101010101L,
                0x0101010101010101L,
                0x01010101bbbbbbbbL)));
  }

  @ParameterizedTest
  @MethodSource("fromBytesBERangeCases")
  public void fromBytesBERange(final byte[] bytes, final int[] bounds, final UInt256 expected) {
    assertThat(UInt256.fromBytesBE(bytes, bounds[0], bounds[1])).isEqualTo(expected);
  }

  @Test
  public void fromToBytesBE() {
    byte[] input =
        new byte[] {
          1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
          1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
        };
    UInt256 asUint = UInt256.fromBytesBE(input);
    BigInteger asBigInt = new BigInteger(1, input);
    assertThat(asUint.toBytesBE()).isEqualTo(asBigInt.toByteArray());
  }

  @Test
  public void smallInts() {
    UInt256 number = UInt256.fromInt(523);
    UInt256 modulus = UInt256.fromInt(27);
    UInt256 remainder = number.mod(modulus);
    UInt256 expected = UInt256.fromInt(523 % 27);
    assertThat(remainder).isEqualTo(expected);
  }

  @Test
  public void smallMod() {
    byte[] num_arr =
        new byte[] {
          1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
          1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
        };
    UInt256 number = UInt256.fromBytesBE(num_arr);
    UInt256 modulus = UInt256.fromInt(27);
    int remainder = number.mod(modulus).intValue();
    BigInteger big_number = new BigInteger(1, num_arr);
    BigInteger big_modulus = BigInteger.valueOf(27L);
    int expected = big_number.mod(big_modulus).intValue();
    assertThat(remainder).isEqualTo(expected);
  }

  @Test
  public void smallModFullDividend() {
    byte[] num_arr =
        new byte[] {
          1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
          1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, -127
        };
    UInt256 number = UInt256.fromBytesBE(num_arr);
    UInt256 modulus = UInt256.fromInt(27);
    int remainder = number.mod(modulus).intValue();
    BigInteger big_number = new BigInteger(1, num_arr);
    BigInteger big_modulus = BigInteger.valueOf(27L);
    int expected = big_number.mod(big_modulus).intValue();
    assertThat(remainder).isEqualTo(expected);
  }

  @Test
  public void bigMod() {
    byte[] num_arr =
        new byte[] {
          1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
          1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
        };
    byte[] mod_arr = new byte[] {-111, 126, 78, 12};
    UInt256 number = UInt256.fromBytesBE(num_arr);
    UInt256 modulus = UInt256.fromBytesBE(mod_arr);
    Bytes32 remainder = Bytes32.leftPad(Bytes.wrap(number.mod(modulus).toBytesBE()));
    BigInteger big_number = new BigInteger(1, num_arr);
    BigInteger big_modulus = new BigInteger(1, mod_arr);
    Bytes32 expected = Bytes32.leftPad(Bytes.wrap(big_number.mod(big_modulus).toByteArray()));
    assertThat(remainder).isEqualTo(expected);
  }

  @Test
  public void bigModWithExtraCarry() {
    byte[] num_arr =
        new byte[] {
          -126, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
          1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 123
        };
    byte[] mod_arr = new byte[] {12, 126, 78, -11};
    UInt256 number = UInt256.fromBytesBE(num_arr);
    UInt256 modulus = UInt256.fromBytesBE(mod_arr);
    Bytes32 remainder = Bytes32.leftPad(Bytes.wrap(number.mod(modulus).toBytesBE()));
    BigInteger big_number = new BigInteger(1, num_arr);
    BigInteger big_modulus = new BigInteger(1, mod_arr);
    Bytes32 expected = Bytes32.leftPad(Bytes.wrap(big_number.mod(big_modulus).toByteArray()));
    assertThat(remainder).isEqualTo(expected);
  }

  @ParameterizedTest
  @MethodSource("modTestCases")
  public void mod(final String dividend, final String divisor) {
    BigInteger big_number = new BigInteger(dividend, 16);
    BigInteger big_modulus = new BigInteger(divisor, 16);
    UInt256 number = UInt256.fromBytesBE(big_number.toByteArray());
    UInt256 modulus = UInt256.fromBytesBE(big_modulus.toByteArray());
    Bytes32 remainder = Bytes32.leftPad(Bytes.wrap(number.mod(modulus).toBytesBE()));
    Bytes32 expected = Bytes32.leftPad(Bytes.wrap(big_number.mod(big_modulus).toByteArray()));
    assertThat(remainder).isEqualTo(expected);
  }

  public static Stream<Arguments> modTestCases() {
    return Stream.of(
        arguments("0000000067e36864", "001fff"),
        arguments("022b1c8c1227a00000", "038d7ea4c68000"),
        arguments("1000000000000000000000000000000000000000000000000", "ff00000000000000"),
        arguments("ff00000000000000000000000000000000", "100000000000000000000000000000000"),
        arguments("ff00000000000000000000000000000000", "100000000000000000000000000000001"),
        arguments(
            "1000000000000000000000000000000000000000000000000",
            "ff000000000000000000000000000000"),
        arguments(
            "1000000000000000000000000000000000000000000000000",
            "100000000000000000000000000000001"),
        arguments(
            "000000000000000000ff00000000000000000000000000000000000000000000",
            "0000000000000000000000000000000000fe0000000000000000000000000001"),
        arguments("020000000000000000000000000000000000", "02000000000000000000"),
        arguments("10000000000000000010000000000000000", "200000000000000ff"),
        arguments(
            "ff000000000000000000000000000000000000000000000000000000",
            "1000000000000000000000002000000000000000000000000"),
        arguments("800000000000000080", "80"),
        arguments("cea0c5cc171fa61277e5604a3bc8aef4de3d3882", "7dae7454bb193b1c28e64a6a935bc3"),
        // mulSubOverflow - addBack bugs
        // Modulus192 path (b.u3==0, b.u2!=0)
        arguments(
            "7effffff8000000000000000000000000000000000000000d900000000000001",
            "7effffff800000007effffff800000008000ff0000010000"),
        // Modulus128 path (b.u3==0, b.u2==0, b.u1!=0)
        arguments(
            "7effffff800000000000000000000000d900000000000001",
            "7effffff800000007fffffffffffffff"));
  }

  @Test
  public void modRandom() {
    final Random random = new Random(41335);
    for (int i = 0; i < SAMPLE_SIZE; i++) {
      final byte[] a = new byte[32];
      final byte[] b = new byte[32];
      random.nextBytes(a);
      random.nextBytes(b);
      BigInteger aInt = new BigInteger(1, a);
      BigInteger bInt = new BigInteger(1, b);
      int comp = aInt.compareTo(bInt);
      BigInteger big_number;
      BigInteger big_modulus;
      UInt256 number;
      UInt256 modulus;
      if (comp >= 0) {
        big_number = aInt;
        number = UInt256.fromBytesBE(a);
        big_modulus = bInt;
        modulus = UInt256.fromBytesBE(b);
      } else {
        big_number = bInt;
        number = UInt256.fromBytesBE(b);
        big_modulus = aInt;
        modulus = UInt256.fromBytesBE(a);
      }
      Bytes32 remainder = Bytes32.leftPad(Bytes.wrap(number.mod(modulus).toBytesBE()));
      Bytes32 expected =
          BigInteger.ZERO.compareTo(big_modulus) == 0
              ? Bytes32.ZERO
              : bigIntTo32B(big_number.mod(big_modulus));
      assertThat(remainder)
          .withFailMessage(
              String.format(
                  "Failure detected:\n%s.MOD(%s)\n", number.toHexString(), modulus.toHexString()))
          .isEqualTo(expected);
    }
  }

  @ParameterizedTest
  @MethodSource("addModTestCases")
  public void addMod(final String a, final String b, final String modulus) {
    BigInteger xbig = new BigInteger(a, 16);
    BigInteger ybig = new BigInteger(b, 16);
    BigInteger mbig = new BigInteger(modulus, 16);
    UInt256 x = UInt256.fromBytesBE(xbig.toByteArray());
    UInt256 y = UInt256.fromBytesBE(ybig.toByteArray());
    UInt256 m = UInt256.fromBytesBE(mbig.toByteArray());
    Bytes32 remainder = Bytes32.leftPad(Bytes.wrap(x.addMod(y, m).toBytesBE()));
    Bytes32 expected =
        BigInteger.ZERO.compareTo(mbig) == 0 ? Bytes32.ZERO : bigIntTo32B(xbig.add(ybig).mod(mbig));
    assertThat(remainder).isEqualTo(expected);
  }

  public static Stream<Arguments> addModTestCases() {
    return Stream.of(
        // reference tests
        arguments("000000010000000000000000000000000000000000000000", "0000c350", "000003e8"),
        arguments(
            "fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe",
            "fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe",
            "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        // reduceNormalised bugs
        arguments(
            "62d900c9700000000000000000023f00bc1814ff00000000000000ca22300806",
            "ffffffffffffffffb4fffff4befff4f4f4d4f4f504f4f4bef5f5100b0bf4f5f6",
            "13464637e8bdc0e53b895d7b79348a784"),
        arguments(
            "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "80008e949e9e9ec0cf4f4d4f4f4f41523410af5f20b0b7606f4d4f439f5f6000",
            "1800000000000000080000000000000017ffffffffffffffd"));
  }

  @Test
  public void addModRandom() {
    final Random random = new Random(42);
    for (int i = 0; i < SAMPLE_SIZE; i++) {
      int aSize = random.nextInt(1, 33);
      int bSize = random.nextInt(1, 33);
      int cSize = random.nextInt(1, 33);
      final byte[] aArray = new byte[aSize];
      final byte[] bArray = new byte[bSize];
      final byte[] cArray = new byte[cSize];
      random.nextBytes(aArray);
      random.nextBytes(bArray);
      random.nextBytes(cArray);
      BigInteger aInt = new BigInteger(1, aArray);
      BigInteger bInt = new BigInteger(1, bArray);
      BigInteger cInt = new BigInteger(1, cArray);
      UInt256 a = UInt256.fromBytesBE(aArray);
      UInt256 b = UInt256.fromBytesBE(bArray);
      UInt256 c = UInt256.fromBytesBE(cArray);
      Bytes32 remainder = Bytes32.leftPad(Bytes.wrap(a.addMod(b, c).toBytesBE()));
      Bytes32 expected =
          BigInteger.ZERO.compareTo(cInt) == 0
              ? Bytes32.ZERO
              : bigIntTo32B(aInt.add(bInt).mod(cInt));
      assertThat(remainder)
          .withFailMessage(
              String.format(
                  "Failure detected:\n%s.ADDMOD(%s, %s)\n",
                  a.toHexString(), b.toHexString(), c.toHexString()))
          .isEqualTo(expected);
    }
  }

  @ParameterizedTest
  @MethodSource("mulModTestCases")
  public void mulMod(final String a, final String b, final String modulus) {
    Bytes aBytes = Bytes.fromHexString(a);
    Bytes bBytes = Bytes.fromHexString(b);
    Bytes modBytes = Bytes.fromHexString(modulus);
    BigInteger aInt = new BigInteger(1, aBytes.toArrayUnsafe());
    BigInteger bInt = new BigInteger(1, bBytes.toArrayUnsafe());
    BigInteger mInt = new BigInteger(1, modBytes.toArrayUnsafe());
    UInt256 x = UInt256.fromBytesBE(aBytes.toArrayUnsafe());
    UInt256 y = UInt256.fromBytesBE(bBytes.toArrayUnsafe());
    UInt256 m = UInt256.fromBytesBE(modBytes.toArrayUnsafe());
    Bytes32 remainder = Bytes32.leftPad(Bytes.wrap(x.mulMod(y, m).toBytesBE()));
    Bytes32 expected = bigIntTo32B(aInt.multiply(bInt).mod(mInt));
    assertThat(remainder).isEqualTo(expected);
  }

  public static Stream<Arguments> mulModTestCases() {
    return Stream.of(
        // reference tests
        arguments(
            "0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe",
            "0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe",
            "0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe"),
        arguments(
            "0x000000000000000000000000ffffffffffffffffffffffffffffffffffffffff",
            "0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe",
            "0x000000000000000000000000ffffffffffffffffffffffffffffffffffffffff"),
        arguments(
            "0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe",
            "0xffffffffffffffffffffffffb195148ca348dc57a7331852b390ccefa7b0c18b",
            "0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe"),
        // mulSubOverflow bugs
        arguments(
            "0x0000000000000001000000000000000000000000000000000000000000000001",
            "0x0000000000000001000000000000000000000000000000000000000000000000",
            "0x0000000000000001000000000000000000000000000000000000000000000000"),
        // mulSubOverflow - addBack bugs
        // Modulus256 path (b.u3!=0) via mulMod
        arguments(
            "0x7effffff8000000000000000000000000000000000000000d900000000000001",
            "0x010000000000000000",
            "0x7effffff800000007effffff800000008000ff00000100007effffff80000000"),
        // UInt128 M-R path: modReduceNormalisedSlowPath(UInt576) branch coverage
        // (128-bit modulus, at least one operand > 128 bits triggers multiply-then-reduce)
        // Branch 5 (else, 3 reduceSteps): product ~258 bits
        arguments(
            "0x0100000000000000010000000000000001",
            "0x0100000000000000010000000000000001",
            "0x80000000000000000000000000000001"),
        // Branch 4 (4 reduceSteps): product ~321 bits
        arguments(
            "0x01000000000000000000000000000000010000000000000001",
            "0x0100000000000000010000000000000001",
            "0x80000000000000000000000000000001"),
        // Branch 3 (5 reduceSteps): product ~384 bits
        arguments(
            "0x8000000000000000000000000000000000000000000000010000000000000001",
            "0x0100000000000000010000000000000001",
            "0x80000000000000000000000000000001"),
        // Branch 2 (6 reduceSteps): product ~448 bits
        arguments(
            "0x8000000000000000000000000000000000000000000000010000000000000001",
            "0x01000000000000000000000000000000010000000000000001",
            "0x80000000000000000000000000000001"),
        // Branch 1 via v.u7 >= u1 (7 reduceSteps): product ~512 bits, shift=0
        arguments(
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "0x80000000000000000000000000000001"),
        // Branch 1 via v.u8 != 0 (7 reduceSteps): shifted product > 512 bits, shift=1
        arguments(
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "0x40000000000000000000000000000001"),
        // UInt128 mulSubOverflow through M-R: triggers v2 == u1 in reduceStep
        arguments(
            "0x7effffff80000000000000000000000000000000000000000000000000000001",
            "0x01000000000000000000000000000000000000000000000001",
            "0x7effffff800000007fffffffffffffff"),
        // Noisy: 256-bit x 64-bit operands, 128-bit modulus
        arguments(
            "0xdeadbeefcafebabedeadbeefcafebabedeadbeefcafebabedeadbeefcafebabe",
            "0xcafebabedeadbeef",
            "0x80000000000000000000000000000001"),
        // UInt192 M-R path: modReduceNormalisedSlowPath(UInt576) branch coverage
        // (192-bit modulus, at least one operand > 192 bits triggers multiply-then-reduce)
        // Branch 4 (else, 3 reduceSteps): product ~321 bits
        arguments(
            "0x01000000000000000000000000000000010000000000000001",
            "0x0100000000000000010000000000000001",
            "0x800000000000000000000000000000000000000000000001"),
        // Branch 3 (4 reduceSteps): product ~384 bits
        arguments(
            "0x8000000000000000000000000000000000000000000000010000000000000001",
            "0x0100000000000000010000000000000001",
            "0x800000000000000000000000000000000000000000000001"),
        // Branch 2 (5 reduceSteps): product ~448 bits
        arguments(
            "0x8000000000000000000000000000000000000000000000010000000000000001",
            "0x01000000000000000000000000000000010000000000000001",
            "0x800000000000000000000000000000000000000000000001"),
        // Branch 1 via v.u7 >= u2 (6 reduceSteps): product ~512 bits, shift=0
        arguments(
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "0x800000000000000000000000000000000000000000000001"),
        // Branch 1 via v.u8 != 0 (6 reduceSteps): shifted product > 512 bits, shift=1
        arguments(
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "0x400000000000000000000000000000000000000000000001"),
        // UInt192 mulSubOverflow through M-R: triggers v3 == u2 in reduceStep
        arguments(
            "0x8200000000000000000000000000000000000000000000000000000000000001",
            "0x01000000000000000000000000000000000000000000000001",
            "0x8200000000000000fe000004000000ffff000000fffff700"),
        // Noisy: 256-bit x 128-bit operands, 192-bit modulus
        arguments(
            "0xdeadbeefcafebabedeadbeefcafebabedeadbeefcafebabedeadbeefcafebabe",
            "0xcafebabedeadbeefcafebabedeadbeef",
            "0x800000000000000000000000000000000000000000000001"));
  }

  @Test
  public void mulModRandom() {
    final Random random = new Random(123);
    for (int i = 0; i < SAMPLE_SIZE; i++) {
      int aSize = random.nextInt(1, 33);
      int bSize = random.nextInt(1, 33);
      int cSize = random.nextInt(1, 33);
      final byte[] aArray = new byte[aSize];
      final byte[] bArray = new byte[bSize];
      final byte[] cArray = new byte[cSize];
      random.nextBytes(aArray);
      random.nextBytes(bArray);
      random.nextBytes(cArray);
      BigInteger aInt = new BigInteger(1, aArray);
      BigInteger bInt = new BigInteger(1, bArray);
      BigInteger cInt = new BigInteger(1, cArray);
      UInt256 a = UInt256.fromBytesBE(aArray);
      UInt256 b = UInt256.fromBytesBE(bArray);
      UInt256 c = UInt256.fromBytesBE(cArray);
      Bytes32 remainder = Bytes32.leftPad(Bytes.wrap(a.mulMod(b, c).toBytesBE()));
      Bytes32 expected =
          BigInteger.ZERO.compareTo(cInt) == 0
              ? Bytes32.ZERO
              : bigIntTo32B(aInt.multiply(bInt).mod(cInt));
      assertThat(remainder)
          .withFailMessage(
              String.format(
                  "Failure detected:\n%s.MULMOD(%s, %s)\n",
                  a.toHexString(), b.toHexString(), c.toHexString()))
          .isEqualTo(expected);
    }
  }

  @Test
  public void mulModRandomWideMR() {
    // Targeted random test for the M-R (multiply-then-reduce) path in UInt128 and UInt192.
    // Generates 128-bit or 192-bit moduli with a 256-bit first operand (always exceeds modulus
    // width) and a varying-width second operand to exercise different branches of
    // modReduceNormalisedSlowPath(UInt576).
    final Random random = new Random(456789);
    for (int i = 0; i < SAMPLE_SIZE; i++) {
      // Generate modulus of 128-bit or 192-bit width
      int modWidth = random.nextBoolean() ? 16 : 24;
      final byte[] modArray = new byte[modWidth];
      random.nextBytes(modArray);
      modArray[0] |= (byte) 0x80; // ensure exact width (MSB set)

      // First operand: always 256 bits (exceeds both 128 and 192 modulus width)
      final byte[] aArray = new byte[32];
      random.nextBytes(aArray);
      aArray[0] |= (byte) 0x80; // ensure 256 bits

      // Second operand: varying width to hit different slow-path branches
      int bWidth = random.nextInt(1, 33);
      final byte[] bArray = new byte[bWidth];
      random.nextBytes(bArray);

      BigInteger aInt = new BigInteger(1, aArray);
      BigInteger bInt = new BigInteger(1, bArray);
      BigInteger mInt = new BigInteger(1, modArray);
      UInt256 a = UInt256.fromBytesBE(aArray);
      UInt256 b = UInt256.fromBytesBE(bArray);
      UInt256 m = UInt256.fromBytesBE(modArray);
      Bytes32 remainder = Bytes32.leftPad(Bytes.wrap(a.mulMod(b, m).toBytesBE()));
      Bytes32 expected =
          BigInteger.ZERO.compareTo(mInt) == 0
              ? Bytes32.ZERO
              : bigIntTo32B(aInt.multiply(bInt).mod(mInt));
      assertThat(remainder)
          .withFailMessage(
              String.format(
                  "Failure detected:\n%s.MULMOD(%s, %s)\n",
                  a.toHexString(), b.toHexString(), m.toHexString()))
          .isEqualTo(expected);
    }
  }

  @Test
  public void signedModRandom() {
    final Random random = new Random(432);
    for (int i = 0; i < SAMPLE_SIZE; i++) {
      int aSize = random.nextInt(1, 33);
      int bSize = random.nextInt(1, 33);
      boolean neg = random.nextBoolean();
      byte[] aArray = new byte[aSize];
      byte[] bArray = new byte[bSize];
      random.nextBytes(aArray);
      random.nextBytes(bArray);

      aArray = negate(aArray, random.nextBoolean());
      bArray = negate(bArray, random.nextBoolean());

      if ((aSize < 32) && (neg)) {
        byte[] tmp = new byte[32];
        Arrays.fill(tmp, (byte) 0xFF);
        System.arraycopy(aArray, 0, tmp, 32 - aArray.length, aArray.length);
        aArray = tmp;
      }
      UInt256 a = UInt256.fromBytesBE(aArray);
      UInt256 b = UInt256.fromBytesBE(bArray);
      UInt256 r = a.signedMod(b);
      BigInteger aInt = a.isNegative() ? new BigInteger(aArray) : new BigInteger(1, aArray);
      BigInteger bInt = b.isNegative() ? new BigInteger(bArray) : new BigInteger(1, bArray);
      Bytes32 remainder = Bytes32.leftPad(Bytes.wrap(r.toBytesBE()));
      Bytes32 expected;
      BigInteger rem;
      if (BigInteger.ZERO.compareTo(bInt) == 0) expected = Bytes32.ZERO;
      else {
        rem = aInt.abs().mod(bInt.abs());
        if ((aInt.compareTo(BigInteger.ZERO) < 0) && (rem.compareTo(BigInteger.ZERO) != 0)) {
          rem = rem.negate();
          expected = bigIntTo32B(rem, -1);
        } else {
          expected = bigIntTo32B(rem, 1);
        }
      }
      assertThat(remainder)
          .withFailMessage(
              String.format("Failure detected:\n%s.SMOD(%s)\n", a.toHexString(), b.toHexString()))
          .isEqualTo(expected);
    }
  }

  @Test
  public void divRandom() {
    final Random random = new Random(45532);
    for (int i = 0; i < SAMPLE_SIZE; i++) {
      int aSize = random.nextInt(1, 33);
      int bSize = random.nextInt(1, 33);
      byte[] aArray = new byte[aSize];
      byte[] bArray = new byte[bSize];
      random.nextBytes(aArray);
      random.nextBytes(bArray);
      UInt256 a = UInt256.fromBytesBE(aArray);
      UInt256 b = UInt256.fromBytesBE(bArray);
      UInt256 q = a.div(b);
      BigInteger aInt = new BigInteger(1, aArray);
      BigInteger bInt = new BigInteger(1, bArray);
      Bytes32 qBytes = Bytes32.leftPad(Bytes.wrap(q.toBytesBE()));
      Bytes32 expected = Bytes32.ZERO;
      if (BigInteger.ZERO.compareTo(bInt) != 0) {
        BigInteger quotient = aInt.divide(bInt);
        expected = bigIntTo32B(quotient, 1);
      }
      assertThat(qBytes)
          .withFailMessage(
              String.format("Failure detected:\n%s.DIV(%s)\n", a.toHexString(), b.toHexString()))
          .isEqualTo(expected);
    }
  }

  @Test
  public void signedDivRandom() {
    final Random random = new Random(957467);
    for (int i = 0; i < SAMPLE_SIZE; i++) {
      int aSize = random.nextInt(1, 33);
      int bSize = random.nextInt(1, 33);
      byte[] aArray = new byte[aSize];
      byte[] bArray = new byte[bSize];
      random.nextBytes(aArray);
      random.nextBytes(bArray);

      aArray = negate(aArray, random.nextBoolean());
      bArray = negate(bArray, random.nextBoolean());

      UInt256 a = UInt256.fromBytesBE(aArray);
      UInt256 b = UInt256.fromBytesBE(bArray);
      UInt256 q = a.signedDiv(b);
      BigInteger aInt = a.isNegative() ? new BigInteger(aArray) : new BigInteger(1, aArray);
      BigInteger bInt = b.isNegative() ? new BigInteger(bArray) : new BigInteger(1, bArray);
      Bytes32 qBytes = Bytes32.leftPad(Bytes.wrap(q.toBytesBE()));
      Bytes32 expected = Bytes32.ZERO;
      if (BigInteger.ZERO.compareTo(bInt) != 0) {
        BigInteger quotient = aInt.divide(bInt);
        expected = bigIntTo32B(quotient, quotient.signum());
      }
      assertThat(qBytes)
          .withFailMessage(
              String.format("Failure detected:\n%s.SDIV(%s)\n", a.toHexString(), b.toHexString()))
          .isEqualTo(expected);
    }
  }

  private static byte[] negate(final byte[] array, final boolean negate) {
    if (!negate || array.length >= 32) {
      return array;
    }
    byte[] tmp = new byte[32];
    Arrays.fill(tmp, (byte) 0xFF);
    System.arraycopy(array, 0, tmp, 32 - array.length, array.length);
    return tmp;
  }

  @ParameterizedTest
  @MethodSource("testCases")
  void div_sdiv(final String numerator, final String denominator, final int sign) {
    byte[] aArray = Bytes32.leftPad(Bytes.fromHexString(numerator)).toArray();
    byte[] bArray = Bytes32.leftPad(Bytes.fromHexString(denominator)).toArray();
    final UInt256 a = UInt256.fromBytesBE(aArray);
    final UInt256 b = UInt256.fromBytesBE(bArray);

    BigInteger aInt = sign < 0 ? new BigInteger(aArray) : new BigInteger(1, aArray);
    BigInteger bInt = sign < 0 ? new BigInteger(bArray) : new BigInteger(1, bArray);

    final Bytes32 qBytes =
        sign < 0
            ? Bytes32.leftPad(Bytes.wrap(a.signedDiv(b).toBytesBE()))
            : Bytes32.leftPad(Bytes.wrap(a.div(b).toBytesBE()));

    Bytes32 expected = Bytes32.ZERO;
    if (BigInteger.ZERO.compareTo(bInt) != 0) {
      BigInteger quotient = aInt.divide(bInt);
      expected = bigIntTo32B(quotient, quotient.signum());
    }
    assertThat(qBytes).isEqualTo(expected);
  }

  static Collection<Object[]> testCases() {
    return Arrays.stream(
            new Object[][] {
              {"0x00", "0x01"},
              {"0x50", "0x21"},
              {
                "0x120d7a733f5016ad9fae51cb9896e15a96147719fe0379d0cb2642a6951e0a5c",
                "0x007cdab49aba612fb02bd738a74c76789bc9a911c90296502a35df43e939e6e2"
              },
              {"0xa7f576de3a6c", "0xfffffffffef1c296a4c6"},
              {"0xffffffffffffffffffffffff6bacfb1469f9a4d5674a85b75f951d72d7a58e4a", "0x020000"},
              {"0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "0x01"},
              {"0x01", "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
              {
                "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
                "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
              },
              {
                "0x8000000000000000000000000000000000000000000000000000000000000000",
                "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
              },
              {"0x1598209296af93c13b2f5fde7d8e99", "0x09244c1368"},
              {
                "0xfffffffffffffff9309d38241af6a2545b52958d000000000000000000000000",
                "0xb17217f7d1cf79abc9e3b398"
              },
              {"0xa7f576de3a6c", "0xa7f576de3a6c"},
              {"0x9c2c35e6c180771cda86cde561fe7609b9e89e8e5b", "0x993951396a774e675e93bea2e77c"},
              {
                "0xa73fc792edbfb1038115f77a37613b8f5b64837e28768c9dd90828",
                "0x0700b2d7adda7612da7f95"
              },
              {"0xbf1256135bb3f72de074d0f237", "0x8b63235ac1765530"},
              {"0x5b35862b0027a502b1d4cbc4a09e25", "0x932542f4003763"},
              {
                // Multiply and subtract overflows and we need to decrement quotient estimation -
                // UInt192 case
                "0x8200000000000000000000000000000000000000000000000000000000000000",
                "0x8200000000000000fe000004000000ffff000000fffff700"
              },
              {
                // Multiply and subtract overflows and we need to decrement quotient estimation -
                // UInt128 case
                "0x820000000000000000000000000000000000000000000000",
                "0x8200000000000000fe00000000000001"
              },
            })
        .flatMap(
            inputs ->
                IntStream.of(-1, 1)
                    .mapToObj(
                        sign -> {
                          Object[] newInputs = Arrays.copyOf(inputs, inputs.length + 1);
                          newInputs[inputs.length] = sign;
                          return newInputs;
                        }))
        .toList();
  }

  // Shift amounts covering all word-shift branches (0-3) with intra-word shifts of
  // 0, 1 and 63, plus the 0 and 256 special cases.
  private static final int[] SHIFT_AMOUNTS = {
    0, 1, 7, 8, 31, 32, 63, 64, 65, 100, 127, 128, 129, 191, 192, 193, 255, 256
  };

  private static final BigInteger TWO_POW_256 = BigInteger.ONE.shiftLeft(256);

  private static Stream<Arguments> shiftTestCases() {
    String[] values = {
      "0x00",
      "0x01",
      "0xdeadbeef",
      "0x8000000000000000",
      "0xffffffffffffffff",
      "0x010000000000000000",
      "0x80000000000000000000000000000000",
      "0xdeadbeefcafebabedeadbeefcafebabe",
      "0x0100000000000000000000000000000000",
      "0x800000000000000000000000000000000000000000000000",
      "0x01000000000000000000000000000000000000000000000000",
      "0x8000000000000000000000000000000000000000000000000000000000000000",
      "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
      "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
    };
    return Arrays.stream(values)
        .flatMap(v -> IntStream.of(SHIFT_AMOUNTS).mapToObj(s -> arguments(v, s)));
  }

  @ParameterizedTest
  @MethodSource("shiftTestCases")
  void shiftLeft(final String value, final int shift) {
    byte[] array = Bytes32.leftPad(Bytes.fromHexString(value)).toArray();
    UInt256 x = UInt256.fromBytesBE(array);
    BigInteger xInt = new BigInteger(1, array);
    Bytes32 result = Bytes32.leftPad(Bytes.wrap(x.shiftLeft(shift).toBytesBE()));
    Bytes32 expected = bigIntTo32B(xInt.shiftLeft(shift).mod(TWO_POW_256));
    assertThat(result)
        .withFailMessage(String.format("Failure detected:\n%s.SHL(%d)\n", x.toHexString(), shift))
        .isEqualTo(expected);
  }

  @ParameterizedTest
  @MethodSource("shiftTestCases")
  void shiftRight(final String value, final int shift) {
    byte[] array = Bytes32.leftPad(Bytes.fromHexString(value)).toArray();
    UInt256 x = UInt256.fromBytesBE(array);
    BigInteger xInt = new BigInteger(1, array);
    Bytes32 result = Bytes32.leftPad(Bytes.wrap(x.shiftRight(shift).toBytesBE()));
    Bytes32 expected = bigIntTo32B(xInt.shiftRight(shift));
    assertThat(result)
        .withFailMessage(String.format("Failure detected:\n%s.SHR(%d)\n", x.toHexString(), shift))
        .isEqualTo(expected);
  }

  private static Stream<Arguments> selectedShiftAmounts() {
    return Arrays.stream(
            new int[] {0, 1, 7, 8, 31, 32, 63, 64, 65, 100, 127, 128, 129, 191, 192, 193})
        .mapToObj(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("selectedShiftAmounts")
  void shiftRoundTrip(final int shift) {
    // For a value with only low bits set, shifting left then right by the same amount
    // must be lossless.
    UInt256 x = UInt256.fromLong(0xdeadbeefL);
    assertThat(x.shiftLeft(shift).shiftRight(shift))
        .withFailMessage(String.format("Round trip failed for shift %d", shift))
        .isEqualTo(x);
  }

  @Test
  void compare() {
    assertThat(UInt256.compare(null, UInt256.ONE)).isEqualTo(-1);
    assertThat(UInt256.compare(null, null)).isEqualTo(0);
    assertThat(UInt256.compare(UInt256.ONE, null)).isEqualTo(1);
    assertThat(UInt256.compare(UInt256.ZERO, UInt256.ZERO)).isEqualTo(0);
    assertThat(UInt256.compare(UInt256.ONE, UInt256.ZERO)).isEqualTo(1);
    assertThat(UInt256.compare(UInt256.ZERO, UInt256.ONE)).isEqualTo(-1);
  }
}
