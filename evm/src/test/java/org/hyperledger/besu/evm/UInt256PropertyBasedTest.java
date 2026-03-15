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
import java.util.Arrays;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Assume;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tuple;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

public class UInt256PropertyBasedTest {
  private static final BigInteger TWO_256 = BigInteger.ONE.shiftLeft(256);

  // region Test Data Providers
  // --------------------------------------------------------------------------

  @Provide
  Arbitrary<byte[]> unsigned1to32() {
    return Arbitraries.bytes()
        .array(byte[].class)
        .ofMinSize(1)
        .ofMaxSize(32)
        .map(UInt256PropertyBasedTest::clampUnsigned32);
  }

  @Provide
  Arbitrary<byte[]> unsigned0to64() {
    return Arbitraries.bytes()
        .array(byte[].class)
        .ofMinSize(0)
        .ofMaxSize(64)
        .map(UInt256PropertyBasedTest::clampUnsigned32);
  }

  @Provide
  Arbitrary<byte[]> singleLimbUnsigned1to4() {
    return Arbitraries.bytes()
        .array(byte[].class)
        .ofMinSize(1)
        .ofMaxSize(4)
        .map(UInt256PropertyBasedTest::clampUnsigned32);
  }

  @Provide
  Arbitrary<Integer> shifts() {
    return Arbitraries.integers().between(-512, 512);
  }

  // --------------------------------------------------------------------------
  // endregion

  // region Shaped Generators (from record-refactor regression suite)
  // --------------------------------------------------------------------------

  @Provide
  Arbitrary<byte[]> bytes0to64_shaped() {
    final Arbitrary<Integer> requiredLengths =
        Arbitraries.of(0, 1, 7, 8, 9, 15, 16, 17, 31, 32, 33, 64);

    final Arbitrary<Integer> lengths =
        Arbitraries.frequencyOf(
            Tuple.of(8, requiredLengths), Tuple.of(2, Arbitraries.integers().between(0, 64)));

    final Arbitrary<Pattern> patterns =
        Arbitraries.frequencyOf(
            Tuple.of(4, Arbitraries.of(Pattern.ALL_ZERO)),
            Tuple.of(4, Arbitraries.of(Pattern.ALL_FF)),
            Tuple.of(4, Arbitraries.of(Pattern.LIMB_SIGN_BITS)),
            Tuple.of(10, Arbitraries.of(Pattern.RANDOM)));

    return lengths.flatMap(
        len ->
            patterns.flatMap(
                pat -> {
                  if (len == 0) {
                    return Arbitraries.of(new byte[0]);
                  }
                  return Arbitraries.bytes()
                      .array(byte[].class)
                      .ofSize(len)
                      .map(b -> applyPattern(b, pat));
                }));
  }

  @Provide
  Arbitrary<byte[]> bytes32_shaped() {
    return bytes0to64_shaped().map(UInt256PropertyBasedTest::toBytes32Unsigned);
  }

  @Provide
  Arbitrary<byte[]> bytes33to64_shaped() {
    return bytes0to64_shaped()
        .filter(b -> b.length >= 33 && b.length <= 64)
        .map(UInt256PropertyBasedTest::forceNonZeroHighBytes);
  }

  @Provide
  Arbitrary<Integer> shifts_extreme() {
    final Arbitrary<Integer> edges =
        Arbitraries.of(
            -512, -257, -256, -129, -128, -65, -64, -1, 0, 1, 63, 64, 65, 127, 128, 129, 255, 256,
            257, 512);
    return Arbitraries.frequencyOf(
        Tuple.of(7, edges), Tuple.of(3, Arbitraries.integers().between(-512, 512)));
  }

  private enum Pattern {
    ALL_ZERO,
    ALL_FF,
    LIMB_SIGN_BITS,
    RANDOM
  }

  private static byte[] applyPattern(final byte[] bytes, final Pattern pat) {
    switch (pat) {
      case ALL_ZERO:
        Arrays.fill(bytes, (byte) 0x00);
        return bytes;
      case ALL_FF:
        Arrays.fill(bytes, (byte) 0xFF);
        return bytes;
      case LIMB_SIGN_BITS:
        Arrays.fill(bytes, (byte) 0x00);
        forceMsbAtIndexIfPresent(bytes, 0);
        forceMsbAtIndexIfPresent(bytes, 8);
        forceMsbAtIndexIfPresent(bytes, 16);
        forceMsbAtIndexIfPresent(bytes, 24);
        forceMsbAtIndexIfPresent(bytes, bytes.length - 1);
        return bytes;
      case RANDOM:
      default:
        return bytes;
    }
  }

  private static void forceMsbAtIndexIfPresent(final byte[] bytes, final int index) {
    if (index < 0 || index >= bytes.length) {
      return;
    }
    bytes[index] = (byte) (bytes[index] | 0x80);
  }

  // --------------------------------------------------------------------------
  // endregion

  // region Serialization Tests
  // --------------------------------------------------------------------------

  @Property
  void property_roundTripUnsigned_toFromBytesBE(@ForAll("unsigned0to64") final byte[] any) {
    // Arrange
    final byte[] be = clampUnsigned32(any);

    // Act
    final UInt256 u = UInt256.fromBytesBE(be);
    final byte[] back = u.toBytesBE();

    // Assert
    assertThat(back).hasSize(32);
    byte[] expected = bigUnsignedToBytes32(toBigUnsigned(be));
    assertThat(back).containsExactly(expected);
  }

  // --------------------------------------------------------------------------
  // endregion

  // region Comparison Tests
  // --------------------------------------------------------------------------

  @Property
  void property_equals_compare_consistent(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] b) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);

    // Act
    final int cmp = UInt256.compare(ua, ub);
    final boolean eq = ua.equals(ub);

    // Assert
    assertThat(cmp == 0).isEqualTo(eq);

    BigInteger ba = toBigUnsigned(a);
    BigInteger bb = toBigUnsigned(b);
    int bc = ba.compareTo(bb);
    assertThat(Integer.signum(cmp)).isEqualTo(Integer.signum(bc));
  }

  // --------------------------------------------------------------------------
  // endregion

  // region Modulo Tests (MOD/SMOD/ADDMOD/MULMOD)
  // --------------------------------------------------------------------------
  @Property
  void property_mod_matchesBigInteger(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] m) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 um = UInt256.fromBytesBE(m);

    // Act
    final byte[] got = ua.mod(um).toBytesBE();

    // Assert
    BigInteger A = toBigUnsigned(a);
    BigInteger M = toBigUnsigned(m);
    byte[] exp = (M.signum() == 0) ? Bytes32.ZERO.toArrayUnsafe() : bigUnsignedToBytes32(A.mod(M));
    assertThat(got).containsExactly(exp);
  }

  @Property
  void property_mod_singleLimb_matchesBigInteger(
      @ForAll("singleLimbUnsigned1to4") final byte[] a,
      @ForAll("singleLimbUnsigned1to4") final byte[] m) {

    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 um = UInt256.fromBytesBE(m);

    // Act
    final byte[] got = ua.mod(um).toBytesBE();

    // Assert
    BigInteger A = toBigUnsigned(a);
    BigInteger M = toBigUnsigned(m);
    byte[] exp = (M.signum() == 0) ? Bytes32.ZERO.toArrayUnsafe() : bigUnsignedToBytes32(A.mod(M));
    assertThat(got).containsExactly(exp);
  }

  @Property
  void property_signedMod_matchesEvmSemantics(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] m) {

    // Arrange
    final byte[] a32 = Bytes32.leftPad(Bytes.wrap(a)).toArrayUnsafe();
    final byte[] m32 = Bytes32.leftPad(Bytes.wrap(m)).toArrayUnsafe();
    final BigInteger A = new BigInteger(a32);
    final BigInteger M = new BigInteger(m32);
    final UInt256 ua = UInt256.fromBytesBE(a32);
    final UInt256 um = UInt256.fromBytesBE(m32);

    // Act
    byte[] got = ua.signedMod(um).toBytesBE();

    // Assert
    byte[] expected =
        (M.signum() == 0) ? Bytes32.ZERO.toArrayUnsafe() : computeSignedModExpected(A, M);

    assertThat(got).containsExactly(expected);
  }

  @Property
  void property_addMod_matchesBigInteger(
      @ForAll("unsigned1to32") final byte[] a,
      @ForAll("unsigned1to32") final byte[] b,
      @ForAll("unsigned1to32") final byte[] m) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);
    final UInt256 um = UInt256.fromBytesBE(m);

    // Act
    byte[] got = ua.addMod(ub, um).toBytesBE();

    // Assert
    BigInteger A = toBigUnsigned(a);
    BigInteger B = toBigUnsigned(b);
    BigInteger M = toBigUnsigned(m);
    byte[] exp =
        (M.signum() == 0) ? Bytes32.ZERO.toArrayUnsafe() : bigUnsignedToBytes32(A.add(B).mod(M));
    assertThat(got).containsExactly(exp);
  }

  @Property
  void property_mulMod_matchesBigInteger(
      @ForAll("unsigned1to32") final byte[] a,
      @ForAll("unsigned1to32") final byte[] b,
      @ForAll("unsigned1to32") final byte[] m) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);
    final UInt256 um = UInt256.fromBytesBE(m);

    // Act
    byte[] got = ua.mulMod(ub, um).toBytesBE();

    // Assert
    BigInteger A = toBigUnsigned(a);
    BigInteger B = toBigUnsigned(b);
    BigInteger M = toBigUnsigned(m);
    byte[] exp =
        (M.signum() == 0)
            ? Bytes32.ZERO.toArrayUnsafe()
            : bigUnsignedToBytes32(A.multiply(B).mod(M));
    assertThat(got).containsExactly(exp);
  }

  @Property
  void property_divByZero_invariants() {
    // Arrange
    UInt256 x = UInt256.fromBytesBE(new byte[] {1, 2, 3, 4});
    UInt256 zero = UInt256.ZERO;

    // Act & Assert
    assertThat(x.mod(zero).toBytesBE()).containsExactly(Bytes32.ZERO.toArrayUnsafe());
    assertThat(x.signedMod(zero).toBytesBE()).containsExactly(Bytes32.ZERO.toArrayUnsafe());
    assertThat(x.addMod(x, zero).toBytesBE()).containsExactly(Bytes32.ZERO.toArrayUnsafe());
    assertThat(x.mulMod(x, zero).toBytesBE()).containsExactly(Bytes32.ZERO.toArrayUnsafe());
    assertThat(x.div(zero).toBytesBE()).containsExactly(Bytes32.ZERO.toArrayUnsafe());
    assertThat(x.signedDiv(zero).toBytesBE()).containsExactly(Bytes32.ZERO.toArrayUnsafe());
  }

  @Property
  void property_div_matchesBigInteger(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] d) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ud = UInt256.fromBytesBE(d);

    // Act
    byte[] got = ua.div(ud).toBytesBE();

    // Assert
    BigInteger A = toBigUnsigned(a);
    BigInteger D = toBigUnsigned(d);
    byte[] exp =
        (D.signum() == 0) ? Bytes32.ZERO.toArrayUnsafe() : bigUnsignedToBytes32(A.divide(D));
    assertThat(got).containsExactly(exp);
  }

  @Property
  void property_signedDiv_matchesEvmSemantics(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] d) {
    // Arrange
    final byte[] a32 = Bytes32.leftPad(Bytes.wrap(a)).toArrayUnsafe();
    final byte[] d32 = Bytes32.leftPad(Bytes.wrap(d)).toArrayUnsafe();
    final BigInteger A = new BigInteger(a32);
    final BigInteger D = new BigInteger(d32);
    final UInt256 ua = UInt256.fromBytesBE(a32);
    final UInt256 ud = UInt256.fromBytesBE(d32);

    // Act
    byte[] got = ua.signedDiv(ud).toBytesBE();

    // Assert
    byte[] expected =
        (D.signum() == 0) ? Bytes32.ZERO.toArrayUnsafe() : computeSignedDivExpected(A, D);
    assertThat(got).containsExactly(expected);
  }

  // --------------------------------------------------------------------------
  // endregion

  // region AND Operation Tests
  // --------------------------------------------------------------------------

  @Property
  void property_and_matchesBytesAnd(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] b) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);

    // Act
    final byte[] got = ua.and(ub).toBytesBE();

    // Assert - compare with Bytes.and() (existing implementation)
    final Bytes bytesA = Bytes32.leftPad(Bytes.wrap(a));
    final Bytes bytesB = Bytes32.leftPad(Bytes.wrap(b));
    final byte[] expected = bytesA.and(bytesB).toArrayUnsafe();

    assertThat(got).containsExactly(expected);
  }

  @Property
  void property_and_matchesBigInteger(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] b) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);

    // Act
    final byte[] got = ua.and(ub).toBytesBE();

    // Assert - compare with BigInteger.and()
    BigInteger A = toBigUnsigned(a);
    BigInteger B = toBigUnsigned(b);
    byte[] exp = bigUnsignedToBytes32(A.and(B));
    assertThat(got).containsExactly(exp);
  }

  @Property
  void property_and_commutative(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] b) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);

    // Act & Assert - A & B = B & A
    assertThat(ua.and(ub)).isEqualTo(ub.and(ua));
  }

  @Property
  void property_and_associative(
      @ForAll("unsigned1to32") final byte[] a,
      @ForAll("unsigned1to32") final byte[] b,
      @ForAll("unsigned1to32") final byte[] c) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);
    final UInt256 uc = UInt256.fromBytesBE(c);

    // Act & Assert - (A & B) & C = A & (B & C)
    assertThat(ua.and(ub).and(uc)).isEqualTo(ua.and(ub.and(uc)));
  }

  @Property
  void property_and_identity(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 allOnes =
        UInt256.fromBytesBE(
            new byte[] {
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF
            });

    // Act & Assert - A & 0xFF...FF = A (identity)
    assertThat(ua.and(allOnes)).isEqualTo(ua);
  }

  @Property
  void property_and_zero(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 zero = UInt256.ZERO;

    // Act & Assert - A & 0 = 0
    assertThat(ua.and(zero)).isEqualTo(zero);
    assertThat(zero.and(ua)).isEqualTo(zero);
  }

  @Property
  void property_and_self(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);

    // Act & Assert - A & A = A (idempotent)
    assertThat(ua.and(ua)).isEqualTo(ua);
  }

  @Property
  void property_and_idempotent(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);

    // Act & Assert - (A & A) & A = A
    assertThat(ua.and(ua).and(ua)).isEqualTo(ua);
  }

  @Property
  void property_and_result_bounded(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] b) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);

    // Act
    final UInt256 result = ua.and(ub);

    // Assert - A & B <= min(A, B)
    assertThat(UInt256.compare(result, ua)).isLessThanOrEqualTo(0);
    assertThat(UInt256.compare(result, ub)).isLessThanOrEqualTo(0);
  }

  @Property
  void property_and_with_complement_is_zero(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final byte[] aBytes32 = Bytes32.leftPad(Bytes.wrap(a)).toArrayUnsafe();

    // Create bitwise complement
    byte[] complementBytes = new byte[32];
    for (int i = 0; i < 32; i++) {
      complementBytes[i] = (byte) ~aBytes32[i];
    }
    final UInt256 complement = UInt256.fromBytesBE(complementBytes);

    // Act & Assert - A & ~A = 0
    assertThat(ua.and(complement)).isEqualTo(UInt256.ZERO);
  }

  @Property
  void property_and_specific_patterns() {
    // Test specific bit patterns
    final UInt256 pattern1 =
        UInt256.fromBytesBE(
            new byte[] {
              (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA,
              (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA,
              (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA,
              (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA,
              (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA,
              (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA,
              (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA,
              (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA
            }); // 10101010...

    final UInt256 pattern2 =
        UInt256.fromBytesBE(
            new byte[] {
              (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
              (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
              (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
              (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
              (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
              (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
              (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
              (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55
            }); // 01010101...

    // Act & Assert - alternating patterns should give 0
    assertThat(pattern1.and(pattern2)).isEqualTo(UInt256.ZERO);

    // Verify with Bytes implementation
    final Bytes bytes1 = Bytes.wrap(pattern1.toBytesBE());
    final Bytes bytes2 = Bytes.wrap(pattern2.toBytesBE());
    assertThat(pattern1.and(pattern2).toBytesBE())
        .containsExactly(bytes1.and(bytes2).toArrayUnsafe());
  }

  // --------------------------------------------------------------------------
  // endregion

  // region XOR Operation Tests
  // --------------------------------------------------------------------------

  @Property
  void property_xor_matchesBytesXor(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] b) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);

    // Act
    final byte[] got = ua.xor(ub).toBytesBE();

    // Assert - compare with Bytes.xor() (existing implementation)
    final Bytes bytesA = Bytes32.leftPad(Bytes.wrap(a));
    final Bytes bytesB = Bytes32.leftPad(Bytes.wrap(b));
    final byte[] expected = bytesA.xor(bytesB).toArrayUnsafe();

    assertThat(got).containsExactly(expected);
  }

  @Property
  void property_xor_matchesBigInteger(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] b) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);

    // Act
    final byte[] got = ua.xor(ub).toBytesBE();

    // Assert - compare with BigInteger.xor()
    BigInteger A = toBigUnsigned(a);
    BigInteger B = toBigUnsigned(b);
    byte[] exp = bigUnsignedToBytes32(A.xor(B));
    assertThat(got).containsExactly(exp);
  }

  @Property
  void property_xor_commutative(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] b) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);

    // Act & Assert - A ^ B = B ^ A
    assertThat(ua.xor(ub)).isEqualTo(ub.xor(ua));
  }

  @Property
  void property_xor_associative(
      @ForAll("unsigned1to32") final byte[] a,
      @ForAll("unsigned1to32") final byte[] b,
      @ForAll("unsigned1to32") final byte[] c) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);
    final UInt256 uc = UInt256.fromBytesBE(c);

    // Act & Assert - (A ^ B) ^ C = A ^ (B ^ C)
    assertThat((ua.xor(ub)).xor(uc)).isEqualTo(ua.xor(ub.xor(uc)));
  }

  @Property
  void property_xor_identity(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 zero = UInt256.ZERO;

    // Act & Assert - A ^ 0 = A
    assertThat(ua.xor(zero)).isEqualTo(ua);
    assertThat(zero.xor(ua)).isEqualTo(ua);
  }

  @Property
  void property_xor_self_is_zero(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);

    // Act & Assert - A ^ A = 0 (self-inverse)
    assertThat(ua.xor(ua)).isEqualTo(UInt256.ZERO);
  }

  @Property
  void property_xor_involutive(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] b) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);

    // Act & Assert - (A ^ B) ^ B = A (applying twice returns original)
    assertThat(ua.xor(ub).xor(ub)).isEqualTo(ua);
    assertThat(ub.xor(ua).xor(ua)).isEqualTo(ub);
  }

  @Property
  void property_xor_with_allOnes_is_complement(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 allOnes = UInt256.MAX;

    // Act
    final UInt256 result = ua.xor(allOnes);

    // Assert - A ^ 0xFF...FF = ~A (bitwise complement)
    final byte[] aBytes32 = Bytes32.leftPad(Bytes.wrap(a)).toArrayUnsafe();
    byte[] complementBytes = new byte[32];
    for (int i = 0; i < 32; i++) {
      complementBytes[i] = (byte) ~aBytes32[i];
    }

    assertThat(result.toBytesBE()).containsExactly(complementBytes);
  }

  @Property
  void property_xor_distributes_over_and(
      @ForAll("unsigned1to32") final byte[] a,
      @ForAll("unsigned1to32") final byte[] b,
      @ForAll("unsigned1to32") final byte[] c) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);
    final UInt256 uc = UInt256.fromBytesBE(c);

    // Act & Assert - A ^ (B & C) = (A ^ B) & (A ^ C)
    // Note: This is NOT true for XOR! XOR doesn't distribute over AND
    // But we can test: A & (B ^ C) = (A & B) ^ (A & C) - this IS true
    UInt256 left = ua.and(ub.xor(uc));
    UInt256 right = ua.and(ub).xor(ua.and(uc));

    assertThat(left).isEqualTo(right);
  }

  @Property
  void property_xor_specific_patterns() {
    // Test specific bit patterns
    final UInt256 pattern1 =
        UInt256.fromBytesBE(
            new byte[] {
              (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA,
              (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA,
              (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA,
              (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA,
              (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA,
              (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA,
              (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA,
              (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA
            }); // 10101010...

    final UInt256 pattern2 =
        UInt256.fromBytesBE(
            new byte[] {
              (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
              (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
              (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
              (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
              (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
              (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
              (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
              (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55
            }); // 01010101...

    final UInt256 allOnes = UInt256.MAX;
    // Act & Assert - 0xAA XOR 0x55 = 0xFF
    assertThat(pattern1.xor(pattern2)).isEqualTo(allOnes);

    // Verify with Bytes implementation
    final Bytes bytes1 = Bytes.wrap(pattern1.toBytesBE());
    final Bytes bytes2 = Bytes.wrap(pattern2.toBytesBE());
    assertThat(pattern1.xor(pattern2).toBytesBE())
        .containsExactly(bytes1.xor(bytes2).toArrayUnsafe());
  }

  @Property
  void property_xor_reversible(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] b) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);

    // Act
    final UInt256 encrypted = ua.xor(ub);
    final UInt256 decrypted = encrypted.xor(ub);

    // Assert - XOR is its own inverse (encryption/decryption property)
    assertThat(decrypted).isEqualTo(ua);
  }

  // --------------------------------------------------------------------------
  // endregion

  // region OR Operation Tests
  // --------------------------------------------------------------------------

  @Property
  void property_or_matchesBytesOr(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] b) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);
    // Act
    final byte[] got = ua.or(ub).toBytesBE();
    // Assert - compare with Bytes.or() (existing implementation)
    final Bytes bytesA = Bytes32.leftPad(Bytes.wrap(a));
    final Bytes bytesB = Bytes32.leftPad(Bytes.wrap(b));
    final byte[] expected = bytesA.or(bytesB).toArrayUnsafe();
    assertThat(got).containsExactly(expected);
  }

  @Property
  void property_or_matchesBigInteger(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] b) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);
    // Act
    final byte[] got = ua.or(ub).toBytesBE();
    // Assert - compare with BigInteger.or()
    BigInteger A = toBigUnsigned(a);
    BigInteger B = toBigUnsigned(b);
    byte[] exp = bigUnsignedToBytes32(A.or(B));
    assertThat(got).containsExactly(exp);
  }

  @Property
  void property_or_commutative(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] b) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);
    // Act & Assert - A | B = B | A
    assertThat(ua.or(ub)).isEqualTo(ub.or(ua));
  }

  @Property
  void property_or_associative(
      @ForAll("unsigned1to32") final byte[] a,
      @ForAll("unsigned1to32") final byte[] b,
      @ForAll("unsigned1to32") final byte[] c) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);
    final UInt256 uc = UInt256.fromBytesBE(c);
    // Act & Assert - (A | B) | C = A | (B | C)
    assertThat((ua.or(ub)).or(uc)).isEqualTo(ua.or(ub.or(uc)));
  }

  @Property
  void property_or_identity(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 zero = UInt256.ZERO;
    // Act & Assert - A | 0 = A (identity)
    assertThat(ua.or(zero)).isEqualTo(ua);
    assertThat(zero.or(ua)).isEqualTo(ua);
  }

  @Property
  void property_or_self(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    // Act & Assert - A | A = A (idempotent)
    assertThat(ua.or(ua)).isEqualTo(ua);
  }

  @Property
  void property_or_idempotent(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    // Act & Assert - (A | A) | A = A
    assertThat(ua.or(ua).or(ua)).isEqualTo(ua);
  }

  @Property
  void property_or_with_allOnes(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 allOnes = UInt256.MAX;
    // Act & Assert - A | 0xFF...FF = 0xFF...FF (domination)
    assertThat(ua.or(allOnes)).isEqualTo(allOnes);
    assertThat(allOnes.or(ua)).isEqualTo(allOnes);
  }

  @Property
  void property_or_result_bounded(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] b) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);
    // Act
    final UInt256 result = ua.or(ub);
    // Assert - A | B >= max(A, B)
    assertThat(UInt256.compare(result, ua)).isGreaterThanOrEqualTo(0);
    assertThat(UInt256.compare(result, ub)).isGreaterThanOrEqualTo(0);
  }

  @Property
  void property_or_absorbs_and(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] b) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);
    // Act & Assert - A | (A & B) = A (absorption law)
    assertThat(ua.or(ua.and(ub))).isEqualTo(ua);
  }

  @Property
  void property_or_distributes_over_and(
      @ForAll("unsigned1to32") final byte[] a,
      @ForAll("unsigned1to32") final byte[] b,
      @ForAll("unsigned1to32") final byte[] c) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);
    final UInt256 uc = UInt256.fromBytesBE(c);
    // Act & Assert - A | (B & C) = (A | B) & (A | C) (distributive law)
    UInt256 left = ua.or(ub.and(uc));
    UInt256 right = ua.or(ub).and(ua.or(uc));
    assertThat(left).isEqualTo(right);
  }

  @Property
  void property_or_with_complement_is_allOnes(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final byte[] aBytes32 = Bytes32.leftPad(Bytes.wrap(a)).toArrayUnsafe();
    // Create bitwise complement
    byte[] complementBytes = new byte[32];
    for (int i = 0; i < 32; i++) {
      complementBytes[i] = (byte) ~aBytes32[i];
    }
    final UInt256 complement = UInt256.fromBytesBE(complementBytes);
    final UInt256 allOnes = UInt256.MAX;
    // Act & Assert - A | ~A = 0xFF...FF
    assertThat(ua.or(complement)).isEqualTo(allOnes);
  }

  @Property
  void property_or_specific_patterns() {
    // Test specific bit patterns
    final UInt256 pattern1 =
        UInt256.fromBytesBE(
            new byte[] {
              (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA,
              (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA,
              (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA,
              (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA,
              (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA,
              (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA,
              (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA,
              (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA
            }); // 10101010...
    final UInt256 pattern2 =
        UInt256.fromBytesBE(
            new byte[] {
              (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
              (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
              (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
              (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
              (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
              (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
              (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
              (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55
            }); // 01010101...
    final UInt256 allOnes = UInt256.MAX;
    // Act & Assert - 0xAA OR 0x55 = 0xFF
    assertThat(pattern1.or(pattern2)).isEqualTo(allOnes);
    // Verify with Bytes implementation
    final Bytes bytes1 = Bytes.wrap(pattern1.toBytesBE());
    final Bytes bytes2 = Bytes.wrap(pattern2.toBytesBE());
    assertThat(pattern1.or(pattern2).toBytesBE())
        .containsExactly(bytes1.or(bytes2).toArrayUnsafe());
  }

  @Property
  void property_or_de_morgans_law(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] b) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);
    final byte[] aBytes32 = Bytes32.leftPad(Bytes.wrap(a)).toArrayUnsafe();
    final byte[] bBytes32 = Bytes32.leftPad(Bytes.wrap(b)).toArrayUnsafe();

    // Create complements
    byte[] notABytes = new byte[32];
    byte[] notBBytes = new byte[32];
    for (int i = 0; i < 32; i++) {
      notABytes[i] = (byte) ~aBytes32[i];
      notBBytes[i] = (byte) ~bBytes32[i];
    }
    final UInt256 notA = UInt256.fromBytesBE(notABytes);
    final UInt256 notB = UInt256.fromBytesBE(notBBytes);

    // Act
    // De Morgan's Law: ~(A | B) = ~A & ~B
    UInt256 left = ua.or(ub);
    byte[] leftBytes = left.toBytesBE();
    byte[] notLeft = new byte[32];
    for (int i = 0; i < 32; i++) {
      notLeft[i] = (byte) ~leftBytes[i];
    }
    UInt256 notAorB = UInt256.fromBytesBE(notLeft);

    UInt256 right = notA.and(notB);

    // Assert - ~(A | B) = ~A & ~B
    assertThat(notAorB).isEqualTo(right);
  }

  @Property
  void property_or_relationship_with_and_xor(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] b) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);

    // Act & Assert - A | B = (A & B) | (A ^ B)
    // This is because OR sets a bit if it's in either operand,
    // which is the same as bits in both (AND) OR bits in exactly one (XOR)
    UInt256 left = ua.or(ub);
    UInt256 right = ua.and(ub).or(ua.xor(ub));
    assertThat(left).isEqualTo(right);
  }

  // --------------------------------------------------------------------------
  // endregion

  // region NOT Operation Tests
  // --------------------------------------------------------------------------

  @Property
  void property_not_matchesBytesNot(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    // Act
    final byte[] got = ua.not().toBytesBE();
    // Assert - compare with Bytes.not() (existing implementation)
    final Bytes bytesA = Bytes32.leftPad(Bytes.wrap(a));
    final byte[] expected = bytesA.not().toArrayUnsafe();
    assertThat(got).containsExactly(expected);
  }

  @Property
  void property_not_matchesBigInteger(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    // Act
    final byte[] got = ua.not().toBytesBE();
    // Assert - compare with BigInteger.not()
    BigInteger A = toBigUnsigned(a);
    // BigInteger.not() does two's complement, we need bitwise NOT for 256 bits
    // So we XOR with all ones: ~A = A XOR 0xFF...FF
    BigInteger allOnes = BigInteger.ONE.shiftLeft(256).subtract(BigInteger.ONE);
    byte[] exp = bigUnsignedToBytes32(A.xor(allOnes));
    assertThat(got).containsExactly(exp);
  }

  @Property
  void property_not_manualComplement(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final byte[] aBytes32 = Bytes32.leftPad(Bytes.wrap(a)).toArrayUnsafe();

    // Create expected result manually
    byte[] expected = new byte[32];
    for (int i = 0; i < 32; i++) {
      expected[i] = (byte) ~aBytes32[i];
    }

    // Act
    final byte[] got = ua.not().toBytesBE();

    // Assert
    assertThat(got).containsExactly(expected);
  }

  @Property
  void property_not_involutive(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    // Act & Assert - ~~A = A (double negation returns original)
    assertThat(ua.not().not()).isEqualTo(ua);
  }

  @Property
  void property_not_different_from_original(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 allOnes = UInt256.MAX;

    // Act
    final UInt256 notA = ua.not();

    // Assert - ~A != A (except for special patterns)
    // Only exception: if all bits are alternating or special case
    // For most random values, A != ~A
    if (!ua.equals(allOnes) && !ua.equals(UInt256.ZERO)) {
      assertThat(notA).isNotEqualTo(ua);
    }
  }

  @Property
  void property_not_with_and_is_zero(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    // Act & Assert - A & ~A = 0
    assertThat(ua.and(ua.not())).isEqualTo(UInt256.ZERO);
  }

  @Property
  void property_not_with_or_is_allOnes(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    // Act & Assert - A | ~A = 0xFF...FF
    assertThat(ua.or(ua.not())).isEqualTo(UInt256.MAX);
  }

  @Property
  void property_not_with_xor_is_allOnes(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    // Act & Assert - A ^ ~A = 0xFF...FF
    assertThat(ua.xor(ua.not())).isEqualTo(UInt256.MAX);
  }

  @Property
  void property_not_de_morgans_and(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] b) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);

    // Act & Assert - ~(A & B) = ~A | ~B (De Morgan's Law)
    UInt256 left = ua.and(ub).not();
    UInt256 right = ua.not().or(ub.not());
    assertThat(left).isEqualTo(right);
  }

  @Property
  void property_not_de_morgans_or(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] b) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);

    // Act & Assert - ~(A | B) = ~A & ~B (De Morgan's Law)
    UInt256 left = ua.or(ub).not();
    UInt256 right = ua.not().and(ub.not());
    assertThat(left).isEqualTo(right);
  }

  @Property
  void property_not_zero_is_allOnes() {
    // Arrange
    final UInt256 zero = UInt256.ZERO;
    final UInt256 allOnes = UInt256.MAX;

    // Act & Assert - ~0 = 0xFF...FF
    assertThat(zero.not()).isEqualTo(allOnes);
  }

  @Property
  void property_not_allOnes_is_zero() {
    // Arrange
    final UInt256 allOnes = UInt256.MAX;
    final UInt256 zero = UInt256.ZERO;

    // Act & Assert - ~0xFF...FF = 0
    assertThat(allOnes.not()).isEqualTo(zero);
  }

  @Property
  void property_not_specific_patterns() {
    // Test specific bit patterns
    final UInt256 pattern1 =
        UInt256.fromBytesBE(
            new byte[] {
              (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA,
              (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA,
              (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA,
              (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA,
              (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA,
              (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA,
              (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA,
              (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA
            }); // 10101010...

    final UInt256 expected =
        UInt256.fromBytesBE(
            new byte[] {
              (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
              (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
              (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
              (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
              (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
              (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
              (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
              (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55
            }); // 01010101...

    // Act & Assert - ~0xAA = 0x55
    assertThat(pattern1.not()).isEqualTo(expected);

    // Verify with Bytes implementation
    final Bytes bytes1 = Bytes.wrap(pattern1.toBytesBE());
    assertThat(pattern1.not().toBytesBE()).containsExactly(bytes1.not().toArrayUnsafe());
  }

  @Property
  void property_not_each_bit_flipped(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final byte[] aBytes32 = Bytes32.leftPad(Bytes.wrap(a)).toArrayUnsafe();

    // Act
    final byte[] got = ua.not().toBytesBE();

    // Assert - verify each bit is flipped
    for (int i = 0; i < 32; i++) {
      // Each byte should be bitwise complement
      assertThat(got[i]).isEqualTo((byte) ~aBytes32[i]);
    }
  }

  @Property
  void property_not_xor_equivalence(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 allOnes = UInt256.MAX;

    // Act & Assert - ~A = A ^ 0xFF...FF
    assertThat(ua.not()).isEqualTo(ua.xor(allOnes));
  }

  @Property
  void property_not_sum_with_original(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 allOnes = UInt256.MAX;

    // Act
    // When we add A + ~A (bitwise), we should get all 1s in each bit position
    // But with carry, A + ~A + 1 = 0 (two's complement)
    // For our purposes: A XOR ~A = 0xFF...FF

    // Assert - A XOR ~A = 0xFF...FF
    assertThat(ua.xor(ua.not())).isEqualTo(allOnes);
  }

  // --------------------------------------------------------------------------
  // endregion

  // region Simple ADD tests
  // --------------------------------------------------------------------------
  @Property
  void property_add_matchesBigInteger(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] b) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);

    // Act
    final byte[] got = ua.add(ub).toBytesBE();

    // Assert
    BigInteger A = toBigUnsigned(a);
    BigInteger B = toBigUnsigned(b);
    // Addition wraps around at 2^256
    byte[] expected = bigUnsignedToBytes32(A.add(B));
    assertThat(got).containsExactly(expected);
  }

  @Property
  void property_add_commutative(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] b) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);

    // Act & Assert
    assertThat(ua.add(ub)).isEqualTo(ub.add(ua));
  }

  @Property
  void property_add_associative(
      @ForAll("unsigned1to32") final byte[] a,
      @ForAll("unsigned1to32") final byte[] b,
      @ForAll("unsigned1to32") final byte[] c) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);
    final UInt256 uc = UInt256.fromBytesBE(c);

    // Act & Assert
    assertThat(ua.add(ub).add(uc)).isEqualTo(ua.add(ub.add(uc)));
  }

  @Property
  void property_add_identity(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 zero = UInt256.ZERO;

    // Act & Assert
    assertThat(ua.add(zero)).isEqualTo(ua);
    assertThat(zero.add(ua)).isEqualTo(ua);
  }

  @Property
  void property_add_singleLimb_matchesBigInteger(
      @ForAll("singleLimbUnsigned1to4") final byte[] a,
      @ForAll("singleLimbUnsigned1to4") final byte[] b) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);

    // Act
    final byte[] got = ua.add(ub).toBytesBE();

    // Assert
    BigInteger A = toBigUnsigned(a);
    BigInteger B = toBigUnsigned(b);
    byte[] expected = bigUnsignedToBytes32(A.add(B));
    assertThat(got).containsExactly(expected);
  }

  @Property
  void property_add_one_increment(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 one = UInt256.fromBytesBE(new byte[] {1});

    // Act
    final byte[] got = ua.add(one).toBytesBE();

    // Assert
    BigInteger A = toBigUnsigned(a);
    byte[] expected = bigUnsignedToBytes32(A.add(BigInteger.ONE));
    assertThat(got).containsExactly(expected);
  }

  @Property
  void property_add_self_doubles(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);

    // Act
    final byte[] got = ua.add(ua).toBytesBE();

    // Assert - verify A + A = 2 * A using BigInteger
    BigInteger A = toBigUnsigned(a);
    byte[] expected = bigUnsignedToBytes32(A.multiply(BigInteger.TWO));
    assertThat(got).containsExactly(expected);
  }

  @Property
  void property_add_max_values() {
    // Arrange - MAX_UINT256 = 2^256 - 1 (all bits set to 1)
    final UInt256 max =
        UInt256.fromBytesBE(
            new byte[] {
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF
            });
    final UInt256 one = UInt256.fromBytesBE(new byte[] {1});
    final UInt256 zero = UInt256.ZERO;

    // Act & Assert - MAX + 1 should wrap to 0
    assertThat(max.add(one)).isEqualTo(zero);

    // MAX + MAX should wrap to MAX - 1 (i.e., 2^256 - 2)
    BigInteger maxBig = BigInteger.ONE.shiftLeft(256).subtract(BigInteger.ONE);
    byte[] expMaxPlusMax = bigUnsignedToBytes32(maxBig.add(maxBig));
    assertThat(max.add(max).toBytesBE()).containsExactly(expMaxPlusMax);

    // Verify MAX + MAX = 2^256 - 2 (wraps around)
    final UInt256 expectedMaxPlusMax =
        UInt256.fromBytesBE(
            new byte[] {
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFE // Last byte is 0xFE
            });
    assertThat(max.add(max)).isEqualTo(expectedMaxPlusMax);
  }

  @Property
  void property_add_max_with_random(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange - MAX_UINT256
    final UInt256 max =
        UInt256.fromBytesBE(
            new byte[] {
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF
            });
    final UInt256 ua = UInt256.fromBytesBE(a);

    // Act
    final byte[] got = max.add(ua).toBytesBE();

    // Assert - verify MAX + A wraps correctly
    BigInteger maxBig = BigInteger.ONE.shiftLeft(256).subtract(BigInteger.ONE);
    BigInteger A = toBigUnsigned(a);
    byte[] exp = bigUnsignedToBytes32(maxBig.add(A));
    assertThat(got).containsExactly(exp);

    // Also verify: MAX + A = A - 1 (due to wrapping)
    BigInteger expectedWrapped = A.subtract(BigInteger.ONE);
    if (expectedWrapped.signum() < 0) {
      // If A is 0, then MAX + 0 = MAX
      expectedWrapped = maxBig;
    }
    byte[] expWrapped = bigUnsignedToBytes32(expectedWrapped);
    assertThat(got).containsExactly(expWrapped);
  }

  @Property
  void property_add_near_max_boundary() {
    // Arrange - test values near the max boundary
    final UInt256 max =
        UInt256.fromBytesBE(
            new byte[] {
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF
            });

    // MAX - 1
    final UInt256 maxMinusOne =
        UInt256.fromBytesBE(
            new byte[] {
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFE
            });

    final UInt256 one = UInt256.fromBytesBE(new byte[] {1});
    final UInt256 two = UInt256.fromBytesBE(new byte[] {2});

    // Act & Assert
    // (MAX - 1) + 1 = MAX
    assertThat(maxMinusOne.add(one)).isEqualTo(max);

    // (MAX - 1) + 2 = 0 (wraps)
    assertThat(maxMinusOne.add(two)).isEqualTo(UInt256.ZERO);

    // (MAX - 1) + (MAX - 1) should wrap correctly
    BigInteger maxMinusOneBig = BigInteger.ONE.shiftLeft(256).subtract(BigInteger.TWO);
    byte[] exp = bigUnsignedToBytes32(maxMinusOneBig.add(maxMinusOneBig));
    assertThat(maxMinusOne.add(maxMinusOne).toBytesBE()).containsExactly(exp);
  }

  // --------------------------------------------------------------------------
  // endregion

  // region Byte-Array ADD Tests (static UInt256.add(byte[], byte[]))
  // --------------------------------------------------------------------------

  @Property
  void property_addBytes_matchesBigInteger(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] b) {
    // Arrange
    final byte[] a32 = toBytes32Unsigned(a);
    final byte[] b32 = toBytes32Unsigned(b);

    // Act
    final byte[] got = UInt256.add(a32, b32);

    // Assert
    BigInteger A = toBigUnsigned(a);
    BigInteger B = toBigUnsigned(b);
    byte[] expected = bigUnsignedToBytes32(A.add(B));
    assertThat(got).containsExactly(expected);
  }

  @Property
  void property_addBytes_commutative(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] b) {
    // Arrange
    final byte[] a32 = toBytes32Unsigned(a);
    final byte[] b32 = toBytes32Unsigned(b);

    // Act & Assert
    assertThat(UInt256.add(a32, b32)).containsExactly(UInt256.add(b32, a32));
  }

  @Property
  void property_addBytes_identity(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final byte[] a32 = toBytes32Unsigned(a);
    final byte[] zero = new byte[32];

    // Act & Assert — x + 0 = x
    assertThat(UInt256.add(a32, zero)).containsExactly(a32);
    assertThat(UInt256.add(zero, a32)).containsExactly(a32);
  }

  @Property
  void property_addBytes_consistent_with_UInt256_add(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] b) {
    // Arrange
    final byte[] a32 = toBytes32Unsigned(a);
    final byte[] b32 = toBytes32Unsigned(b);
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);

    // Act
    final byte[] fromBytes = UInt256.add(a32, b32);
    final byte[] fromUInt256 = ua.add(ub).toBytesBE();

    // Assert — byte[] and UInt256 paths must agree
    assertThat(fromBytes).containsExactly(fromUInt256);
  }

  @Property
  void property_addBytes_singleLimb_matchesBigInteger(
      @ForAll("singleLimbUnsigned1to4") final byte[] a,
      @ForAll("singleLimbUnsigned1to4") final byte[] b) {
    // Arrange
    final byte[] a32 = toBytes32Unsigned(a);
    final byte[] b32 = toBytes32Unsigned(b);

    // Act
    final byte[] got = UInt256.add(a32, b32);

    // Assert
    BigInteger A = toBigUnsigned(a);
    BigInteger B = toBigUnsigned(b);
    byte[] expected = bigUnsignedToBytes32(A.add(B));
    assertThat(got).containsExactly(expected);
  }

  // --------------------------------------------------------------------------
  // endregion

  // region Byte-Array SUB Tests (static UInt256.sub(byte[], byte[]))
  // --------------------------------------------------------------------------

  @Property
  void property_subBytes_matchesBigInteger(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] b) {
    // Arrange
    final byte[] a32 = toBytes32Unsigned(a);
    final byte[] b32 = toBytes32Unsigned(b);

    // Act
    final byte[] got = UInt256.sub(a32, b32);

    // Assert — wrapping subtraction mod 2^256
    BigInteger A = toBigUnsigned(a);
    BigInteger B = toBigUnsigned(b);
    BigInteger result = A.subtract(B).mod(TWO_256);
    byte[] expected = bigUnsignedToBytes32(result);
    assertThat(got).containsExactly(expected);
  }

  @Property
  void property_subBytes_identity(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final byte[] a32 = toBytes32Unsigned(a);
    final byte[] zero = new byte[32];

    // Act & Assert — x - 0 = x
    assertThat(UInt256.sub(a32, zero)).containsExactly(a32);
  }

  @Property
  void property_subBytes_self_is_zero(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final byte[] a32 = toBytes32Unsigned(a);

    // Act & Assert — x - x = 0
    assertThat(UInt256.sub(a32, a32)).containsExactly(new byte[32]);
  }

  @Property
  void property_subBytes_add_inverse(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] b) {
    // Arrange
    final byte[] a32 = toBytes32Unsigned(a);
    final byte[] b32 = toBytes32Unsigned(b);

    // Act & Assert — (x + y) - y = x
    byte[] sum = UInt256.add(a32, b32);
    assertThat(UInt256.sub(sum, b32)).containsExactly(a32);
  }

  @Property
  void property_subBytes_anti_commutative(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] b) {
    // Arrange — fresh copies for each sub call (sub/neg may mutate input arrays)
    final byte[] aMinusB = UInt256.sub(toBytes32Unsigned(a), toBytes32Unsigned(b));
    final byte[] bMinusA = UInt256.sub(toBytes32Unsigned(b), toBytes32Unsigned(a));

    // Assert — (a - b) + (b - a) = 0
    assertThat(UInt256.add(aMinusB, bMinusA)).containsExactly(new byte[32]);
  }

  @Property
  void property_subBytes_consistent_with_add_neg(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] b) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);
    final byte[] a32 = toBytes32Unsigned(a);
    final byte[] b32 = toBytes32Unsigned(b);

    // Act — sub(a,b) should equal a + neg(b) via UInt256
    final byte[] fromSub = UInt256.sub(a32, b32);
    final byte[] fromAddNeg = ua.add(ub.neg()).toBytesBE();

    // Assert
    assertThat(fromSub).containsExactly(fromAddNeg);
  }

  @Property
  void property_subBytes_singleLimb_matchesBigInteger(
      @ForAll("singleLimbUnsigned1to4") final byte[] a,
      @ForAll("singleLimbUnsigned1to4") final byte[] b) {
    // Arrange
    final byte[] a32 = toBytes32Unsigned(a);
    final byte[] b32 = toBytes32Unsigned(b);

    // Act
    final byte[] got = UInt256.sub(a32, b32);

    // Assert
    BigInteger A = toBigUnsigned(a);
    BigInteger B = toBigUnsigned(b);
    BigInteger result = A.subtract(B).mod(TWO_256);
    byte[] expected = bigUnsignedToBytes32(result);
    assertThat(got).containsExactly(expected);
  }

  // --------------------------------------------------------------------------
  // endregion

  // region MUL Tests (UInt256.mul)
  // --------------------------------------------------------------------------

  @Property
  void property_mul_matchesBigInteger(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] b) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);

    // Act
    final byte[] got = ua.mul(ub).toBytesBE();

    // Assert — wrapping multiplication mod 2^256
    BigInteger A = toBigUnsigned(a);
    BigInteger B = toBigUnsigned(b);
    byte[] expected = bigUnsignedToBytes32(A.multiply(B));
    assertThat(got).containsExactly(expected);
  }

  @Property
  void property_mul_commutative(
      @ForAll("unsigned1to32") final byte[] a, @ForAll("unsigned1to32") final byte[] b) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);

    // Act & Assert — a * b = b * a
    assertThat(ua.mul(ub)).isEqualTo(ub.mul(ua));
  }

  @Property
  void property_mul_associative(
      @ForAll("unsigned1to32") final byte[] a,
      @ForAll("unsigned1to32") final byte[] b,
      @ForAll("unsigned1to32") final byte[] c) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);
    final UInt256 uc = UInt256.fromBytesBE(c);

    // Act & Assert — (a * b) * c = a * (b * c)
    assertThat(ua.mul(ub).mul(uc)).isEqualTo(ua.mul(ub.mul(uc)));
  }

  @Property
  void property_mul_identity(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 one = UInt256.fromBytesBE(new byte[] {1});

    // Act & Assert — a * 1 = a
    assertThat(ua.mul(one)).isEqualTo(ua);
    assertThat(one.mul(ua)).isEqualTo(ua);
  }

  @Property
  void property_mul_zero(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 zero = UInt256.ZERO;

    // Act & Assert — a * 0 = 0
    assertThat(ua.mul(zero)).isEqualTo(zero);
    assertThat(zero.mul(ua)).isEqualTo(zero);
  }

  @Property
  void property_mul_singleLimb_matchesBigInteger(
      @ForAll("singleLimbUnsigned1to4") final byte[] a,
      @ForAll("singleLimbUnsigned1to4") final byte[] b) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);

    // Act
    final byte[] got = ua.mul(ub).toBytesBE();

    // Assert
    BigInteger A = toBigUnsigned(a);
    BigInteger B = toBigUnsigned(b);
    byte[] expected = bigUnsignedToBytes32(A.multiply(B));
    assertThat(got).containsExactly(expected);
  }

  @Property
  void property_mul_distributive_over_add(
      @ForAll("unsigned1to32") final byte[] a,
      @ForAll("unsigned1to32") final byte[] b,
      @ForAll("unsigned1to32") final byte[] c) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);
    final UInt256 uc = UInt256.fromBytesBE(c);

    // Act & Assert — a * (b + c) = a*b + a*c
    UInt256 left = ua.mul(ub.add(uc));
    UInt256 right = ua.mul(ub).add(ua.mul(uc));
    assertThat(left).isEqualTo(right);
  }

  @Property
  void property_mul_by_two_equals_add_self(@ForAll("unsigned1to32") final byte[] a) {
    // Arrange
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 two = UInt256.fromBytesBE(new byte[] {2});

    // Act & Assert — a * 2 = a + a
    assertThat(ua.mul(two)).isEqualTo(ua.add(ua));
  }

  // --------------------------------------------------------------------------
  // endregion

  // region Record Refactor Regression Tests (shaped generators, fixed seeds)
  // --------------------------------------------------------------------------

  @Property(seed = "3735928559")
  void property_fromBytesBE_toBytesBE_roundTrip_canonical32(
      @ForAll("bytes0to64_shaped") final byte[] input) {
    // Arrange.
    final byte[] expected = canonicalUnsigned256ToBytes32(input);

    // Act.
    final UInt256 u = UInt256.fromBytesBE(input);
    final byte[] got = u.toBytesBE();

    // Assert.
    assertThat(got).hasSize(32);
    assertThat(got).containsExactly(expected);
  }

  @Property(seed = "2718281828")
  void property_fromBytesBE_ignores_high_bytes_above_32(
      @ForAll("bytes33to64_shaped") final byte[] input) {
    // Arrange.
    final byte[] low32 = Arrays.copyOfRange(input, input.length - 32, input.length);

    // Act.
    final UInt256 a = UInt256.fromBytesBE(input);
    final UInt256 b = UInt256.fromBytesBE(low32);

    // Assert.
    assertThat(a).isEqualTo(b);
  }

  @Property(seed = "3141592653")
  void property_fromBytesBE_toBytesBE_is_identity_on_32_bytes(
      @ForAll("bytes32_shaped") final byte[] be32) {
    // Arrange.
    final byte[] input = Arrays.copyOf(be32, be32.length);

    // Act.
    final byte[] got = UInt256.fromBytesBE(input).toBytesBE();

    // Assert.
    assertThat(got).containsExactly(input);
  }

  @Property(seed = "1618033988")
  void property_compare_zero_iff_equals(
      @ForAll("bytes32_shaped") final byte[] a, @ForAll("bytes32_shaped") final byte[] b) {
    // Arrange.
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);

    // Act.
    final int cmp = UInt256.compare(ua, ub);

    // Assert.
    assertThat(cmp == 0).isEqualTo(ua.equals(ub));
  }

  @Property(seed = "1414213562")
  void property_compare_sign_matches_unsigned_big_integer(
      @ForAll("bytes32_shaped") final byte[] a, @ForAll("bytes32_shaped") final byte[] b) {
    // Arrange.
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);
    final BigInteger A = new BigInteger(1, a);
    final BigInteger B = new BigInteger(1, b);

    // Act.
    final int cmp = UInt256.compare(ua, ub);

    // Assert.
    assertThat(Integer.signum(cmp)).isEqualTo(Integer.signum(A.compareTo(B)));
  }

  @Property(seed = "1123581321")
  void property_shiftLeft_matches_big_integer_mod_2_256(
      @ForAll("bytes32_shaped") final byte[] a, @ForAll("shifts_extreme") final int shift) {
    Assume.that(shift >= 0 && shift < 64);
    // Arrange.
    final BigInteger A = new BigInteger(1, a);
    final byte[] expected = expectedShl(A, shift);

    // Act.
    final byte[] got = UInt256.fromBytesBE(a).shiftLeft(shift).toBytesBE();

    // Assert.
    assertThat(got).containsExactly(expected);
  }

  @Property(seed = "867530900")
  void property_shiftRight_matches_big_integer_mod_2_256(
      @ForAll("bytes32_shaped") final byte[] a, @ForAll("shifts_extreme") final int shift) {
    Assume.that(shift >= 0 && shift < 64);
    // Arrange.
    final BigInteger A = new BigInteger(1, a);
    final byte[] expected = expectedShr(A, shift);

    // Act.
    final byte[] got = UInt256.fromBytesBE(a).shiftRight(shift).toBytesBE();

    // Assert.
    assertThat(got).containsExactly(expected);
  }

  @Property(seed = "314159265")
  void property_div_matches_big_integer_unsigned(
      @ForAll("bytes0to64_shaped") final byte[] a, @ForAll("bytes0to64_shaped") final byte[] d) {
    // Arrange.
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ud = UInt256.fromBytesBE(d);
    final BigInteger A = toBigUnsignedMod256(a);
    final BigInteger D = toBigUnsignedMod256(d);
    final byte[] expected = expectedDiv(A, D);

    // Act.
    final byte[] got = ua.div(ud).toBytesBE();

    // Assert.
    assertThat(got).containsExactly(expected);
  }

  @Property(seed = "271828182")
  void property_signedDiv_matches_evm_semantics(
      @ForAll("bytes0to64_shaped") final byte[] a, @ForAll("bytes0to64_shaped") final byte[] d) {
    // Arrange.
    final byte[] a32 = toBytes32Unsigned(a);
    final byte[] d32 = toBytes32Unsigned(d);
    final UInt256 ua = UInt256.fromBytesBE(a32);
    final UInt256 ud = UInt256.fromBytesBE(d32);
    final BigInteger A = new BigInteger(a32);
    final BigInteger D = new BigInteger(d32);
    final byte[] expected = expectedSignedDiv(A, D);

    // Act.
    final byte[] got = ua.signedDiv(ud).toBytesBE();

    // Assert.
    assertThat(got).containsExactly(expected);
  }

  @Property(seed = "34243534534")
  void property_signedDiv_division_by_minus_one(@ForAll("bytes0to64_shaped") final byte[] a) {
    // Arrange.
    final byte[] a32 = toBytes32Unsigned(a);
    final byte[] d32 = applyPattern(new byte[32], Pattern.ALL_FF);
    final UInt256 ua = UInt256.fromBytesBE(a32);
    final UInt256 ud = UInt256.fromBytesBE(d32);
    final BigInteger A = new BigInteger(a32);
    final BigInteger D = new BigInteger(d32);
    final byte[] expected = expectedSignedDiv(A, D);

    // Act.
    final byte[] got = ua.signedDiv(ud).toBytesBE();

    // Assert.
    assertThat(got).containsExactly(expected);
  }

  @Property(seed = "123456789")
  void property_mod_matches_big_integer_unsigned(
      @ForAll("bytes0to64_shaped") final byte[] a, @ForAll("bytes0to64_shaped") final byte[] m) {
    // Arrange.
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 um = UInt256.fromBytesBE(m);
    final BigInteger A = toBigUnsignedMod256(a);
    final BigInteger M = toBigUnsignedMod256(m);
    final byte[] expected = expectedMod(A, M);

    // Act.
    final byte[] got = ua.mod(um).toBytesBE();

    // Assert.
    assertThat(got).containsExactly(expected);
  }

  @Property(seed = "987654321")
  void property_signedMod_matches_evm_semantics(
      @ForAll("bytes0to64_shaped") final byte[] a, @ForAll("bytes0to64_shaped") final byte[] m) {
    // Arrange.
    final byte[] a32 = toBytes32Unsigned(a);
    final byte[] m32 = toBytes32Unsigned(m);
    final UInt256 ua = UInt256.fromBytesBE(a32);
    final UInt256 um = UInt256.fromBytesBE(m32);
    final BigInteger A = new BigInteger(a32);
    final BigInteger M = new BigInteger(m32);
    final byte[] expected = expectedSignedMod(A, M);

    // Act.
    final byte[] got = ua.signedMod(um).toBytesBE();

    // Assert.
    assertThat(got).containsExactly(expected);
  }

  @Property(seed = "42424242")
  void property_addMod_matches_big_integer_unsigned(
      @ForAll("bytes0to64_shaped") final byte[] a,
      @ForAll("bytes0to64_shaped") final byte[] b,
      @ForAll("bytes0to64_shaped") final byte[] m) {
    // Arrange.
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);
    final UInt256 um = UInt256.fromBytesBE(m);
    final BigInteger A = toBigUnsignedMod256(a);
    final BigInteger B = toBigUnsignedMod256(b);
    final BigInteger M = toBigUnsignedMod256(m);
    final byte[] expected = expectedAddMod(A, B, M);

    // Act.
    final byte[] got = ua.addMod(ub, um).toBytesBE();

    // Assert.
    assertThat(got).containsExactly(expected);
  }

  @Property(seed = "13371337")
  void property_mulMod_matches_big_integer_unsigned(
      @ForAll("bytes0to64_shaped") final byte[] a,
      @ForAll("bytes0to64_shaped") final byte[] b,
      @ForAll("bytes0to64_shaped") final byte[] m) {
    // Arrange.
    final UInt256 ua = UInt256.fromBytesBE(a);
    final UInt256 ub = UInt256.fromBytesBE(b);
    final UInt256 um = UInt256.fromBytesBE(m);
    final BigInteger A = toBigUnsignedMod256(a);
    final BigInteger B = toBigUnsignedMod256(b);
    final BigInteger M = toBigUnsignedMod256(m);
    final byte[] expected = expectedMulMod(A, B, M);

    // Act.
    final byte[] got = ua.mulMod(ub, um).toBytesBE();

    // Assert.
    assertThat(got).containsExactly(expected);
  }

  // --------------------------------------------------------------------------
  // endregion

  // region Utility Methods
  // --------------------------------------------------------------------------

  private static byte[] clampUnsigned32(final byte[] any) {
    if (any.length == 0) {
      return new byte[] {0};
    }
    int len = Math.max(1, Math.min(32, any.length));
    byte[] out = new byte[len];
    System.arraycopy(any, 0, out, 0, len);
    return out;
  }

  private static byte[] bigUnsignedToBytes32(final BigInteger x) {
    BigInteger y = x.mod(BigInteger.ONE.shiftLeft(256));

    byte[] ba = y.toByteArray();
    if (ba.length == 0) {
      return new byte[32];
    }

    if (ba.length == 32) {
      return ba;
    }

    if (ba.length < 32) {
      byte[] out = new byte[32];
      System.arraycopy(ba, 0, out, 32 - ba.length, ba.length);
      return out;
    }

    // If bigger than 32, take lower 32 bytes.
    byte[] out = new byte[32];
    System.arraycopy(ba, ba.length - 32, out, 0, 32);

    return out;
  }

  private static BigInteger toBigUnsigned(final byte[] be) {
    return new BigInteger(1, be);
  }

  private static byte[] computeSignedModExpected(final BigInteger A, final BigInteger M) {

    BigInteger r = A.abs().mod(M.abs());

    if (A.signum() < 0 && r.signum() != 0) {
      return padNegative(r);
    }

    return bigUnsignedToBytes32(r);
  }

  private static byte[] computeSignedDivExpected(final BigInteger A, final BigInteger D) {
    BigInteger q = A.abs().divide(D.abs());

    if ((A.signum() < 0) != (D.signum() < 0) && q.signum() != 0) {
      return padNegative(q);
    }

    return bigUnsignedToBytes32(q);
  }

  private static byte[] padNegative(final BigInteger r) {
    BigInteger neg = r.negate();
    byte[] rb = neg.toByteArray();
    byte[] padded = new byte[32];
    Arrays.fill(padded, (byte) 0xFF);
    System.arraycopy(rb, 0, padded, 32 - rb.length, rb.length);
    return padded;
  }

  private static byte[] canonicalUnsigned256ToBytes32(final byte[] be) {
    if (be.length == 0) {
      return new byte[32];
    }
    final BigInteger x = new BigInteger(1, be).mod(TWO_256);
    return bigUnsignedToBytes32(x);
  }

  private static byte[] toBytes32Unsigned(final byte[] be) {
    if (be.length == 32) {
      return Arrays.copyOf(be, 32);
    }
    if (be.length == 0) {
      return new byte[32];
    }
    if (be.length < 32) {
      final byte[] out = new byte[32];
      System.arraycopy(be, 0, out, 32 - be.length, be.length);
      return out;
    }
    return Arrays.copyOfRange(be, be.length - 32, be.length);
  }

  private static byte[] forceNonZeroHighBytes(final byte[] be) {
    if (be.length <= 32) {
      return be;
    }
    final int highLen = be.length - 32;
    boolean anyNonZero = false;
    for (int i = 0; i < highLen; i++) {
      anyNonZero |= (be[i] != 0);
    }
    if (!anyNonZero) {
      be[0] = 1;
    }
    return be;
  }

  private static BigInteger toBigUnsignedMod256(final byte[] be) {
    if (be.length == 0) {
      return BigInteger.ZERO;
    }
    return new BigInteger(1, be).mod(TWO_256);
  }

  private static byte[] expectedDiv(final BigInteger A, final BigInteger D) {
    if (D.signum() == 0) {
      return new byte[32];
    }
    return bigUnsignedToBytes32(A.divide(D));
  }

  private static byte[] expectedSignedDiv(final BigInteger A, final BigInteger D) {
    if (D.signum() == 0) {
      return new byte[32];
    }
    BigInteger q = A.abs().divide(D.abs());
    if ((A.signum() < 0) != (D.signum() < 0) && q.signum() != 0) {
      return padNegative(q);
    }
    return bigUnsignedToBytes32(q);
  }

  private static byte[] expectedMod(final BigInteger A, final BigInteger M) {
    if (M.signum() == 0) {
      return new byte[32];
    }
    return bigUnsignedToBytes32(A.mod(M));
  }

  private static byte[] expectedAddMod(final BigInteger A, final BigInteger B, final BigInteger M) {
    if (M.signum() == 0) {
      return new byte[32];
    }
    return bigUnsignedToBytes32(A.add(B).mod(M));
  }

  private static byte[] expectedMulMod(final BigInteger A, final BigInteger B, final BigInteger M) {
    if (M.signum() == 0) {
      return new byte[32];
    }
    return bigUnsignedToBytes32(A.multiply(B).mod(M));
  }

  private static byte[] expectedSignedMod(final BigInteger A, final BigInteger M) {
    if (M.signum() == 0) {
      return new byte[32];
    }
    BigInteger r = A.abs().mod(M.abs());
    if (A.signum() < 0 && r.signum() != 0) {
      return padNegative(r);
    }
    return bigUnsignedToBytes32(r);
  }

  private static byte[] expectedShl(final BigInteger A, final int shift) {
    if (shift < 0 || shift >= 256) {
      return new byte[32];
    }
    return bigUnsignedToBytes32(A.shiftLeft(shift));
  }

  private static byte[] expectedShr(final BigInteger A, final int shift) {
    if (shift < 0 || shift >= 256) {
      return new byte[32];
    }
    return bigUnsignedToBytes32(A.shiftRight(shift));
  }

  // endregion
}
