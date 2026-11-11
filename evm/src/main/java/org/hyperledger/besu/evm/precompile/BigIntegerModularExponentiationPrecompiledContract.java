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
package org.hyperledger.besu.evm.precompile;

import static org.hyperledger.besu.evm.internal.Words.clampedAdd;
import static org.hyperledger.besu.evm.internal.Words.clampedMultiply;
import static org.hyperledger.besu.evm.internal.Words.clampedToInt;
import static org.hyperledger.besu.evm.internal.Words.clampedToLong;

import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.nativelib.arithmetic.LibArithmetic;

import java.math.BigInteger;
import java.util.Optional;

import com.sun.jna.ptr.IntByReference;
import jakarta.validation.constraints.NotNull;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.MutableBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Big integer modular exponentiation precompiled contract defined in EIP-198. */
public class BigIntegerModularExponentiationPrecompiledContract
    extends AbstractPrecompiledContract {

  private static final Logger LOG =
      LoggerFactory.getLogger(BigIntegerModularExponentiationPrecompiledContract.class);

  /** Use native Arithmetic libraries. */
  private static boolean useNative;

  /** The constant BASE_OFFSET. */
  public static final int BASE_OFFSET = 96;

  private static final int PARAMETER_LENGTH = 32;
  private static final int BASE_LENGTH_OFFSET = 0;
  private static final int EXPONENT_LENGTH_OFFSET = 32;
  private static final int MODULUS_LENGTH_OFFSET = 64;

  private final long upperBound;

  /**
   * Instantiates a new BigInteger modular exponentiation precompiled contract.
   *
   * @param gasCalculator the gas calculator
   */
  BigIntegerModularExponentiationPrecompiledContract(
      final GasCalculator gasCalculator, final long upperBound) {
    super("MODEXP", gasCalculator);
    this.upperBound = upperBound;
  }

  /** Disable native Arithmetic libraries. */
  public static void disableNative() {
    useNative = false;
  }

  /**
   * Attempt to enable the native library for ModExp
   *
   * @return true if the native library was enabled.
   */
  public static boolean maybeEnableNative() {
    try {
      useNative = LibArithmetic.ENABLED;
    } catch (UnsatisfiedLinkError | NoClassDefFoundError ule) {
      LOG.info("modexp native precompile not available: {}", ule.getMessage());
      useNative = false;
    }
    return useNative;
  }

  /**
   * Check if native Arithmetic libraries are enabled.
   *
   * @return the boolean
   */
  public static boolean isNative() {
    return useNative;
  }

  @Override
  public long gasRequirement(final Bytes input) {
    return gasCalculator().modExpGasCost(input);
  }

  @NotNull
  @Override
  public PrecompileContractResult computePrecompile(
      final Bytes input, @NotNull final MessageFrame messageFrame) {
    // https://eips.ethereum.org/EIPS/eip-7823
    // We introduce an upper bound to the inputs of the precompile,
    // each of the length inputs (length_of_BASE, length_of_EXPONENT and length_of_MODULUS)
    // MUST be less than or equal to 1024 bytes
    final long length_of_BASE = baseLength(input);
    final long length_of_EXPONENT = exponentLength(input);
    final long length_of_MODULUS = modulusLength(input);
    if (length_of_BASE > upperBound
        || length_of_EXPONENT > upperBound
        || length_of_MODULUS > upperBound) {
      return PrecompileContractResult.halt(
          null, Optional.of(ExceptionalHaltReason.PRECOMPILE_ERROR));
    }

    // OPTIMIZATION: overwrite native setting for this case
    if (LibArithmetic.ENABLED) {
      final int baseOffset = clampedToInt(BASE_OFFSET);
      final int baseLength = clampedToInt(length_of_BASE);
      final int modulusLength = clampedToInt(length_of_MODULUS);
      if ((extractLastByte(input, baseOffset, baseLength) & 1) != 1) {
        return computeNative(input, modulusLength);
      }
    }

    if (useNative) {
      final int modulusLength = clampedToInt(length_of_MODULUS);
      return computeNative(input, modulusLength);
    } else {
      final int baseLength = clampedToInt(length_of_BASE);
      final int exponentLength = clampedToInt(length_of_EXPONENT);
      final int modulusLength = clampedToInt(length_of_MODULUS);
      return computeDefault(input, baseLength, exponentLength, modulusLength);
    }
  }

  @NotNull
  private PrecompileContractResult computeDefault(
      final Bytes input, final int baseLength, final int exponentLength, final int modulusLength) {
    // If baseLength and modulusLength are zero
    // we could have a massively overflowing exp because it wouldn't have been filtered out at the
    // gas cost phase
    if ((baseLength == 0) && (modulusLength == 0)) {
      return PrecompileContractResult.success(Bytes.EMPTY);
    }
    final int exponentOffset = BASE_OFFSET + baseLength;
    final int modulusOffset = exponentOffset + exponentLength;
    final BigInteger base = extractParameter(input, BASE_OFFSET, baseLength);
    final BigInteger exp = extractParameter(input, exponentOffset, exponentLength);
    final BigInteger mod = extractParameter(input, modulusOffset, modulusLength);

    final Bytes modExp;
    // Result must be the length of the modulus.
    final MutableBytes result = MutableBytes.create(modulusLength);
    if (mod.compareTo(BigInteger.ZERO) == 0) {
      modExp = MutableBytes.EMPTY;
    } else {
      // BigInteger zero-pads positive values whose most significant bit is a 1 if
      // the padding was not there.
      modExp = Bytes.wrap(base.modPow(exp, mod).toByteArray()).trimLeadingZeros();
    }

    modExp.copyTo(result, result.size() - modExp.size());
    return PrecompileContractResult.success(result);
  }

  /**
   * Equation to estimate the multiplication complexity.
   *
   * @param x the x
   * @return the long
   */
  public static long multiplicationComplexity(final long x) {
    if (x <= 64) {
      return square(x);
    } else if (x <= 1024) {
      return clampedAdd((square(x) / 4), clampedMultiply(x, 96)) - 3072;
    } else {
      return clampedAdd((square(x) / 16), clampedMultiply(480, x)) - 199680;
    }
  }

  /**
   * Base length.
   *
   * @param input the input
   * @return the long
   */
  public static long baseLength(final Bytes input) {
    return extractParameterLong(input, BASE_LENGTH_OFFSET, PARAMETER_LENGTH);
  }

  /**
   * Exponent length.
   *
   * @param input the input
   * @return the long
   */
  public static long exponentLength(final Bytes input) {
    return extractParameterLong(input, EXPONENT_LENGTH_OFFSET, PARAMETER_LENGTH);
  }

  /**
   * Modulus length.
   *
   * @param input the input
   * @return the long
   */
  public static long modulusLength(final Bytes input) {
    return extractParameterLong(input, MODULUS_LENGTH_OFFSET, PARAMETER_LENGTH);
  }

  /**
   * Extract parameter.
   *
   * @param input the input
   * @param offset the offset
   * @param length the length
   * @return the big integer
   */
  public static BigInteger extractParameter(final Bytes input, final int offset, final int length) {
    if (offset >= input.size() || length == 0) {
      return BigInteger.ZERO;
    } else if (offset + length < input.size()) {
      return new BigInteger(1, input.slice(offset, length).toArray());
    } else {
      byte[] raw = new byte[length];
      Bytes partial = input.slice(offset);
      System.arraycopy(partial.toArray(), 0, raw, 0, partial.size());
      return new BigInteger(1, raw);
    }
  }

  private static byte extractLastByte(final Bytes input, final int offset, final int length) {
    if (offset >= input.size() || length == 0) {
      return 0;
    } else if (offset + length <= input.size()) {
      return input.get(offset + length - 1);
    }
    Bytes partial = input.slice(offset);
    return partial.get(partial.size() - 1);
  }

  /**
   * Extract parameter.
   *
   * @param input the input
   * @param offset the offset
   * @param length the length
   * @return the long
   */
  public static long extractParameterLong(final Bytes input, final int offset, final int length) {
    if (offset >= input.size() || length == 0) {
      return 0;
    }
    Bytes num;
    if (offset + length <= input.size()) {
      num = input.slice(offset, length).trimLeadingZeros();
    } else {
      // Ethereum's memory is always infinitely full of zeros, but we don't store those zeros, just
      // what we write.  If we are asked for a range that is outside the written memory create a
      // result of the correct size (defaults to zeros) and copy the memory we do have into there.
      MutableBytes mut = MutableBytes.create(length);
      input.slice(offset).copyTo(mut, 0);
      num = mut.trimLeadingZeros();
    }
    return clampedToLong(num);
  }

  private static long square(final long n) {
    return clampedMultiply(n, n);
  }

  private PrecompileContractResult computeNative(
      final @NotNull Bytes input, final int modulusLength) {
    final IntByReference o_len = new IntByReference(modulusLength);

    final byte[] result = new byte[modulusLength];
    final int errorNo =
        LibArithmetic.modexp_precompiled(input.toArrayUnsafe(), input.size(), result, o_len);
    if (errorNo == 0) {
      return PrecompileContractResult.success(Bytes.wrap(result, 0, o_len.getValue()));
    } else {
      return PrecompileContractResult.halt(
          null, Optional.of(ExceptionalHaltReason.PRECOMPILE_ERROR));
    }
  }
}
