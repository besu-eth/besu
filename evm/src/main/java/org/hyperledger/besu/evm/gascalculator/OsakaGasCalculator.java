/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.evm.gascalculator;

import static org.hyperledger.besu.datatypes.Address.BLS12_MAP_FP2_TO_G2;
import static org.hyperledger.besu.datatypes.Address.P256_VERIFY;
import static org.hyperledger.besu.evm.internal.Words.clampedAdd;
import static org.hyperledger.besu.evm.internal.Words.clampedMultiply;
import static org.hyperledger.besu.evm.internal.Words.clampedToInt;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.internal.Words;
import org.hyperledger.besu.evm.precompile.BigIntegerModularExponentiationPrecompiledContract;

import java.math.BigInteger;

import org.apache.tuweni.bytes.Bytes;

/**
 * Gas Calculator for Osaka
 *
 * <UL>
 *   <LI>TBD
 * </UL>
 */
public class OsakaGasCalculator extends PragueGasCalculator {
  /** constant indicating the maximum integer value for L1 precompile addresses (0xFF) */
  public static final int MAX_L1_PRECOMPILE = 255;

  /** configured maximum l2 precompile address value for this fork / gas calculator. */
  protected int maxL2Precompile;

  /** Instantiates a new Osaka Gas Calculator. */
  public OsakaGasCalculator() {
    this(BLS12_MAP_FP2_TO_G2.getBytes().getInt(16), P256_VERIFY.getBytes().getInt(16));
  }

  /**
   * Instantiates a new Osaka Gas Calculator
   *
   * @param maxPrecompile the max precompile address from the L1 precompile range (0x01 - 0xFF)
   * @param maxL2Precompile max precompile address from the L2 precompile space (0x0100 - 0x01FF)
   */
  protected OsakaGasCalculator(final int maxPrecompile, final int maxL2Precompile) {
    super(maxPrecompile);
    this.maxL2Precompile = maxL2Precompile;
  }

  /**
   * Instantiates a new Osaka Gas Calculator, uses default P256_VERIFY as max L2 precompile.
   *
   * @param maxPrecompile the max precompile address from the L1 precompile range (0x01 - 0xFF)
   */
  protected OsakaGasCalculator(final int maxPrecompile) {
    this(maxPrecompile, P256_VERIFY.getBytes().getInt(16));
  }

  @Override
  public boolean isPrecompile(final Address address) {
    final byte[] addressBytes = address.getBytes().toArrayUnsafe();

    // First 18 bytes must be zero:
    for (int i = 0; i < 18; i++) {
      if (addressBytes[i] != 0) {
        return false;
      }
    }
    // Interpret last two bytes as big-endian unsigned short.
    final int precompileValue = address.getBytes().getInt(16);

    // values in range [1, 0x01FF] inclusive to include L1 and L2 precompiles,
    // assert max precompile in each range:
    return (precompileValue > 0)
        && ((precompileValue <= MAX_L1_PRECOMPILE)
            ? precompileValue <= this.maxPrecompile
            : precompileValue <= maxL2Precompile);
  }

  @Override
  public long modExpGasCost(final Bytes input) {
    final long baseLength = BigIntegerModularExponentiationPrecompiledContract.baseLength(input);
    final long exponentLength =
        BigIntegerModularExponentiationPrecompiledContract.exponentLength(input);
    final long modulusLength =
        BigIntegerModularExponentiationPrecompiledContract.modulusLength(input);
    final long exponentOffset =
        clampedAdd(BigIntegerModularExponentiationPrecompiledContract.BASE_OFFSET, baseLength);

    final long maxLength = Math.max(modulusLength, baseLength);
    long multiplicationComplexity = 16;
    long words = clampedAdd(maxLength, 7L) / 8L;
    words = Words.clampedMultiply(words, words);
    if (maxLength > 32) {
      multiplicationComplexity = clampedMultiply(words, 2L);
    }

    final long firstExponentBytesCap =
        Math.min(exponentLength, ByzantiumGasCalculator.MAX_FIRST_EXPONENT_BYTES);
    final BigInteger firstExpBytes =
        BigIntegerModularExponentiationPrecompiledContract.extractParameter(
            input, clampedToInt(exponentOffset), clampedToInt(firstExponentBytesCap));
    final long adjustedExponentLength = adjustedExponentLength(exponentLength, firstExpBytes);

    long gasRequirement =
        clampedMultiply(multiplicationComplexity, Math.max(adjustedExponentLength, 1L));

    return Math.max(gasRequirement, 500L);
  }

  /**
   * Adjusted exponent length.
   *
   * @param exponentLength the exponent length
   * @param firstExpBytes the first exp bytes
   * @return the long
   */
  public static long adjustedExponentLength(
      final long exponentLength, final BigInteger firstExpBytes) {
    final int bitLength = bitLength(firstExpBytes);
    if (exponentLength <= WORD_SIZE) {
      return bitLength;
    } else {
      return clampedAdd(clampedMultiply(16, (exponentLength - WORD_SIZE)), bitLength);
    }
  }
}
