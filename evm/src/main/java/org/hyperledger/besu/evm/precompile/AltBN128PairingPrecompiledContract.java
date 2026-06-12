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

import org.hyperledger.besu.crypto.altbn128.AltBn128Fq12Pairer;
import org.hyperledger.besu.crypto.altbn128.AltBn128Fq2Point;
import org.hyperledger.besu.crypto.altbn128.AltBn128Point;
import org.hyperledger.besu.crypto.altbn128.Fq;
import org.hyperledger.besu.crypto.altbn128.Fq12;
import org.hyperledger.besu.crypto.altbn128.Fq2;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.nativelib.gnark.LibGnarkEIP196;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import jakarta.validation.constraints.NotNull;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The AltBN128Pairing precompiled contract. */
public class AltBN128PairingPrecompiledContract extends AbstractAltBnPrecompiledContract {
  private static final Logger LOG =
      LoggerFactory.getLogger(AltBN128PairingPrecompiledContract.class);
  private static final int FIELD_LENGTH = 32;
  private static final int PARAMETER_LENGTH = 192;
  private static final String PRECOMPILE_NAME = "BN254_PAIRING";

  private static final Cache<Integer, PrecompileInputResultTuple> bnPairingCache =
      AbstractPrecompiledContract.resultCacheBuilder()
          .expireAfterWrite(15, TimeUnit.MINUTES) // Evict 15 minutes after each entry is written
          .build();

  /** The constant FALSE. */
  static final Bytes FALSE =
      Bytes.fromHexString("0x0000000000000000000000000000000000000000000000000000000000000000");

  /** The constant TRUE. */
  public static final Bytes TRUE =
      Bytes.fromHexString("0x0000000000000000000000000000000000000000000000000000000000000001");

  private final long pairingGasCost;
  private final long baseGasCost;

  AltBN128PairingPrecompiledContract(
      final GasCalculator gasCalculator, final long pairingGasCost, final long baseGasCost) {
    super(
        PRECOMPILE_NAME,
        gasCalculator,
        LibGnarkEIP196.EIP196_PAIR_OPERATION_RAW_VALUE,
        (Integer.MAX_VALUE / PARAMETER_LENGTH)
            * PARAMETER_LENGTH, // round down to nearest multiple of 192
        LibGnarkEIP196.EIP196_PAIR_PREALLOCATE_FOR_RESULT_BYTES);
    this.pairingGasCost = pairingGasCost;
    this.baseGasCost = baseGasCost;
  }

  /**
   * Create Istanbul AltBN128Pairing precompiled contract.
   *
   * @param gasCalculator the gas calculator
   * @return the AltBN128Pairing precompiled contract
   */
  public static AltBN128PairingPrecompiledContract istanbul(final GasCalculator gasCalculator) {
    return new AltBN128PairingPrecompiledContract(gasCalculator, 34_000L, 45_000L);
  }

  @Override
  public long gasRequirement(final Bytes input) {
    final int parameters = input.size() / PARAMETER_LENGTH;
    return (pairingGasCost * parameters) + baseGasCost;
  }

  @NotNull
  @Override
  public PrecompileContractResult computePrecompile(
      final Bytes input, @NotNull final MessageFrame messageFrame) {
    if (input.isEmpty()) {
      return PrecompileContractResult.success(TRUE);
    }
    if (input.size() % PARAMETER_LENGTH != 0) {
      return PrecompileContractResult.halt(
          null, Optional.of(ExceptionalHaltReason.PRECOMPILE_ERROR));
    }
    PrecompileInputResultTuple res;
    Integer cacheKey = null;
    if (enableResultCaching) {
      cacheKey = getCacheKey(input, input.size());
      res = bnPairingCache.getIfPresent(cacheKey);
      if (res != null) {
        if (res.cachedInput().equals(input)) {
          cacheEventConsumer.accept(new CacheEvent(PRECOMPILE_NAME, CacheMetric.HIT));
          return res.cachedResult();
        } else {
          LOG.debug(
              "false positive altbn128Pairing {}, cache key {}, cached input: {}, input: {}",
              input.getClass().getSimpleName(),
              cacheKey,
              res.cachedInput().toHexString(),
              input.toHexString());
          cacheEventConsumer.accept(new CacheEvent(PRECOMPILE_NAME, CacheMetric.FALSE_POSITIVE));
        }
      } else {
        cacheEventConsumer.accept(new CacheEvent(PRECOMPILE_NAME, CacheMetric.MISS));
      }
    }
    if (useNative) {
      res =
          new PrecompileInputResultTuple(
              enableResultCaching ? input.copy() : input, computeNative(input, messageFrame));
    } else {
      res =
          new PrecompileInputResultTuple(
              enableResultCaching ? input.copy() : input, computeDefault(input));
    }
    if (cacheKey != null) {
      bnPairingCache.put(cacheKey, res);
    }

    return res.cachedResult();
  }

  @NotNull
  private static PrecompileContractResult computeDefault(final Bytes input) {
    final int parameters = input.size() / PARAMETER_LENGTH;
    final List<AltBn128Point> a = new ArrayList<>();
    final List<AltBn128Fq2Point> b = new ArrayList<>();
    for (int i = 0; i < parameters; ++i) {
      final BigInteger p1_x = extractParameter(input, i * PARAMETER_LENGTH, FIELD_LENGTH);
      final BigInteger p1_y = extractParameter(input, i * PARAMETER_LENGTH + 32, FIELD_LENGTH);
      final AltBn128Point p1 = new AltBn128Point(Fq.create(p1_x), Fq.create(p1_y));
      if (!p1.isOnCurve()) {
        return PrecompileContractResult.halt(
            null, Optional.of(ExceptionalHaltReason.PRECOMPILE_ERROR));
      }
      a.add(p1);

      final BigInteger p2_xImag = extractParameter(input, i * PARAMETER_LENGTH + 64, FIELD_LENGTH);
      final BigInteger p2_xReal = extractParameter(input, i * PARAMETER_LENGTH + 96, FIELD_LENGTH);
      final BigInteger p2_yImag = extractParameter(input, i * PARAMETER_LENGTH + 128, FIELD_LENGTH);
      final BigInteger p2_yReal = extractParameter(input, i * PARAMETER_LENGTH + 160, FIELD_LENGTH);
      final Fq2 p2_x = Fq2.create(p2_xReal, p2_xImag);
      final Fq2 p2_y = Fq2.create(p2_yReal, p2_yImag);
      final AltBn128Fq2Point p2 = new AltBn128Fq2Point(p2_x, p2_y);
      if (!p2.isOnCurve() || !p2.isInGroup()) {
        return PrecompileContractResult.halt(
            null, Optional.of(ExceptionalHaltReason.PRECOMPILE_ERROR));
      }
      b.add(p2);
    }

    Fq12 exponent = Fq12.one();
    for (int i = 0; i < parameters; ++i) {
      exponent = exponent.multiply(AltBn128Fq12Pairer.pair(a.get(i), b.get(i)));
    }

    if (AltBn128Fq12Pairer.finalize(exponent).equals(Fq12.one())) {
      return PrecompileContractResult.success(TRUE);
    } else {
      return PrecompileContractResult.success(FALSE);
    }
  }

  private static BigInteger extractParameter(
      final Bytes input, final int offset, final int length) {
    if (offset > input.size() || length == 0) {
      return BigInteger.ZERO;
    }
    final byte[] raw = Arrays.copyOfRange(input.toArrayUnsafe(), offset, offset + length);
    return new BigInteger(1, raw);
  }
}
