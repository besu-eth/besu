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

import org.hyperledger.besu.crypto.altbn128.AltBn128Point;
import org.hyperledger.besu.crypto.altbn128.Fq;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.nativelib.gnark.LibGnarkEIP196;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Optional;

import com.github.benmanes.caffeine.cache.Cache;
import jakarta.validation.constraints.NotNull;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.MutableBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The AltBN128Add precompiled contract. */
public class AltBN128AddPrecompiledContract extends AbstractAltBnPrecompiledContract {
  private static final Logger LOG = LoggerFactory.getLogger(AltBN128AddPrecompiledContract.class);
  private static final int PARAMETER_LENGTH = 128;
  private static final String PRECOMPILE_NAME = "BN254_ADD";

  private final long gasCost;
  private static final Cache<Integer, PrecompileInputResultTuple> bnAddCache =
      AbstractPrecompiledContract.resultCacheBuilder().build();

  AltBN128AddPrecompiledContract(final GasCalculator gasCalculator, final long gasCost) {
    super(
        PRECOMPILE_NAME,
        gasCalculator,
        LibGnarkEIP196.EIP196_ADD_OPERATION_RAW_VALUE,
        PARAMETER_LENGTH,
        LibGnarkEIP196.EIP196_PREALLOCATE_FOR_RESULT_BYTES);
    this.gasCost = gasCost;
  }

  @Override
  public long gasRequirement(final Bytes input) {
    return gasCost;
  }

  @NotNull
  @Override
  public PrecompileContractResult computePrecompile(
      final Bytes input, @NotNull final MessageFrame messageFrame) {

    PrecompileInputResultTuple res;
    Integer cacheKey = null;
    final Bytes cachedInput =
        input.size() > PARAMETER_LENGTH ? input.slice(0, PARAMETER_LENGTH) : input;

    if (enableResultCaching) {
      cacheKey = getCacheKey(input, PARAMETER_LENGTH);
      res = bnAddCache.getIfPresent(cacheKey);
      if (res != null) {
        if (res.cachedInput().equals(cachedInput)) {
          cacheEventConsumer.accept(new CacheEvent(PRECOMPILE_NAME, CacheMetric.HIT));
          return res.cachedResult();
        } else {
          LOG.debug(
              "false positive altbn128Add {}, cache key {}, cached input: {}, input: {}",
              input.getClass().getSimpleName(),
              cacheKey,
              res.cachedInput().toHexString(),
              cachedInput.toHexString());

          cacheEventConsumer.accept(new CacheEvent(PRECOMPILE_NAME, CacheMetric.FALSE_POSITIVE));
        }
      } else {
        cacheEventConsumer.accept(new CacheEvent(PRECOMPILE_NAME, CacheMetric.MISS));
      }
    }
    if (useNative) {
      res =
          new PrecompileInputResultTuple(
              enableResultCaching ? cachedInput.copy() : input, computeNative(input, messageFrame));
    } else {
      res =
          new PrecompileInputResultTuple(
              enableResultCaching ? cachedInput.copy() : input, computeDefault(input));
    }
    if (cacheKey != null) {
      bnAddCache.put(cacheKey, res);
    }
    return res.cachedResult();
  }

  private static PrecompileContractResult computeDefault(final Bytes input) {
    final BigInteger x1 = extractParameter(input, 0, 32);
    final BigInteger y1 = extractParameter(input, 32, 32);
    final BigInteger x2 = extractParameter(input, 64, 32);
    final BigInteger y2 = extractParameter(input, 96, 32);

    final AltBn128Point p1 = new AltBn128Point(Fq.create(x1), Fq.create(y1));
    final AltBn128Point p2 = new AltBn128Point(Fq.create(x2), Fq.create(y2));
    if (!p1.isOnCurve() || !p2.isOnCurve()) {
      return PrecompileContractResult.halt(
          null, Optional.of(ExceptionalHaltReason.PRECOMPILE_ERROR));
    }
    final AltBn128Point sum = p1.add(p2);
    final Bytes x = sum.getX().toBytes();
    final Bytes y = sum.getY().toBytes();
    final MutableBytes result = MutableBytes.create(64);
    x.copyTo(result, 32 - x.size());
    y.copyTo(result, 64 - y.size());

    return PrecompileContractResult.success(result.copy());
  }

  private static BigInteger extractParameter(
      final Bytes input, final int offset, final int length) {
    if (offset > input.size() || length == 0) {
      return BigInteger.ZERO;
    }
    final byte[] raw = Arrays.copyOfRange(input.toArray(), offset, offset + length);
    return new BigInteger(1, raw);
  }
}
