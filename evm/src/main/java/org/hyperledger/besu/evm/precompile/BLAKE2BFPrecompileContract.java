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

import static java.util.Arrays.copyOfRange;
import static org.hyperledger.besu.crypto.Blake2bfMessageDigest.Blake2bfDigest.MESSAGE_LENGTH_BYTES;

import org.hyperledger.besu.crypto.Hash;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import java.math.BigInteger;
import java.util.Optional;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import jakarta.validation.constraints.NotNull;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The BLAKE2BF precompile contract. */
// https://github.com/keep-network/go-ethereum/pull/4
public class BLAKE2BFPrecompileContract extends AbstractPrecompiledContract {

  private static final Logger LOG = LoggerFactory.getLogger(BLAKE2BFPrecompileContract.class);
  private static final String PRECOMPILE_NAME = "BLAKE2F";

  private static final Cache<Integer, PrecompileInputResultTuple> blakeCache =
      Caffeine.newBuilder()
          .maximumWeight(16_000_000)
          .weigher((k, v) -> Integer.BYTES + ((PrecompileInputResultTuple) v).cachedInput().size())
          .build();

  /**
   * Instantiates a new BLAKE2BF precompile contract.
   *
   * @param gasCalculator the gas calculator
   */
  BLAKE2BFPrecompileContract(final GasCalculator gasCalculator) {
    super(PRECOMPILE_NAME, gasCalculator);
  }

  @Override
  public long gasRequirement(final Bytes input) {
    if (input.size() != MESSAGE_LENGTH_BYTES) {
      // Input is malformed, we can't read the number of rounds.
      // Precompile can't be executed, so we set its price to 0.
      return 0L;
    }
    if ((input.get(212) & 0xFE) != 0) {
      // Input is malformed, F value can be only 0 or 1
      return 0L;
    }

    final byte[] roundsBytes = copyOfRange(input.toArrayUnsafe(), 0, 4);
    final BigInteger rounds = new BigInteger(1, roundsBytes);
    return rounds.longValueExact();
  }

  @NotNull
  @Override
  public PrecompileContractResult computePrecompile(
      final Bytes input, @NotNull final MessageFrame messageFrame) {
    if (input.size() != MESSAGE_LENGTH_BYTES) {
      LOG.trace(
          "Incorrect input length.  Expected {} and got {}", MESSAGE_LENGTH_BYTES, input.size());
      return PrecompileContractResult.halt(
          null, Optional.of(ExceptionalHaltReason.PRECOMPILE_ERROR));
    }
    if ((input.get(212) & 0xFE) != 0) {
      LOG.trace("Incorrect finalization flag, expected 0 or 1 and got {}", input.get(212));
      return PrecompileContractResult.halt(
          null, Optional.of(ExceptionalHaltReason.PRECOMPILE_ERROR));
    }
    PrecompileInputResultTuple res = null;
    Integer cacheKey = null;
    if (enableResultCaching) {
      cacheKey = getCacheKey(input, input.size());
      res = blakeCache.getIfPresent(cacheKey);
      if (res != null) {
        if (res.cachedInput().equals(input)) {
          cacheEventConsumer.accept(new CacheEvent(PRECOMPILE_NAME, CacheMetric.HIT));
          return res.cachedResult();
        } else {
          LOG.info(
              "false positive blake2bf {}, cache key {}, cached input: {}, input: {}",
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

    res =
        new PrecompileInputResultTuple(
            enableResultCaching ? input.copy() : input,
            PrecompileContractResult.success(Hash.blake2bf(input)));

    if (cacheKey != null) {
      blakeCache.put(cacheKey, res);
    }

    return res.cachedResult();
  }
}
