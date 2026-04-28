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
package org.hyperledger.besu.evm.precompile;

import static org.hyperledger.besu.evm.precompile.AbstractPrecompiledContract.cacheEventConsumer;

import org.hyperledger.besu.crypto.Hash;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.internal.Words;

import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import ethereum.ckzg4844.CKZG4844JNI;
import jakarta.validation.constraints.NotNull;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The KZGPointEval precompile contract. */
public class KZGPointEvalPrecompiledContract implements PrecompiledContract {
  private static final AtomicBoolean loaded = new AtomicBoolean(false);

  private static final String PRECOMPILE_NAME = "KZGPointEval";
  private static final Cache<Integer, PrecompileInputResultTuple> kzgCache =
      Caffeine.newBuilder()
          .maximumWeight(16_000_000)
          .weigher((k, v) -> Integer.BYTES + ((PrecompileInputResultTuple) v).cachedInput().size())
          .build();

  /** Default result caching to false unless otherwise set. */
  protected static Boolean enableResultCaching = Boolean.FALSE;

  private static final Logger LOG = LoggerFactory.getLogger(KZGPointEvalPrecompiledContract.class);

  private static Bytes successResult;

  private static void loadLib() {
    CKZG4844JNI.loadNativeLibrary();
    Bytes fieldElementsPerBlob =
        Bytes32.wrap(Words.intBytes(CKZG4844JNI.FIELD_ELEMENTS_PER_BLOB).xor(Bytes32.ZERO));
    Bytes blsModulus =
        Bytes32.wrap(Bytes.of(CKZG4844JNI.BLS_MODULUS.toByteArray()).xor(Bytes32.ZERO));

    successResult = Bytes.concatenate(fieldElementsPerBlob, blsModulus);
  }

  /**
   * Init the C-KZG native lib using a file as trusted setup
   *
   * @param trustedSetupFile the file with the trusted setup
   * @throws IllegalStateException is the trusted setup was already loaded
   */
  public static void init(final Path trustedSetupFile) {
    if (loaded.compareAndSet(false, true)) {
      loadLib();
      final String trustedSetupResourceName = trustedSetupFile.toAbsolutePath().toString();
      LOG.info("Loading trusted setup from user-specified resource {}", trustedSetupResourceName);
      CKZG4844JNI.loadTrustedSetup(trustedSetupResourceName, 0);
    } else {
      throw new IllegalStateException("KZG trusted setup was already loaded");
    }
  }

  /**
   * Init the C-KZG native lib using mainnet trusted setup
   *
   * @throws IllegalStateException is the trusted setup was already loaded
   */
  public static void init() {
    if (loaded.compareAndSet(false, true)) {
      loadLib();
      final String trustedSetupResourceName = "/kzg-trusted-setups/mainnet.txt";
      LOG.info(
          "Loading network trusted setup from classpath resource {}", trustedSetupResourceName);
      CKZG4844JNI.loadTrustedSetupFromResource(
          trustedSetupResourceName, KZGPointEvalPrecompiledContract.class, 0);
    }
  }

  /** free up resources. */
  @VisibleForTesting
  public static void tearDown() {
    CKZG4844JNI.freeTrustedSetup();
    loaded.set(false);
  }

  /**
   * Enable or disable precompile result caching.
   *
   * @param enablePrecompileCaching boolean indicating whether to cache precompile results
   */
  public static void setPrecompileCaching(final boolean enablePrecompileCaching) {
    enableResultCaching = enablePrecompileCaching;
  }

  /** Default constructor. */
  KZGPointEvalPrecompiledContract() {}

  @Override
  public String getName() {
    return "KZG_POINT_EVALUATION";
  }

  @Override
  public long gasRequirement(final Bytes input) {
    // As defined in EIP-4844
    return 50000;
  }

  @NotNull
  @Override
  public PrecompileContractResult computePrecompile(
      final Bytes input, @NotNull final MessageFrame messageFrame) {

    if (input.size() != 192) {
      return PrecompileContractResult.halt(
          null, Optional.of(ExceptionalHaltReason.PRECOMPILE_ERROR));
    }

    PrecompileInputResultTuple res;
    Integer cacheKey = null;

    if (enableResultCaching) {
      cacheKey = AbstractPrecompiledContract.getCacheKey(input, input.size());
      res = kzgCache.getIfPresent(cacheKey);
      if (res != null) {
        if (res.cachedInput().equals(input)) {
          cacheEventConsumer.accept(
              new AbstractPrecompiledContract.CacheEvent(
                  PRECOMPILE_NAME, AbstractPrecompiledContract.CacheMetric.HIT));
          return res.cachedResult();
        } else {
          LOG.debug(
              "false positive kzgPointEval {}, cache key {}, cached input: {}, input: {}",
              input.getClass().getSimpleName(),
              cacheKey,
              res.cachedInput().toHexString(),
              input.toHexString());

          cacheEventConsumer.accept(
              new AbstractPrecompiledContract.CacheEvent(
                  PRECOMPILE_NAME, AbstractPrecompiledContract.CacheMetric.FALSE_POSITIVE));
        }
      } else {
        cacheEventConsumer.accept(
            new AbstractPrecompiledContract.CacheEvent(
                PRECOMPILE_NAME, AbstractPrecompiledContract.CacheMetric.MISS));
      }
    }

    Bytes32 versionedHash = Bytes32.wrap(input.slice(0, 32));
    Bytes z = input.slice(32, 32);
    Bytes y = input.slice(64, 32);
    Bytes commitment = input.slice(96, 48);
    Bytes proof = input.slice(144, 48);
    if (versionedHash.get(0) != 0x01) { // unsupported hash version
      return PrecompileContractResult.halt(
          null, Optional.of(ExceptionalHaltReason.PRECOMPILE_ERROR));
    } else {
      byte[] hash = Hash.sha256(commitment).toArrayUnsafe();
      hash[0] = 0x01;
      if (!versionedHash.equals(Bytes32.wrap(hash))) {
        return PrecompileContractResult.halt(
            null, Optional.of(ExceptionalHaltReason.PRECOMPILE_ERROR));
      }
    }
    try {
      boolean proved =
          CKZG4844JNI.verifyKzgProof(
              commitment.toArray(), z.toArray(), y.toArray(), proof.toArray());

      if (proved) {
        res =
            new PrecompileInputResultTuple(
                enableResultCaching ? input.copy() : input,
                PrecompileContractResult.success(successResult));
      } else {
        res =
            new PrecompileInputResultTuple(
                enableResultCaching ? input.copy() : input,
                PrecompileContractResult.halt(
                    null, Optional.of(ExceptionalHaltReason.PRECOMPILE_ERROR)));
      }
      if (cacheKey != null) {
        kzgCache.put(cacheKey, res);
      }
      return res.cachedResult();
    } catch (RuntimeException kzgFailed) {
      LOG.debug("Native KZG failed", kzgFailed);
      res =
          new PrecompileInputResultTuple(
              enableResultCaching ? input.copy() : input,
              PrecompileContractResult.halt(
                  null, Optional.of(ExceptionalHaltReason.PRECOMPILE_ERROR)));
      if (cacheKey != null) {
        kzgCache.put(cacheKey, res);
      }
      return res.cachedResult();
    }
  }
}
