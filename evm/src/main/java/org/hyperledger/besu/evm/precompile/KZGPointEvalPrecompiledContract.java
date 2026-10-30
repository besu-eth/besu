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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.github.benmanes.caffeine.cache.Cache;
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

  private static final CompletableFuture<Void> NO_BACKGROUND_LOAD =
      CompletableFuture.completedFuture(null);

  /**
   * Completes once the trusted setup is loaded. It is only replaced when the setup loads on a
   * background thread; an eager load leaves it completed, so waiting on it costs nothing.
   */
  private static volatile CompletableFuture<Void> trustedSetup = NO_BACKGROUND_LOAD;

  private static final String PRECOMPILE_NAME = "KZGPointEval";
  private static final Cache<Bytes, PrecompileInputResultTuple> kzgCache =
      AbstractPrecompiledContract.resultCacheBuilder().build();
  private static final int PREFIX_LENGTH = 192;

  /** Default result caching to false unless otherwise set. */
  protected static Boolean enableResultCaching = Boolean.FALSE;

  private static final Logger LOG = LoggerFactory.getLogger(KZGPointEvalPrecompiledContract.class);

  private static Bytes successResult;

  @VisibleForTesting
  static void loadLib() {
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
    init(trustedSetupFile, false);
  }

  /**
   * Init the C-KZG native lib using a file as trusted setup
   *
   * @param trustedSetupFile the file with the trusted setup
   * @param loadInBackground whether to load the trusted setup on a background thread, see {@link
   *     #awaitTrustedSetup()}
   * @throws IllegalStateException if the trusted setup was already loaded
   */
  public static void init(final Path trustedSetupFile, final boolean loadInBackground) {
    if (loaded.compareAndSet(false, true)) {
      loadLib();
      final String trustedSetupResourceName = trustedSetupFile.toAbsolutePath().toString();
      LOG.info("Loading trusted setup from user-specified resource {}", trustedSetupResourceName);
      loadTrustedSetup(
          () -> CKZG4844JNI.loadTrustedSetup(trustedSetupResourceName, 0), loadInBackground);
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
    init(false);
  }

  /**
   * Init the C-KZG native lib using mainnet trusted setup
   *
   * @param loadInBackground whether to load the trusted setup on a background thread, see {@link
   *     #awaitTrustedSetup()}
   */
  public static void init(final boolean loadInBackground) {
    if (loaded.compareAndSet(false, true)) {
      loadLib();
      final String trustedSetupResourceName = "/kzg-trusted-setups/mainnet.txt";
      LOG.info(
          "Loading network trusted setup from classpath resource {}", trustedSetupResourceName);
      loadTrustedSetup(
          () ->
              CKZG4844JNI.loadTrustedSetupFromResource(
                  trustedSetupResourceName, KZGPointEvalPrecompiledContract.class, 0),
          loadInBackground);
    }
  }

  /**
   * Load the trusted setup, either on the calling thread or on a background thread.
   *
   * <p>On a background thread, the load runs alongside the rest of startup. Every KZG operation
   * waits for it through {@link #awaitTrustedSetup()}, so only the time at which the setup is
   * loaded changes.
   *
   * @param load the native call that loads the trusted setup
   * @param loadInBackground whether to run the load on a background thread
   */
  @VisibleForTesting
  static void loadTrustedSetup(final Runnable load, final boolean loadInBackground) {
    if (!loadInBackground) {
      trustedSetup = NO_BACKGROUND_LOAD;
      load.run();
      return;
    }
    LOG.info("Loading the trusted setup on a background thread");
    final long start = System.nanoTime();
    trustedSetup =
        CompletableFuture.runAsync(
                load,
                task -> Thread.ofPlatform().name("kzg-trusted-setup-loader").daemon().start(task))
            .whenComplete(
                (ignored, loadFailed) -> {
                  if (loadFailed == null) {
                    LOG.info(
                        "Loaded the trusted setup in {} ms",
                        (System.nanoTime() - start) / 1_000_000);
                  } else {
                    LOG.error("Failed to load the KZG trusted setup", loadFailed);
                  }
                });
  }

  /**
   * Wait until the trusted setup is loaded. Every KZG operation calls this before using the setup.
   * It returns at once unless the setup is loading on a background thread.
   *
   * @throws IllegalStateException if the trusted setup failed to load on the background thread
   */
  public static void awaitTrustedSetup() {
    try {
      trustedSetup.join();
    } catch (final CompletionException loadFailed) {
      throw new IllegalStateException(
          "KZG trusted setup failed to load: " + loadFailed.getCause(), loadFailed.getCause());
    }
  }

  /** free up resources. */
  @VisibleForTesting
  public static void tearDown() {
    try {
      // a background load must finish before the setup is freed
      trustedSetup.join();
      CKZG4844JNI.freeTrustedSetup();
    } catch (final CompletionException loadFailed) {
      // nothing was loaded, so there is nothing to free
    } finally {
      trustedSetup = NO_BACKGROUND_LOAD;
      loaded.set(false);
    }
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

    if (input.size() != PREFIX_LENGTH) {
      return PrecompileContractResult.halt(
          null, Optional.of(ExceptionalHaltReason.PRECOMPILE_ERROR));
    }

    PrecompileInputResultTuple res;
    Bytes cacheKey = null;

    if (enableResultCaching) {
      cacheKey = AbstractPrecompiledContract.getCacheKey(input, PREFIX_LENGTH);
      res = kzgCache.getIfPresent(cacheKey);
      if (res != null) {
        if (res.cachedInput().equals(input)) {
          cacheEventConsumer.accept(
              new AbstractPrecompiledContract.CacheEvent(
                  PRECOMPILE_NAME, AbstractPrecompiledContract.CacheMetric.HIT));
          return res.cachedResult();
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug(
                "false positive kzgPointEval {}, cache key {}, cached input: {}, input: {}",
                input.getClass().getSimpleName(),
                cacheKey,
                res.cachedInput().toHexString(),
                input.toHexString());
          }

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
    // Outside the try below: a failed load must not be mistaken for an invalid proof.
    awaitTrustedSetup();
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
