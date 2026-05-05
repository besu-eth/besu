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

import org.hyperledger.besu.crypto.Hash;
import org.hyperledger.besu.crypto.SECP256K1;
import org.hyperledger.besu.crypto.SECPPublicKey;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.nativelib.secp256k1.LibSecp256k1;
import org.hyperledger.besu.nativelib.secp256k1.LibSecp256k1JNI;

import java.math.BigInteger;
import java.util.Optional;

import com.github.benmanes.caffeine.cache.Cache;
import jakarta.validation.constraints.NotNull;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes;
import org.apache.tuweni.bytes.MutableBytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The ECREC precompiled contract. */
public class ECRECPrecompiledContract extends AbstractPrecompiledContract {

  private static final Logger LOG = LoggerFactory.getLogger(ECRECPrecompiledContract.class);
  private static final int V_BASE = 27;
  private static final int SEMANTIC_INPUT_LENGTH = 128;
  final SignatureAlgorithm signatureAlgorithm;
  private static final String PRECOMPILE_NAME = "ECREC";
  private static final Cache<Integer, PrecompileInputResultTuple> ecrecCache =
      AbstractPrecompiledContract.resultCacheBuilder().build();

  /**
   * Instantiates a new ECREC precompiled contract with the default signature algorithm.
   *
   * @param gasCalculator the gas calculator
   */
  public ECRECPrecompiledContract(final GasCalculator gasCalculator) {
    this(gasCalculator, SignatureAlgorithmFactory.getInstance());
  }

  /**
   * Configure a new ECRecover precompile with a specific signature algorithm and gas.
   *
   * @param gasCalculator the gas calculator
   * @param signatureAlgorithm the algorithm (such as secp256k1 or secp256r1)
   */
  public ECRECPrecompiledContract(
      final GasCalculator gasCalculator, final SignatureAlgorithm signatureAlgorithm) {
    super(PRECOMPILE_NAME, gasCalculator);
    this.signatureAlgorithm = signatureAlgorithm;
  }

  @Override
  public long gasRequirement(final Bytes input) {
    return gasCalculator().getEcrecPrecompiledContractGasCost();
  }

  @NotNull
  @Override
  public PrecompileContractResult computePrecompile(
      final Bytes input, @NotNull final MessageFrame messageFrame) {
    final int size = input.size();
    final Bytes safeInput =
        size >= 128 ? input : Bytes.wrap(input, MutableBytes.create(128 - size));
    // Note that the Yellow Paper defines v as the next 32 bytes (so 32..63). Yet, v is a simple
    // byte in ECDSARECOVER and the Yellow Paper is not very clear on this mismatch, but it appears
    // it is simply the last byte of those 32 bytes that needs to be used. It does appear we need
    // to check the rest of the bytes are zero though.
    if (!safeInput.slice(32, 31).isZero()) {
      return PrecompileContractResult.success(Bytes.EMPTY);
    }

    PrecompileInputResultTuple res;
    Integer cacheKey = null;
    final Bytes cachedInput =
        input.size() > SEMANTIC_INPUT_LENGTH ? input.slice(0, SEMANTIC_INPUT_LENGTH) : input;
    if (enableResultCaching) {
      cacheKey = getCacheKey(input, SEMANTIC_INPUT_LENGTH);
      res = ecrecCache.getIfPresent(cacheKey);

      if (res != null) {
        if (res.cachedInput().equals(cachedInput)) {
          cacheEventConsumer.accept(new CacheEvent(PRECOMPILE_NAME, CacheMetric.HIT));
          return res.cachedResult();
        } else {
          LOG.debug(
              "false positive ecrecover {}, cache key {}, cached input: {}, input: {}",
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

    Bytes resultBytes;
    if (signatureAlgorithm.isNative() && isK1PrecompileSpecificNativeAvailable()) {
      resultBytes = computeK1Native(safeInput);
    } else {
      resultBytes = computeDefault(safeInput);
    }
    res =
        new PrecompileInputResultTuple(
            enableResultCaching ? cachedInput.copy() : input,
            PrecompileContractResult.success(resultBytes));

    if (enableResultCaching) {
      ecrecCache.put(cacheKey, res);
    }
    return res.cachedResult();
  }

  private boolean isK1PrecompileSpecificNativeAvailable() {
    if (!SECP256K1.CURVE_NAME.equals(signatureAlgorithm.getCurveName())) {
      return false;
    }

    try {
      // LibSecp256k1 must load before LibSecp256k1JNI
      return LibSecp256k1.CONTEXT != null && LibSecp256k1JNI.ENABLED;
    } catch (UnsatisfiedLinkError | NoClassDefFoundError ule) {
      LOG.info("ecrecover precompile-specific native lib not available: {}", ule.getMessage());
      return false;
    }
  }

  @NotNull
  private Bytes computeK1Native(final Bytes safeInput) {
    final Bytes32 messageHash = Bytes32.wrap(safeInput, 0);
    final int recId = safeInput.get(63) - V_BASE;
    final byte[] sigBytes = safeInput.slice(64, 64).toArrayUnsafe();

    final LibSecp256k1JNI.ECRecoverResult ecres =
        LibSecp256k1JNI.ecrecover(messageHash.toArrayUnsafe(), sigBytes, recId);

    if (!(ecres.status() == 0) || ecres.publicKey().isEmpty()) {
      return Bytes.EMPTY;
    } else {
      final Bytes32 hashed = Hash.keccak256(Bytes.wrap(ecres.publicKey().get()));
      final MutableBytes32 result = MutableBytes32.create();
      hashed.slice(12).copyTo(result, 12);
      return result;
    }
  }

  @NotNull
  private Bytes computeDefault(final Bytes safeInput) {
    try {
      final Bytes32 messageHash = Bytes32.wrap(safeInput, 0);
      final int recId = safeInput.get(63) - V_BASE;
      final BigInteger r = safeInput.slice(64, 32).toUnsignedBigInteger();
      final BigInteger s = safeInput.slice(96, 32).toUnsignedBigInteger();

      final SECPSignature signature;
      signature = signatureAlgorithm.createSignature(r, s, (byte) recId);

      // SECP256K1#PublicKey#recoverFromSignature throws an Illegal argument exception
      // when it is unable to recover the key. There is not a straightforward way to
      // check the arguments ahead of time to determine if the fail will happen and
      // the library needs to be updated.
      final Optional<SECPPublicKey> recovered =
          signatureAlgorithm.recoverPublicKeyFromSignature(messageHash, signature);
      if (recovered.isEmpty()) {
        return Bytes.EMPTY;
      } else {
        final Bytes32 hashed = Hash.keccak256(recovered.get().getEncodedBytes());
        final MutableBytes32 result = MutableBytes32.create();
        hashed.slice(12).copyTo(result, 12);
        return result;
      }
    } catch (final IllegalArgumentException e) {
      return Bytes.EMPTY;
    }
  }
}
