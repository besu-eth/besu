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
package org.hyperledger.besu.evm.precompile;

import org.hyperledger.besu.crypto.SECP256R1;
import org.hyperledger.besu.crypto.SECPPublicKey;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.nativelib.boringssl.BoringSSLPrecompiles;

import java.math.BigInteger;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import jakarta.validation.constraints.NotNull;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.bouncycastle.asn1.sec.SECNamedCurves;
import org.bouncycastle.asn1.x9.X9ECParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation of EIP-7951. */
public class P256VerifyPrecompiledContract extends AbstractPrecompiledContract {
  private static final Logger LOG = LoggerFactory.getLogger(P256VerifyPrecompiledContract.class);
  private static final String PRECOMPILE_NAME = "P256VERIFY";
  private static final Bytes32 VALID = Bytes32.leftPad(Bytes.of(1), (byte) 0);
  private static final Bytes INVALID = Bytes.EMPTY;
  private static final int SECP256R1_INPUT_LENGTH = 160;
  private static final SignatureAlgorithm SECP256R1_SIGNATURE_ALGORITHM_INSTANCE;

  static final X9ECParameters R1_PARAMS = SECNamedCurves.getByName("secp256r1");
  static final BigInteger N = R1_PARAMS.getN();
  static final BigInteger P = R1_PARAMS.getCurve().getField().getCharacteristic();

  /** The constant useNative. */
  // use the BoringSSL native library implementation, if it is available
  private static boolean useNativeBoringSSL;

  static {
    maybeEnableNativeBoringSSL();
    SECP256R1_SIGNATURE_ALGORITHM_INSTANCE = new SECP256R1();
  }

  /**
   * Attempt to enable the BoringSSL native library for P256Verify contract Note, if BoringSSL is
   * disabled, then native SECP256R1 may still be enabled
   *
   * @return true if the native library was enabled.
   */
  public static boolean maybeEnableNativeBoringSSL() {
    try {
      useNativeBoringSSL = BoringSSLPrecompiles.ENABLED;
    } catch (UnsatisfiedLinkError | NoClassDefFoundError ule) {
      LOG.info(
          "BoringSSL secp256r1 p256verify native precompile not available: {}", ule.getMessage());
      useNativeBoringSSL = false;
    }
    return useNativeBoringSSL;
  }

  /** Disable native. Note SECP256R1 must additionally be disabled to fully disable native */
  public static void disableNativeBoringSSL() {
    useNativeBoringSSL = false;
  }

  /**
   * Is native BoringSSL boolean.
   *
   * @return the boolean indicating whether to use the BoringSSL native library implementation, if
   *     it is available
   */
  public static boolean isNativeBoringSSL() {
    return useNativeBoringSSL;
  }

  private final GasCalculator gasCalculator;
  private final SignatureAlgorithm signatureAlgorithm;

  private static final Cache<Integer, PrecompileInputResultTuple> p256VerifyCache =
      Caffeine.newBuilder().maximumSize(1000).build();

  /**
   * Instantiates a new Abstract precompiled contract.
   *
   * @param gasCalculator the gas calculator
   */
  public P256VerifyPrecompiledContract(final GasCalculator gasCalculator) {
    this(gasCalculator, SECP256R1_SIGNATURE_ALGORITHM_INSTANCE);
  }

  /**
   * Instantiates a new Abstract precompiled contract.
   *
   * @param gasCalculator the gas calculator
   * @param signatureAlgorithm the signature algorithm to use
   */
  public P256VerifyPrecompiledContract(
      final GasCalculator gasCalculator, final SignatureAlgorithm signatureAlgorithm) {
    super(PRECOMPILE_NAME, gasCalculator);
    this.gasCalculator = gasCalculator;
    this.signatureAlgorithm = signatureAlgorithm;
  }

  @Override
  public long gasRequirement(final Bytes input) {
    return gasCalculator.getP256VerifyPrecompiledContractGasCost();
  }

  @Override
  public PrecompileContractResult computePrecompile(
      final Bytes input, final MessageFrame messageFrame) {
    if (input.size() != SECP256R1_INPUT_LENGTH) {
      LOG.debug(
          "Invalid input length for P256VERIFY precompile: expected {} bytes but got {}",
          SECP256R1_INPUT_LENGTH,
          input.size());
      return PrecompileContractResult.success(INVALID);
    }
    PrecompileInputResultTuple res = null;
    Integer cacheKey = null;
    if (enableResultCaching) {
      cacheKey = getCacheKey(input);
      res = p256VerifyCache.getIfPresent(cacheKey);

      if (res != null) {
        if (res.cachedInput().equals(input)) {
          cacheEventConsumer.accept(new CacheEvent(PRECOMPILE_NAME, CacheMetric.HIT));
          return res.cachedResult();
        } else {
          LOG.debug(
              "false positive p256verify {}, cache key {}, cached input: {}, input: {}",
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

    try {
      if (useNativeBoringSSL) {
        res = computeNative(input);
      } else {
        res = computeDefault(input);
      }

      if (enableResultCaching) {
        p256VerifyCache.put(cacheKey, res);
      }
      return res.cachedResult();

    } catch (Exception e) {
      LOG.warn("P256VERIFY verification failed: {}", e.getMessage());
      System.err.println("P256VERIFY verification failed: " + e.getMessage());
      return PrecompileContractResult.success(INVALID);
    }
  }

  @NotNull
  private static PrecompileInputResultTuple computeNative(final Bytes input) {
    PrecompileInputResultTuple res;
    BoringSSLPrecompiles.P256VerifyResult result =
        BoringSSLPrecompiles.p256Verify(input.toArrayUnsafe(), input.size());

    if (result.status != 0) {
      LOG.atTrace().setMessage("Verify failed: {}").addArgument(result.error).log();
    }
    res =
        new PrecompileInputResultTuple(
            enableResultCaching ? input.copy() : input,
            PrecompileContractResult.success(result.status == 0 ? VALID : INVALID));
    return res;
  }

  // This may still use native SECP256R1 depending on how SignatureAlgorithm is configured
  @NotNull
  PrecompileInputResultTuple computeDefault(final Bytes input) {
    final Bytes messageHash = input.slice(0, 32);
    final Bytes rBytes = input.slice(32, 32);
    final Bytes sBytes = input.slice(64, 32);
    final Bytes pubKeyBytes = input.slice(96, 64);
    final BigInteger qx = pubKeyBytes.slice(0, 32).toUnsignedBigInteger();
    final BigInteger qy = pubKeyBytes.slice(32, 32).toUnsignedBigInteger();

    // Convert r and s to BigIntegers (unsigned)
    final BigInteger r = rBytes.toUnsignedBigInteger();
    final BigInteger s = sBytes.toUnsignedBigInteger();

    // Check r, s in (0, n)
    if (r.signum() <= 0 || r.compareTo(N) >= 0 || s.signum() <= 0 || s.compareTo(N) >= 0) {
      LOG.trace("Invalid r or s: must satisfy 0 < r,s < n");
      return new PrecompileInputResultTuple(
          enableResultCaching ? input.copy() : input, PrecompileContractResult.success(INVALID));
    }

    // Check qx, qy in [0, p)
    if (qx.signum() < 0 || qx.compareTo(P) >= 0 || qy.signum() < 0 || qy.compareTo(P) >= 0) {
      LOG.trace("Invalid qx or qy: must satisfy 0 <= qx,qy < p");
      return new PrecompileInputResultTuple(
          enableResultCaching ? input.copy() : input, PrecompileContractResult.success(INVALID));
    }

    // Check point not at infinity (qx, qy ≠ 0,0), and non-trivial infinity encoding
    if ((qx.signum() == 0 && qy.signum() == 0)) {
      LOG.trace("Invalid public key: point at infinity");
      return new PrecompileInputResultTuple(
          enableResultCaching ? input.copy() : input, PrecompileContractResult.success(INVALID));
    }
    try {
      final org.bouncycastle.math.ec.ECPoint ecPoint = R1_PARAMS.getCurve().createPoint(qx, qy);
      signatureAlgorithm.getCurve().validatePublicPoint(ecPoint);
    } catch (IllegalArgumentException e) {
      LOG.trace("Public key not on curve: {}", e.getMessage());
      return new PrecompileInputResultTuple(
          enableResultCaching ? input.copy() : input, PrecompileContractResult.success(INVALID));
    }
    // Create the signature; recID is not used in verification - use 0
    final SECPSignature signature = signatureAlgorithm.createSignature(r, s, (byte) 0);
    final SECPPublicKey publicKey = signatureAlgorithm.createPublicKey(pubKeyBytes);

    final boolean isValid = signatureAlgorithm.verifyMalleable(messageHash, signature, publicKey);

    return new PrecompileInputResultTuple(
        enableResultCaching ? input.copy() : input,
        PrecompileContractResult.success(isValid ? VALID : INVALID));
  }
}
