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
package org.hyperledger.besu.crypto;

import org.hyperledger.besu.nativelib.secp256r1.LibSECP256R1;
import org.hyperledger.besu.nativelib.secp256r1.Signature;
import org.hyperledger.besu.nativelib.secp256r1.besuNativeEC.BesuNativeEC;

import java.math.BigInteger;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.bouncycastle.crypto.signers.DSAKCalculator;
import org.bouncycastle.crypto.signers.RandomDSAKCalculator;
import org.bouncycastle.math.ec.custom.sec.SecP256R1Curve;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The SECP256R1 implementation. */
public class SECP256R1 extends AbstractSECP256 {

  private static final Logger LOG = LoggerFactory.getLogger(SECP256R1.class);

  /** The constant CURVE_NAME. */
  public static final String CURVE_NAME = "secp256r1";

  private boolean useNative;
  private final LibSECP256R1 libSECP256R1 = new LibSECP256R1();

  /** Instantiates a new SECP256R1. */
  public SECP256R1() {
    super(CURVE_NAME, SecP256R1Curve.q);
    try {
      useNative = BesuNativeEC.ENABLED;
    } catch (UnsatisfiedLinkError ule) {
      LOG.info("secp256r1 native precompile not available: {}", ule.getMessage());
      useNative = false;
    }
  }

  @Override
  public void disableNative() {
    useNative = false;
  }

  @Override
  public boolean isNative() {
    return useNative;
  }

  /**
   * Attempt to enable the native library for secp256r1
   *
   * @return true if the native library was enabled.
   */
  @Override
  public boolean maybeEnableNative() {
    try {
      useNative = BesuNativeEC.ENABLED;
    } catch (UnsatisfiedLinkError | NoClassDefFoundError e) {
      LOG.info("Native secp256r1 not available - {}", e.getMessage());
      useNative = false;
    }
    return useNative;
  }

  /**
   * Check if the native library is available.
   *
   * @return true if the native library is available, false otherwise.
   */
  public static boolean isNativeAvailable() {
    try {
      return BesuNativeEC.ENABLED;
    } catch (UnsatisfiedLinkError | NoClassDefFoundError e) {
      return false;
    }
  }

  /**
   * SECP256R1 is using the non-deterministic implementation of K calculation (standard)
   *
   * @return an instance of RandomDSAKCalculator
   */
  @Override
  public DSAKCalculator getKCalculator() {
    return new RandomDSAKCalculator();
  }

  @Override
  public String getCurveName() {
    return CURVE_NAME;
  }

  @Override
  public SECPSignature sign(final Bytes32 dataHash, final KeyPair keyPair) {
    if (useNative) {
      return signNative(dataHash, keyPair);
    } else {
      return super.sign(dataHash, keyPair);
    }
  }

  @Override
  public boolean verify(final Bytes data, final SECPSignature signature, final SECPPublicKey pub) {
    if (useNative) {
      return verifyNative(data, signature, pub, false);
    } else {
      return super.verify(data, signature, pub);
    }
  }

  @Override
  public boolean verifyMalleable(
      final Bytes data, final SECPSignature signature, final SECPPublicKey pub) {
    if (useNative) {
      return verifyNative(data, signature, pub, true);
    } else {
      return super.verify(data, signature, pub);
    }
  }

  @Override
  public Optional<SECPPublicKey> recoverPublicKeyFromSignature(
      final Bytes32 dataHash, final SECPSignature signature) {
    if (useNative) {
      return recoverPublicKeyFromSignatureNative(dataHash, signature);
    } else {
      return super.recoverPublicKeyFromSignature(dataHash, signature);
    }
  }

  private SECPSignature signNative(final Bytes32 dataHash, final KeyPair keyPair) {
    Signature nativeSignature =
        libSECP256R1.sign(
            dataHash.toArrayUnsafe(),
            keyPair.getPrivateKey().getEncoded(),
            keyPair.getPublicKey().getEncoded());

    return new SECPSignature(
        new BigInteger(nativeSignature.getR()),
        new BigInteger(nativeSignature.getS()),
        nativeSignature.getV());
  }

  @Override
  protected @Nullable BigInteger recoverFromSignature(
      final int recId, final BigInteger r, final BigInteger s, final Bytes32 dataHash) {
    if (useNative) {
      return recoverPublicKeyFromSignatureNative(dataHash, new SECPSignature(r, s, (byte) recId))
          .map(key -> new BigInteger(1, key.getEncoded()))
          .orElse(null);
    } else {
      return super.recoverFromSignature(recId, r, s, dataHash);
    }
  }

  private Optional<SECPPublicKey> recoverPublicKeyFromSignatureNative(
      final Bytes32 dataHash, final SECPSignature signature) {
    byte[] recoveredKey;

    try {
      recoveredKey =
          libSECP256R1.keyRecovery(
              dataHash.toArrayUnsafe(),
              signature.getR().toByteArray(),
              signature.getS().toByteArray(),
              signature.getRecId());
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }

    return Optional.of(SECPPublicKey.create(Bytes.of(recoveredKey), ALGORITHM));
  }

  private boolean verifyNative(
      final Bytes data,
      final SECPSignature signature,
      final SECPPublicKey pub,
      final boolean allowMalleable) {

    return libSECP256R1.verify(
        data.toArrayUnsafe(),
        signature.getR().toByteArray(),
        signature.getS().toByteArray(),
        pub.getEncoded(),
        allowMalleable);
  }
}
