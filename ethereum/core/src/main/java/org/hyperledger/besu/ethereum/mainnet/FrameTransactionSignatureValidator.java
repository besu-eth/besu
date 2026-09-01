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
package org.hyperledger.besu.ethereum.mainnet;

import org.hyperledger.besu.crypto.Hash;
import org.hyperledger.besu.crypto.SECP256R1;
import org.hyperledger.besu.crypto.SECPPublicKey;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.core.FrameSignature;

import java.math.BigInteger;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.bouncycastle.math.ec.ECPoint;

/**
 * Protocol-side validation of EIP-8141 signature entries. Runs outside the EVM, before any frame
 * executes; the ecrecover and P256VERIFY precompiles are therefore never entered.
 */
public final class FrameTransactionSignatureValidator {

  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();

  private static final BigInteger SECP256K1N =
      new BigInteger("fffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141", 16);
  private static final BigInteger SECP256K1N_HALF = SECP256K1N.shiftRight(1);
  private static final BigInteger SECP256R1N =
      new BigInteger("ffffffff00000000ffffffffffffffffbce6faada7179e84f3b9cac2fc632551", 16);
  private static final BigInteger SECP256R1N_HALF = SECP256R1N.shiftRight(1);

  private static final SECP256R1 SECP256R1_ALGORITHM = new SECP256R1();

  private FrameTransactionSignatureValidator() {}

  /**
   * Validates one signature entry against the transaction sender and canonical signature hash.
   *
   * @param signature the signature entry
   * @param sender the declared transaction sender
   * @param signatureHash the canonical transaction signature hash
   * @return true when the entry is valid
   */
  public static boolean validate(
      final FrameSignature signature, final Address sender, final Bytes32 signatureHash) {
    final Bytes32 msg;
    if (signature.msg().isEmpty()) {
      msg = signatureHash;
    } else if (signature.msg().size() == 32) {
      if (signature.msg().isZero()) {
        // The all-zero explicit digest is reserved for the canonical-hash case.
        return false;
      }
      msg = Bytes32.wrap(signature.msg());
    } else {
      return false;
    }

    if (!signature.signer().isEmpty() && signature.signer().size() != Address.SIZE) {
      return false;
    }

    return switch (signature.scheme()) {
      case FrameSignature.SCHEME_SECP256K1 ->
          validateSecp256k1(signature, signature.resolvedSigner(sender), msg);
      case FrameSignature.SCHEME_P256 ->
          validateP256(signature, signature.resolvedSigner(sender), msg);
      case FrameSignature.SCHEME_ARBITRARY -> signature.signer().isEmpty();
      default -> false;
    };
  }

  private static boolean validateSecp256k1(
      final FrameSignature signature, final Address resolvedSigner, final Bytes32 msg) {
    final Bytes raw = signature.signature();
    if (raw.size() != 65) {
      return false;
    }
    final int v = Byte.toUnsignedInt(raw.get(0));
    final BigInteger r = raw.slice(1, 32).toUnsignedBigInteger();
    final BigInteger s = raw.slice(33, 32).toUnsignedBigInteger();
    if (v > 1
        || r.signum() <= 0
        || r.compareTo(SECP256K1N) >= 0
        || s.signum() <= 0
        || s.compareTo(SECP256K1N_HALF) > 0) {
      return false;
    }
    final SECPSignature secpSignature;
    try {
      secpSignature = SIGNATURE_ALGORITHM.createSignature(r, s, (byte) v);
    } catch (final IllegalArgumentException iae) {
      return false;
    }
    final Optional<SECPPublicKey> publicKey =
        SIGNATURE_ALGORITHM.recoverPublicKeyFromSignature(msg, secpSignature);
    return publicKey
        .map(key -> Address.extract(Bytes32.wrap(Hash.keccak256(key.getEncodedBytes()))))
        .map(recovered -> recovered.getBytes().equals(resolvedSigner.getBytes()))
        .orElse(false);
  }

  private static boolean validateP256(
      final FrameSignature signature, final Address resolvedSigner, final Bytes32 msg) {
    final Bytes raw = signature.signature();
    if (raw.size() != 128) {
      return false;
    }
    final BigInteger r = raw.slice(0, 32).toUnsignedBigInteger();
    final BigInteger s = raw.slice(32, 32).toUnsignedBigInteger();
    // Canonical, low-s encodings only; P256VERIFY itself accepts high-s, so signers must
    // normalize before use.
    if (r.signum() <= 0
        || r.compareTo(SECP256R1N) >= 0
        || s.signum() <= 0
        || s.compareTo(SECP256R1N_HALF) > 0) {
      return false;
    }
    final Bytes publicKey = raw.slice(64, 64);
    final Address derivedSigner = Address.extract(Bytes32.wrap(Hash.keccak256(publicKey)));
    if (!derivedSigner.getBytes().equals(resolvedSigner.getBytes())) {
      return false;
    }
    // On-curve and infinity checks matching the EIP-7951 precompile.
    final BigInteger qx = raw.slice(64, 32).toUnsignedBigInteger();
    final BigInteger qy = raw.slice(96, 32).toUnsignedBigInteger();
    if (qx.signum() == 0 && qy.signum() == 0) {
      return false;
    }
    try {
      final ECPoint point = SECP256R1_ALGORITHM.getCurve().getCurve().createPoint(qx, qy);
      SECP256R1_ALGORITHM.getCurve().validatePublicPoint(point);
    } catch (final IllegalArgumentException iae) {
      return false;
    }
    try {
      final SECPSignature secpSignature = SECP256R1_ALGORITHM.createSignature(r, s, (byte) 0);
      final SECPPublicKey secpPublicKey = SECP256R1_ALGORITHM.createPublicKey(publicKey);
      return SECP256R1_ALGORITHM.verify(msg, secpSignature, secpPublicKey);
    } catch (final IllegalArgumentException iae) {
      return false;
    }
  }
}
