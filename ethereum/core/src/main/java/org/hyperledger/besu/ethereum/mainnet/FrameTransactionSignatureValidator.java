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
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;

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
   * Why a signature entry was rejected.
   *
   * <p>EIP-8141 separates two kinds of rejection, and tests assert on the distinction: an entry
   * whose <em>shape</em> or declared signer is wrong is a frame format error, while a well-formed
   * entry whose cryptography does not hold is a signature error. Note that the length of the
   * signature blob itself counts as a signature error, because it is a property of the scheme's
   * encoding rather than of the entry's structure.
   */
  public enum Invalidity {
    /** The explicit digest is neither absent nor exactly 32 bytes. */
    DIGEST_SIZE(
        TransactionInvalidReason.INVALID_TRANSACTION_FORMAT,
        "frame signature digest must be empty or 32 bytes"),
    /** The all-zero explicit digest is reserved for the canonical-hash case. */
    DIGEST_EXPLICIT_ZERO(
        TransactionInvalidReason.INVALID_TRANSACTION_FORMAT,
        "explicit zero frame signature digest is invalid"),
    /** The declared signer is present but is not a 20-byte address. */
    SIGNER_SIZE(
        TransactionInvalidReason.INVALID_TRANSACTION_FORMAT,
        "frame signature signer must be empty or a 20-byte address"),
    /** The scheme byte is not one of the defined schemes. */
    UNKNOWN_SCHEME(
        TransactionInvalidReason.INVALID_TRANSACTION_FORMAT, "unknown frame signature scheme"),
    /** An ARBITRARY entry declared a signer. */
    ARBITRARY_WITH_SIGNER(
        TransactionInvalidReason.INVALID_TRANSACTION_FORMAT,
        "ARBITRARY frame signature entries must not declare a signer"),
    /** Recovery or key derivation succeeded but produced a different address. */
    SIGNER_MISMATCH(
        TransactionInvalidReason.INVALID_TRANSACTION_FORMAT,
        "frame signature signer does not match the recovered signer"),
    /** The secp256k1 signature blob is not 65 bytes. */
    SECP256K1_SIGNATURE_SIZE(
        TransactionInvalidReason.INVALID_SIGNATURE, "secp256k1 frame signature must be 65 bytes"),
    /** A secp256k1 v, r or s value lies outside its permitted range. */
    SECP256K1_VALUE_OUT_OF_RANGE(
        TransactionInvalidReason.INVALID_SIGNATURE,
        "secp256k1 frame signature values out of range"),
    /** Public key recovery failed for a well-formed secp256k1 signature. */
    SECP256K1_RECOVERY_FAILED(
        TransactionInvalidReason.INVALID_SIGNATURE, "secp256k1 frame signature recovery failed"),
    /** The P256 signature blob is not 128 bytes. */
    P256_SIGNATURE_SIZE(
        TransactionInvalidReason.INVALID_SIGNATURE, "P256 frame signature must be 128 bytes"),
    /** A P256 r or s value lies outside its permitted range. */
    P256_VALUE_OUT_OF_RANGE(
        TransactionInvalidReason.INVALID_SIGNATURE, "P256 frame signature values out of range"),
    /** The supplied P256 public key is the point at infinity or is not on the curve. */
    P256_PUBLIC_KEY_INVALID(
        TransactionInvalidReason.INVALID_SIGNATURE,
        "P256 frame signature public key is not on the curve"),
    /** The P256 signature does not verify against the supplied key and digest. */
    P256_VERIFICATION_FAILED(
        TransactionInvalidReason.INVALID_SIGNATURE, "P256 frame signature verification failed");

    private final TransactionInvalidReason reason;
    private final String message;

    Invalidity(final TransactionInvalidReason reason, final String message) {
      this.reason = reason;
      this.message = message;
    }

    /**
     * The transaction invalid reason to report.
     *
     * @return the reason
     */
    public TransactionInvalidReason reason() {
      return reason;
    }

    /**
     * The human readable rejection message.
     *
     * @return the message
     */
    public String message() {
      return message;
    }
  }

  /**
   * Validates one signature entry against the transaction sender and canonical signature hash.
   *
   * @param signature the signature entry
   * @param sender the declared transaction sender
   * @param signatureHash the canonical transaction signature hash
   * @return empty when the entry is valid, otherwise why it was rejected
   */
  public static Optional<Invalidity> validate(
      final FrameSignature signature, final Address sender, final Bytes32 signatureHash) {
    final Bytes32 msg;
    if (signature.msg().isEmpty()) {
      msg = signatureHash;
    } else if (signature.msg().size() == 32) {
      if (signature.msg().isZero()) {
        return Optional.of(Invalidity.DIGEST_EXPLICIT_ZERO);
      }
      msg = Bytes32.wrap(signature.msg());
    } else {
      return Optional.of(Invalidity.DIGEST_SIZE);
    }

    if (!signature.signer().isEmpty() && signature.signer().size() != Address.SIZE) {
      return Optional.of(Invalidity.SIGNER_SIZE);
    }

    return switch (signature.scheme()) {
      case FrameSignature.SCHEME_SECP256K1 ->
          validateSecp256k1(signature, signature.resolvedSigner(sender), msg);
      case FrameSignature.SCHEME_P256 ->
          validateP256(signature, signature.resolvedSigner(sender), msg);
      case FrameSignature.SCHEME_ARBITRARY ->
          signature.signer().isEmpty()
              ? Optional.empty()
              : Optional.of(Invalidity.ARBITRARY_WITH_SIGNER);
      default -> Optional.of(Invalidity.UNKNOWN_SCHEME);
    };
  }

  private static Optional<Invalidity> validateSecp256k1(
      final FrameSignature signature, final Address resolvedSigner, final Bytes32 msg) {
    final Bytes raw = signature.signature();
    if (raw.size() != 65) {
      return Optional.of(Invalidity.SECP256K1_SIGNATURE_SIZE);
    }
    final int v = Byte.toUnsignedInt(raw.get(0));
    final BigInteger r = raw.slice(1, 32).toUnsignedBigInteger();
    final BigInteger s = raw.slice(33, 32).toUnsignedBigInteger();
    if (v > 1
        || r.signum() <= 0
        || r.compareTo(SECP256K1N) >= 0
        || s.signum() <= 0
        || s.compareTo(SECP256K1N_HALF) > 0) {
      return Optional.of(Invalidity.SECP256K1_VALUE_OUT_OF_RANGE);
    }
    final SECPSignature secpSignature;
    try {
      secpSignature = SIGNATURE_ALGORITHM.createSignature(r, s, (byte) v);
    } catch (final IllegalArgumentException iae) {
      return Optional.of(Invalidity.SECP256K1_VALUE_OUT_OF_RANGE);
    }
    final Optional<SECPPublicKey> publicKey =
        SIGNATURE_ALGORITHM.recoverPublicKeyFromSignature(msg, secpSignature);
    if (publicKey.isEmpty()) {
      return Optional.of(Invalidity.SECP256K1_RECOVERY_FAILED);
    }
    final Address recovered =
        Address.extract(Bytes32.wrap(Hash.keccak256(publicKey.get().getEncodedBytes())));
    return recovered.getBytes().equals(resolvedSigner.getBytes())
        ? Optional.empty()
        : Optional.of(Invalidity.SIGNER_MISMATCH);
  }

  private static Optional<Invalidity> validateP256(
      final FrameSignature signature, final Address resolvedSigner, final Bytes32 msg) {
    final Bytes raw = signature.signature();
    if (raw.size() != 128) {
      return Optional.of(Invalidity.P256_SIGNATURE_SIZE);
    }
    final BigInteger r = raw.slice(0, 32).toUnsignedBigInteger();
    final BigInteger s = raw.slice(32, 32).toUnsignedBigInteger();
    // Canonical, low-s encodings only; P256VERIFY itself accepts high-s, so signers must
    // normalize before use.
    if (r.signum() <= 0
        || r.compareTo(SECP256R1N) >= 0
        || s.signum() <= 0
        || s.compareTo(SECP256R1N_HALF) > 0) {
      return Optional.of(Invalidity.P256_VALUE_OUT_OF_RANGE);
    }
    final Bytes publicKey = raw.slice(64, 64);
    final Address derivedSigner = Address.extract(Bytes32.wrap(Hash.keccak256(publicKey)));
    if (!derivedSigner.getBytes().equals(resolvedSigner.getBytes())) {
      return Optional.of(Invalidity.SIGNER_MISMATCH);
    }
    // On-curve and infinity checks matching the EIP-7951 precompile.
    final BigInteger qx = raw.slice(64, 32).toUnsignedBigInteger();
    final BigInteger qy = raw.slice(96, 32).toUnsignedBigInteger();
    if (qx.signum() == 0 && qy.signum() == 0) {
      return Optional.of(Invalidity.P256_PUBLIC_KEY_INVALID);
    }
    try {
      final ECPoint point = SECP256R1_ALGORITHM.getCurve().getCurve().createPoint(qx, qy);
      SECP256R1_ALGORITHM.getCurve().validatePublicPoint(point);
    } catch (final IllegalArgumentException iae) {
      return Optional.of(Invalidity.P256_PUBLIC_KEY_INVALID);
    }
    try {
      final SECPSignature secpSignature = SECP256R1_ALGORITHM.createSignature(r, s, (byte) 0);
      final SECPPublicKey secpPublicKey = SECP256R1_ALGORITHM.createPublicKey(publicKey);
      return SECP256R1_ALGORITHM.verify(msg, secpSignature, secpPublicKey)
          ? Optional.empty()
          : Optional.of(Invalidity.P256_VERIFICATION_FAILED);
    } catch (final IllegalArgumentException iae) {
      return Optional.of(Invalidity.P256_VERIFICATION_FAILED);
    }
  }
}
