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
package org.hyperledger.besu.ethereum.mainnet;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.crypto.Hash;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECP256R1;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.core.FrameSignature;
import org.hyperledger.besu.ethereum.mainnet.FrameTransactionSignatureValidator.Invalidity;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;

import java.math.BigInteger;
import java.util.Optional;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class FrameTransactionSignatureValidatorTest {

  private static final SignatureAlgorithm SECP256K1 = SignatureAlgorithmFactory.getInstance();
  private static final BigInteger SECP256K1_N = SECP256K1.getCurve().getN();
  private static final SECP256R1 P256 = new SECP256R1();
  private static final BigInteger P256_N = P256.getCurve().getN();
  private static final BigInteger P256_P =
      P256.getCurve().getCurve().getField().getCharacteristic();

  private static final Bytes32 SIGNATURE_HASH =
      Bytes32.fromHexString("0x8141814181418141814181418141814181418141814181418141814181418141");
  private static final Bytes32 OTHER_DIGEST =
      Bytes32.fromHexString("0x2222222222222222222222222222222222222222222222222222222222222222");
  private static final Address OTHER_ADDRESS =
      Address.fromHexString("0x000000000000000000000000000000000000dead");

  // RIP-7212 / EIP-7951 P256VERIFY test vector: a valid, low-s signature of DIGEST by (QX, QY).
  private static final Bytes32 P256_DIGEST =
      Bytes32.fromHexString("0x4cee90eb86eaa050036147a12d49004b6b9c72bd725d39d4785011fe190f0b4d");
  private static final Bytes32 P256_R =
      Bytes32.fromHexString("0xa73bd4903f0ce3b639bbbf6e8e80d16931ff4bcf5993d58468e8fb19086e8cac");
  private static final Bytes32 P256_S =
      Bytes32.fromHexString("0x36dbcd03009df8c59286b162af3bd7fcc0450c9aa81be5d10d312af6c66b1d60");
  private static final Bytes32 P256_QX =
      Bytes32.fromHexString("0x4aebd3099c618202fcfe16ae7770b0c49ab5eadf74b754204a3bb6060e44eff3");
  private static final Bytes32 P256_QY =
      Bytes32.fromHexString("0x7618b065f9832de4ca6ca971a7a1adc826d0f7c00181a5fb2ddf79ae00b4e10e");

  private final KeyPair keyPair = SECP256K1.generateKeyPair();
  private final Address signer = Address.extract(keyPair.getPublicKey());

  // ---------------------------------------------------------------------------------------------
  // Entry shape (checked before the scheme)
  // ---------------------------------------------------------------------------------------------

  @ParameterizedTest
  @ValueSource(ints = {1, 31, 33, 64})
  void digestThatIsNeitherEmptyNorThirtyTwoBytesIsAFormatError(final int size) {
    final FrameSignature entry =
        new FrameSignature(
            FrameSignature.SCHEME_ARBITRARY,
            Bytes.EMPTY,
            Bytes.repeat((byte) 1, size),
            Bytes.EMPTY);

    assertThat(validate(entry, signer)).contains(Invalidity.DIGEST_SIZE);
  }

  @Test
  void explicitZeroDigestIsRejected() {
    final FrameSignature entry =
        new FrameSignature(
            FrameSignature.SCHEME_SECP256K1,
            Bytes.EMPTY,
            Bytes32.ZERO,
            secp256k1Signature(Bytes32.ZERO, keyPair));

    assertThat(validate(entry, signer)).contains(Invalidity.DIGEST_EXPLICIT_ZERO);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 19, 21, 32})
  void signerThatIsNeitherEmptyNorAnAddressIsAFormatError(final int size) {
    final FrameSignature entry =
        new FrameSignature(
            FrameSignature.SCHEME_SECP256K1,
            Bytes.repeat((byte) 1, size),
            Bytes.EMPTY,
            secp256k1Signature(SIGNATURE_HASH, keyPair));

    assertThat(validate(entry, signer)).contains(Invalidity.SIGNER_SIZE);
  }

  @ParameterizedTest
  @ValueSource(ints = {3, 4, 0xff, -1})
  void undefinedSchemeIsRejected(final int scheme) {
    final FrameSignature entry =
        new FrameSignature(scheme, Bytes.EMPTY, Bytes.EMPTY, Bytes.repeat((byte) 1, 65));

    assertThat(validate(entry, signer)).contains(Invalidity.UNKNOWN_SCHEME);
  }

  // ---------------------------------------------------------------------------------------------
  // ARBITRARY
  // ---------------------------------------------------------------------------------------------

  @Test
  void arbitraryEntryWithoutSignerIsAcceptedWhateverItsBytes() {
    assertThat(
            validate(
                new FrameSignature(
                    FrameSignature.SCHEME_ARBITRARY, Bytes.EMPTY, Bytes.EMPTY, Bytes.EMPTY),
                signer))
        .isEmpty();
    assertThat(
            validate(
                new FrameSignature(
                    FrameSignature.SCHEME_ARBITRARY,
                    Bytes.EMPTY,
                    OTHER_DIGEST,
                    Bytes.fromHexString("0xdeadbeef")),
                signer))
        .isEmpty();
  }

  @Test
  void arbitraryEntryWithSignerIsRejected() {
    final FrameSignature entry =
        new FrameSignature(
            FrameSignature.SCHEME_ARBITRARY, signer.getBytes(), Bytes.EMPTY, Bytes.EMPTY);

    assertThat(validate(entry, signer)).contains(Invalidity.ARBITRARY_WITH_SIGNER);
  }

  // ---------------------------------------------------------------------------------------------
  // secp256k1
  // ---------------------------------------------------------------------------------------------

  @Test
  void secp256k1EntryOverTheSignatureHashRecoversTheSender() {
    final FrameSignature entry =
        new FrameSignature(
            FrameSignature.SCHEME_SECP256K1,
            Bytes.EMPTY,
            Bytes.EMPTY,
            secp256k1Signature(SIGNATURE_HASH, keyPair));

    assertThat(validate(entry, signer)).isEmpty();
  }

  @Test
  void secp256k1EntryWithDeclaredSignerRecoversTheSigner() {
    final FrameSignature entry =
        new FrameSignature(
            FrameSignature.SCHEME_SECP256K1,
            signer.getBytes(),
            Bytes.EMPTY,
            secp256k1Signature(SIGNATURE_HASH, keyPair));

    // The declared signer, not the sender, is what the recovered address must match.
    assertThat(validate(entry, OTHER_ADDRESS)).isEmpty();
  }

  @Test
  void secp256k1EntryOverAnExplicitDigestIsCheckedAgainstThatDigest() {
    final Bytes signatureOverDigest = secp256k1Signature(OTHER_DIGEST, keyPair);

    assertThat(
            validate(
                new FrameSignature(
                    FrameSignature.SCHEME_SECP256K1,
                    Bytes.EMPTY,
                    OTHER_DIGEST,
                    signatureOverDigest),
                signer))
        .isEmpty();
    // The same signature, read against the canonical hash, recovers some other key.
    assertThat(
            validate(
                new FrameSignature(
                    FrameSignature.SCHEME_SECP256K1, Bytes.EMPTY, Bytes.EMPTY, signatureOverDigest),
                signer))
        .contains(Invalidity.SIGNER_MISMATCH);
  }

  @Test
  void secp256k1EntryRecoveringAnotherAddressIsASignerMismatch() {
    final FrameSignature entry =
        new FrameSignature(
            FrameSignature.SCHEME_SECP256K1,
            Bytes.EMPTY,
            Bytes.EMPTY,
            secp256k1Signature(SIGNATURE_HASH, keyPair));

    assertThat(validate(entry, OTHER_ADDRESS)).contains(Invalidity.SIGNER_MISMATCH);
  }

  @Test
  void secp256k1HighSIsRejected() {
    final SECPSignature signature = SECP256K1.sign(SIGNATURE_HASH, keyPair);
    // (r, n - s) with the other parity verifies too; only the low-s form is canonical.
    final Bytes highS =
        secp256k1Bytes(
            1 - signature.getRecId(), signature.getR(), SECP256K1_N.subtract(signature.getS()));
    final FrameSignature entry =
        new FrameSignature(FrameSignature.SCHEME_SECP256K1, Bytes.EMPTY, Bytes.EMPTY, highS);

    assertThat(validate(entry, signer)).contains(Invalidity.SECP256K1_VALUE_OUT_OF_RANGE);
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 27, 28, 0xff})
  void secp256k1ParityAboveOneIsRejected(final int v) {
    final SECPSignature signature = SECP256K1.sign(SIGNATURE_HASH, keyPair);
    final FrameSignature entry =
        new FrameSignature(
            FrameSignature.SCHEME_SECP256K1,
            Bytes.EMPTY,
            Bytes.EMPTY,
            secp256k1Bytes(v, signature.getR(), signature.getS()));

    assertThat(validate(entry, signer)).contains(Invalidity.SECP256K1_VALUE_OUT_OF_RANGE);
  }

  @Test
  void secp256k1ROrSOutsideTheCurveOrderIsRejected() {
    final SECPSignature signature = SECP256K1.sign(SIGNATURE_HASH, keyPair);
    final BigInteger r = signature.getR();
    final BigInteger s = signature.getS();

    assertThat(secp256k1Entry(0, BigInteger.ZERO, s))
        .contains(Invalidity.SECP256K1_VALUE_OUT_OF_RANGE);
    assertThat(secp256k1Entry(0, SECP256K1_N, s)).contains(Invalidity.SECP256K1_VALUE_OUT_OF_RANGE);
    assertThat(secp256k1Entry(0, BigInteger.TWO.pow(256).subtract(BigInteger.ONE), s))
        .contains(Invalidity.SECP256K1_VALUE_OUT_OF_RANGE);
    assertThat(secp256k1Entry(0, r, BigInteger.ZERO))
        .contains(Invalidity.SECP256K1_VALUE_OUT_OF_RANGE);
    // The low-s bound itself is accepted as a value; it simply recovers some other key.
    assertThat(secp256k1Entry(0, r, SECP256K1_N.shiftRight(1)))
        .isNotEqualTo(Optional.of(Invalidity.SECP256K1_VALUE_OUT_OF_RANGE));
    assertThat(secp256k1Entry(0, r, SECP256K1_N.shiftRight(1).add(BigInteger.ONE)))
        .contains(Invalidity.SECP256K1_VALUE_OUT_OF_RANGE);
  }

  @Test
  void secp256k1RThatIsNotACurvePointFailsRecovery() {
    // x = 5 is a valid scalar but 5^3 + 7 is not a square mod p, so no point has it as its x.
    assertThat(secp256k1Entry(0, BigInteger.valueOf(5), BigInteger.ONE))
        .contains(Invalidity.SECP256K1_RECOVERY_FAILED);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 64, 66, 128})
  void secp256k1SignatureOfTheWrongLengthIsRejected(final int size) {
    final FrameSignature entry =
        new FrameSignature(
            FrameSignature.SCHEME_SECP256K1,
            Bytes.EMPTY,
            Bytes.EMPTY,
            Bytes.repeat((byte) 1, size));

    assertThat(validate(entry, signer)).contains(Invalidity.SECP256K1_SIGNATURE_SIZE);
  }

  // ---------------------------------------------------------------------------------------------
  // P256
  // ---------------------------------------------------------------------------------------------

  @Test
  void p256KnownAnswerVectorIsAccepted() {
    final Address derived = p256Signer(P256_QX, P256_QY);
    final Bytes signature = Bytes.concatenate(P256_R, P256_S, P256_QX, P256_QY);

    // With the signer declared ...
    assertThat(
            validate(
                new FrameSignature(
                    FrameSignature.SCHEME_P256, derived.getBytes(), P256_DIGEST, signature),
                OTHER_ADDRESS))
        .isEmpty();
    // ... and with the signer defaulting to the sender.
    assertThat(
            validate(
                new FrameSignature(FrameSignature.SCHEME_P256, Bytes.EMPTY, P256_DIGEST, signature),
                derived))
        .isEmpty();
  }

  @Test
  void p256SignatureOverTheSignatureHashIsAccepted() {
    final KeyPair p256KeyPair = P256.generateKeyPair();
    final Bytes publicKey = p256KeyPair.getPublicKey().getEncodedBytes();
    final SECPSignature signature = P256.sign(SIGNATURE_HASH, p256KeyPair);
    final BigInteger lowS =
        signature.getS().compareTo(P256_N.shiftRight(1)) > 0
            ? P256_N.subtract(signature.getS())
            : signature.getS();
    final Bytes raw = Bytes.concatenate(uint256(signature.getR()), uint256(lowS), publicKey);
    final Address derived = Address.extract(Hash.keccak256(publicKey));

    assertThat(
            validate(
                new FrameSignature(FrameSignature.SCHEME_P256, Bytes.EMPTY, Bytes.EMPTY, raw),
                derived))
        .isEmpty();
  }

  @Test
  void p256HighSIsRejectedEvenThoughItVerifies() {
    final Bytes highS = uint256(P256_N.subtract(P256_S.toUnsignedBigInteger()));

    assertThat(p256Entry(P256_R, highS, P256_QX, P256_QY, P256_DIGEST))
        .contains(Invalidity.P256_VALUE_OUT_OF_RANGE);
  }

  @Test
  void p256ROrSOutsideTheCurveOrderIsRejected() {
    final Bytes32 n = uint256(P256_N);

    assertThat(p256Entry(Bytes32.ZERO, P256_S, P256_QX, P256_QY, P256_DIGEST))
        .contains(Invalidity.P256_VALUE_OUT_OF_RANGE);
    assertThat(p256Entry(n, P256_S, P256_QX, P256_QY, P256_DIGEST))
        .contains(Invalidity.P256_VALUE_OUT_OF_RANGE);
    assertThat(p256Entry(P256_R, Bytes32.ZERO, P256_QX, P256_QY, P256_DIGEST))
        .contains(Invalidity.P256_VALUE_OUT_OF_RANGE);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 64, 127, 129})
  void p256SignatureOfTheWrongLengthIsRejected(final int size) {
    final FrameSignature entry =
        new FrameSignature(
            FrameSignature.SCHEME_P256, Bytes.EMPTY, P256_DIGEST, Bytes.repeat((byte) 1, size));

    assertThat(validate(entry, OTHER_ADDRESS)).contains(Invalidity.P256_SIGNATURE_SIZE);
  }

  @Test
  void p256KeyOffTheCurveIsRejected() {
    final Bytes32 offCurveY = uint256(P256_QY.toUnsignedBigInteger().add(BigInteger.ONE));

    assertThat(p256Entry(P256_R, P256_S, P256_QX, offCurveY, P256_DIGEST))
        .contains(Invalidity.P256_PUBLIC_KEY_INVALID);
  }

  @Test
  void p256KeyAtInfinityIsRejected() {
    assertThat(p256Entry(P256_R, P256_S, Bytes32.ZERO, Bytes32.ZERO, P256_DIGEST))
        .contains(Invalidity.P256_PUBLIC_KEY_INVALID);
  }

  @Test
  void p256KeyCoordinateNotBelowTheFieldPrimeIsRejected() {
    final Bytes32 p = uint256(P256_P);

    assertThat(p256Entry(P256_R, P256_S, p, P256_QY, P256_DIGEST))
        .contains(Invalidity.P256_PUBLIC_KEY_INVALID);
  }

  @Test
  void p256SignatureOverAnotherDigestFailsVerification() {
    assertThat(p256Entry(P256_R, P256_S, P256_QX, P256_QY, OTHER_DIGEST))
        .contains(Invalidity.P256_VERIFICATION_FAILED);
  }

  @Test
  void p256KeyNotMatchingTheSignerIsASignerMismatch() {
    final FrameSignature entry =
        new FrameSignature(
            FrameSignature.SCHEME_P256,
            Bytes.EMPTY,
            P256_DIGEST,
            Bytes.concatenate(P256_R, P256_S, P256_QX, P256_QY));

    // The signer defaults to the sender, whose address is not derived from this key.
    assertThat(validate(entry, OTHER_ADDRESS)).contains(Invalidity.SIGNER_MISMATCH);
  }

  // ---------------------------------------------------------------------------------------------
  // Reasons
  // ---------------------------------------------------------------------------------------------

  @ParameterizedTest
  @EnumSource(Invalidity.class)
  void shapeErrorsAreFormatErrorsAndCryptographicErrorsAreSignatureErrors(
      final Invalidity invalidity) {
    final Set<Invalidity> formatErrors =
        Set.of(
            Invalidity.DIGEST_SIZE,
            Invalidity.DIGEST_EXPLICIT_ZERO,
            Invalidity.SIGNER_SIZE,
            Invalidity.UNKNOWN_SCHEME,
            Invalidity.ARBITRARY_WITH_SIGNER,
            Invalidity.SIGNER_MISMATCH);

    assertThat(invalidity.reason())
        .isEqualTo(
            formatErrors.contains(invalidity)
                ? TransactionInvalidReason.INVALID_TRANSACTION_FORMAT
                : TransactionInvalidReason.INVALID_SIGNATURE);
    assertThat(invalidity.message()).isNotBlank();
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private static Optional<Invalidity> validate(final FrameSignature entry, final Address sender) {
    return FrameTransactionSignatureValidator.validate(entry, sender, SIGNATURE_HASH);
  }

  private Optional<Invalidity> secp256k1Entry(final int v, final BigInteger r, final BigInteger s) {
    return validate(
        new FrameSignature(
            FrameSignature.SCHEME_SECP256K1, Bytes.EMPTY, Bytes.EMPTY, secp256k1Bytes(v, r, s)),
        signer);
  }

  private static Optional<Invalidity> p256Entry(
      final Bytes r, final Bytes s, final Bytes qx, final Bytes qy, final Bytes32 digest) {
    return validate(
        new FrameSignature(
            FrameSignature.SCHEME_P256,
            p256Signer(qx, qy).getBytes(),
            digest,
            Bytes.concatenate(r, s, qx, qy)),
        OTHER_ADDRESS);
  }

  private static Bytes secp256k1Signature(final Bytes32 digest, final KeyPair keyPair) {
    final SECPSignature signature = SECP256K1.sign(digest, keyPair);
    return secp256k1Bytes(signature.getRecId(), signature.getR(), signature.getS());
  }

  private static Bytes secp256k1Bytes(final int v, final BigInteger r, final BigInteger s) {
    return Bytes.concatenate(Bytes.of(v), uint256(r), uint256(s));
  }

  private static Bytes32 uint256(final BigInteger value) {
    return Bytes32.leftPad(Bytes.wrap(value.toByteArray()).trimLeadingZeros());
  }

  /** EIP-8141: a P256 signer is the last 20 bytes of keccak256(qx || qy). */
  private static Address p256Signer(final Bytes qx, final Bytes qy) {
    return Address.extract(Hash.keccak256(Bytes.concatenate(qx, qy)));
  }
}
