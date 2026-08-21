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
package org.hyperledger.besu.crypto;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.math.BigInteger;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Regression coverage for the EIP-7702 code-delegation signature bound gap.
 *
 * <p>{@link CodeDelegationSignature#create} bounds {@code r} and {@code s} only by {@code 2^256},
 * not by the secp256k1 curve order {@code n} that {@link SECPSignature#create} enforces, so an
 * authorization tuple can carry an {@code r} anywhere below {@code 2^256}. Before the fix the two
 * signature backends disagreed about what such a tuple means: native libsecp256k1's compact parser
 * rejects {@code r >= n}, while BouncyCastle bounds {@code r} only by the field prime {@code p} and
 * would recover a usable key for the band {@code n < r < p}. Recovered authority feeds
 * consensus-critical state mutation, so a network mixing {@code --Xsecp256k1-native-enabled=true}
 * and {@code false} nodes computed different world state from the same transaction.
 *
 * <p>Separately, for any {@code r} that is a valid scalar but not the x-coordinate of a curve point
 * — which includes plenty of values well inside {@code [1, n)} — BouncyCastle's {@code
 * decompressKey} threw {@link IllegalArgumentException} where native returned an empty result.
 *
 * <p>Both backends must now return the same empty result for every unrecoverable input.
 */
class CodeDelegationRBoundRecoveryTest {

  /** secp256k1 curve order (n). */
  private static final BigInteger N =
      new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141", 16);

  /** secp256k1 field prime (p). Values in (n, p) are the former divergence band. */
  private static final BigInteger P =
      new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2F", 16);

  private static final Bytes32 DATA_HASH =
      Bytes32.fromHexString("0x5cb4a1a2e0b9f7d3c8e6f0a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5");

  private SECP256K1 nativeImpl;
  private SECP256K1 bouncyCastleImpl;

  @BeforeEach
  void setUp() {
    nativeImpl = new SECP256K1();
    assumeTrue(
        nativeImpl.maybeEnableNative(),
        "native libsecp256k1 must load for the cross-backend comparison to be meaningful");

    bouncyCastleImpl = new SECP256K1();
    bouncyCastleImpl.disableNative();
  }

  /** Recovery outcome rendered as a comparable string so a throw is distinguishable from empty. */
  private static String outcome(final SECP256K1 impl, final BigInteger r, final BigInteger s) {
    final CodeDelegationSignature signature = CodeDelegationSignature.create(r, s, (byte) 0);
    final Throwable thrown =
        catchThrowable(() -> impl.recoverPublicKeyFromSignature(DATA_HASH, signature));
    if (thrown != null) {
      return "threw " + thrown.getClass().getSimpleName();
    }
    final Optional<SECPPublicKey> key = impl.recoverPublicKeyFromSignature(DATA_HASH, signature);
    return key.map(k -> "key:" + k.getEncodedBytes()).orElse("empty");
  }

  @Test
  void backendsAgreeAcrossTheFormerDivergenceBand() {
    for (int i = 0; i < 40; i++) {
      final BigInteger r = N.add(BigInteger.valueOf(i));
      assertThat(r).as("test values must stay inside the band").isLessThan(P);

      assertThat(outcome(nativeImpl, r, BigInteger.ONE))
          .as("native and BouncyCastle must agree for r = n+%d", i)
          .isEqualTo(outcome(bouncyCastleImpl, r, BigInteger.ONE));
    }
  }

  @Test
  void bothBackendsRejectRAboveCurveOrder() {
    // n+2 previously recovered a usable authority under BouncyCastle while native returned empty.
    final BigInteger bandR = N.add(BigInteger.TWO);

    assertThat(outcome(nativeImpl, bandR, BigInteger.ONE)).isEqualTo("empty");
    assertThat(outcome(bouncyCastleImpl, bandR, BigInteger.ONE)).isEqualTo("empty");
  }

  @Test
  void bothBackendsRejectRThatIsNotACurvePointRatherThanThrowing() {
    // n+1 is in the band and is not a valid curve x-coordinate; BouncyCastle used to throw here.
    final BigInteger nonPointR = N.add(BigInteger.ONE);

    assertThat(outcome(nativeImpl, nonPointR, BigInteger.ONE)).isEqualTo("empty");
    assertThat(outcome(bouncyCastleImpl, nonPointR, BigInteger.ONE)).isEqualTo("empty");
  }

  @Test
  void backendsAgreeForSmallInRangeRValuesThatAreNotCurvePoints() {
    // The wider version of the same defect: r values well inside [1, n) that are not curve
    // x-coordinates made BouncyCastle throw while native returned empty. That reached ordinary
    // transaction sender recovery too, not just EIP-7702.
    for (int i = 1; i <= 60; i++) {
      final BigInteger r = BigInteger.valueOf(i);
      assertThat(outcome(nativeImpl, r, BigInteger.ONE))
          .as("native and BouncyCastle must agree for r = %d", i)
          .isEqualTo(outcome(bouncyCastleImpl, r, BigInteger.ONE));
    }
  }

  @Test
  void bothBackendsRejectZeroAndOutOfRangeComponents() {
    assertThat(outcome(nativeImpl, BigInteger.ZERO, BigInteger.ONE)).isEqualTo("empty");
    assertThat(outcome(bouncyCastleImpl, BigInteger.ZERO, BigInteger.ONE)).isEqualTo("empty");

    assertThat(outcome(nativeImpl, BigInteger.ONE, BigInteger.ZERO)).isEqualTo("empty");
    assertThat(outcome(bouncyCastleImpl, BigInteger.ONE, BigInteger.ZERO)).isEqualTo("empty");

    assertThat(outcome(nativeImpl, BigInteger.ONE, N)).isEqualTo("empty");
    assertThat(outcome(bouncyCastleImpl, BigInteger.ONE, N)).isEqualTo("empty");
  }

  @Test
  void backendsAgreeAcrossTheFormerDivergenceBandForSToo() {
    // isRecoverable guards s as well as r, so the band behaviour must be symmetric. Every other
    // band case here fixes s = 1 and varies r; this varies s against a valid r to lock the
    // other half of the contract.
    final BigInteger validR = BigInteger.ONE;

    for (int i = 0; i < 40; i++) {
      final BigInteger s = N.add(BigInteger.valueOf(i));
      assertThat(s).as("test values must stay inside the band").isLessThan(P);

      assertThat(outcome(nativeImpl, validR, s))
          .as("native and BouncyCastle must agree for s = n+%d", i)
          .isEqualTo(outcome(bouncyCastleImpl, validR, s));
    }
  }

  @Test
  void bothBackendsRejectSAboveCurveOrder() {
    // The s counterpart of bothBackendsRejectRAboveCurveOrder.
    final BigInteger bandS = N.add(BigInteger.ONE);

    assertThat(outcome(nativeImpl, BigInteger.ONE, bandS)).isEqualTo("empty");
    assertThat(outcome(bouncyCastleImpl, BigInteger.ONE, bandS)).isEqualTo("empty");
  }

  @Test
  void bothBackendsStillRejectRExactlyAtTheCurveOrder() {
    // Pre-existing contract, locked by CodeDelegationTest; r == n was already rejected by both
    // backends before the fix and must stay rejected.
    assertThat(outcome(nativeImpl, N, BigInteger.ONE)).isEqualTo("empty");
    assertThat(outcome(bouncyCastleImpl, N, BigInteger.ONE)).isEqualTo("empty");
  }

  @Test
  void genuineSignaturesStillRecoverIdenticallyOnBothBackends() {
    // The guard must be a no-op for real signatures: r and s from a real signing operation are
    // already inside [1, n).
    final KeyPair keyPair = nativeImpl.generateKeyPair();
    final SECPSignature signature = nativeImpl.sign(DATA_HASH, keyPair);

    final Optional<SECPPublicKey> fromNative =
        nativeImpl.recoverPublicKeyFromSignature(DATA_HASH, signature);
    final Optional<SECPPublicKey> fromBouncyCastle =
        bouncyCastleImpl.recoverPublicKeyFromSignature(DATA_HASH, signature);

    assertThat(fromNative).contains(keyPair.getPublicKey());
    assertThat(fromBouncyCastle).isEqualTo(fromNative);
  }

  @Test
  void ordinarySecpSignatureCreateStillRejectsWhatCodeDelegationAccepts() {
    // The asymmetry that makes the range check at recovery necessary in the first place: the
    // ordinary constructor rejects out-of-range r outright, the code-delegation one accepts it and
    // relies on recovery returning an empty authority.
    final BigInteger bandR = N.add(BigInteger.ONE);

    assertThat(catchThrowable(() -> SECPSignature.create(bandR, BigInteger.ONE, (byte) 0, N)))
        .as("SECPSignature.create must reject r >= n")
        .isInstanceOf(IllegalArgumentException.class);

    assertThat(CodeDelegationSignature.create(bandR, BigInteger.ONE, (byte) 0))
        .as("CodeDelegationSignature.create still accepts it, by design")
        .isNotNull();
  }
}
