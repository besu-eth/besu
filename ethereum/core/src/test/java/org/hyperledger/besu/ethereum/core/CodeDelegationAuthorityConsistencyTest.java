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
package org.hyperledger.besu.ethereum.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.hyperledger.besu.crypto.CodeDelegationSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;

import java.math.BigInteger;
import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Regression coverage for the EIP-7702 code-delegation authority divergence, observed through
 * {@link CodeDelegation#authorizer()} — the exact call {@code
 * CodeDelegationProcessor#processCodeDelegation} makes before mutating world state.
 *
 * <p>Backend selection is flipped the same way Besu itself does at startup, by calling {@code
 * disableNative()} / {@code maybeEnableNative()} on the shared {@link SignatureAlgorithmFactory}
 * singleton (see {@code BesuCommand}, where {@code --Xsecp256k1-native-enabled} is handled), so
 * what these tests observe is what a node launched with that flag would compute.
 *
 * <p>Note {@code CodeDelegationProcessor#isCodeDelegationValid} gates chainId, nonce and {@code s}
 * (against the half curve order) but never bounds {@code r}, so an out-of-range {@code r} reaches
 * {@code authorizer()} unfiltered — which is why the range check belongs at recovery.
 *
 * @see org.hyperledger.besu.crypto.CodeDelegationRBoundRecoveryTest the crypto-level coverage
 */
class CodeDelegationAuthorityConsistencyTest {

  /** secp256k1 curve order (n). */
  private static final BigInteger N =
      new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141", 16);

  private static final BigInteger CHAIN_ID = BigInteger.ONE;
  private static final Address DELEGATE_ADDRESS =
      Address.fromHexString("0x1234567890abcdef1234567890abcdef12345678");
  private static final long NONCE = 100;

  @AfterEach
  void restoreNativeBackend() {
    SignatureAlgorithmFactory.getInstance().maybeEnableNative();
  }

  /** Computes the authority under a given backend; a fresh delegation avoids the memoized value. */
  private static String authorityUnder(final boolean useNative, final BigInteger r) {
    final SignatureAlgorithm algorithm = SignatureAlgorithmFactory.getInstance();
    if (useNative) {
      assumeTrue(
          algorithm.maybeEnableNative(),
          "native libsecp256k1 must be available for cross-backend comparison");
    } else {
      algorithm.disableNative();
    }

    final CodeDelegation delegation =
        new CodeDelegation(
            CHAIN_ID,
            DELEGATE_ADDRESS,
            NONCE,
            CodeDelegationSignature.create(r, BigInteger.ONE, (byte) 0));

    final Throwable thrown = catchThrowable(delegation::authorizer);
    if (thrown != null) {
      return "threw " + thrown.getClass().getSimpleName();
    }
    final Optional<Address> authority = delegation.authorizer();
    return authority.map(Address::toHexString).orElse("empty");
  }

  private static void assertBothBackendsYieldEmptyAuthority(final BigInteger r, final String why) {
    assertThat(authorityUnder(true, r)).as("native, %s", why).isEqualTo("empty");
    assertThat(authorityUnder(false, r)).as("BouncyCastle, %s", why).isEqualTo("empty");
  }

  @Test
  void authorityIsEmptyOnBothBackendsForRAboveCurveOrder() {
    // r = n+2 previously yielded an authority under BouncyCastle and none under native, so the
    // two nodes wrote different world state from the same authorization tuple.
    assertBothBackendsYieldEmptyAuthority(N.add(BigInteger.TWO), "r = n+2 is out of range");
  }

  @Test
  void authorityIsEmptyRatherThanThrowingWhenRIsNotAValidCurvePoint() {
    // r = n+1 is in the band and not a valid curve x-coordinate. BouncyCastle used to escape
    // authorizer() with an IllegalArgumentException, which CodeDelegationProcessor does not guard.
    assertBothBackendsYieldEmptyAuthority(
        N.add(BigInteger.ONE), "r = n+1 is out of range and not a curve point");
  }

  @Test
  void authorityIsEmptyOnBothBackendsForRExactlyAtTheCurveOrder() {
    // Pre-existing contract, already locked by CodeDelegationTest.
    assertBothBackendsYieldEmptyAuthority(N, "r = n is out of range");
  }

  @Test
  void authorityIsEmptyOnBothBackendsForInRangeRThatIsNotACurvePoint() {
    // r = 5 is a perfectly valid scalar but not a curve x-coordinate; BouncyCastle used to throw.
    assertBothBackendsYieldEmptyAuthority(
        BigInteger.valueOf(5), "r = 5 is in range but not a curve point");
  }

  @Test
  void genuinelySignedDelegationResolvesToTheSameAuthorityOnBothBackends() {
    // The guard must not change behaviour for real authorizations.
    final CodeDelegation delegation =
        (CodeDelegation)
            CodeDelegation.builder()
                .chainId(CHAIN_ID)
                .address(DELEGATE_ADDRESS)
                .nonce(NONCE)
                .signAndBuild(SignatureAlgorithmFactory.getInstance().generateKeyPair());

    assumeTrue(
        SignatureAlgorithmFactory.getInstance().maybeEnableNative(),
        "native libsecp256k1 must be available for cross-backend comparison");
    final Optional<Address> nativeAuthority = delegation.authorizer();

    // A fresh instance is required because authorizer() memoizes its result per delegation.
    final CodeDelegation sameDelegation =
        new CodeDelegation(
            delegation.chainId(), delegation.address(), delegation.nonce(), delegation.signature());
    SignatureAlgorithmFactory.getInstance().disableNative();
    final Optional<Address> bouncyCastleAuthority = sameDelegation.authorizer();

    assertThat(nativeAuthority).isPresent();
    assertThat(bouncyCastleAuthority).isEqualTo(nativeAuthority);
  }
}
