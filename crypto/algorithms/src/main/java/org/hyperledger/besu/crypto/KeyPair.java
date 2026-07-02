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

import static com.google.common.base.Preconditions.checkNotNull;

import java.math.BigInteger;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.Objects;

import org.bouncycastle.crypto.params.ECDomainParameters;
import org.bouncycastle.jcajce.provider.asymmetric.ec.BCECPrivateKey;
import org.bouncycastle.jcajce.provider.asymmetric.ec.BCECPublicKey;

/** The Key pair class. */
public class KeyPair {

  private final SECPPrivateKey privateKey;
  private final SECPPublicKey publicKey;

  /**
   * Instantiates a new Key pair.
   *
   * @param privateKey the private key
   * @param publicKey the public key
   */
  public KeyPair(final SECPPrivateKey privateKey, final SECPPublicKey publicKey) {
    checkNotNull(privateKey);
    checkNotNull(publicKey);
    this.privateKey = privateKey;
    this.publicKey = publicKey;
  }

  /**
   * Create key pair.
   *
   * @param privateKey the private key
   * @param curve the curve
   * @param algorithm the algorithm
   * @return the key pair
   */
  public static KeyPair create(
      final SECPPrivateKey privateKey, final ECDomainParameters curve, final String algorithm) {
    return new KeyPair(privateKey, SECPPublicKey.create(privateKey, curve, algorithm));
  }

  /**
   * Generate key pair.
   *
   * @param keyPairGenerator the key pair generator
   * @param algorithm the algorithm
   * @return the key pair
   */
  public static KeyPair generate(final KeyPairGenerator keyPairGenerator, final String algorithm) {
    final java.security.KeyPair rawKeyPair = keyPairGenerator.generateKeyPair();
    final BCECPrivateKey privateKey = (BCECPrivateKey) rawKeyPair.getPrivate();
    final BCECPublicKey publicKey = (BCECPublicKey) rawKeyPair.getPublic();

    final BigInteger privateKeyValue = privateKey.getD();

    // Ethereum does not use encoded public keys like bitcoin - see
    // https://en.bitcoin.it/wiki/Elliptic_Curve_Digital_Signature_Algorithm for details
    // Additionally, as the first bit is a constant prefix (0x04) we ignore this value
    final byte[] publicKeyBytes = publicKey.getQ().getEncoded(false);
    final BigInteger publicKeyValue =
        new BigInteger(1, Arrays.copyOfRange(publicKeyBytes, 1, publicKeyBytes.length));

    return new KeyPair(
        SECPPrivateKey.create(privateKeyValue, algorithm),
        SECPPublicKey.create(publicKeyValue, algorithm));
  }

  @Override
  public int hashCode() {
    return Objects.hash(privateKey, publicKey);
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof KeyPair that)) {
      return false;
    }

    return this.privateKey.equals(that.privateKey) && this.publicKey.equals(that.publicKey);
  }

  @Override
  public String toString() {
    return ("KeyPair{privateKey: "
        + this.privateKey.toString()
        + ", publicKey: "
        + this.publicKey.toString()
        + "]");
  }

  /**
   * Gets private key.
   *
   * @return the private key
   */
  public SECPPrivateKey getPrivateKey() {
    return privateKey;
  }

  /**
   * Gets public key.
   *
   * @return the public key
   */
  public SECPPublicKey getPublicKey() {
    return publicKey;
  }
}
