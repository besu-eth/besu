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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.math.BigInteger;
import java.util.Arrays;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.MutableBytes;
import org.bouncycastle.crypto.params.ECDomainParameters;
import org.bouncycastle.math.ec.ECPoint;
import org.bouncycastle.math.ec.FixedPointCombMultiplier;

/** The Secp public key. */
public class SECPPublicKey implements java.security.PublicKey {

  /** The constant BYTE_LENGTH. */
  public static final int BYTE_LENGTH = 64;

  /** Encoded Bytes */
  private final Bytes encoded;

  /** Algorithm */
  private final String algorithm;

  /**
   * Create secp public key.
   *
   * @param key the key
   * @param algorithm the algorithm
   * @return the secp public key
   */
  public static SECPPublicKey create(final BigInteger key, final String algorithm) {
    checkNotNull(key);
    return create(toBytes64(key.toByteArray()), algorithm);
  }

  /**
   * Create secp public key.
   *
   * @param encoded the encoded
   * @param algorithm the algorithm
   * @return the secp public key
   */
  public static SECPPublicKey create(final Bytes encoded, final String algorithm) {
    return new SECPPublicKey(encoded, algorithm);
  }

  /**
   * Create secp public key.
   *
   * @param privateKey the private key
   * @param curve the curve
   * @param algorithm the algorithm
   * @return the secp public key
   */
  public static SECPPublicKey create(
      final SECPPrivateKey privateKey, final ECDomainParameters curve, final String algorithm) {
    BigInteger privKey = privateKey.getEncodedBytes().toUnsignedBigInteger();

    /*
     * TODO: FixedPointCombMultiplier currently doesn't support scalars longer than the group
     * order, but that could change in future versions.
     */
    if (privKey.bitLength() > curve.getN().bitLength()) {
      privKey = privKey.mod(curve.getN());
    }

    final ECPoint point = new FixedPointCombMultiplier().multiply(curve.getG(), privKey);
    return SECPPublicKey.create(
        Bytes.wrap(Arrays.copyOfRange(point.getEncoded(false), 1, 65)), algorithm);
  }

  private static Bytes toBytes64(final byte[] backing) {
    if (backing.length == BYTE_LENGTH) {
      return Bytes.wrap(backing);
    } else if (backing.length > BYTE_LENGTH) {
      return Bytes.wrap(backing, backing.length - BYTE_LENGTH, BYTE_LENGTH);
    } else {
      final MutableBytes res = MutableBytes.create(BYTE_LENGTH);
      Bytes.wrap(backing).copyTo(res, BYTE_LENGTH - backing.length);
      return res;
    }
  }

  private SECPPublicKey(final Bytes encoded, final String algorithm) {
    checkNotNull(encoded);
    checkNotNull(algorithm);
    checkArgument(
        encoded.size() == BYTE_LENGTH,
        "Encoding must be %s bytes long, got %s",
        BYTE_LENGTH,
        encoded.size());
    this.encoded = encoded;
    this.algorithm = algorithm;
  }

  /**
   * Returns this public key as an {@link ECPoint} of Bouncy Castle, to facilitate cryptographic
   * operations.
   *
   * @param curve The elliptic curve (e.g. SECP256K1) represented as its domain parameters
   * @return This public key represented as an Elliptic Curve point.
   */
  public ECPoint asEcPoint(final ECDomainParameters curve) {
    // 0x04 is the prefix for uncompressed keys.
    final Bytes val = Bytes.concatenate(Bytes.of(0x04), encoded);
    return curve.getCurve().decodePoint(val.toArrayUnsafe());
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof SECPPublicKey that)) {
      return false;
    }

    return this.encoded.equals(that.encoded) && this.algorithm.equals(that.algorithm);
  }

  @Override
  public byte[] getEncoded() {
    return encoded.toArrayUnsafe();
  }

  /**
   * Gets encoded bytes.
   *
   * @return the encoded bytes
   */
  public Bytes getEncodedBytes() {
    return encoded;
  }

  @Override
  public String getAlgorithm() {
    return algorithm;
  }

  @Override
  public String getFormat() {
    return null;
  }

  @Override
  public int hashCode() {
    return encoded.hashCode();
  }

  @Override
  public String toString() {
    return encoded.toString();
  }
}
