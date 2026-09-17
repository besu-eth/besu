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
import java.util.Objects;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.MutableBytes;
import org.apache.tuweni.units.bigints.UInt256;

/** The Secp signature. */
public class SECPSignature {

  /** The constant BYTES_REQUIRED. */
  public static final int BYTES_REQUIRED = 65;

  /**
   * The recovery id to reconstruct the public key used to create the signature.
   *
   * <p>The recId is an index from 0 to 3 which indicates which of the 4 possible keys is the
   * correct one. Because the key recovery operation yields multiple potential keys, the correct key
   * must either be stored alongside the signature, or you must be willing to try each recId in turn
   * until you find one that outputs the key you are expecting.
   */
  private final byte recId;

  private final BigInteger r;
  private final BigInteger s;

  private final Supplier<Bytes> encoded = Suppliers.memoize(this::_encodedBytes);

  /**
   * Instantiates a new SECPSignature.
   *
   * @param r the r
   * @param s the s
   * @param recId the rec id
   */
  public SECPSignature(final BigInteger r, final BigInteger s, final byte recId) {
    this.r = r;
    this.s = s;
    this.recId = recId;
  }

  /**
   * Creates a new signature object given its parameters.
   *
   * @param r the 'r' part of the signature.
   * @param s the 's' part of the signature.
   * @param recId the recovery id part of the signature.
   * @param curveOrder The order (n) of the used curve
   * @return the created {@link SECPSignature} object.
   * @throws NullPointerException if {@code r} or {@code s} are {@code null}.
   * @throws IllegalArgumentException if any argument is invalid (for instance, {@code v} is neither
   *     27 or 28).
   */
  public static SECPSignature create(
      final BigInteger r, final BigInteger s, final byte recId, final BigInteger curveOrder) {
    checkNotNull(r);
    checkNotNull(s);
    checkInBounds("r", r, curveOrder);
    checkInBounds("s", s, curveOrder);
    if (recId != 0 && recId != 1) {
      throw new IllegalArgumentException(
          "Invalid 'recId' value, should be 0 or 1 but got " + recId);
    }
    return new SECPSignature(r, s, recId);
  }

  private static void checkInBounds(
      final String name, final BigInteger i, final BigInteger curveOrder) {
    if (i.compareTo(BigInteger.ONE) < 0) {
      throw new IllegalArgumentException(
          String.format("Invalid '%s' value, should be >= 1 but got %s", name, i));
    }

    if (i.compareTo(curveOrder) >= 0) {
      throw new IllegalArgumentException(
          String.format("Invalid '%s' value, should be < %s but got %s", curveOrder, name, i));
    }
  }

  /**
   * Decode secp signature.
   *
   * @param bytes the bytes
   * @param curveOrder the curve order
   * @return the secp signature
   */
  public static SECPSignature decode(final Bytes bytes, final BigInteger curveOrder) {
    checkArgument(
        bytes.size() == BYTES_REQUIRED, "encoded SECP256K1 signature must be 65 bytes long");

    final BigInteger r = bytes.slice(0, 32).toUnsignedBigInteger();
    final BigInteger s = bytes.slice(32, 32).toUnsignedBigInteger();
    final byte recId = bytes.get(64);
    return SECPSignature.create(r, s, recId, curveOrder);
  }

  /**
   * Encoded bytes.
   *
   * @return the bytes
   */
  public Bytes encodedBytes() {
    return encoded.get();
  }

  private Bytes _encodedBytes() {
    final MutableBytes bytes = MutableBytes.create(BYTES_REQUIRED);
    UInt256.valueOf(r).copyTo(bytes, 0);
    UInt256.valueOf(s).copyTo(bytes, 32);
    bytes.set(64, recId);
    return bytes;
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof SECPSignature that)) {
      return false;
    }

    return this.r.equals(that.r) && this.s.equals(that.s) && this.recId == that.recId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(r, s, recId);
  }

  /**
   * Gets rec id.
   *
   * @return the rec id
   */
  public byte getRecId() {
    return recId;
  }

  /**
   * Gets r.
   *
   * @return the r
   */
  public BigInteger getR() {
    return r;
  }

  /**
   * Gets s.
   *
   * @return the s
   */
  public BigInteger getS() {
    return s;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("Signature").append("{");
    sb.append("r=").append(r).append(", ");
    sb.append("s=").append(s).append(", ");
    sb.append("recId=").append(recId);
    return sb.append("}").toString();
  }
}
