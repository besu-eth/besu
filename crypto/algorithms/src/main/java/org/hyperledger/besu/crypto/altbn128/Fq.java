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
package org.hyperledger.besu.crypto.altbn128;

import java.math.BigInteger;
import java.util.Objects;

import com.google.common.base.MoreObjects;
import org.apache.tuweni.bytes.Bytes;

/**
 * Adapted from the pc_ecc (Apache 2 License) implementation:
 * https://github.com/ethereum/py_ecc/blob/master/py_ecc/bn128/bn128_field_elements.py
 */
public class Fq implements FieldElement<Fq> {

  private static final BigInteger TWO = BigInteger.valueOf(2);

  /**
   * fq that represents 0.
   *
   * @return the fq
   */
  public static Fq zero() {
    return create(0);
  }

  /**
   * fq that represents 1.
   *
   * @return the fq
   */
  public static Fq one() {
    return create(1);
  }

  private final BigInteger n;

  /**
   * Create fq.
   *
   * @param n the n
   * @return the fq
   */
  public static Fq create(final BigInteger n) {
    return new Fq(n);
  }

  /**
   * Create fq.
   *
   * @param n the n
   * @return the fq
   */
  static Fq create(final long n) {
    return create(BigInteger.valueOf(n));
  }

  private Fq(final BigInteger n) {
    this.n = n;
  }

  /**
   * To bytes.
   *
   * @return the bytes
   */
  public Bytes toBytes() {
    return Bytes.wrap(n.toByteArray()).trimLeadingZeros();
  }

  @Override
  public boolean isZero() {
    return n.compareTo(BigInteger.ZERO) == 0;
  }

  @Override
  public boolean isValid() {
    return n.compareTo(FIELD_MODULUS) < 0;
  }

  @Override
  public Fq add(final Fq other) {
    final BigInteger result = n.add(other.n).mod(FIELD_MODULUS);
    return new Fq(result);
  }

  @Override
  public Fq subtract(final Fq other) {
    final BigInteger result = n.subtract(other.n).mod(FIELD_MODULUS);
    return new Fq(result);
  }

  @Override
  public Fq multiply(final int val) {
    return multiply(new Fq(BigInteger.valueOf(val)));
  }

  @Override
  public Fq multiply(final Fq other) {
    final BigInteger result = n.multiply(other.n).mod(FIELD_MODULUS);
    return new Fq(result);
  }

  @Override
  public Fq divide(final Fq other) {
    final BigInteger inverse = inverse(other.n, FIELD_MODULUS);
    final BigInteger result = n.multiply(inverse).mod(FIELD_MODULUS);
    return new Fq(result);
  }

  private BigInteger inverse(final BigInteger a, final BigInteger n) {
    if (a.compareTo(BigInteger.ZERO) == 0) {
      return BigInteger.ZERO;
    }
    BigInteger lm = BigInteger.ONE;
    BigInteger hm = BigInteger.ZERO;
    BigInteger low = a.mod(n);
    BigInteger high = n;
    while (low.compareTo(BigInteger.ONE) > 0) {
      final BigInteger r = high.divide(low);
      final BigInteger nm = hm.subtract(lm.multiply(r));
      final BigInteger neww = high.subtract(low.multiply(r));
      high = low;
      hm = lm;
      low = neww;
      lm = nm;
    }
    return lm.mod(n);
  }

  @Override
  public Fq negate() {
    return new Fq(n.negate());
  }

  @Override
  public Fq power(final int n) {
    if (n == 0) {
      return one();
    } else if (n == 1) {
      return this;
    } else if (n % 2 == 0) {
      return multiply(this).power(n / 2);
    } else {
      return multiply(this).power(n / 2).multiply(this);
    }
  }

  @Override
  public Fq power(final BigInteger n) {
    if (n.compareTo(BigInteger.ZERO) == 0) {
      return one();
    }
    if (n.compareTo(BigInteger.ONE) == 0) {
      return this;
    } else if (n.mod(TWO).compareTo(BigInteger.ZERO) == 0) {
      return multiply(this).power(n.divide(TWO));
    } else {
      return multiply(this).power(n.divide(TWO)).multiply(this);
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(Fq.class).add("n", n).toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(n);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof Fq other)) {
      return false;
    }

    return n.compareTo(other.n) == 0;
  }
}
