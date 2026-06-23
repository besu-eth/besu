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

/**
 * Adapted from the pc_ecc (Apache 2 License) implementation:
 * https://github.com/ethereum/py_ecc/blob/master/py_ecc/bn128/bn128_field_elements.py
 *
 * @param <U> the type parameter
 */
@SuppressWarnings("rawtypes")
public abstract class AbstractFieldPoint<U extends AbstractFieldPoint> implements FieldPoint<U> {

  private static final BigInteger TWO = BigInteger.valueOf(2);

  /** The X. */
  @SuppressWarnings("rawtypes")
  protected final FieldElement x;

  /** The Y. */
  @SuppressWarnings("rawtypes")
  protected final FieldElement y;

  /**
   * Instantiates a new Abstract field point.
   *
   * @param x the x
   * @param y the y
   */
  @SuppressWarnings("rawtypes")
  AbstractFieldPoint(final FieldElement x, final FieldElement y) {
    this.x = x;
    this.y = y;
  }

  /**
   * Infinity u.
   *
   * @return the u
   */
  protected abstract U infinity();

  /**
   * New instance of generic type U.
   *
   * @param x the x
   * @param y the y
   * @return the U
   */
  @SuppressWarnings("rawtypes")
  protected abstract U newInstance(final FieldElement x, final FieldElement y);

  @Override
  public boolean isInfinity() {
    return x.isZero() && y.isZero();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public U add(final U other) {
    if (isInfinity() || other.isInfinity()) {
      return isInfinity() ? other : (U) this;
    } else if (equals(other)) {
      return doub();
    } else if (x.equals(other.x)) {
      return infinity();
    } else {
      final FieldElement x1 = x;
      final FieldElement y1 = y;
      final FieldElement x2 = other.x;
      final FieldElement y2 = other.y;

      final FieldElement m = y2.subtract(y1).divide(x2.subtract(x1));
      final FieldElement mSquared = m.power(2);
      final FieldElement newX = mSquared.subtract(x1).subtract(x2);
      final FieldElement newY = m.negate().multiply(newX).add(m.multiply(x1)).subtract(y1);

      return newInstance(newX, newY);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public U multiply(final U other) {
    return null;
  }

  @SuppressWarnings("unchecked")
  @Override
  public U multiply(final BigInteger n) {
    if (n.compareTo(BigInteger.ZERO) == 0) {
      return infinity();
    } else if (n.compareTo(BigInteger.ONE) == 0) {
      return newInstance(x, y);
    } else if (n.mod(TWO).compareTo(BigInteger.ZERO) == 0) {
      return (U) doub().multiply(n.divide(TWO));
    } else {
      return (U) doub().multiply(n.divide(TWO)).add(this);
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public U doub() {
    final FieldElement xSquared = x.power(2);
    final FieldElement m = xSquared.multiply(3).divide(y.multiply(2));
    final FieldElement mSquared = m.power(2);
    final FieldElement newX = mSquared.subtract(x.multiply(2));
    final FieldElement newY = m.negate().multiply(newX).add(m.multiply(x)).subtract(y);
    return newInstance(newX, newY);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public U negate() {
    if (isInfinity()) {
      return (U) this;
    }

    return newInstance(x, y.negate());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass()).add("x", x).add("y", y).toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(x, y);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof AbstractFieldPoint other)) {
      return false;
    }

    return Objects.equals(x, other.x) && Objects.equals(y, other.y);
  }
}
