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

import org.jspecify.annotations.Nullable;

/**
 * Adapted from the pc_ecc (Apache 2 License) implementation:
 * https://github.com/ethereum/py_ecc/blob/master/py_ecc/bn128/bn128_field_elements.py
 *
 * @param <T> the type parameter
 */
@SuppressWarnings("rawtypes")
public interface FieldPoint<T extends FieldPoint> {

  /**
   * Is infinity boolean.
   *
   * @return the boolean
   */
  boolean isInfinity();

  /**
   * Add t.
   *
   * @param other the other
   * @return the t
   */
  T add(T other);

  /**
   * Multiply t.
   *
   * @param other the other
   * @return the t
   */
  @Nullable T multiply(T other);

  /**
   * Multiply t.
   *
   * @param n the n
   * @return the t
   */
  T multiply(BigInteger n);

  /**
   * Doub t.
   *
   * @return the t
   */
  T doub();

  /**
   * Negate t.
   *
   * @return the t
   */
  T negate();
}
