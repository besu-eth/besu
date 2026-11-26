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
package org.hyperledger.besu.datatypes;

import java.math.BigInteger;
import java.util.NoSuchElementException;

/**
 * The chain id, as introduced by EIP-155, or its absence for transactions that are not replay
 * protected.
 *
 * <p>Chain ids are values: two chain ids with the same value are equal, even when they are not the
 * same instance, and the same instance can be shared by every chain id with that value. For this
 * reason, they must be compared with {@link Object#equals(Object)}, never by identity, and must not
 * be used for synchronization.
 */
public interface ChainId {

  /**
   * Whether the chain id is present.
   *
   * @return true if the chain id is present
   */
  boolean isPresent();

  /**
   * Whether the chain id is absent.
   *
   * @return true if the chain id is absent
   */
  boolean isAbsent();

  /**
   * Whether the chain id is zero, that EIP-7702 uses for authorizations valid on any chain.
   *
   * @return true if the chain id is zero
   */
  boolean isZero();

  /**
   * Whether the chain id is present and its value fits in a long.
   *
   * @return true if the value can be read with {@link #toLong()}
   */
  boolean fitsLong();

  /**
   * Returns the value as a long.
   *
   * @return the value
   * @throws NoSuchElementException if this chain id is absent
   * @throws ArithmeticException if the value does not fit in a long
   */
  long toLong();

  /**
   * Returns the value as a {@link BigInteger}.
   *
   * @return the value
   * @throws NoSuchElementException if this chain id is absent
   */
  BigInteger toBigInteger();

  /**
   * Returns the value as a hexadecimal string with no leading zeros, prefixed by {@code 0x}.
   *
   * @return the hexadecimal representation of the value
   * @throws NoSuchElementException if this chain id is absent
   */
  String toHexString();
}
