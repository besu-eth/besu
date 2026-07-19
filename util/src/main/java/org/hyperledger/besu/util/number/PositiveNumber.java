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
package org.hyperledger.besu.util.number;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Integer.parseInt;

import java.util.Objects;

/** The Positive number. */
public class PositiveNumber {

  private final int value;

  private PositiveNumber(final int value) {
    this.value = value;
  }

  /**
   * Parse a string representing a positive number.
   *
   * @param str A string representing a valid positive number strictly greater than 0.
   * @return The parsed int.
   * @throws IllegalArgumentException if the provided string is {@code null}.
   * @throws IllegalArgumentException if the string is either not a number, or not a positive number
   */
  public static PositiveNumber fromString(final String str) {
    checkArgument(str != null);
    PositiveNumber positiveNumber = new PositiveNumber(parseInt(str));
    checkArgument(positiveNumber.getValue() > 0);
    return positiveNumber;
  }

  /**
   * Instantiate positive number from int.
   *
   * @param val the val
   * @return the positive number
   */
  public static PositiveNumber fromInt(final int val) {
    checkArgument(val > 0);
    return new PositiveNumber(val);
  }

  /**
   * Gets value.
   *
   * @return the value
   */
  public int getValue() {
    return value;
  }

  @Override
  public boolean equals(final Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof PositiveNumber that)) {
      return false;
    }
    return value == that.value;
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }

  @Override
  public String toString() {
    return "+" + value;
  }
}
