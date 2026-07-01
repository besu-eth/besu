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

/** The Percentage utility. */
public class Percentage {
  /** Represent 0% */
  public static final Percentage ZERO = new Percentage(0);

  private final int value;

  private Percentage(final int value) {
    this.value = value;
  }

  /**
   * Parse a string representing a percentage.
   *
   * @param str A string representing a valid percentage between 0 and 100 inclusive.
   * @return The percentage.
   * @throws IllegalArgumentException if the provided string is {@code null}.
   * @throws IllegalArgumentException if the string is either not a number, or not a percentage
   */
  public static Percentage fromString(final String str) {
    checkArgument(str != null);
    return fromInt(parseInt(str));
  }

  /**
   * Instantiate percentage.
   *
   * @param val the val
   * @return the percentage
   */
  public static Percentage fromInt(final int val) {
    checkArgument(val >= 0 && val <= 100);
    return new Percentage(val);
  }

  /**
   * Gets value.
   *
   * @return the value
   */
  public int getValue() {
    return value;
  }

  /**
   * Gets value as float.
   *
   * @return the value as float
   */
  public float getValueAsFloat() {
    return value;
  }

  @Override
  public boolean equals(final Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof Percentage that)) {
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
    return String.valueOf(value);
  }
}
