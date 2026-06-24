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
import static java.lang.Float.parseFloat;

import java.util.Objects;

/** The Fraction. */
public class Fraction {

  private final float value;

  private Fraction(final float value) {
    this.value = value;
  }

  /**
   * From percentage to fraction.
   *
   * @param percentage the percentage
   * @return the fraction
   */
  public static Fraction fromPercentage(final int percentage) {
    return fromFloat((float) percentage / 100.0f);
  }

  /**
   * From percentage to fraction.
   *
   * @param percentage the percentage
   * @return the fraction
   */
  public static Fraction fromPercentage(final Percentage percentage) {
    return fromFloat(percentage.getValueAsFloat() / 100.0f);
  }

  /**
   * To percentage.
   *
   * @return the percentage
   */
  public Percentage toPercentage() {
    return Percentage.fromInt((int) (value * 100.0f));
  }

  /**
   * Parse a string representing a fraction.
   *
   * @param str A string representing a valid positive number strictly between 0 and 1.
   * @return The parsed double.
   * @throws IllegalArgumentException if the provided string is {@code null}.
   * @throws IllegalArgumentException if the string is either not a number, or not a fraction
   */
  public static Fraction fromString(final String str) {
    checkArgument(str != null);
    return fromFloat(parseFloat(str));
  }

  /**
   * From float to fraction.
   *
   * @param val the val
   * @return the fraction
   */
  public static Fraction fromFloat(final float val) {
    checkArgument(val > 0.0f && val <= 1.0f);
    return new Fraction(val);
  }

  /**
   * Gets value.
   *
   * @return the value
   */
  public float getValue() {
    return value;
  }

  @Override
  public boolean equals(final Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof Fraction that)) {
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
