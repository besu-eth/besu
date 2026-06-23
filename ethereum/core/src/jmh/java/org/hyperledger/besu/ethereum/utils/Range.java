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
package org.hyperledger.besu.ethereum.utils;

import java.util.Comparator;
import java.util.Objects;

/**
 * An immutable, inclusive range bounded by two endpoints of type {@code T}.
 *
 * <p>The endpoints are ordered on construction using the supplied {@link Comparator}, so {@link
 * #minimum} always holds the smaller (or equal) value and {@link #maximum} the larger (or equal)
 * value, regardless of the order in which they were passed to the constructor.
 *
 * @param <T> the type of the range's endpoints
 */
public class Range<T> {
  /** The lower endpoint of the range; never greater than {@link #maximum}. */
  public final T minimum;

  /** The upper endpoint of the range; never less than {@link #minimum}. */
  public final T maximum;

  private final Comparator<T> comparator;

  /**
   * Creates a range from two endpoints, ordering them so that {@link #minimum} is not greater than
   * {@link #maximum} according to {@code comparator}.
   *
   * @param value1 one endpoint of the range; must not be {@code null}
   * @param value2 the other endpoint of the range; must not be {@code null}
   * @param comparator the comparator used to order the endpoints and to evaluate {@link
   *     #isWithin(Object, Object)}
   * @throws NullPointerException if either endpoint is {@code null}
   */
  public Range(final T value1, final T value2, final Comparator<T> comparator) {
    Objects.requireNonNull(value1, "value1");
    Objects.requireNonNull(value2, "value2");
    if (comparator.compare(value1, value2) <= 0) {
      this.minimum = value1;
      this.maximum = value2;
    } else {
      this.minimum = value2;
      this.maximum = value1;
    }
    this.comparator = comparator;
  }

  /**
   * Tests whether this range is entirely contained within the inclusive bounds {@code [lowerBound,
   * upperBound]}, i.e. whether {@code lowerBound <= minimum} and {@code maximum <= upperBound}
   * according to this range's comparator.
   *
   * @param lowerBound the inclusive lower bound to check against
   * @param upperBound the inclusive upper bound to check against
   * @return {@code true} if both endpoints of this range fall within {@code [lowerBound,
   *     upperBound]}, {@code false} otherwise
   */
  public boolean isWithin(final T lowerBound, final T upperBound) {
    return Objects.compare(minimum, lowerBound, comparator) >= 0
        && Objects.compare(maximum, upperBound, comparator) <= 0;
  }
}
