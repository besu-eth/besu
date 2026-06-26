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

import java.util.Arrays;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;

/**
 * A wrapper class that holds a {@link Bytes} value.
 *
 * <p>This class serves as a base holder for byte sequences, implementing {@link Comparable} to
 * allow for ordered collections and comparisons. Subclasses can extend this to create specialized
 * byte holders with domain-specific semantics.
 *
 * <p>NOTE: For performance reasons - prefer to always call `getBytes()` instead first as it will
 * give better chances of locally inlining any subsequent method calls done upon `Bytes` including
 * their implementations at runtime. This class contains several annotated methods with @Deprecated
 * to desincentivize its use and prefer calling `getBytes()` first. The reason is that for the case
 * of these annotated methods, all implementations of `Bytes` get channelled through these single
 * places and that makes it harder for the JIT profiler to inline code from implementations.
 */
public class BytesHolder implements Comparable<BytesHolder> {
  private final Bytes value;

  /**
   * Constructs a BytesHolder with the given bytes value.
   *
   * @param value the bytes value to hold
   */
  protected BytesHolder(final Bytes value) {
    Objects.requireNonNull(value, "value cannot be null");
    this.value = value;
  }

  /**
   * Returns the underlying bytes value.
   *
   * @return the bytes value held by this instance
   */
  @JsonValue
  public final Bytes getBytes() {
    return value;
  }

  /**
   * Provides the number of bytes this value represents.
   *
   * @return The number of bytes this value represents.
   */
  @Deprecated
  public final int size() {
    return value.size();
  }

  /**
   * Provides this value represented as hexadecimal, starting with "0x".
   *
   * @return This value represented as hexadecimal, starting with "0x".
   */
  @Deprecated
  public final String toHexString() {
    return value.toHexString();
  }

  /**
   * Creates a default BytesHolder instance with the given bytes value.
   *
   * @param value the bytes value to hold
   * @return a new BytesHolder instance
   */
  @VisibleForTesting
  public static BytesHolder createDefaultHolder(final Bytes value) {
    return new BytesHolder(value);
  }

  /**
   * Compares this BytesHolder to another object for equality.
   *
   * <p>Two BytesHolder instances are equal if they contain the same byte sequence.
   *
   * @param obj the object to compare with
   * @return {@code true} if the objects are equal, {@code false} otherwise
   */
  @Override
  @Deprecated
  public final boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof BytesHolder other)) {
      return false;
    }
    return Arrays.equals(value.toArrayUnsafe(), other.value.toArrayUnsafe());
  }

  /**
   * Returns the hash code of this BytesHolder.
   *
   * @return the hash code based on the underlying bytes value
   */
  @Override
  @Deprecated
  public final int hashCode() {
    return value.hashCode();
  }

  /**
   * Returns a string representation of this BytesHolder.
   *
   * @return the string representation of the underlying bytes value
   */
  @Override
  @Deprecated
  public final String toString() {
    return value.toString();
  }

  /**
   * Compares this BytesHolder with another for ordering.
   *
   * <p>The comparison is performed lexicographically on the underlying byte sequences.
   *
   * @param bytesHolder the BytesHolder to compare with
   * @return a negative integer, zero, or a positive integer as this BytesHolder is less than, equal
   *     to, or greater than the specified BytesHolder
   * @throws NullPointerException if bytesHolder is null
   */
  @Override
  @Deprecated
  public final int compareTo(final BytesHolder bytesHolder) {
    Objects.requireNonNull(bytesHolder, "bytesHolder cannot be null");
    return value.compareTo(bytesHolder.value);
  }
}
