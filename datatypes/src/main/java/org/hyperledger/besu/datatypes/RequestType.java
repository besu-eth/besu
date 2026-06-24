/*
 * Copyright contributors to Hyperledger Besu.
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

/** Enum representing different types of requests with associated serialized type values. */
public enum RequestType {
  /** DEPOSITS */
  DEPOSIT(0x00),
  /** WITHDRAWAL */
  WITHDRAWAL(0x01),
  /** CONSOLIDATION */
  CONSOLIDATION(0x02),
  /** BUILDER DEPOSIT (EIP-8282) */
  BUILDER_DEPOSIT(0x03),
  /** BUILDER EXIT (EIP-8282) */
  BUILDER_EXIT(0x04);

  private final int typeValue;

  RequestType(final int typeValue) {
    this.typeValue = typeValue;
  }

  /**
   * Gets the serialized type value of the request type.
   *
   * @return the serialized type value as a byte.
   */
  public byte getSerializedType() {
    return (byte) this.typeValue;
  }

  /**
   * Returns the {@link RequestType} corresponding to the given serialized type value.
   *
   * @param serializedTypeValue the serialized type value.
   * @return the corresponding {@link RequestType}.
   * @throws IllegalArgumentException if the serialized type value does not correspond to any {@link
   *     RequestType}.
   */
  public static RequestType of(final int serializedTypeValue) {
    return switch (serializedTypeValue) {
      case 0x00 -> DEPOSIT;
      case 0x01 -> WITHDRAWAL;
      case 0x02 -> CONSOLIDATION;
      case 0x03 -> BUILDER_DEPOSIT;
      case 0x04 -> BUILDER_EXIT;
      default ->
          throw new InvalidRequestTypeException(
              String.format("Unsupported request type: 0x%02X", serializedTypeValue));
    };
  }

  /**
   * Exception thrown when an invalid request type is encountered.
   *
   * <p>This exception is thrown when a serialized type value does not correspond to any {@link
   * RequestType}.
   */
  public static class InvalidRequestTypeException extends IllegalArgumentException {
    /**
     * Constructs an {@link InvalidRequestTypeException} with the specified detail message.
     *
     * @param message the detail message.
     */
    public InvalidRequestTypeException(final String message) {
      super(message);
    }
  }
}
