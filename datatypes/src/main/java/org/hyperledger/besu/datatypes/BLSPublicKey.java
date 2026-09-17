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

import static com.google.common.base.Preconditions.checkArgument;

import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.tuweni.bytes.Bytes;
import org.jspecify.annotations.Nullable;

/** A BLS public key. */
public class BLSPublicKey extends BytesHolder {

  /** The constant SIZE. */
  public static final int SIZE = 48;

  /**
   * Instantiates a new BLSPublicKey.
   *
   * @param bytes the bytes
   */
  protected BLSPublicKey(final Bytes bytes) {
    super(bytes);
  }

  /**
   * Wrap public key.
   *
   * @param value the value
   * @return the BLS public key
   */
  public static BLSPublicKey wrap(final Bytes value) {
    checkArgument(
        value.size() == SIZE, "A BLS public key must be %s bytes long, got %s", SIZE, value.size());
    return new BLSPublicKey(value);
  }

  /**
   * Creates a bls public key from the given RLP-encoded input.
   *
   * @param input The input to read from
   * @return the input's corresponding public key
   */
  public static BLSPublicKey readFrom(final RLPInput input) {
    final Bytes bytes = input.readBytes();
    if (bytes.size() != SIZE) {
      throw new RLPException(
          String.format("BLSPublicKey unexpected size of %s (needs %s)", bytes.size(), SIZE));
    }
    return BLSPublicKey.wrap(bytes);
  }

  /**
   * Parse a hexadecimal string representing an public key.
   *
   * @param str A hexadecimal string (with or without the leading '0x') representing a valid bls
   *     public key.
   * @return The parsed bls public key: {@code null} if the provided string is {@code null}.
   * @throws IllegalArgumentException if the string is either not hexadecimal, or not the valid
   *     representation of a BLSPublicKey.
   */
  @JsonCreator
  public static @Nullable BLSPublicKey fromHexString(final String str) {
    if (str == null) return null;
    return wrap(Bytes.fromHexStringLenient(str, SIZE));
  }
}
