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

/** A BLS Signature * */
public class BLSSignature extends BytesHolder {

  /** The constant SIZE. */
  public static final int SIZE = 96;

  /**
   * Instantiates a new BLSSignature.
   *
   * @param bytes the bytes
   */
  protected BLSSignature(final Bytes bytes) {
    super(bytes);
  }

  /**
   * Wrap signature.
   *
   * @param value the value
   * @return the signature
   */
  public static BLSSignature wrap(final Bytes value) {
    checkArgument(
        value.size() == SIZE, "A signature must be %s bytes long, got %s", SIZE, value.size());
    return new BLSSignature(value);
  }

  /**
   * Creates a signature from the given RLP-encoded input.
   *
   * @param input The input to read from
   * @return the input's corresponding signature
   */
  public static BLSSignature readFrom(final RLPInput input) {
    final Bytes bytes = input.readBytes();
    if (bytes.size() != SIZE) {
      throw new RLPException(
          String.format("BLSSignature unexpected size of %s (needs %s)", bytes.size(), SIZE));
    }
    return BLSSignature.wrap(bytes);
  }

  /**
   * Parse a hexadecimal string representing a signature.
   *
   * @param str A hexadecimal string (with or without the leading '0x') representing a valid bls
   *     signature.
   * @return The parsed signature: {@code null} if the provided string is {@code null}.
   * @throws IllegalArgumentException if the string is either not hexadecimal, or not the valid
   *     representation of a bls signature.
   */
  @JsonCreator
  public static @Nullable BLSSignature fromHexString(final String str) {
    if (str == null) return null;
    return wrap(Bytes.fromHexStringLenient(str, SIZE));
  }
}
