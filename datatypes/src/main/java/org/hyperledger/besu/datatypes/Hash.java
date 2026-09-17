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

import static org.hyperledger.besu.crypto.Hash.keccak256;
import static org.hyperledger.besu.crypto.Hash.sha256;

import org.hyperledger.besu.ethereum.rlp.RLP;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** A 32-bytes hash value as used in Ethereum blocks, usually the result of the KEC algorithm. */
public class Hash extends BytesHolder {

  /** The constant ZERO. */
  public static final Hash ZERO = new Hash(Bytes32.ZERO);

  /** Last hash */
  public static final Hash LAST = new Hash(Bytes32.fromHexString("F".repeat(64)));

  /**
   * Hash of an RLP encoded trie hash with no content, or
   * "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"
   */
  public static final Hash EMPTY_TRIE_HASH = Hash.hash(RLP.NULL);

  /**
   * Hash of a zero length RLP list, or
   * "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347"
   */
  public static final Hash EMPTY_LIST_HASH = Hash.hash(RLP.EMPTY_LIST);

  /**
   * Hash of an empty string, or
   * "0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
   */
  public static final Hash EMPTY = hash(Bytes.EMPTY);

  /**
   * Hash of empty requests or "0xe3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
   */
  public static final Hash EMPTY_REQUESTS_HASH = Hash.wrap(sha256(Bytes.EMPTY));

  /**
   * Hash of empty block access list or
   * "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347"
   */
  public static final Hash EMPTY_BAL_HASH = EMPTY_LIST_HASH;

  /**
   * Instantiates a new Hash.
   *
   * @param bytes raw bytes
   */
  protected Hash(final Bytes32 bytes) {
    super(bytes);
  }

  /**
   * Convert value to keccak256 hash.
   *
   * @param value the value
   * @return the hash
   */
  public static Hash hash(final Bytes value) {
    return new Hash(keccak256(value));
  }

  /**
   * Wrap bytes to hash.
   *
   * @param bytes the bytes
   * @return the hash
   */
  @JsonCreator
  public static Hash wrap(final Bytes32 bytes) {
    return new Hash(bytes);
  }

  /**
   * Parse an hexadecimal string representing a hash value.
   *
   * @param str An hexadecimal string (with or without the leading '0x') representing a valid hash
   *     value.
   * @return The parsed hash.
   * @throws NullPointerException if the provided string is {@code null}.
   * @throws IllegalArgumentException if the string is either not hexadecimal, or not the valid
   *     representation of a hash (not 32 bytes).
   */
  public static Hash fromHexString(final String str) {
    return new Hash(Bytes32.fromHexStringStrict(str));
  }

  /**
   * From hex string lenient hash.
   *
   * @param str the str
   * @return the hash
   */
  public static Hash fromHexStringLenient(final String str) {
    return new Hash(Bytes32.fromHexStringLenient(str));
  }

  /***
   * For logging purposes, this method returns a shortened hex representation
   *
   * @return shortened string with only the beginning and the end of the hex representation
   */
  public String toShortLogString() {
    final var hexRepresentation = getBytes().toFastHex(false);
    String firstPart = hexRepresentation.substring(0, 5);
    String lastPart = hexRepresentation.substring(hexRepresentation.length() - 5);
    return firstPart + "....." + lastPart;
  }
}
