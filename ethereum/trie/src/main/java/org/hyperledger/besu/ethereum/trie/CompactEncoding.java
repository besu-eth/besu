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
package org.hyperledger.besu.ethereum.trie;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.tuweni.bytes.Bytes;

public abstract class CompactEncoding {
  private CompactEncoding() {}

  public static final byte LEAF_TERMINATOR = 0x10;

  /**
   * Converts a byte sequence into a path by splitting each byte into two nibbles. The resulting
   * path is terminated with a leaf terminator.
   *
   * @param bytes the byte sequence to convert into a path
   * @return the resulting path
   */
  public static byte[] bytesToPath(final byte[] bytes) {
    final byte[] path = new byte[bytes.length * 2 + 1];
    int j = 0;
    for (int i = 0; i < bytes.length; i += 1, j += 2) {
      final byte b = bytes[i];
      path[j] = (byte) ((b >>> 4) & 0x0f);
      path[j + 1] = (byte) (b & 0x0f);
    }
    path[j] = LEAF_TERMINATOR;
    return path;
  }

  /**
   * Converts a path into a byte sequence by combining each pair of nibbles into a byte. The path
   * must be a leaf path, i.e., it must be terminated with a leaf terminator.
   *
   * @param path the path to convert into a byte sequence
   * @return the resulting byte sequence
   * @throws IllegalArgumentException if the path is empty or not a leaf path, or if it contains
   *     elements larger than a nibble
   */
  public static byte[] pathToBytes(final byte[] path) {
    checkArgument(path.length > 0, "Path must not be empty");
    checkArgument(path[path.length - 1] == LEAF_TERMINATOR, "Path must be a leaf path");
    final byte[] bytes = new byte[(path.length - 1) / 2];
    int bytesPos = 0;
    for (int pathPos = 0; pathPos < path.length - 1; pathPos += 2, bytesPos += 1) {
      final byte high = path[pathPos];
      final byte low = path[pathPos + 1];
      if ((high & 0xf0) != 0 || (low & 0xf0) != 0) {
        throw new IllegalArgumentException("Invalid path: contains elements larger than a nibble");
      }
      bytes[bytesPos] = (byte) (high << 4 | low);
    }
    return bytes;
  }

  /**
   * Encodes a path into a compact form. The encoding includes a metadata byte that indicates
   * whether the path is a leaf path and whether its length is odd or even.
   *
   * @param path the path to encode
   * @return the encoded path
   * @throws IllegalArgumentException if the path contains elements larger than a nibble
   */
  public static byte[] encode(final byte[] path) {
    int size = path.length;
    final boolean isLeaf = size > 0 && path[size - 1] == LEAF_TERMINATOR;
    if (isLeaf) {
      size = size - 1;
    }

    final byte[] encoded = new byte[(size + 2) / 2];
    int i = 0;
    int j = 0;

    if (size % 2 == 1) {
      // add first nibble to magic
      final byte high = (byte) (isLeaf ? 0x03 : 0x01);
      final byte low = path[i++];
      if ((low & 0xf0) != 0) {
        throw new IllegalArgumentException("Invalid path: contains elements larger than a nibble");
      }
      encoded[j++] = (byte) (high << 4 | low);
    } else {
      final byte high = (byte) (isLeaf ? 0x02 : 0x00);
      encoded[j++] = (byte) (high << 4);
    }

    while (i < size) {
      final byte high = path[i++];
      final byte low = path[i++];
      if ((high & 0xf0) != 0 || (low & 0xf0) != 0) {
        throw new IllegalArgumentException("Invalid path: contains elements larger than a nibble");
      }
      encoded[j++] = (byte) (high << 4 | low);
    }

    return encoded;
  }

  /**
   * Decodes a path from its compact form. The decoding process takes into account the metadata byte
   * that indicates whether the path is a leaf path and whether its length is odd or even.
   *
   * @param encoded the array holding the encoded path
   * @param offset the offset of the encoded path in the array
   * @param size the size of the encoded path
   * @return the decoded path
   * @throws IllegalArgumentException if the encoded path is empty or its metadata byte is invalid
   */
  public static byte[] decode(final byte[] encoded, final int offset, final int size) {
    checkArgument(size > 0);
    final byte metadata = encoded[offset];
    checkArgument((metadata & 0xc0) == 0, "Invalid compact encoding");

    final boolean isLeaf = (metadata & 0x20) != 0;

    final int pathLength = ((size - 1) * 2) + (isLeaf ? 1 : 0);
    final byte[] path;
    int i = 0;

    if ((metadata & 0x10) != 0) {
      // need to use lower nibble of metadata
      path = new byte[pathLength + 1];
      path[i++] = (byte) (metadata & 0x0f);
    } else {
      path = new byte[pathLength];
    }

    for (int j = 1; j < size; j++) {
      final byte b = encoded[offset + j];
      path[i++] = (byte) ((b >>> 4) & 0x0f);
      path[i++] = (byte) (b & 0x0f);
    }

    if (isLeaf) {
      path[i] = LEAF_TERMINATOR;
    }

    return path;
  }

  public static byte[] decode(final byte[] encoded) {
    return decode(encoded, 0, encoded.length);
  }

  /** See {@link #bytesToPath(byte[])}. */
  public static Bytes bytesToPath(final Bytes bytes) {
    return Bytes.wrap(bytesToPath(bytes.toArrayUnsafe()));
  }

  /** See {@link #pathToBytes(byte[])}. */
  public static Bytes pathToBytes(final Bytes path) {
    return Bytes.wrap(pathToBytes(path.toArrayUnsafe()));
  }

  /** See {@link #encode(byte[])}. */
  public static Bytes encode(final Bytes path) {
    return Bytes.wrap(encode(path.toArrayUnsafe()));
  }

  /** See {@link #decode(byte[], int, int)}. */
  public static Bytes decode(final Bytes encoded) {
    return Bytes.wrap(decode(encoded.toArrayUnsafe()));
  }
}
