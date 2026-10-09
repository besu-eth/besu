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
package org.hyperledger.besu.ethereum.trie;

/**
 * RLP encoding of trie nodes straight into pre-sized byte arrays. Sizes are computed first, then
 * items are written at a position and the next position is returned.
 */
public final class TrieRlp {

  /** RLP encoding of the empty byte string, which is also the encoding of an empty node. */
  public static final byte NULL = (byte) 0x80;

  /** Size of a hash reference to a node: a 32 bytes string with its one byte prefix. */
  public static final int HASH_REF_SIZE = 1 + Keccak256.SIZE;

  private static final int SHORT_STRING_OFFSET = 0x80;
  private static final int LONG_STRING_OFFSET = 0xb7;
  private static final int SHORT_LIST_OFFSET = 0xc0;
  private static final int LONG_LIST_OFFSET = 0xf7;
  private static final int MAX_SHORT_LENGTH = 55;

  private TrieRlp() {}

  /** Encoded size of a byte string. */
  public static int bytesSize(final byte[] bytes) {
    if (isSingleByte(bytes)) {
      return 1;
    }
    return headerSize(bytes.length) + bytes.length;
  }

  /** Encoded size of a list with the given payload size. */
  public static int listSize(final int payloadSize) {
    return headerSize(payloadSize) + payloadSize;
  }

  public static int writeListHeader(final byte[] out, final int pos, final int payloadSize) {
    return writeHeader(out, pos, payloadSize, SHORT_LIST_OFFSET, LONG_LIST_OFFSET);
  }

  public static int writeBytes(final byte[] out, final int pos, final byte[] bytes) {
    if (isSingleByte(bytes)) {
      out[pos] = bytes[0];
      return pos + 1;
    }
    final int start = writeHeader(out, pos, bytes.length, SHORT_STRING_OFFSET, LONG_STRING_OFFSET);
    System.arraycopy(bytes, 0, out, start, bytes.length);
    return start + bytes.length;
  }

  public static int writeHash(final byte[] out, final int pos, final byte[] hash) {
    out[pos] = (byte) (SHORT_STRING_OFFSET + Keccak256.SIZE);
    System.arraycopy(hash, 0, out, pos + 1, Keccak256.SIZE);
    return pos + HASH_REF_SIZE;
  }

  /** Encodes a hash as a 32 bytes string. */
  public static byte[] encodeHash(final byte[] hash) {
    final byte[] out = new byte[HASH_REF_SIZE];
    writeHash(out, 0, hash);
    return out;
  }

  private static boolean isSingleByte(final byte[] bytes) {
    return bytes.length == 1 && (bytes[0] & 0xff) < SHORT_STRING_OFFSET;
  }

  private static int headerSize(final int length) {
    return length <= MAX_SHORT_LENGTH ? 1 : 1 + lengthOfLength(length);
  }

  private static int lengthOfLength(final int length) {
    return (Integer.SIZE - Integer.numberOfLeadingZeros(length) + 7) / 8;
  }

  private static int writeHeader(
      final byte[] out,
      final int pos,
      final int length,
      final int shortOffset,
      final int longOffset) {
    if (length <= MAX_SHORT_LENGTH) {
      out[pos] = (byte) (shortOffset + length);
      return pos + 1;
    }
    final int lengthOfLength = lengthOfLength(length);
    out[pos] = (byte) (longOffset + lengthOfLength);
    for (int i = 0; i < lengthOfLength; i++) {
      out[pos + 1 + i] = (byte) (length >>> (8 * (lengthOfLength - 1 - i)));
    }
    return pos + 1 + lengthOfLength;
  }
}
