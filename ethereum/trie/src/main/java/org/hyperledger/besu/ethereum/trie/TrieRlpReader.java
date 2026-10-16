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

import java.util.Arrays;

/**
 * Strict RLP reader over a byte array, covering what trie node decoding needs. The whole input must
 * be a single item, and non canonical encodings are rejected.
 */
public final class TrieRlpReader {

  private final byte[] data;
  private int pos;
  private int limit;
  private int[] listLimits = new int[4];
  private int depth;

  // header of the item at itemPos, parsed lazily
  private int itemPos = -1;
  private boolean itemIsList;
  private int payloadOffset;
  private int payloadLength;

  public TrieRlpReader(final byte[] data) {
    this.data = data;
    this.limit = data.length;
    if (data.length > 0) {
      parseItem();
      if (itemEnd() != data.length) {
        throw new MalformedRlpException(
            "Input has extra data after RLP encoding: encoding ends at byte "
                + itemEnd()
                + " but input has size "
                + data.length);
      }
    }
  }

  public byte[] data() {
    return data;
  }

  public boolean nextIsList() {
    parseItem();
    return itemIsList;
  }

  public boolean nextIsNull() {
    parseItem();
    return !itemIsList && payloadLength == 0;
  }

  /** Offset of the payload of the next item, which must be a byte string. */
  public int bytesOffset() {
    requireBytes();
    return payloadOffset;
  }

  /** Length of the payload of the next item, which must be a byte string. */
  public int bytesLength() {
    requireBytes();
    return payloadLength;
  }

  public void skipNext() {
    parseItem();
    pos = itemEnd();
  }

  /** Reads the next item, which must be a 32 bytes string. */
  public byte[] readHash() {
    requireBytes();
    if (payloadLength != Keccak256.SIZE) {
      throw new MalformedRlpException(
          "Cannot read a 32 bytes value, current item is of size " + payloadLength);
    }
    final byte[] hash = Arrays.copyOfRange(data, payloadOffset, payloadOffset + Keccak256.SIZE);
    pos = itemEnd();
    return hash;
  }

  /** Enters the next item, which must be a list, and returns its number of elements. */
  public int enterList() {
    parseItem();
    if (!itemIsList) {
      throw new MalformedRlpException("Expected current item to be a list, but it is not");
    }
    if (depth == listLimits.length) {
      listLimits = Arrays.copyOf(listLimits, depth * 2);
    }
    listLimits[depth++] = limit;
    limit = itemEnd();
    final int firstElement = payloadOffset;

    pos = firstElement;
    int count = 0;
    while (pos < limit) {
      parseItem();
      pos = itemEnd();
      count++;
    }
    pos = firstElement;
    return count;
  }

  public void leaveList() {
    if (depth == 0) {
      throw new MalformedRlpException("Not within an RLP list");
    }
    if (pos != limit) {
      throw new MalformedRlpException("Not at the end of the current list");
    }
    limit = listLimits[--depth];
    itemPos = -1;
  }

  private void requireBytes() {
    parseItem();
    if (itemIsList) {
      throw new MalformedRlpException(
          "Expected current item to be a byte string, but it is a list");
    }
  }

  private int itemEnd() {
    return payloadOffset + payloadLength;
  }

  private void parseItem() {
    if (itemPos == pos) {
      return;
    }
    if (pos >= limit) {
      throw new MalformedRlpException("Cannot read an item, reached end of current list");
    }
    final int prefix = data[pos] & 0xff;
    if (prefix < 0x80) {
      itemIsList = false;
      payloadOffset = pos;
      payloadLength = 1;
    } else if (prefix <= 0xb7) {
      itemIsList = false;
      payloadOffset = pos + 1;
      payloadLength = prefix - 0x80;
      if (payloadLength == 1 && payloadOffset < limit && (data[payloadOffset] & 0xff) < 0x80) {
        throw new MalformedRlpException(
            "Single byte value should have been written without a prefix");
      }
    } else if (prefix < 0xc0) {
      itemIsList = false;
      readLongLength(prefix - 0xb7);
    } else if (prefix <= 0xf7) {
      itemIsList = true;
      payloadOffset = pos + 1;
      payloadLength = prefix - 0xc0;
    } else {
      itemIsList = true;
      readLongLength(prefix - 0xf7);
    }
    if (payloadLength > limit - payloadOffset) {
      throw new MalformedRlpException(
          "Item payload of "
              + payloadLength
              + " bytes at offset "
              + payloadOffset
              + " exceeds the enclosing item");
    }
    itemPos = pos;
  }

  private void readLongLength(final int lengthOfLength) {
    final int start = pos + 1;
    if (lengthOfLength > 4 || lengthOfLength > limit - start) {
      throw new MalformedRlpException("Invalid RLP item: cannot read its payload size");
    }
    if (data[start] == 0) {
      throw new MalformedRlpException("Malformed RLP item: size of payload has leading zeros");
    }
    int length = 0;
    for (int i = 0; i < lengthOfLength; i++) {
      length = (length << 8) | (data[start + i] & 0xff);
    }
    if (length < 56) {
      // also catches sizes overflowing an int
      throw new MalformedRlpException(
          "Malformed RLP item: written as a long item, but size " + length + " < 56 bytes");
    }
    payloadOffset = start + lengthOfLength;
    payloadLength = length;
  }

  /** Thrown when the input is not a valid RLP encoding. */
  public static final class MalformedRlpException extends RuntimeException {
    public MalformedRlpException(final String message) {
      super(message);
    }
  }
}
