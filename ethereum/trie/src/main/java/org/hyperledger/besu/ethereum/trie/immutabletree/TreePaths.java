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
package org.hyperledger.besu.ethereum.trie.immutabletree;

import org.hyperledger.besu.ethereum.trie.CompactEncoding;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.MutableBytes;

/** Helpers for nibble paths: one nibble per byte, leaf paths end with the leaf terminator. */
final class TreePaths {

  static final int RADIX = 16;

  /** Single-nibble paths, shared so that child locations and prefixes don't allocate. */
  private static final Bytes[] NIBBLES = new Bytes[RADIX + 1];

  static {
    for (int i = 0; i <= RADIX; i++) {
      NIBBLES[i] = Bytes.of((byte) i);
    }
  }

  private TreePaths() {}

  /**
   * Returns the one-nibble path {@code [nibble]}; {@code nibble(16)} is the bare leaf terminator.
   */
  static Bytes nibble(final int nibble) {
    return NIBBLES[nibble];
  }

  static boolean isTerminator(final byte nibble) {
    return nibble == CompactEncoding.LEAF_TERMINATOR;
  }

  /** Concatenates two paths into a flat array, so that long-lived nodes don't hold views. */
  static Bytes concat(final Bytes first, final Bytes second) {
    if (second.isEmpty()) {
      return first;
    }
    if (first.isEmpty()) {
      return second;
    }
    final MutableBytes joined = MutableBytes.create(first.size() + second.size());
    first.copyTo(joined, 0);
    second.copyTo(joined, first.size());
    return joined;
  }

  /**
   * Length of the common prefix of {@code a} and {@code b.slice(bOffset)}, without allocating the
   * slice.
   */
  static int commonPrefixLength(final Bytes a, final Bytes b, final int bOffset) {
    final int max = Math.min(a.size(), b.size() - bOffset);
    int i = 0;
    while (i < max && a.get(i) == b.get(bOffset + i)) {
      i++;
    }
    return i;
  }

  /** Length of the common prefix of {@code a.slice(offset)} and {@code b.slice(offset)}. */
  static int commonPrefixLength(final Bytes a, final Bytes b, final int offset, final int limit) {
    final int max = Math.min(Math.min(a.size(), b.size()) - offset, limit);
    int i = 0;
    while (i < max && a.get(offset + i) == b.get(offset + i)) {
      i++;
    }
    return i;
  }
}
