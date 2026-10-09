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
import java.util.HexFormat;

/**
 * Operations on nibble paths and locations, stored one nibble per byte.
 *
 * <p>Path and location arrays are shared between nodes and are never modified once created.
 */
public final class Nibbles {

  public static final byte[] EMPTY = new byte[0];

  private static final HexFormat HEX = HexFormat.of();

  private Nibbles() {}

  /** Length of the common prefix of {@code path} and {@code other} read from {@code offset}. */
  public static int commonPrefixLength(final byte[] path, final byte[] other, final int offset) {
    final int mismatch = Arrays.mismatch(path, 0, path.length, other, offset, other.length);
    return mismatch < 0 ? path.length : mismatch;
  }

  /** The nibbles of {@code path} from {@code from}, sharing the array when nothing is cut. */
  public static byte[] slice(final byte[] path, final int from) {
    return from == 0 ? path : Arrays.copyOfRange(path, from, path.length);
  }

  /** The nibbles of {@code path} between {@code from} and {@code to}. */
  public static byte[] slice(final byte[] path, final int from, final int to) {
    return from == 0 && to == path.length ? path : Arrays.copyOfRange(path, from, to);
  }

  public static byte[] concat(final byte[] first, final byte[] second) {
    if (first.length == 0) {
      return second;
    }
    if (second.length == 0) {
      return first;
    }
    final byte[] result = Arrays.copyOf(first, first.length + second.length);
    System.arraycopy(second, 0, result, first.length, second.length);
    return result;
  }

  public static byte[] append(final byte[] path, final int nibble) {
    final byte[] result = Arrays.copyOf(path, path.length + 1);
    result[path.length] = (byte) nibble;
    return result;
  }

  public static byte[] prepend(final int nibble, final byte[] path) {
    final byte[] result = new byte[path.length + 1];
    result[0] = (byte) nibble;
    System.arraycopy(path, 0, result, 1, path.length);
    return result;
  }

  /** Hex representation prefixed by 0x. */
  public static String toHexString(final byte[] bytes) {
    return bytes == null ? "null" : "0x" + HEX.formatHex(bytes);
  }
}
