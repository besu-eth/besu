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
package org.hyperledger.besu.util;

import java.util.HexFormat;

import org.apache.tuweni.bytes.Bytes;

/** Utility class for Hexadecimal operations. */
public class HexUtils {

  private HexUtils() {}

  private static final char[] HEX = "0123456789abcdef".toCharArray();

  /**
   * Optimized version of org.apache.tuweni.bytes.Bytes.toFastHex that avoids the megamorphic get
   * and size calls
   *
   * @param abytes The bytes to convert
   * @param prefix whether to include the "0x" prefix
   * @return The hex string representation
   */
  public static String toFastHex(final Bytes abytes, final boolean prefix) {
    final byte[] bytes = abytes.toArrayUnsafe();
    final int size = bytes.length;

    final int offset = prefix ? 2 : 0;

    final int resultSize = (size * 2) + offset;

    final char[] result = new char[resultSize];

    if (prefix) {
      result[0] = '0';
      result[1] = 'x';
    }

    for (int i = 0; i < size; i++) {
      byte b = bytes[i];
      int pos = i * 2;
      result[pos + offset] = HEX[b >> 4 & 15];
      result[pos + offset + 1] = HEX[b & 15];
    }

    return new String(result);
  }

  /**
   * Faster version of {@link Bytes#fromHexString} for the form the JSON-RPC APIs use: a 0x prefix
   * and an even number of digits. Any other string, including one with a character that is not a
   * hex digit, is decoded, or rejected, by {@link Bytes#fromHexString}.
   *
   * @param hex the hex string
   * @return the bytes
   */
  public static Bytes fromHexString(final String hex) {
    if (hex.length() % 2 == 0 && hex.startsWith("0x")) {
      try {
        return Bytes.wrap(HexFormat.of().parseHex(hex, 2, hex.length()));
      } catch (final IllegalArgumentException e) {
        // not all hex digits, rejected below with tuweni's message
      }
    }
    return Bytes.fromHexString(hex);
  }
}
