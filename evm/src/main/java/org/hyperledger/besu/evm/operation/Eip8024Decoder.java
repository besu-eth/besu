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
package org.hyperledger.besu.evm.operation;

/**
 * Shared decoder tables for EIP-8024 operations (DUPN, SWAPN, EXCHANGE).
 *
 * <p>EIP-8024 uses special immediate encoding to avoid bytes that could be confused with JUMPDEST
 * (0x5b) or PUSH opcodes (0x60-0x7f), ensuring backward compatibility with existing deployed
 * contracts.
 */
// Thread-safety note: All static arrays in this class are effectively immutable.
// They are populated once during class initialization and never modified thereafter,
// making them safe for concurrent read access in a multi-threaded environment.
@SuppressWarnings("MutablePublicArray") // Performance: hot-path direct array access is intentional
public final class Eip8024Decoder {

  private Eip8024Decoder() {
    // Utility class
  }

  /** Marker value indicating an invalid immediate in packed pair encoding. */
  public static final int INVALID_PAIR = -1;

  // ==================== Single Operand Decoding (DUPN, SWAPN) ====================

  /**
   * Decode table for single operand instructions (DUPN, SWAPN). Maps immediate byte value to
   * decoded n value using branchless formula: n = (x + 145) % 256. Only meaningful for valid
   * immediates where {@link #VALID_SINGLE}[x] is true. Valid range: 0-90 maps to 145-235, 128-255
   * maps to 17-144. Invalid range: 91-127 (entries left at default 0).
   */
  public static final int[] DECODE_SINGLE = new int[256];

  /**
   * Validity table for single operand immediates. True for valid immediates (0-90, 128-255), false
   * for invalid (91-127).
   */
  public static final boolean[] VALID_SINGLE = new boolean[256];

  // ==================== Pair Operand Decoding (EXCHANGE) ====================

  /**
   * Validity table for pair operand immediates. True for valid immediates (0-81, 128-255), false
   * for invalid (82-127).
   */
  public static final boolean[] VALID_PAIR = new boolean[256];

  /**
   * Packed decode table for pair operand instructions (EXCHANGE). Combines validity check and both
   * indices into a single lookup for better cache performance.
   *
   * <p>Encoding: If valid, low 8 bits contain n, bits 8-15 contain m. If invalid, value is {@link
   * #INVALID_PAIR} (-1).
   *
   * <p>Usage:
   *
   * <pre>{@code
   * int packed = DECODE_PAIR_PACKED[imm];
   * if (packed == INVALID_PAIR) {
   *   // handle invalid
   * }
   * int n = packed & 0xFF;
   * int m = (packed >>> 8) & 0xFF;
   * }</pre>
   */
  public static final int[] DECODE_PAIR_PACKED = new int[256];

  static {
    initializeSingleDecodeTables();
    initializePairDecodeTables();
  }

  /**
   * Initialize decode tables for single operand per EIP-8024 decode_single specification:
   *
   * <pre>
   * n = (x + 145) % 256 (branchless)
   * Valid range: 0-90 (n = 145-235) and 128-255 (n = 17-144)
   * Invalid range: 91-127 (preserves JUMPDEST 0x5b and PUSH 0x60-0x7f)
   * </pre>
   */
  private static void initializeSingleDecodeTables() {
    for (int x = 0; x <= 90; x++) {
      DECODE_SINGLE[x] = (x + 145) % 256;
      VALID_SINGLE[x] = true;
    }
    for (int x = 128; x <= 255; x++) {
      DECODE_SINGLE[x] = (x + 145) % 256;
      VALID_SINGLE[x] = true;
    }
    // 91-127 remain at default values (0 and false)
  }

  /**
   * Initialize decode tables for pair operand per EIP-8024 decode_pair specification:
   *
   * <pre>
   * k = x ^ 143 (branchless XOR)
   * q, r = divmod(k, 16)
   * if q < r: return (q + 1, r + 1)
   * else: return (r + 1, 29 - q)
   * </pre>
   *
   * <p>Valid range: 0-81 and 128-255. Invalid range: 82-127.
   */
  private static void initializePairDecodeTables() {
    // Initialize all as invalid first
    for (int x = 0; x < 256; x++) {
      DECODE_PAIR_PACKED[x] = INVALID_PAIR;
    }

    for (int x = 0; x <= 81; x++) {
      decodePairIntoTables(x, x ^ 143);
      VALID_PAIR[x] = true;
    }
    for (int x = 128; x <= 255; x++) {
      decodePairIntoTables(x, x ^ 143);
      VALID_PAIR[x] = true;
    }
    // 82-127 remain at default values (0, 0, false, INVALID_PAIR)
  }

  private static void decodePairIntoTables(final int x, final int k) {
    final int q = k / 16;
    final int r = k % 16;
    final int n;
    final int m;
    if (q < r) {
      n = q + 1;
      m = r + 1;
    } else {
      n = r + 1;
      m = 29 - q;
    }

    // Populate packed table: n in low 8 bits, m in bits 8-15
    DECODE_PAIR_PACKED[x] = (m << 8) | n;
  }

  /**
   * Extracts the n value from a packed pair encoding.
   *
   * @param packed the packed value from {@link #DECODE_PAIR_PACKED}
   * @return the n index value
   */
  public static int unpackN(final int packed) {
    return packed & 0xFF;
  }

  /**
   * Extracts the m value from a packed pair encoding.
   *
   * @param packed the packed value from {@link #DECODE_PAIR_PACKED}
   * @return the m index value
   */
  public static int unpackM(final int packed) {
    return (packed >>> 8) & 0xFF;
  }

  /**
   * Decodes a single immediate byte to the stack index n.
   *
   * @param imm the immediate byte value (0-255)
   * @return the decoded n value, or -1 if the immediate is invalid
   */
  public static int decodeSingle(final int imm) {
    if (imm < 0 || imm > 255 || !VALID_SINGLE[imm]) {
      return -1;
    }
    return DECODE_SINGLE[imm];
  }

  /**
   * Decodes a pair immediate byte to the stack indices n and m.
   *
   * @param imm the immediate byte value (0-255)
   * @return an array of [n, m], or null if the immediate is invalid
   */
  public static int[] decodePair(final int imm) {
    if (imm < 0 || imm > 255) {
      return null;
    }
    final int packed = DECODE_PAIR_PACKED[imm];
    if (packed == INVALID_PAIR) {
      return null;
    }
    return new int[] {unpackN(packed), unpackM(packed)};
  }
}
