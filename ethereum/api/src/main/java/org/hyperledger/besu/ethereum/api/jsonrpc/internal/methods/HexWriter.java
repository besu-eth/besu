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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/**
 * Zero-allocation hex encoding of byte arrays directly into an output buffer using a short-wide
 * store via {@link VarHandle}, writing two hex ASCII chars per input byte in a single store
 * instruction.
 *
 * <p>The caller must ensure {@code dest} has enough room: at most {@code max(3, 2 + len * 2)}
 * bytes. When {@code len == 0}, three bytes are written ({@code 0x0}).
 */
public final class HexWriter {

  /**
   * byte value to two adjacent hex ASCII bytes: {@code HEX_PAIR[(b & 0xFF) << 1]} is high nibble.
   */
  static final byte[] HEX_PAIR = new byte[512];

  /** byte value to two hex ASCII bytes packed as a native-endian {@code short}. */
  private static final short[] HEX_SHORT = new short[256];

  private static final VarHandle SHORT_HANDLE =
      MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.nativeOrder());

  static {
    final byte[] hex = {
      '0', '1', '2', '3', '4', '5', '6', '7',
      '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
    };
    for (int i = 0; i < 256; i++) {
      final byte hi = hex[(i >> 4) & 0xF];
      final byte lo = hex[i & 0xF];
      HEX_PAIR[i << 1] = hi;
      HEX_PAIR[(i << 1) + 1] = lo;
      if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
        HEX_SHORT[i] = (short) ((lo << 8) | (hi & 0xFF));
      } else {
        HEX_SHORT[i] = (short) ((hi << 8) | (lo & 0xFF));
      }
    }
  }

  private HexWriter() {}

  /**
   * Encodes {@code bytes[0..len)} directly into {@code dest} at {@code destPos} as {@code
   * 0x}-prefixed hex.
   *
   * @param stripLeading if true, leading zero bytes and the high zero nibble of the first non-zero
   *     byte are omitted (e.g. {@code 0x1}); if false, every byte produces two hex chars (e.g.
   *     {@code 0x0001}).
   * @return the new write position in {@code dest}
   */
  public static int encodeTo(
      final byte[] bytes,
      final int len,
      final byte[] dest,
      final int destPos,
      final boolean stripLeading) {
    int wp = destPos;
    dest[wp++] = '0';
    dest[wp++] = 'x';

    if (len == 0) {
      dest[wp++] = '0';
      return wp;
    }

    int start = 0;
    if (stripLeading) {
      while (start < len && bytes[start] == 0) start++;

      if (start == len) {
        dest[wp++] = '0';
        return wp;
      }

      // First non-zero byte — emit nibbles individually to strip a leading zero nibble
      final int idx = (bytes[start] & 0xFF) << 1;
      if (HEX_PAIR[idx] != '0') {
        dest[wp++] = HEX_PAIR[idx];
      }
      dest[wp++] = HEX_PAIR[idx + 1];
      start++;
    }

    // Remaining bytes — one short-wide store (2 hex chars) per input byte
    for (int i = start; i < len; i++) {
      SHORT_HANDLE.set(dest, wp, HEX_SHORT[bytes[i] & 0xFF]);
      wp += 2;
    }
    return wp;
  }
}
