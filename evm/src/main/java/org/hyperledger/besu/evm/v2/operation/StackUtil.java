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
package org.hyperledger.besu.evm.v2.operation;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BytesHolder;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.UInt256;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.jspecify.annotations.Nullable;

/**
 * Static utility for reading/writing typed values on the flat {@code long[]} V2 operand stack. Each
 * 256-bit word occupies 4 consecutive longs in big-endian limb order: {@code [u3, u2, u1, u0]}
 * where u3 is the most-significant limb. This class has a default modifier (package-private)
 * because it shouldn't be used outside the EVM operations
 */
final class StackUtil {

  private static final VarHandle LONG_BE =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);
  private static final VarHandle INT_BE =
      MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);

  private StackUtil() {}

  /**
   * Writes a zero-valued 256-bit word at the given stack slot.
   *
   * @param stack the flat limb array (4 longs per 256-bit word)
   * @param top the slot index to write to
   */
  static void pushZero(final long[] stack, final int top) {
    final int offset = top << 2;
    stack[offset] = 0;
    stack[offset + 1] = 0;
    stack[offset + 2] = 0;
    stack[offset + 3] = 0;
  }

  /**
   * Writes a {@link Wei} value as four big-endian limbs at the given stack slot.
   *
   * @param wei the Wei value to write
   * @param stack the flat limb array
   * @param top the slot index to write to
   */
  static void pushWei(final Wei wei, final long[] stack, final int top) {
    // TODO EVMv2 store this representation at Wei object construction time when switching from v2
    // to v1
    int offset = top << 2;
    final byte[] b = wei.toArrayUnsafe();
    stack[offset] = (long) LONG_BE.get(b, 0);
    stack[offset + 1] = (long) LONG_BE.get(b, 8);
    stack[offset + 2] = (long) LONG_BE.get(b, 16);
    stack[offset + 3] = (long) LONG_BE.get(b, 24);
  }

  /**
   * Writes a primitive {@code long} as a zero-extended 256-bit word at the given stack slot. The
   * three most-significant limbs are set to zero and the value is written as the least-significant
   * limb {@code u0}.
   *
   * @param value the unsigned long value to write
   * @param stack the flat limb array
   * @param top the slot index to write to
   */
  static void pushLong(final long value, final long[] stack, final int top) {
    final int offset = top << 2;
    stack[offset] = 0L;
    stack[offset + 1] = 0L;
    stack[offset + 2] = 0L;
    stack[offset + 3] = value;
  }

  /**
   * Writes a 160-bit {@link Address} as a right-aligned 256-bit word at the given stack slot. The
   * upper 96 bits are zero-padded so the address occupies the low 160-bit of the word, matching the
   * layout expected by {@link #readAddressAt(long[], int, int)}.
   *
   * @param address the address to write
   * @param stack the flat limb array
   * @param top the slot index to write to
   */
  static void pushAddress(final Address address, final long[] stack, final int top) {
    final int offset = top << 2;
    final byte[] b = address.getBytes().toArrayUnsafe();
    stack[offset] = 0L;
    stack[offset + 1] = ((int) INT_BE.get(b, 0)) & 0xFFFFFFFFL;
    stack[offset + 2] = (long) LONG_BE.get(b, 4);
    stack[offset + 3] = (long) LONG_BE.get(b, 12);
  }

  /**
   * Writes a {@link Bytes32} value as four big-endian limbs at the given stack slot. A {@code null}
   * value is written as a zero word.
   *
   * @param value the 32-byte value to write, or {@code null} for zero
   * @param stack the flat limb array
   * @param top the slot index to write to
   */
  static void pushBytes32(final @Nullable Bytes32 value, final long[] stack, final int top) {
    final int offset = top << 2;
    if (value == null) {
      stack[offset] = 0L;
      stack[offset + 1] = 0L;
      stack[offset + 2] = 0L;
      stack[offset + 3] = 0L;
      return;
    }
    final byte[] b = value.toArrayUnsafe();
    stack[offset] = (long) LONG_BE.get(b, 0);
    stack[offset + 1] = (long) LONG_BE.get(b, 8);
    stack[offset + 2] = (long) LONG_BE.get(b, 16);
    stack[offset + 3] = (long) LONG_BE.get(b, 24);
  }

  /**
   * Writes a 32-byte hash value as four big-endian limbs at the given stack slot.
   *
   * @param hash the hash value to write
   * @param stack the flat limb array
   * @param top the slot index to write to
   */
  static void pushHash(final BytesHolder hash, final long[] stack, final int top) {
    final int offset = top << 2;
    final byte[] b = hash.getBytes().toArrayUnsafe();
    stack[offset] = (long) LONG_BE.get(b, 0);
    stack[offset + 1] = (long) LONG_BE.get(b, 8);
    stack[offset + 2] = (long) LONG_BE.get(b, 16);
    stack[offset + 3] = (long) LONG_BE.get(b, 24);
  }

  /**
   * Extracts a 160-bit {@link Address} from a 256-bit stack word at the given depth below the top
   * of stack.
   *
   * @param stack the flat limb array
   * @param top current stack-top (item count)
   * @param depth 0 for the topmost item, 1 for the item below, etc.
   * @return the address formed from the lower 160 bits of the stack word
   */
  static Address readAddressAt(final long[] stack, final int top, final int depth) {
    final int off = (top - 1 - depth) << 2;
    byte[] bytes = new byte[20];
    INT_BE.set(bytes, 0, (int) stack[off + 1]);
    LONG_BE.set(bytes, 4, stack[off + 2]);
    LONG_BE.set(bytes, 12, stack[off + 3]);
    return Address.wrap(Bytes.wrap(bytes));
  }

  /**
   * Writes a value of up to 32 bytes as a right-aligned 256-bit word at the given stack slot. A
   * {@code null} value is written as a zero word.
   *
   * @param value the value to write, at most 32 bytes, or {@code null} for zero
   * @param stack the flat limb array
   * @param top the slot index to write to
   */
  static void pushBytes(final @Nullable Bytes value, final long[] stack, final int top) {
    if (value == null) {
      pushZero(stack, top);
    } else if (value.size() == Bytes32.SIZE) {
      pushBytes32(Bytes32.wrap(value), stack, top);
    } else {
      pushBytes32(Bytes32.leftPad(value), stack, top);
    }
  }

  /**
   * Reads the 256-bit word at the given depth below the top of the stack as 32 big-endian bytes.
   *
   * @param stack the flat limb array
   * @param top current stack-top (item count)
   * @param depth 0 for the topmost item, 1 for the item below, etc.
   * @return the word as 32 bytes
   */
  static Bytes32 readBytes32At(final long[] stack, final int top, final int depth) {
    final int off = (top - 1 - depth) << 2;
    final byte[] bytes = new byte[32];
    LONG_BE.set(bytes, 0, stack[off]);
    LONG_BE.set(bytes, 8, stack[off + 1]);
    LONG_BE.set(bytes, 16, stack[off + 2]);
    LONG_BE.set(bytes, 24, stack[off + 3]);
    return Bytes32.wrap(bytes);
  }

  /**
   * Reads the 256-bit word at the given depth below the top of the stack as a {@link Wei} value.
   *
   * @param stack the flat limb array
   * @param top current stack-top (item count)
   * @param depth 0 for the topmost item, 1 for the item below, etc.
   * @return the word as a Wei value
   */
  static Wei readWeiAt(final long[] stack, final int top, final int depth) {
    return Wei.wrap(readBytes32At(stack, top, depth));
  }

  /**
   * Returns whether the 256-bit word at the given depth below the top of the stack is zero.
   *
   * @param stack the flat limb array
   * @param top current stack-top (item count)
   * @param depth 0 for the topmost item, 1 for the item below, etc.
   * @return true if all four limbs are zero
   */
  static boolean isZeroAt(final long[] stack, final int top, final int depth) {
    final int off = (top - 1 - depth) << 2;
    return (stack[off] | stack[off + 1] | stack[off + 2] | stack[off + 3]) == 0;
  }

  /**
   * Reads the 256-bit word at the given depth below the top of the stack as a non-negative {@code
   * long}, clamping every value that does not fit to {@link Long#MAX_VALUE}, as {@code
   * Words.clampedToLong} does for the v1 stack.
   *
   * @param stack the flat limb array
   * @param top current stack-top (item count)
   * @param depth 0 for the topmost item, 1 for the item below, etc.
   * @return the word as a long, or {@link Long#MAX_VALUE} if it does not fit
   */
  static long clampedToLong(final long[] stack, final int top, final int depth) {
    final int off = (top - 1 - depth) << 2;
    final long u0 = stack[off + 3];
    if ((stack[off] | stack[off + 1] | stack[off + 2]) != 0 || u0 < 0) {
      return Long.MAX_VALUE;
    }
    return u0;
  }

  /**
   * Reads the 256-bit word at the given depth below the top of the stack as a non-negative {@code
   * int}, clamping every value that does not fit to {@link Integer#MAX_VALUE}, as {@code
   * Words.clampedToInt} does for the v1 stack.
   *
   * @param stack the flat limb array
   * @param top current stack-top (item count)
   * @param depth 0 for the topmost item, 1 for the item below, etc.
   * @return the word as an int, or {@link Integer#MAX_VALUE} if it does not fit
   */
  static int clampedToInt(final long[] stack, final int top, final int depth) {
    final int off = (top - 1 - depth) << 2;
    final long u0 = stack[off + 3];
    if ((stack[off] | stack[off + 1] | stack[off + 2]) != 0 || (u0 >>> 31) != 0) {
      return Integer.MAX_VALUE;
    }
    return (int) u0;
  }

  /**
   * Compares the words at two limb offsets as unsigned 256-bit integers.
   *
   * @param stack the flat limb array
   * @param aOffset the limb offset of the first word
   * @param bOffset the limb offset of the second word
   * @return negative, zero or positive as the first word is less than, equal to or greater than the
   *     second
   */
  static int compareUnsigned(final long[] stack, final int aOffset, final int bOffset) {
    if (stack[aOffset] != stack[bOffset]) {
      return Long.compareUnsigned(stack[aOffset], stack[bOffset]);
    }
    if (stack[aOffset + 1] != stack[bOffset + 1]) {
      return Long.compareUnsigned(stack[aOffset + 1], stack[bOffset + 1]);
    }
    if (stack[aOffset + 2] != stack[bOffset + 2]) {
      return Long.compareUnsigned(stack[aOffset + 2], stack[bOffset + 2]);
    }
    return Long.compareUnsigned(stack[aOffset + 3], stack[bOffset + 3]);
  }

  /**
   * Compares the words at two limb offsets as two's complement signed 256-bit integers.
   *
   * @param stack the flat limb array
   * @param aOffset the limb offset of the first word
   * @param bOffset the limb offset of the second word
   * @return negative, zero or positive as the first word is less than, equal to or greater than the
   *     second
   */
  static int compareSigned(final long[] stack, final int aOffset, final int bOffset) {
    if (stack[aOffset] != stack[bOffset]) {
      return Long.compare(stack[aOffset], stack[bOffset]);
    }
    return compareUnsigned(stack, aOffset, bOffset);
  }

  /**
   * Copies the 256-bit word at one stack slot to another.
   *
   * @param stack the flat limb array
   * @param from the slot index to read
   * @param to the slot index to write
   */
  static void copyWord(final long[] stack, final int from, final int to) {
    final int src = from << 2;
    final int dst = to << 2;
    stack[dst] = stack[src];
    stack[dst + 1] = stack[src + 1];
    stack[dst + 2] = stack[src + 2];
    stack[dst + 3] = stack[src + 3];
  }

  /**
   * Exchanges the 256-bit words at two stack slots.
   *
   * @param stack the flat limb array
   * @param a the first slot index
   * @param b the second slot index
   */
  static void swapWords(final long[] stack, final int a, final int b) {
    final int aOffset = a << 2;
    final int bOffset = b << 2;
    final long a0 = stack[aOffset];
    final long a1 = stack[aOffset + 1];
    final long a2 = stack[aOffset + 2];
    final long a3 = stack[aOffset + 3];
    stack[aOffset] = stack[bOffset];
    stack[aOffset + 1] = stack[bOffset + 1];
    stack[aOffset + 2] = stack[bOffset + 2];
    stack[aOffset + 3] = stack[bOffset + 3];
    stack[bOffset] = a0;
    stack[bOffset + 1] = a1;
    stack[bOffset + 2] = a2;
    stack[bOffset + 3] = a3;
  }

  /**
   * Convert {@code length} bytes starting at {@code offset} (inclusive) to a UInt256 value. If
   * {@code length} is less than 32, the most significant bytes of the UInt256 are set to 0. If
   * {@code length} is greater than 32, the most significant bytes are truncated (the low-order 32
   * bytes are kept).
   *
   * @param bytes raw bytes in BigEndian order.
   * @param offset start index of the bytes array to convert, inclusive
   * @param length amount of bytes to take for conversion, caller must ensure >= 0 - unguarded
   * @return Big-endian UInt256 represented by the bytes.
   */
  static UInt256 fromBytesBE(final byte[] bytes, final int offset, final int length) {
    long u0 = 0, u1 = 0, u2 = 0, u3 = 0;
    int end = offset + length;
    if (bytes.length >= 8) {
      int i0 = Math.max(offset, end - 8);
      int i1 = Math.max(offset, end - 16);
      int i2 = Math.max(offset, end - 24);
      int i3 = Math.max(offset, end - 32);
      u0 = getLongUnsafe(bytes, i0, end);
      u1 = getLongUnsafe(bytes, i1, i0);
      u2 = getLongUnsafe(bytes, i2, i1);
      u3 = getLongUnsafe(bytes, i3, i2);
      return new UInt256(u3, u2, u1, u0);
    } else if (bytes.length != 0) {
      u0 = getLongSlow(bytes, offset, end);
    }
    return new UInt256(u3, u2, u1, u0);
  }

  /**
   * Convert a sequence of bytes starting at {@code from} (inclusive) to {@code to} (exclusive) to a
   * long value. If {@code to - from < 8}, the most significant bytes are set to 0. This method
   * cannot be used when {@code to - from > 8} or {@code to > bytes.length} as it will give
   * incorrect results, or it may crash.
   *
   * @param bytes raw bytes in BigEndian order
   * @param from start index of the bytes array to convert, inclusive
   * @param to end index of the bytes array, exclusive
   * @return Big-endian long value
   */
  static long getLongBE(final byte[] bytes, final int from, final int to) {
    long value = 0;
    if (bytes.length >= 8) {
      value = getLongUnsafe(bytes, from, to);
    } else if (bytes.length != 0) {
      value = getLongSlow(bytes, from, to);
    }
    return value;
  }

  private static long getLongUnsafe(final byte[] bytes, final int from, final int to) {
    assert to <= bytes.length && to - from <= 8
        : "to=" + to + " from=" + from + " indices out of bounds";

    if (from >= to) return 0L;
    int start = Math.max(0, to - 8);
    long value = (long) LONG_BE.get(bytes, start);
    // shift value to trim off any suffix of bits
    int shift = (Long.BYTES - (to - start)) * 8;
    value >>>= shift;
    // mask out value to trim off any prefix of bits
    shift = (Long.BYTES - (to - from)) * 8;
    return value & (-1L >>> shift);
  }

  private static long getLongSlow(final byte[] bytes, final int from, final int to) {
    assert to <= bytes.length && to - from <= 8
        : "to=" + to + " from=" + from + " indices out of bounds";

    long value = 0;
    for (int i = to - 1, shift = 0; i >= from; i--, shift += 8) {
      value |= ((bytes[i] & 0xFFL) << shift);
    }
    return value;
  }
}
