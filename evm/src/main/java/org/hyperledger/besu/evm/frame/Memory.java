/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.evm.frame;

import org.hyperledger.besu.evm.internal.Words;

import java.util.Arrays;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes;

/**
 * An EVM memory implementation.
 *
 * <p>Note: this is meant to map to I in Section 9.1 "Basics" and Section 9.4.1 "Machine State" in
 * the Yellow Paper Revision 59dccd.
 */
public class Memory {

  // See below.
  private static final long MAX_BYTES = Integer.MAX_VALUE;

  /**
   * The data stored within the memory.
   *
   * <p>Note that the current Ethereum spec don't put a limit on memory, so we could theoretically
   * overflow this. A byte array implementation limits us to 2 GiB. But that would cost over 51
   * trillion gas. So this is likely a reasonable limitation, at least at first.
   */
  private byte[] memBytes;

  private int activeWords;

  /** Instantiates a new Memory. */
  public Memory() {
    memBytes = new byte[0];
  }

  private static RuntimeException overflow(final long v) {
    return overflow(String.valueOf(v));
  }

  private static RuntimeException overflow(final String v) {
    final String msg = "Memory index or length %s too large, cannot be larger than %d";
    throw new IllegalStateException(String.format(msg, v, MAX_BYTES));
  }

  private void checkByteIndex(final long v) {
    // We can have at most MAX_BYTES, so an index can only at most MAX_BYTES - 1.
    if (v < 0 || v >= MAX_BYTES) throw overflow(v);
  }

  private int asByteIndex(final long w) {
    try {
      final long v = Math.toIntExact(w);
      checkByteIndex(v);
      return (int) v;
    } catch (final ArithmeticException | IllegalStateException e) {
      throw overflow(w);
    }
  }

  private static int asByteLength(final long l) {
    try {
      return Math.toIntExact(l);
    } catch (final ArithmeticException | IllegalStateException e) {
      throw overflow(l);
    }
  }

  /**
   * For use in memoryExpansionGasCost() of GasCost. Returns the number of new active words that
   * accommodate at least the number of specified bytes from the provided memory offset.
   *
   * <p>Not that this has to return a UInt256 for Gas calculation, in case someone writes code that
   * require a crazy amount of data. Such allocation should get prohibitive however, and we will end
   * up with an Out-of-Gas error.
   *
   * @param location The offset in memory from which we want to accommodate {@code numBytes}.
   * @param numBytes The minimum number of bytes in memory.
   * @return The number of active words that accommodate at least the number of specified bytes.
   */
  long calculateNewActiveWords(final long location, final long numBytes) {
    if (numBytes == 0) {
      return activeWords;
    }

    // Words.clampedAdd is total, so the prior try/catch was dead code. Compute the
    // saturated byte size directly and short-circuit when it exceeds the backing
    // store's addressable range, so the caller's gas accounting clamps to
    // Long.MAX_VALUE via memoryCost() instead of producing an intermediate word
    // count that downstream code would have to truncate to an int.
    final long byteSize = Words.clampedAdd(Words.clampedAdd(location, numBytes), 31);
    if (byteSize > MAX_BYTES) {
      return Long.MAX_VALUE >> 5;
    }
    return Math.max(byteSize / 32, activeWords);
  }

  /**
   * Expands the active words to accommodate the specified byte position.
   *
   * @param offset The location in memory to start with.
   * @param numBytes The number of bytes to get.
   */
  void ensureCapacityForBytes(final long offset, final long numBytes) {
    // Do not increase the memory capacity if no bytes are being written
    // regardless of what the address may be.
    if (numBytes == 0) {
      return;
    }
    final long lastByteIndex = Math.addExact(offset, numBytes);
    final long lastWordRequired = ((lastByteIndex - 1) / Bytes32.SIZE);
    maybeExpandCapacity((int) lastWordRequired + 1);
  }

  /**
   * Expands the memory to the specified number of active words.
   *
   * @param newActiveWords The new number of active words to expand to.
   */
  private void maybeExpandCapacity(final int newActiveWords) {
    if (activeWords >= newActiveWords) return;

    int neededSize = newActiveWords * Bytes32.SIZE;
    if (neededSize > memBytes.length) {
      int newSize = Math.max(neededSize, memBytes.length * 2);
      byte[] newMem = new byte[newSize];
      System.arraycopy(memBytes, 0, newMem, 0, memBytes.length);
      memBytes = newMem;
    }
    activeWords = newActiveWords;
  }

  /**
   * Returns true if the object is equal to this memory instance; otherwise false.
   *
   * @param other The object to compare this memory instance with.
   * @return True if the object is equal to this memory instance.
   */
  @Override
  public boolean equals(final Object other) {
    if (other == null) return false;
    if (other == this) return true;
    if (!(other instanceof Memory that)) return false;

    return Arrays.equals(memBytes, that.memBytes);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(memBytes);
  }

  /**
   * Returns the current number of active bytes stored in memory.
   *
   * @return The current number of active bytes stored in memory.
   */
  int getActiveBytes() {
    return activeWords * Bytes32.SIZE;
  }

  /**
   * Returns the current number of active words stored in memory.
   *
   * @return The current number of active words stored in memory.
   */
  public int getActiveWords() {
    return activeWords;
  }

  /**
   * Returns a copy of bytes from memory.
   *
   * @param location The location in memory to start with.
   * @param numBytes The number of bytes to get.
   * @return A fresh copy of the bytes from memory starting at {@code location} and extending {@code
   *     numBytes}.
   */
  public Bytes getBytes(final long location, final long numBytes) {
    // Note: if length == 0, we don't require any memory expansion, whatever location is. So
    // we must call asByteIndex(location) after this check so as it doesn't throw if the location
    // is too big but the length is 0 (which is somewhat nonsensical, but is exercise by some
    // tests).
    final int length = asByteLength(numBytes);
    if (length == 0) {
      return Bytes.EMPTY;
    }

    final int start = asByteIndex(location);
    ensureCapacityForBytes(start, length);
    return Bytes.wrap(Arrays.copyOfRange(memBytes, start, start + length));
  }

  /**
   * Returns a copy of bytes by peeking into memory without expanding the active words.
   *
   * @param location The location in memory to start with.
   * @param numBytes The number of bytes to get.
   * @return A fresh copy of the bytes from memory starting at {@code location} and extending {@code
   *     numBytes}.
   */
  public Bytes getBytesWithoutGrowth(final long location, final long numBytes) {
    // Note: if length == 0, we don't require any memory expansion, whatever location is. So
    // we must call asByteIndex(location) after this check so as it doesn't throw if the location
    // is too big but the length is 0 (which is somewhat nonsensical, but is exercise by some
    // tests).
    final int length = asByteLength(numBytes);
    if (length == 0) {
      return Bytes.EMPTY;
    }

    final int start = asByteIndex(location);

    // Arrays.copyOfRange would throw if start > memBytes.length, so just return the expected
    // number of zeros without expanding the memory.
    // Otherwise, just follow the happy path.
    if (start > memBytes.length) {
      return Bytes.wrap(new byte[(int) numBytes]);
    } else {
      return Bytes.wrap(Arrays.copyOfRange(memBytes, start, start + length));
    }
  }

  /**
   * Returns a copy of bytes from memory.
   *
   * @param location The location in memory to start with.
   * @param numBytes The number of bytes to get.
   * @return A fresh copy of the bytes from memory starting at {@code location} and extending {@code
   *     numBytes}.
   */
  public MutableBytes getMutableBytes(final long location, final long numBytes) {
    // Note: if length == 0, we don't require any memory expansion, whatever location is. So
    // we must call asByteIndex(location) after this check so as it doesn't throw if the location
    // is too big but the length is 0 (which is somewhat nonsensical, but is exercise by some
    // tests).
    final int length = asByteLength(numBytes);
    if (length == 0) {
      return MutableBytes.EMPTY;
    }

    final int start = asByteIndex(location);

    ensureCapacityForBytes(start, length);
    return MutableBytes.wrap(memBytes, start, length);
  }

  /**
   * Copy the bytes from the provided number of bytes from the provided value to memory from the
   * provided offset.
   *
   * <p>Note that this method will extend memory to accommodate the location assigned and bytes
   * copied and so never fails.
   *
   * @param memOffset the location in memory at which to start copying the bytes of {@code value}.
   * @param offset the location in the source to start copying.
   * @param length the number of bytes to set in memory. Note that this value may differ from {@code
   *     value.size()}: if {@code numBytes < value.size()} bytes, only {@code numBytes} will be
   *     copied from {@code value}; if {@code numBytes < value.size()}, then only the bytes in
   *     {@code value} will be copied, but the memory will be expanded if necessary to cover {@code
   *     numBytes} (in other words, {@link #getActiveWords()} will return a value consistent with
   *     having set {@code numBytes} bytes, even if less than that have been concretely set due to
   *     {@code value} being smaller).
   * @param bytes the bytes to copy to memory from {@code location}.
   */
  public void setBytes(
      final long memOffset, final long offset, final long length, final Bytes bytes) {

    if (offset >= bytes.size()) {
      clearBytes(memOffset, length);
      return;
    }

    final Bytes toCopy =
        bytes.slice((int) offset, Math.min((int) length, bytes.size() - (int) offset));
    setBytes(memOffset, length, toCopy);
  }

  /**
   * Copy the bytes from the provided number of bytes from the provided value to memory from the
   * provided offset.
   *
   * <p>Note that this method will extend memory to accommodate the location assigned and bytes
   * copied and so never fails.
   *
   * @param location the location in memory at which to start copying the bytes of {@code value}.
   * @param numBytes the number of bytes to set in memory. Note that this value may differ from
   *     {@code value.size()}: if {@code numBytes < value.size()} bytes, only {@code numBytes} will
   *     be copied from {@code value}; if {@code numBytes > value.size()}, then only the bytes in
   *     {@code value} will be copied, but the memory will be expanded if necessary to cover {@code
   *     numBytes} (in other words, {@link #getActiveWords()} will return a value consistent with
   *     having set {@code numBytes} bytes, even if less than that have been concretely set due to
   *     {@code value} being smaller).
   * @param taintedValue the bytes to copy to memory from {@code location}.
   */
  public void setBytes(final long location, final long numBytes, final Bytes taintedValue) {
    if (numBytes == 0) {
      return;
    }

    final int start = asByteIndex(location);
    final int length = asByteLength(numBytes);
    final int srcLength = taintedValue.size();
    final int end = Math.addExact(start, length);

    ensureCapacityForBytes(start, length);
    if (srcLength >= length) {
      System.arraycopy(taintedValue.toArrayUnsafe(), 0, memBytes, start, length);
    } else {
      Arrays.fill(memBytes, start + srcLength, end, (byte) 0);
      if (srcLength > 0) {
        System.arraycopy(taintedValue.toArrayUnsafe(), 0, memBytes, start, srcLength);
      }
    }
  }

  /**
   * Copy the bytes from the value param into memory at the specified offset. In cases where the
   * value does not have numBytes bytes the appropriate amount of zero bytes will be added before
   * writing the value bytes.
   *
   * <p>Note that this method will extend memory to accommodate the location assigned and bytes
   * copied and so never fails.
   *
   * @param location the location in memory at which to start writing the padding and {@code value}
   *     bytes.
   * @param numBytes the number of bytes to set in memory. Note that this value may differ from
   *     {@code value.size()}: if {@code numBytes < value.size()} bytes, only {@code numBytes} will
   *     be copied from {@code value}; if {@code numBytes > value.size()}, then only the bytes in
   *     {@code value} will be copied, but the memory will be expanded if necessary to cover {@code
   *     numBytes} (in other words, {@link #getActiveWords()} will return a value consistent with
   *     having set {@code numBytes} bytes, even if less than that have been concretely set due to
   *     {@code value} being smaller). These create bytes will be added to the left as needed.
   * @param value the bytes to copy into memory starting at {@code location}.
   */
  public void setBytesRightAligned(final long location, final long numBytes, final Bytes value) {
    if (numBytes == 0) {
      return;
    }

    final int start = asByteIndex(location);
    final int length = asByteLength(numBytes);
    final int srcLength = value.size();
    final int end = Math.addExact(start, length);

    ensureCapacityForBytes(start, length);
    if (srcLength >= length) {
      System.arraycopy(value.toArrayUnsafe(), 0, memBytes, start, length);
    } else {
      int divider = end - srcLength;
      Arrays.fill(memBytes, start, divider, (byte) 0);
      if (srcLength > 0) {
        System.arraycopy(value.toArrayUnsafe(), 0, memBytes, divider, srcLength);
      }
    }
  }

  /**
   * Clears (set to 0) some contiguous number of bytes in memory.
   *
   * @param location The location in memory from which to start clearing the bytes.
   * @param numBytes The number of bytes to clear.
   */
  private void clearBytes(final long location, final long numBytes) {
    // See getBytes for why we check length == 0 first, before calling asByteIndex(location).
    final int length = asByteLength(numBytes);
    if (length == 0) {
      return;
    }
    clearBytes(asByteIndex(location), length);
  }

  /**
   * Clears (set to 0) some contiguous number of bytes in memory.
   *
   * @param location The location in memory from which to start clearing the bytes.
   * @param numBytes The number of bytes to clear.
   */
  private void clearBytes(final int location, final int numBytes) {
    if (numBytes == 0) {
      return;
    }

    ensureCapacityForBytes(location, numBytes);
    Arrays.fill(memBytes, location, location + numBytes, (byte) 0);
  }

  /**
   * Sets a single byte in memory at the provided location.
   *
   * @param location the location of the byte to set.
   * @param value the value to set for the byte at {@code location}.
   */
  void setByte(final long location, final byte value) {
    final int start = asByteIndex(location);
    ensureCapacityForBytes(start, 1);
    memBytes[start] = value;
  }

  /**
   * Returns a copy of the 32-bytes word that begins at the specified memory location.
   *
   * @param location The memory location the 256-bit word begins at.
   * @return a copy of the 32-bytes word that begins at the specified memory location.
   */
  public Bytes32 getWord(final long location) {
    final int start = asByteIndex(location);
    ensureCapacityForBytes(start, Bytes32.SIZE);
    return Bytes32.wrap(Arrays.copyOfRange(memBytes, start, start + Bytes32.SIZE));
  }

  /**
   * Sets a 32-bytes word in memory at the provided location.
   *
   * <p>Note that this method will extend memory to accommodate the location assigned and bytes
   * copied and so never fails.
   *
   * @param location the location at which to start setting the bytes.
   * @param bytes the 32 bytes to copy at {@code location}.
   */
  public void setWord(final long location, final Bytes32 bytes) {
    final int start = asByteIndex(location);
    ensureCapacityForBytes(start, Bytes32.SIZE);
    System.arraycopy(bytes.toArrayUnsafe(), 0, memBytes, start, Bytes32.SIZE);
  }

  /**
   * Copies one length of bytes to a new memory location, growing memory if needed.
   *
   * <p>Copying behaves as if the values are copied to an intermediate buffer before writing.
   *
   * @param dst where to copy the bytes _to_
   * @param src where to copy the bytes _from_
   * @param length the number of bytes to copy.
   */
  public void copy(final long dst, final long src, final long length) {
    ensureCapacityForBytes(Math.max(dst, src), length);
    System.arraycopy(memBytes, asByteIndex(src), memBytes, asByteIndex(dst), asByteLength(length));
  }

  @Override
  public String toString() {
    return Bytes.wrap(memBytes).toHexString();
  }
}
