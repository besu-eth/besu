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
package org.hyperledger.besu.ethereum.core.kzg;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.hyperledger.besu.ethereum.core.kzg.CKZG4844Helper.CELLS_PER_EXT_BLOB;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.IntStream;

import org.apache.tuweni.bytes.Bytes;

/**
 * Fixed-width eth/72 cell availability mask.
 *
 * <p>Immutable: {@link #union}, {@link #intersection} and {@link #without} return a new mask rather
 * than changing the receiver, so a mask can be shared, stored and handed to another thread without
 * a defensive copy. A mask is 16 bytes of payload, and the operations on it happen per announcement
 * and per sampling round rather than per cell, so the allocation is not worth trading for the
 * aliasing bugs an in-place version invites.
 */
public final class CellMask {
  private final BitSet mask;

  /** Width of the wire representation, which is fixed however few indexes are set. */
  public static final int BYTE_LENGTH = 16;

  /** A mask with no index set. */
  public static final CellMask EMPTY = new CellMask(new BitSet(CELLS_PER_EXT_BLOB));

  /** A mask with every index set, as a node holding all of a blob's cells announces. */
  public static final CellMask FULL = new CellMask(BitSet.valueOf(fullMaskBytes()));

  public CellMask(final Bytes bytes) {
    checkNotNull(bytes, "cell mask bytes must not be null");
    checkArgument(
        bytes.size() == BYTE_LENGTH,
        "cell mask must be %s bytes, got %s",
        BYTE_LENGTH,
        bytes.size());

    this.mask = BitSet.valueOf(bytes.toArray());
  }

  private CellMask(final BitSet mask) {
    this.mask = mask;
  }

  /**
   * Reads a mask from its fixed width wire representation.
   *
   * @param bytes exactly {@link #BYTE_LENGTH} bytes
   * @return the mask they encode
   */
  public static CellMask fromBytes(final Bytes bytes) {
    return new CellMask(bytes);
  }

  /**
   * A random subset of the held indexes, or this mask itself when it holds no more than {@code
   * size}.
   *
   * <p>Random rather than the lowest indexes: a blob is recoverable from any half of its cells, so
   * a node needs no particular half, but if every node asked for the same one the other would go
   * unrequested across the network and the cells in it would stop being replicated.
   *
   * @param size how many indexes to keep
   * @param random source of the choice
   * @return a new mask holding at most {@code size} of this mask's indexes
   */
  public CellMask randomSubset(final int size, final Random random) {
    if (cardinality() <= size) {
      return this;
    }

    final List<Integer> heldIndexes = new ArrayList<>(mask.stream().boxed().toList());
    Collections.shuffle(heldIndexes, random);

    final BitSet subset = new BitSet(CELLS_PER_EXT_BLOB);
    heldIndexes.subList(0, size).forEach(subset::set);
    return new CellMask(subset);
  }

  /**
   * Whether no index is set.
   *
   * @return true if the mask holds nothing
   */
  public boolean isEmpty() {
    return mask.isEmpty();
  }

  /**
   * Whether every index of an extended blob is set.
   *
   * @return true if the mask holds everything
   */
  public boolean isFull() {
    return mask.cardinality() == CELLS_PER_EXT_BLOB;
  }

  /**
   * How many indexes are set.
   *
   * @return the number of cells the mask holds
   */
  public int cardinality() {
    return mask.cardinality();
  }

  /**
   * The indexes set, in ascending order.
   *
   * @return a stream of the cell indexes the mask holds
   */
  public IntStream streamIndexes() {
    return mask.stream();
  }

  /**
   * The indexes set, in ascending order.
   *
   * @return the cell indexes the mask holds
   */
  public int[] indexes() {
    return mask.stream().toArray();
  }

  /**
   * Tests whether every index set in {@code other} is also set in this mask, i.e. whether {@code
   * other} is a subset of this mask.
   *
   * @param other the mask that must be covered by this one
   * @return true if this mask contains all the indexes of the other mask
   */
  public boolean containsAll(final CellMask other) {
    final BitSet notCovered = (BitSet) other.mask.clone();
    notCovered.andNot(mask);
    return notCovered.isEmpty();
  }

  /**
   * Serializes this mask to its fixed width wire representation. {@link BitSet#toByteArray()} trims
   * trailing zero bytes, so the result is right padded to {@link #BYTE_LENGTH}, otherwise a mask
   * with no high indexes set would not round trip through {@link #fromBytes(Bytes)}.
   *
   * @return exactly {@link #BYTE_LENGTH} bytes
   */
  public Bytes toBytes() {
    final byte[] bytes = new byte[BYTE_LENGTH];
    final byte[] setBytes = mask.toByteArray();
    System.arraycopy(setBytes, 0, bytes, 0, setBytes.length);
    return Bytes.wrap(bytes);
  }

  /**
   * The indexes held by either mask.
   *
   * @param other the mask to combine with this one
   * @return a new mask holding the union
   */
  public CellMask union(final CellMask other) {
    final BitSet combined = copyOfMask();
    combined.or(other.mask);
    return new CellMask(combined);
  }

  /**
   * The indexes held by both masks.
   *
   * @param other the mask to intersect with this one
   * @return a new mask holding the intersection
   */
  public CellMask intersection(final CellMask other) {
    final BitSet common = copyOfMask();
    common.and(other.mask);
    return new CellMask(common);
  }

  /**
   * The indexes held by this mask and not by the other.
   *
   * @param other the mask whose indexes are removed
   * @return a new mask holding the difference
   */
  public CellMask without(final CellMask other) {
    final BitSet remaining = copyOfMask();
    remaining.andNot(other.mask);
    return new CellMask(remaining);
  }

  private BitSet copyOfMask() {
    return (BitSet) mask.clone();
  }

  /**
   * The held indexes, consecutive ones collapsed into ranges: {@code {1-3,5}} rather than {@code
   * {1, 2, 3, 5}}.
   *
   * <p>A mask is 128 bits wide and the ones that matter are usually contiguous — a full mask, a
   * custody run, the half of a blob a peer serves — so listing them one by one turns every line
   * that mentions one into several hundred characters of log.
   *
   * @return the held indexes as ranges
   */
  @Override
  public String toString() {
    final StringBuilder indexes = new StringBuilder("{");
    for (int start = mask.nextSetBit(0); start >= 0; ) {
      // never -1: a BitSet always has a clear bit past the last set one
      final int endExclusive = mask.nextClearBit(start);
      if (indexes.length() > 1) {
        indexes.append(',');
      }
      indexes.append(start);
      if (endExclusive - start > 1) {
        indexes.append('-').append(endExclusive - 1);
      }
      start = mask.nextSetBit(endExclusive);
    }
    return indexes.append('}').toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    final CellMask cellMask = (CellMask) o;
    return Objects.equals(mask, cellMask.mask);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mask);
  }

  private static byte[] fullMaskBytes() {
    final byte[] bytes = new byte[BYTE_LENGTH];
    Arrays.fill(bytes, (byte) 0xFF);
    return bytes;
  }
}
