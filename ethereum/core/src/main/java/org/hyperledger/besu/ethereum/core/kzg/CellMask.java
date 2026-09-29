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

/** Fixed-width eth/72 cell availability mask. */
public final class CellMask {
  private final BitSet mask;

  /** Width of the wire representation, which is fixed however few indexes are set. */
  public static final int BYTE_LENGTH = 16;

  /**
   * A mask with no index set.
   *
   * <p>A method rather than a constant: this type is mutated in place by {@link #merge}, {@link
   * #intersect} and {@link #subtract}, so a shared instance would be corrupted process wide by the
   * first caller that forgot to copy it, and every later holder would silently see someone else's
   * cells.
   *
   * @return a new empty mask, owned by the caller
   */
  public static CellMask empty() {
    return new CellMask(new BitSet(CELLS_PER_EXT_BLOB));
  }

  /**
   * A mask with every index set, as a node holding all of a blob's cells announces.
   *
   * @return a new full mask, owned by the caller
   */
  public static CellMask full() {
    return new CellMask(BitSet.valueOf(fullMaskBytes()));
  }

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
   * An independent copy, which callers need because this type is mutated in place.
   *
   * @return the copy
   */
  public CellMask copy() {
    return new CellMask((BitSet) mask.clone());
  }

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
      return copy();
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
   * Changes this CellMask, merging the other CellMask into it by performing a logical OR operation
   * on their respective BitSet representations.
   *
   * @param other the CellMask to merge into this CellMask.
   */
  public void merge(final CellMask other) {
    mask.or(other.mask);
  }

  /**
   * Changes this CellMask, intersecting the other CellMask into it by performing a logical AND
   * operation on their respective BitSet representations.
   *
   * @param other the CellMask to intersect into this CellMask.
   */
  public void intersect(final CellMask other) {
    mask.and(other.mask);
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

  /**
   * Changes this CellMask, removing from it every index set in the other CellMask.
   *
   * @param other the CellMask whose indexes are removed from this one
   */
  public void subtract(final CellMask other) {
    mask.andNot(other.mask);
  }
}
