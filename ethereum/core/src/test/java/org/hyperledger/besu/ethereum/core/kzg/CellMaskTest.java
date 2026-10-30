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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class CellMaskTest {

  /** A mask with only low indexes set, whose BitSet form trims to a single byte. */
  private static final Bytes ONLY_INDEX_0 =
      Bytes.fromHexString("0x01000000000000000000000000000000");

  private static final Bytes INDEXES_0_AND_1 =
      Bytes.fromHexString("0x03000000000000000000000000000000");

  private static final Bytes INDEXES_0_1_AND_2 =
      Bytes.fromHexString("0x07000000000000000000000000000000");

  @Test
  void toBytesIsAlwaysFixedWidth() {
    // BitSet#toByteArray trims trailing zero bytes, so a mask with only low indexes set would
    // otherwise serialize to fewer than CellMask.BYTE_LENGTH bytes and violate the B_16 wire type.
    assertThat(CellMask.fromBytes(ONLY_INDEX_0).toBytes().size()).isEqualTo(CellMask.BYTE_LENGTH);
    assertThat(CellMask.EMPTY.toBytes().size()).isEqualTo(CellMask.BYTE_LENGTH);
    assertThat(CellMask.FULL.toBytes().size()).isEqualTo(CellMask.BYTE_LENGTH);
  }

  @Test
  void roundTripsThroughBytes() {
    for (final Bytes encoded : new Bytes[] {ONLY_INDEX_0, INDEXES_0_AND_1, INDEXES_0_1_AND_2}) {
      final CellMask mask = CellMask.fromBytes(encoded);
      assertThat(mask.toBytes()).isEqualTo(encoded);
      assertThat(CellMask.fromBytes(mask.toBytes())).isEqualTo(mask);
    }
    assertThat(CellMask.fromBytes(CellMask.FULL.toBytes())).isEqualTo(CellMask.FULL);
    assertThat(CellMask.fromBytes(CellMask.EMPTY.toBytes())).isEqualTo(CellMask.EMPTY);
  }

  @Test
  void rejectsWrongWidth() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> CellMask.fromBytes(Bytes.EMPTY));
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> CellMask.fromBytes(Bytes.repeat((byte) 0xFF, CellMask.BYTE_LENGTH + 1)));
  }

  @Test
  void containsAllIsSubsetNotIntersection() {
    final CellMask smaller = CellMask.fromBytes(INDEXES_0_AND_1);
    final CellMask larger = CellMask.fromBytes(INDEXES_0_1_AND_2);

    assertThat(larger.containsAll(smaller)).isTrue();
    assertThat(smaller.containsAll(smaller)).isTrue();
    // Overlapping but not covering: this is the case an intersection test gets wrong.
    assertThat(smaller.containsAll(larger)).isFalse();

    assertThat(CellMask.FULL.containsAll(smaller)).isTrue();
    assertThat(smaller.containsAll(CellMask.EMPTY)).isTrue();
    assertThat(CellMask.EMPTY.containsAll(smaller)).isFalse();
  }

  @Test
  void containsAllIsFalseForDisjointMasks() {
    final CellMask indexZero = CellMask.fromBytes(ONLY_INDEX_0);
    final CellMask indexTwo = CellMask.fromBytes(Bytes.fromHexString("0x04" + "00".repeat(15)));
    assertThat(indexZero.containsAll(indexTwo)).isFalse();
    assertThat(indexTwo.containsAll(indexZero)).isFalse();
  }

  @Test
  void indexesReturnsOnlySetIndexes() {
    // index 0 and index 2
    final CellMask mask = CellMask.fromBytes(Bytes.fromHexString("0x05" + "00".repeat(15)));
    assertThat(mask.indexes()).containsExactly(0, 2);
    assertThat(mask.cardinality()).isEqualTo(2);

    assertThat(CellMask.EMPTY.indexes()).isEmpty();
    assertThat(CellMask.FULL.indexes()).hasSize(CellMask.FULL.cardinality());
  }

  @Test
  void fullAndEmptyAgreeWithCardinality() {
    assertThat(CellMask.FULL.isFull()).isTrue();
    assertThat(CellMask.FULL.isEmpty()).isFalse();
    assertThat(CellMask.EMPTY.isEmpty()).isTrue();
    assertThat(CellMask.EMPTY.isFull()).isFalse();
  }

  @Test
  void toStringCollapsesConsecutiveIndexesIntoRanges() {
    // A mask is mentioned in almost every line the blobpool logs, and the indexes that matter are
    // usually contiguous, so listing them one by one costs hundreds of characters a line.
    assertThat(maskOf(1, 2, 3, 5)).hasToString("{1-3,5}");
    assertThat(CellMask.FULL).hasToString("{0-127}");
    assertThat(CellMask.EMPTY).hasToString("{}");
  }

  @Test
  void toStringKeepsIsolatedIndexesApart() {
    assertThat(maskOf(0)).hasToString("{0}");
    assertThat(maskOf(0, 2, 4)).hasToString("{0,2,4}");
    // A run of two is collapsed as well, being no longer written out than listed.
    assertThat(maskOf(0, 1)).hasToString("{0-1}");
  }

  @Test
  void toStringRangesReachTheLastIndex() {
    // nextClearBit runs past the end of the mask, so a range ending at 127 must not run away
    // with it.
    assertThat(maskOf(126, 127)).hasToString("{126-127}");
    assertThat(maskOf(0, 127)).hasToString("{0,127}");
  }

  private static CellMask maskOf(final int... indexes) {
    final java.util.BitSet bits = new java.util.BitSet(CKZG4844Helper.CELLS_PER_EXT_BLOB);
    for (final int index : indexes) {
      bits.set(index);
    }
    final byte[] bytes = new byte[CellMask.BYTE_LENGTH];
    final byte[] set = bits.toByteArray();
    System.arraycopy(set, 0, bytes, 0, set.length);
    return CellMask.fromBytes(Bytes.wrap(bytes));
  }

  @Test
  void randomSubsetKeepsOnlyAsManyIndexesAsAsked() {
    final CellMask half = CellMask.FULL.randomSubset(64, new java.util.Random(1));

    assertThat(half.cardinality()).isEqualTo(64);
    assertThat(CellMask.FULL.containsAll(half)).isTrue();
  }

  @Test
  void randomSubsetLeavesASmallMaskAlone() {
    final CellMask custody = maskOf(3, 17, 40);

    assertThat(custody.randomSubset(64, new java.util.Random(1))).isEqualTo(custody);
    assertThat(CellMask.EMPTY.randomSubset(64, new java.util.Random(1))).isEqualTo(CellMask.EMPTY);
    // exactly the size asked for is not more than it
    assertThat(maskOf(1, 2).randomSubset(2, new java.util.Random(1))).isEqualTo(maskOf(1, 2));
  }

  @Test
  void randomSubsetDoesNotFavourTheLowIndexes() {
    // A blob recovers from any half of its cells, so a node needs no particular half; if every
    // node took the lowest one the upper half would go unrequested across the network.
    final java.util.Random random = new java.util.Random(1);
    final java.util.BitSet everChosen = new java.util.BitSet(CKZG4844Helper.CELLS_PER_EXT_BLOB);
    for (int attempt = 0; attempt < 20; attempt++) {
      CellMask.FULL.randomSubset(64, random).streamIndexes().forEach(everChosen::set);
    }

    assertThat(everChosen.cardinality()).isEqualTo(CKZG4844Helper.CELLS_PER_EXT_BLOB);
  }

  @Test
  void randomSubsetOfASmallMaskIsTheMaskItself() {
    final CellMask custody = maskOf(3, 17, 40);
    assertThat(custody.randomSubset(64, new java.util.Random(1))).isEqualTo(custody);
    assertThat(custody.randomSubset(64, new java.util.Random(1)).union(maskOf(99)))
        .isEqualTo(maskOf(3, 17, 40, 99));
    assertThat(custody).isEqualTo(maskOf(3, 17, 40));
  }

  @Test
  void theOperationsLeaveBothMasksAlone() {
    // The reason this type is a value: masks are stored in maps, shared between announcements of
    // one
    // message, and read from other threads, and an in-place operation on any of those corrupts
    // every
    // other holder.
    final CellMask lower = maskOf(0, 1);
    final CellMask upper = maskOf(2, 3);

    assertThat(lower.union(upper)).isEqualTo(maskOf(0, 1, 2, 3));
    assertThat(lower.intersection(maskOf(1, 2))).isEqualTo(maskOf(1));
    assertThat(lower.without(maskOf(1))).isEqualTo(maskOf(0));

    assertThat(lower).isEqualTo(maskOf(0, 1));
    assertThat(upper).isEqualTo(maskOf(2, 3));
  }

  @Test
  void theSharedConstantsCannotBeChanged() {
    CellMask.FULL.without(maskOf(0));
    CellMask.EMPTY.union(CellMask.FULL);

    assertThat(CellMask.FULL.isFull()).isTrue();
    assertThat(CellMask.EMPTY.isEmpty()).isTrue();
  }

  @Test
  void withoutRemovesTheIndexesOfTheOtherMask() {
    assertThat(maskOf(1, 2, 3, 5).without(maskOf(2, 5, 9))).isEqualTo(maskOf(1, 3));
  }

  @Test
  void containsTestsOneIndex() {
    final CellMask mask = maskOf(0, 64, 127);

    assertThat(mask.contains(0)).isTrue();
    assertThat(mask.contains(64)).isTrue();
    assertThat(mask.contains(127)).isTrue();
    assertThat(mask.contains(1)).isFalse();
    assertThat(mask.contains(63)).isFalse();
    assertThat(CellMask.EMPTY.contains(0)).isFalse();
    assertThat(CellMask.FULL.contains(127)).isTrue();
  }
}
