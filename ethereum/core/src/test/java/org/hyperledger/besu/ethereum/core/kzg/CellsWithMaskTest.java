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

import java.util.BitSet;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class CellsWithMaskTest {

  private static CellMask maskOf(final int... indexes) {
    final BitSet bits = new BitSet(CKZG4844Helper.CELLS_PER_EXT_BLOB);
    for (final int index : indexes) {
      bits.set(index);
    }
    final byte[] bytes = new byte[CellMask.BYTE_LENGTH];
    final byte[] set = bits.toByteArray();
    System.arraycopy(set, 0, bytes, 0, set.length);
    return CellMask.fromBytes(Bytes.wrap(bytes));
  }

  private static CellMask rangeMask(final int fromInclusive, final int toExclusive) {
    final int[] indexes = new int[toExclusive - fromInclusive];
    for (int i = 0; i < indexes.length; i++) {
      indexes[i] = fromInclusive + i;
    }
    return maskOf(indexes);
  }

  /** A cell whose every byte is its own index, so a cell identifies the index it belongs to. */
  private static Cell cellFor(final int index) {
    return new Cell(Bytes.repeat((byte) index, Cell.SIZE));
  }

  private static CellsWithMask cellsFor(final CellMask mask) {
    return new CellsWithMask(
        mask.streamIndexes().mapToObj(CellsWithMaskTest::cellFor).toList(), mask);
  }

  /** Every cell the mask claims is readable, and is the cell belonging to that index. */
  private static void assertHoldsExactly(final CellsWithMask cells, final CellMask expectedMask) {
    assertThat(cells.getCellMask()).isEqualTo(expectedMask);
    assertThat(cells.getCells()).hasSize(expectedMask.cardinality());
    expectedMask
        .streamIndexes()
        .forEach(index -> assertThat(cells.getCell(index)).isEqualTo(cellFor(index)));
  }

  @Test
  void mergesDisjointCellSets() {
    CellsWithMask merged = cellsFor(rangeMask(0, 64));
    merged = merged.merge(cellsFor(rangeMask(64, 128)));

    assertHoldsExactly(merged, CellMask.FULL);
  }

  @Test
  void mergesACellSetThatOverlapsWhatIsAlreadyHeld() {
    // Two peers serving overlapping ranges is the normal case: availability is sampled
    // independently per peer, so nothing makes their masks disjoint.
    CellsWithMask merged = cellsFor(rangeMask(0, 64));
    merged = merged.merge(cellsFor(rangeMask(32, 128)));

    assertHoldsExactly(merged, CellMask.FULL);
  }

  @Test
  void mergingTheSameCellsTwiceIsIdempotent() {
    CellsWithMask merged = cellsFor(rangeMask(0, 64));
    merged = merged.merge(cellsFor(rangeMask(0, 64)));

    assertHoldsExactly(merged, rangeMask(0, 64));
  }

  @Test
  void mergesIntoAnEmptyCellSet() {
    CellsWithMask merged = CellsWithMask.EMPTY;
    merged = merged.merge(cellsFor(CellMask.FULL));

    assertHoldsExactly(merged, CellMask.FULL);
  }

  @Test
  void mergesACellSetWhoseIndexesStartAboveZero() {
    // The merged-in cells are addressed by cell index, not by their position in the cell list,
    // which only coincide when the mask starts at zero and has no gaps.
    CellsWithMask merged = cellsFor(maskOf(1));
    merged = merged.merge(cellsFor(maskOf(70, 100)));

    assertHoldsExactly(merged, maskOf(1, 70, 100));
  }

  @Test
  void mergingAnEmptyCellSetChangesNothing() {
    CellsWithMask merged = cellsFor(maskOf(3, 9));
    merged = merged.merge(CellsWithMask.EMPTY);

    assertHoldsExactly(merged, maskOf(3, 9));
  }

  @Test
  void mergeLeavesBothSetsAlone() {
    final CellsWithMask held = cellsFor(maskOf(5));
    final CellsWithMask arriving = cellsFor(maskOf(6));

    assertHoldsExactly(held.merge(arriving), maskOf(5, 6));

    assertHoldsExactly(held, maskOf(5));
    assertHoldsExactly(arriving, maskOf(6));
  }

  @Test
  void theSharedEmptyInstanceCannotBeChanged() {
    CellsWithMask.EMPTY.merge(cellsFor(CellMask.FULL));

    assertThat(CellsWithMask.EMPTY.getCells()).isEmpty();
    assertThat(CellsWithMask.EMPTY.getCellMask()).isEqualTo(CellMask.EMPTY);
  }

  @Test
  void getBlobCellsBytesConcatenatesHeldCellsInIndexOrder() {
    CellsWithMask merged = cellsFor(maskOf(70));
    merged = merged.merge(cellsFor(maskOf(3)));

    assertThat(
            Bytes.wrap(
                merged
                    .getCellMask()
                    .streamIndexes()
                    .mapToObj(merged::getCell)
                    .map(Cell::getData)
                    .toList()))
        .isEqualTo(Bytes.concatenate(cellFor(3).getData(), cellFor(70).getData()));
  }

  @Test
  void mergeIsEquivalentToBuildingFromTheUnion() {
    CellsWithMask merged = cellsFor(maskOf(0, 5, 6, 127));
    merged = merged.merge(cellsFor(maskOf(5, 6, 7, 64)));

    final CellMask union = maskOf(0, 5, 6, 7, 64, 127);
    assertHoldsExactly(merged, union);
    assertThat(merged.getCells()).isEqualTo(cellsFor(union).getCells());
  }

  @Test
  void listedCellsFollowIndexOrderAfterAnOutOfOrderMerge() {
    CellsWithMask merged = cellsFor(maskOf(100));
    merged = merged.merge(cellsFor(maskOf(2, 50)));

    assertThat(merged.getCells()).isEqualTo(List.of(cellFor(2), cellFor(50), cellFor(100)));
  }
}
