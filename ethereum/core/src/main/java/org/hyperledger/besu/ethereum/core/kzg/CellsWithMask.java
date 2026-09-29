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
import static org.hyperledger.besu.ethereum.core.kzg.CKZG4844Helper.CELLS_PER_EXT_BLOB;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PrimitiveIterator;

/**
 * The cells of one blob that this node holds, and the mask saying which they are.
 *
 * <p>Immutable: {@link #merge} returns a new instance. The index of where each cell index sits in
 * the list has to stay in step with the list itself, and building a fresh pair is how that
 * invariant is kept rather than asserted.
 */
public class CellsWithMask {

  /** Holding no cells at all, as a transaction just received over eth/72 does. */
  public static final CellsWithMask EMPTY = new CellsWithMask(List.of(), CellMask.EMPTY);

  private final List<Cell> cells;
  private final CellMask cellMask;
  private final int[] indexMap;

  private CellsWithMask(final List<Cell> cells, final CellMask cellMask, final int[] indexMap) {
    this.cells = cells;
    this.cellMask = cellMask;
    this.indexMap = indexMap;
  }

  public CellsWithMask(final List<Cell> cells, final CellMask cellMask) {
    checkArgument(cells.size() == cellMask.cardinality(), "Cell list does not match mask");
    // maps a cell index to its position in the cells list, or -1 when not held
    final int[] indexMap = new int[CELLS_PER_EXT_BLOB];
    Arrays.fill(indexMap, -1);

    int listIdx = 0;
    final PrimitiveIterator.OfInt itMask = cellMask.streamIndexes().iterator();
    while (itMask.hasNext()) {
      indexMap[itMask.next()] = listIdx++;
    }

    this(List.copyOf(cells), cellMask, indexMap);
  }

  public CellMask getCellMask() {
    return cellMask;
  }

  public Cell getCell(final int index) {
    return cells.get(indexMap[index]);
  }

  public List<Cell> getCells() {
    return cells;
  }

  public CellsWithMask detachedCopy() {
    final List<Cell> detachedCells =
        cells.stream().map(cell -> cell.getData().copy()).map(Cell::new).toList();
    return new CellsWithMask(detachedCells, cellMask);
  }

  /**
   * The cells of both sets.
   *
   * <p>The two may overlap, and routinely do: cell availability is sampled independently per peer,
   * so nothing makes two responses disjoint, and the same response may be merged more than once.
   * Where both sets hold an index, the cell of this set is kept — a cell index identifies the cell,
   * so the two are the same.
   *
   * @param other the cells to add
   * @return a new instance holding both sets of cells
   */
  public CellsWithMask merge(final CellsWithMask other) {
    final CellMask mergedMask = cellMask.union(other.cellMask);

    final List<Cell> mergedCells = new ArrayList<>(mergedMask.cardinality());
    final int[] mergedIndexMap = new int[CELLS_PER_EXT_BLOB];
    Arrays.fill(mergedIndexMap, -1);

    // Walking the union, rather than the two masks one after the other, is what keeps positions and
    // cell count in step: an index held by both would otherwise be counted twice, pushing the later
    // entries of indexMap past the end of the cell list. It also leaves the cells in ascending
    // index
    // order, which is the order getCell reads them back in.
    final PrimitiveIterator.OfInt itMergedMask = mergedMask.streamIndexes().iterator();
    int listIdx = 0;
    while (itMergedMask.hasNext()) {
      final int index = itMergedMask.next();
      // Cells of other are addressed by cell index, not by their position in its cell list: the two
      // only coincide when its mask starts at zero and has no gaps.
      mergedCells.add(indexMap[index] == -1 ? other.getCell(index) : cells.get(indexMap[index]));
      mergedIndexMap[index] = listIdx++;
    }

    return new CellsWithMask(mergedCells, mergedMask, mergedIndexMap);
  }

  @Override
  public String toString() {
    return "cell count=" + cells.size() + ", cellMask=" + cellMask;
  }
}
