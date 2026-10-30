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

import org.hyperledger.besu.ethereum.core.BlobTestFixture;
import org.hyperledger.besu.ethereum.util.TrustedSetupClassLoaderExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

/**
 * Verification of a partially sampled blob transaction, the eth/72 case where a node holds only its
 * custody cells.
 */
class PartialCellVerificationTest extends TrustedSetupClassLoaderExtension {

  /** Cells 0 and 2, standing in for a custody set. */
  private static final CellMask CUSTODY =
      CellMask.fromBytes(Bytes.fromHexString("0x05" + "00".repeat(15)));

  private static final int BLOB_COUNT = 2;

  /** A complete, genuinely computed v1 bundle set. */
  private static BlobsWithCommitments fullBlobs() {
    return CKZG4844Helper.convertToVersion1(
        new BlobTestFixture().createBlobsWithCommitments(BLOB_COUNT));
  }

  /**
   * Reduces every bundle to the cells named by {@code mask}, as a sampling node would hold, and
   * optionally substitutes the first held cell of {@code tamperedBlob}.
   */
  private static BlobsWithCommitments narrowTo(
      final BlobsWithCommitments full,
      final CellMask mask,
      final int tamperedBlob,
      final Cell replacement) {
    final List<CellsWithMask> narrowed = new ArrayList<>(mask.cardinality());

    for (int blob = 0; blob < full.getBlobProofBundles().size(); blob++) {
      final BlobProofBundle bundle = full.getBlobProofBundles().get(blob);
      final CellsWithMask allCells = bundle.getCellsWithMask().orElseThrow();
      final List<Cell> held = new ArrayList<>();
      mask.streamIndexes().forEach(index -> held.add(allCells.getCell(index)));
      if (replacement != null && blob == tamperedBlob) {
        // guard against a vacuous test: the fixture's blobs are mostly zero bytes, so many cells
        // are byte identical and substituting one for another would change nothing
        assertThat(replacement).isNotEqualTo(held.getFirst());
        held.set(0, replacement);
      }
      narrowed.add(new CellsWithMask(held, mask));
    }
    return BlobsWithCommitments.createFromBlobCells(
        full.getKzgCommitments(), narrowed, full.getKzgProofs(), full.getVersionedHashes());
  }

  /** An index whose cell differs from the cell at index 0, for the blob at {@code blobIndex}. */
  private static int indexOfADifferentCell(final BlobsWithCommitments full, final int blobIndex) {
    final CellsWithMask cells =
        full.getBlobProofBundles().get(blobIndex).getCellsWithMask().orElseThrow();
    for (int index = 1; index < CKZG4844Helper.CELLS_PER_EXT_BLOB; index++) {
      if (!cells.getCell(index).equals(cells.getCell(0))) {
        return index;
      }
    }
    throw new IllegalStateException("fixture blob " + blobIndex + " has all cells identical");
  }

  @Test
  void verifiesACompleteTransaction() {
    assertThat(CKZG4844Helper.verify4844Kzg(fullBlobs())).isTrue();
  }

  @Test
  void verifiesOnlyTheCellsActuallyHeld() {
    final BlobsWithCommitments partial = narrowTo(fullBlobs(), CUSTODY, 0, null);

    assertThat(partial.allCellsPresent()).isFalse();
    assertThat(partial.getCellMask()).isEqualTo(CUSTODY);
    // Holding a subset is not an error; the subset must still open the commitments.
    assertThat(CKZG4844Helper.verify4844Kzg(partial)).isTrue();
  }

  @Test
  void rejectsATamperedCellInAPartialTransaction() {
    // This is the case that mattered: cells arrive from whichever peer answered GetCells, and
    // nothing else ties them to the commitments. Before partial verification existed, a peer could
    // return arbitrary bytes here and have them accepted, stored, re-served to other peers and
    // handed to the consensus layer.
    final BlobsWithCommitments tampered =
        narrowTo(fullBlobs(), CUSTODY, 1, new Cell(Bytes.repeat((byte) 0x11, Cell.SIZE)));

    assertThat(CKZG4844Helper.verify4844Kzg(tampered)).isFalse();
  }

  @Test
  void rejectsACellMovedToTheWrongIndex() {
    // A cell that is genuine but presented at an index it does not belong to must not verify,
    // otherwise a peer could satisfy a custody request with cells it happens to hold.
    final BlobsWithCommitments full = fullBlobs();
    // Blob 1 is the one with non-uniform content in the fixture, so it has cells that differ.
    final CellsWithMask allCells =
        full.getBlobProofBundles().get(1).getCellsWithMask().orElseThrow();
    final Cell genuineButElsewhere = allCells.getCell(indexOfADifferentCell(full, 1));

    final BlobsWithCommitments swapped = narrowTo(full, CUSTODY, 1, genuineButElsewhere);

    assertThat(CKZG4844Helper.verify4844Kzg(swapped)).isFalse();
  }

  @Test
  void acceptsATransactionThatHoldsNoCellsYet() {
    // A freshly decoded eth/72 transaction has its blobs elided and no cells at all. There is
    // nothing to verify; its commitments were already bound by the versioned hash check.
    final BlobsWithCommitments bwc = fullBlobs();
    final List<CellsWithMask> empty =
        IntStream.range(0, bwc.getBlobProofBundles().size())
            .mapToObj(_ -> CellsWithMask.EMPTY)
            .toList();

    final BlobsWithCommitments noCells =
        BlobsWithCommitments.createFromBlobCells(
            bwc.getKzgCommitments(), empty, bwc.getKzgProofs(), bwc.getVersionedHashes());

    assertThat(noCells.getCellMask().isEmpty()).isTrue();
    assertThat(CKZG4844Helper.verify4844Kzg(noCells)).isTrue();
  }
}
