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

import org.hyperledger.besu.datatypes.BlobType;
import org.hyperledger.besu.datatypes.VersionedHash;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.MutableBytes;

/** Represents a bundle of proofs for a blob, including KZG commitments and proofs. */
public final class BlobProofBundle {

  private final BlobType blobType;
  private final Optional<Blob> blob;
  private final KZGCommitment kzgCommitment;
  private final List<KZGProof> kzgProof;
  private final VersionedHash versionedHash;
  private final Optional<CellsWithMask> cellsWithMask;

  /**
   * @param blobType the type of the blob
   * @param blob the blob being proven.
   * @param kzgCommitment the KZG commitment for the blob.
   * @param kzgProof the KZG proof for the blob.
   * @param versionedHash the versioned hash of the blob.
   */
  public BlobProofBundle(
      final BlobType blobType,
      final Blob blob,
      final KZGCommitment kzgCommitment,
      final List<KZGProof> kzgProof,
      final VersionedHash versionedHash) {
    checkNotNull(kzgCommitment, "kzgCommitment must not be null");
    checkNotNull(versionedHash, "versionedHash must not be null");
    checkNotNull(blob, "blob must not be null");
    checkNotNull(kzgProof, "kzgProof must not be null");
    if (blobType == BlobType.KZG_PROOF && kzgProof.size() != 1) {
      String errorMessage =
          "Invalid kzgProof size for versionId 0, expected 1 but got " + kzgProof.size();
      throw new IllegalArgumentException(errorMessage);
    }
    if (blobType == BlobType.KZG_CELL_PROOFS
        && kzgProof.size() != CKZG4844Helper.CELL_PROOFS_PER_BLOB) {
      String errorMessage =
          "Invalid kzgProof size for versionId 1, expected "
              + CKZG4844Helper.CELL_PROOFS_PER_BLOB
              + " but got "
              + kzgProof.size();
      throw new IllegalArgumentException(errorMessage);
    }
    this.blobType = blobType;
    this.blob = Optional.of(blob);
    this.kzgCommitment = kzgCommitment;
    this.kzgProof = kzgProof;
    this.versionedHash = versionedHash;
    this.cellsWithMask = Optional.ofNullable(computeCells(blob, blobType));
  }

  public BlobProofBundle(
      final BlobType blobType,
      final CellsWithMask cellsWithMask,
      final KZGCommitment kzgCommitment,
      final List<KZGProof> kzgProof,
      final VersionedHash versionedHash) {
    checkNotNull(cellsWithMask, "cellsWithMask must not be null");
    checkNotNull(kzgCommitment, "kzgCommitment must not be null");
    checkNotNull(versionedHash, "versionedHash must not be null");
    checkNotNull(kzgProof, "kzgProof must not be null");
    if (blobType != BlobType.KZG_CELL_PROOFS) {
      throw new IllegalArgumentException(
          "Cells-only BlobProofBundle requires blob type KZG_CELL_PROOFS");
    }
    if (kzgProof.size() != CKZG4844Helper.CELL_PROOFS_PER_BLOB) {
      String errorMessage =
          "Invalid kzgProof size for versionId 1, expected "
              + CKZG4844Helper.CELL_PROOFS_PER_BLOB
              + " but got "
              + kzgProof.size();
      throw new IllegalArgumentException(errorMessage);
    }
    this.blobType = blobType;
    this.blob = Optional.empty();
    this.cellsWithMask = Optional.of(cellsWithMask);
    this.kzgCommitment = kzgCommitment;
    this.kzgProof = kzgProof;
    this.versionedHash = versionedHash;
  }

  /**
   * A bundle holding a blob and the cells of it that the caller already has, for a blob recovered
   * from its cells: recovery yields the whole cell set, so extending the blob to compute them again
   * would be one extension per blob for a result already in hand.
   *
   * @param blobType the type of the blob, which has to be {@link BlobType#KZG_CELL_PROOFS}
   * @param blob the blob being proven
   * @param extendedCells every cell of the blob, one after another
   * @param kzgCommitment the KZG commitment for the blob
   * @param kzgProof the cell proofs for the blob
   * @param versionedHash the versioned hash of the blob
   */
  public BlobProofBundle(
      final BlobType blobType,
      final Blob blob,
      final Bytes extendedCells,
      final KZGCommitment kzgCommitment,
      final List<KZGProof> kzgProof,
      final VersionedHash versionedHash) {
    checkNotNull(blob, "blob must not be null");
    checkNotNull(extendedCells, "extendedCells must not be null");
    checkNotNull(kzgCommitment, "kzgCommitment must not be null");
    checkNotNull(kzgProof, "kzgProof must not be null");
    checkNotNull(versionedHash, "versionedHash must not be null");
    checkArgument(
        blobType == BlobType.KZG_CELL_PROOFS,
        "A bundle given its cells requires blob type KZG_CELL_PROOFS");
    checkArgument(
        kzgProof.size() == CKZG4844Helper.CELL_PROOFS_PER_BLOB,
        "Invalid kzgProof size for versionId 1, expected %s but got %s",
        CKZG4844Helper.CELL_PROOFS_PER_BLOB,
        kzgProof.size());
    // Holding the blob means every cell of it can be computed, so anything less than the whole set
    // would understate what this bundle can serve.
    checkArgument(
        extendedCells.size() == CKZG4844Helper.CELLS_PER_EXT_BLOB * Cell.SIZE,
        "Expected all %s cells of the blob, got %s bytes",
        CKZG4844Helper.CELLS_PER_EXT_BLOB,
        extendedCells.size());

    this(
        blobType,
        Optional.of(blob),
        Optional.of(cellsOf(extendedCells)),
        kzgCommitment,
        kzgProof,
        versionedHash);
  }

  /**
   * For {@link #detachedCopy}, which copies a bundle that was validated when it was built, so the
   * checks of the public constructors would only repeat, and the cells are copied rather than
   * computed again from the blob.
   */
  private BlobProofBundle(
      final BlobType blobType,
      final Optional<Blob> blob,
      final Optional<CellsWithMask> cellsWithMask,
      final KZGCommitment kzgCommitment,
      final List<KZGProof> kzgProof,
      final VersionedHash versionedHash) {
    this.blobType = blobType;
    this.blob = blob;
    this.cellsWithMask = cellsWithMask;
    this.kzgCommitment = kzgCommitment;
    this.kzgProof = kzgProof;
    this.versionedHash = versionedHash;
  }

  private CellsWithMask computeCells(final Blob blob, final BlobType blobType) {
    if (blobType == BlobType.KZG_CELL_PROOFS) {
      return cellsOf(CKZG4844Helper.computeCells(blob));
    }
    return null;
  }

  /**
   * Every cell of a blob, held as slices of the one array that carries them all, which is the shape
   * the blobpool's memory accounting measures.
   */
  private static CellsWithMask cellsOf(final Bytes cellsBytes) {
    final List<Cell> cells = new ArrayList<>(CKZG4844Helper.CELLS_PER_EXT_BLOB);
    for (int i = 0; i < CKZG4844Helper.CELLS_PER_EXT_BLOB; i++) {
      cells.add(new Cell(cellsBytes.slice(i * Cell.SIZE, Cell.SIZE)));
    }
    return new CellsWithMask(cells, CellMask.FULL);
  }

  public BlobType getBlobType() {
    return blobType;
  }

  public Optional<Blob> getBlob() {
    return blob;
  }

  public KZGCommitment getKzgCommitment() {
    return kzgCommitment;
  }

  public List<KZGProof> getKzgProof() {
    return kzgProof;
  }

  public VersionedHash getVersionedHash() {
    return versionedHash;
  }

  /**
   * The cells this bundle holds, concatenated in ascending cell index order.
   *
   * <p>Only the cells the mask reports are included, so a partially sampled bundle yields fewer
   * than {@link CKZG4844Helper#CELLS_PER_EXT_BLOB}. Cells are looked up by index rather than read
   * in list order, so the result is ordered correctly no matter how the cells were accumulated.
   *
   * @return the held cells, or empty when this bundle carries no cells at all
   */
  public Optional<Bytes> getBlobCellsBytes() {
    return cellsWithMask.map(
        cwm ->
            Bytes.wrap(
                cwm.getCellMask()
                    .streamIndexes()
                    .mapToObj(cwm::getCell)
                    .map(Cell::getData)
                    .toList()));
  }

  public Optional<CellsWithMask> getCellsWithMask() {
    return cellsWithMask;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }
    var that = (BlobProofBundle) obj;
    return this.blobType == that.blobType
        && Objects.equals(this.blob, that.blob)
        && Objects.equals(this.kzgCommitment, that.kzgCommitment)
        && Objects.equals(this.kzgProof, that.kzgProof)
        && Objects.equals(this.versionedHash, that.versionedHash)
        // Two bundles of the same blob can hold different cells of it, and for a bundle that holds
        // no blob the cells are all there is to tell them apart.
        && Objects.equals(this.cellsWithMask, that.cellsWithMask);
  }

  @Override
  public int hashCode() {
    return Objects.hash(blobType, blob, kzgCommitment, kzgProof, versionedHash, cellsWithMask);
  }

  /**
   * A copy sharing no byte array with this one, taking the versioned hash and the proofs to use
   * rather than copying its own.
   *
   * <p>Both are passed in because a sidecar detaches all of its bundles at once and holds them in
   * one shape: the transaction and its sidecar hold the same versioned hashes, and the proofs of
   * every blob come from one list that each bundle views its own part of.
   *
   * @param detachedVersionedHash the versioned hash the copy should hold
   * @param detachedProofs the proofs the copy should hold
   * @return the detached copy
   */
  public BlobProofBundle detachedCopy(
      final VersionedHash detachedVersionedHash, final List<KZGProof> detachedProofs) {

    final KZGCommitment detachedCommitment = new KZGCommitment(kzgCommitment.getData().copy());

    if (blob.isPresent()) {
      // The cells were computed from the blob when this bundle was built, so they are copied
      // rather than computed again: extending a blob into its cells is far more expensive than
      // copying them, and the pool detaches every transaction it adds.
      final Blob detachedBlob = new Blob(blob.get().getData().copy());
      return new BlobProofBundle(
          blobType,
          Optional.of(detachedBlob),
          cellsWithMask.map(BlobProofBundle::detachedCopyOfComputedCells),
          detachedCommitment,
          detachedProofs,
          detachedVersionedHash);
    }

    final CellsWithMask cwm =
        cellsWithMask.orElseThrow(
            () ->
                new IllegalStateException(
                    "Internal error: cellsWithMask must be present when blob is not"));

    final CellsWithMask detachedCellsWithMask = cwm.detachedCopy();

    return new BlobProofBundle(
        blobType, detachedCellsWithMask, detachedCommitment, detachedProofs, detachedVersionedHash);
  }

  /**
   * Copies the cells of a blob into one new array and slices it as {@link #computeCells} does, so
   * the copy is held in the same shape as cells computed from the blob, rather than in the array
   * per cell that {@link CellsWithMask#detachedCopy} would give.
   */
  private static CellsWithMask detachedCopyOfComputedCells(final CellsWithMask computed) {
    final List<Cell> cells = computed.getCells();
    final byte[] copied = new byte[cells.size() * Cell.SIZE];
    final MutableBytes destination = MutableBytes.wrap(copied);
    for (int i = 0; i < cells.size(); i++) {
      cells.get(i).getData().copyTo(destination, i * Cell.SIZE);
    }
    // wrapped as computeCells wraps the array it gets back, so the slices are the same kind
    return cellsOf(Bytes.wrap(copied));
  }
}
