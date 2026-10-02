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

import static com.google.common.base.Preconditions.checkNotNull;

import org.hyperledger.besu.datatypes.BlobType;
import org.hyperledger.besu.datatypes.VersionedHash;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

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

  private CellsWithMask computeCells(final Blob blob, final BlobType blobType) {
    if (blobType == BlobType.KZG_CELL_PROOFS) {
      final Bytes cellsBytes = CKZG4844Helper.computeCells(blob);
      final List<Cell> cells = new ArrayList<>(CKZG4844Helper.CELLS_PER_EXT_BLOB);
      for (int i = 0; i < CKZG4844Helper.CELLS_PER_EXT_BLOB; i++) {
        cells.add(new Cell(cellsBytes.slice(i * Cell.SIZE, Cell.SIZE)));
      }
      return new CellsWithMask(cells, CellMask.FULL);
    }
    return null;
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
      final Blob detachedBlob = new Blob(blob.get().getData().copy());
      return new BlobProofBundle(
          blobType, detachedBlob, detachedCommitment, detachedProofs, detachedVersionedHash);
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
}
