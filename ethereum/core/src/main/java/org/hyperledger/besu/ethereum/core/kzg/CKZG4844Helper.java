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

import org.hyperledger.besu.datatypes.BlobType;

import java.util.ArrayList;
import java.util.List;

import ethereum.ckzg4844.CKZG4844JNI;
import ethereum.ckzg4844.CellsAndProofs;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes48;

/**
 * Utility class for handling KZG-related operations, including converting BlobsWithCommitments to
 * version 1, extracting KZG proofs, and verifying KZG proofs.
 *
 * <p>Wrapper for CKZG4844JNI to provide a higher-level API for KZG operations.
 */
public class CKZG4844Helper {

  /** Number of cell proofs per blob. */
  public static final int CELL_PROOFS_PER_BLOB = 128;

  /** Number of cells an extended blob is split into, and therefore the range of a cell index. */
  public static final int CELLS_PER_EXT_BLOB = CKZG4844JNI.CELLS_PER_EXT_BLOB;

  /**
   * How many cells of a blob are enough to recover the rest of them, and with them the blob itself.
   * The extension doubles the data, so any half of it suffices.
   */
  public static final int CELLS_TO_RECOVER_BLOB = CELLS_PER_EXT_BLOB / 2;

  /** A blob is exactly the un-extended half of its cells. */
  private static final int BLOB_SIZE = CELLS_TO_RECOVER_BLOB * Cell.SIZE;

  /**
   * Converts the given BlobsWithCommitments to version 1.
   *
   * @param blobsWithCommitments the BlobsWithCommitments to convert.
   * @return a new BlobsWithCommitments instance with version 1 and updated proofs.
   * @throws IllegalArgumentException if the blobs with commitments are not valid for conversion.
   */
  public static BlobsWithCommitments convertToVersion1(
      final BlobsWithCommitments blobsWithCommitments) {
    if (blobsWithCommitments.getBlobType() == BlobType.KZG_CELL_PROOFS) {
      return blobsWithCommitments;
    }
    // Check if the blobs with commitments are valid for conversion
    boolean isValidBlobsWithCommitments = verify4844Kzg(blobsWithCommitments);
    if (!isValidBlobsWithCommitments) {
      throw new IllegalArgumentException(
          "Invalid blobs with commitments for conversion to version 1");
    }

    return unsafeConvertToVersion1(blobsWithCommitments);
  }

  /**
   * Extracts KZG proofs from the given byte array.
   *
   * @param input the byte array containing KZG proofs.
   * @return a list of KZGProof objects extracted from the input.
   */
  private static List<KZGProof> extractKZGProofs(final byte[] input) {
    List<KZGProof> proofs = new ArrayList<>(CELL_PROOFS_PER_BLOB);
    int proofSize = KZGProof.SIZE;
    for (int i = 0; i < CELL_PROOFS_PER_BLOB; i++) {
      byte[] proof = new byte[proofSize];
      System.arraycopy(input, i * proofSize, proof, 0, proofSize);
      proofs.add(new KZGProof(Bytes48.wrap(proof)));
    }
    return proofs;
  }

  private static BlobsWithCommitments unsafeConvertToVersion1(final BlobsWithCommitments bwc) {
    checkArgument(bwc.hasBlobData(), "Blob of type 0 must have full data");

    List<List<KZGProof>> version1Proofs =
        bwc.getBlobs().stream().map(CKZG4844Helper::computeBlobKzgProofs).toList();

    return BlobsWithCommitments.createFromBlobsType1(
        bwc.getKzgCommitments(), bwc.getBlobs(), version1Proofs, bwc.getVersionedHashes());
  }

  /**
   * Computes a KZG proof for the given Blob and KZG commitment.
   *
   * @param blob the Blob for which to compute the KZG proof.
   * @param commitment the KZG commitment to use for the proof.
   * @return a KZGProof object computed from the Blob and KZG commitment.
   */
  public static KZGProof computeBlobKzgProof(final Blob blob, final KZGCommitment commitment) {
    Bytes48 proof =
        Bytes48.wrap(
            CKZG4844JNI.computeBlobKzgProof(
                blob.getData().toArrayUnsafe(), commitment.getData().toArrayUnsafe()));
    return new KZGProof(proof);
  }

  /**
   * Computes KZG proofs for the given Blob.
   *
   * @param blob the Blob for which to compute KZG proofs.
   * @return a list of KZGProof objects computed from the Blob.
   */
  public static List<KZGProof> computeBlobKzgProofs(final Blob blob) {
    CellsAndProofs cellProofs = CKZG4844JNI.computeCellsAndKzgProofs(blob.getData().toArray());
    return extractKZGProofs(cellProofs.getProofs());
  }

  /**
   * Recovers the blobs of a sidecar that holds at least {@link #CELLS_TO_RECOVER_BLOB} cells of
   * each of them, which is what a node ends up with after sampling a transaction over eth/72.
   *
   * <p>This does not verify the cells it is given: a recovery from corrupt cells produces a corrupt
   * blob, so callers must have established that the cells open their commitments. {@code
   * TransactionsLimbo} does so as each peer answers, which is also the only point at which a peer
   * answering with bad cells can still be identified.
   *
   * @param sparse a sidecar holding cells and no blobs
   * @return the same commitments, proofs and versioned hashes, with the blobs recovered
   */
  public static BlobsWithCommitments recoverBlobs(final BlobsWithCommitments sparse) {
    return BlobsWithCommitments.createFromBundles(
        sparse.getBlobProofBundles().stream().map(CKZG4844Helper::recoverBundle).toList());
  }

  /**
   * Recovers the blob of a bundle holding enough of its cells, keeping everything the transaction
   * arrived with.
   *
   * @param bundle the bundle to recover the blob of
   * @return a bundle holding the recovered blob and every cell of it
   */
  private static BlobProofBundle recoverBundle(final BlobProofBundle bundle) {
    final CellsWithMask cellsWithMask =
        bundle
            .getCellsWithMask()
            .orElseThrow(
                () -> new IllegalArgumentException("Bundle holds no cells to recover from"));
    final CellMask heldCells = cellsWithMask.getCellMask();
    checkArgument(
        heldCells.cardinality() >= CELLS_TO_RECOVER_BLOB,
        "Recovering a blob needs at least %s of its cells, got %s",
        CELLS_TO_RECOVER_BLOB,
        heldCells.cardinality());

    final CellsAndProofs recovered =
        CKZG4844JNI.recoverCellsAndKzgProofs(
            heldCells.streamIndexes().asLongStream().toArray(),
            bundle.getBlobCellsBytes().orElseThrow().toArrayUnsafe());
    final Bytes extendedCells = Bytes.wrap(recovered.getCells());

    // The recovered cells are handed over rather than left to be computed from the blob: recovery
    // has just produced them, and they are the whole set, so the bundle can serve every cell.
    return new BlobProofBundle(
        bundle.getBlobType(),
        // The extension is systematic: it doubles the data, leaving the blob itself as the first
        // half of the extended cells.
        new Blob(extendedCells.slice(0, BLOB_SIZE)),
        extendedCells,
        bundle.getKzgCommitment(),
        // the proofs the sender committed to, which recovery would only recompute
        bundle.getKzgProof(),
        bundle.getVersionedHash());
  }

  /**
   * Computes the cells for the given Blob.
   *
   * @param blob the Blob for which to compute the cells.
   * @return a Bytes object containing the computed cells.
   */
  public static Bytes computeCells(final Blob blob) {
    return Bytes.wrap(CKZG4844JNI.computeCells(blob.getData().toArrayUnsafe()));
  }

  /**
   * Verifies the KZG proofs in the given BlobsWithCommitments.
   *
   * <p>For {@link BlobType#KZG_CELL_PROOFS} this verifies the cells currently held, which under
   * eth/72 may be a subset: a sampling node only ever obtains its custody columns. Holding fewer
   * cells is not itself an error, but every cell held must open its blob's commitment.
   *
   * @param blobsWithCommitments the BlobsWithCommitments to verify.
   * @return true if the KZG proofs are valid, false otherwise.
   */
  public static boolean verify4844Kzg(final BlobsWithCommitments blobsWithCommitments) {
    return switch (blobsWithCommitments.getBlobType()) {
      case BlobType.KZG_PROOF ->
          CKZG4844JNI.verifyBlobKzgProofBatch(
              blobsWithCommitments.getBlobsByteArray(),
              blobsWithCommitments.getKzgCommitmentsByteArray(),
              blobsWithCommitments.getKzgProofsByteArray(),
              blobsWithCommitments.getBlobProofBundles().size());
      case KZG_CELL_PROOFS -> {
        if (blobsWithCommitments.getCellMask().isEmpty()) {
          // An eth/72 transaction arrives with its blobs elided and no cells at all, so there is
          // nothing to verify yet. Its commitments were already checked against the versioned
          // hashes; each cell is verified as it is sampled.
          yield true;
        }
        yield CKZG4844JNI.verifyCellKzgProofBatch(
            blobsWithCommitments.getKzgCommitmentsByteArray(),
            blobsWithCommitments.getCellIndexes(),
            blobsWithCommitments.getBlobCellsByteArray(),
            blobsWithCommitments.getKzgProofsByteArray());
      }
    };
  }
}
