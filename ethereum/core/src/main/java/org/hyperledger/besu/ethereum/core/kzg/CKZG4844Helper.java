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

import org.hyperledger.besu.datatypes.BlobType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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

    List<BlobProofBundle> version1Bundles =
        blobsWithCommitments.getBlobProofBundles().stream()
            .map(CKZG4844Helper::unsafeConvertToVersion1)
            .collect(Collectors.toList());

    return BlobsWithCommitments.createFromBundles(version1Bundles);
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

  /**
   * Converts the given BlobProofBundle to version 1 without validating proof.
   *
   * @param bundle the BlobProofBundle to convert.
   * @return a new BlobProofBundle instance with version 1 and updated proofs.
   */
  public static BlobProofBundle unsafeConvertToVersion1(final BlobProofBundle bundle) {
    if (bundle.getBlobType() == BlobType.KZG_CELL_PROOFS) {
      return bundle;
    }
    List<KZGProof> kzgCellProofs = computeBlobKzgProofs(bundle.getBlob());
    return new BlobProofBundle(
        BlobType.KZG_CELL_PROOFS,
        bundle.getBlob(),
        bundle.getKzgCommitment(),
        kzgCellProofs,
        bundle.getVersionedHash());
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
      case KZG_CELL_PROOFS ->
          CKZG4844JNI.verifyCellKzgProofBatch(
              blobsWithCommitments.getKzgCommitmentsByteArray(),
              blobsWithCommitments.getCellIndexes(),
              blobsWithCommitments.getBlobCellsByteArray(),
              blobsWithCommitments.getKzgProofsByteArray());
    };
  }
}
