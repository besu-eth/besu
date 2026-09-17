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
import static org.hyperledger.besu.datatypes.BlobType.KZG_PROOF;
import static org.hyperledger.besu.ethereum.core.kzg.CKZG4844Helper.CELL_PROOFS_PER_BLOB;

import org.hyperledger.besu.datatypes.BlobType;
import org.hyperledger.besu.datatypes.VersionedHash;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import org.apache.tuweni.bytes.Bytes;

/** A class to hold the blobs, commitments, proofs, and versioned hashes for a set of blobs. */
public class BlobsWithCommitments implements org.hyperledger.besu.datatypes.BlobsWithCommitments {
  private final BlobType blobType;
  private final List<BlobProofBundle> blobProofBundles;

  /**
   * Constructs a {@link BlobsWithCommitments} instance from a list of {@link BlobProofBundle}.
   *
   * @param blobProofBundles the list of blob proof bundles to be attached to the transaction.
   */
  public BlobsWithCommitments(final List<BlobProofBundle> blobProofBundles) {
    checkArgument(!blobProofBundles.isEmpty(), "BlobProofBundles list cannot be empty");
    BlobType blobType = blobProofBundles.getFirst().getBlobType();
    checkArgument(
        blobProofBundles.stream().allMatch(bundle -> bundle.getBlobType() == blobType),
        "BlobProofBundles must have the same BlobType");
    this.blobProofBundles = blobProofBundles;
    this.blobType = blobType;
  }

  /**
   * Constructs a {@link BlobsWithCommitments} instance.
   *
   * @param blobType blobType for the sidecar.
   * @param kzgCommitments commitments for the blobs.
   * @param blobs list of blobs to be committed to.
   * @param kzgProofs proofs for the commitments.
   * @param versionedHashes hashes of the commitments.
   * @throws InvalidParameterException if the input parameters are invalid.
   */
  public BlobsWithCommitments(
      final BlobType blobType,
      final List<KZGCommitment> kzgCommitments,
      final List<Blob> blobs,
      final List<KZGProof> kzgProofs,
      final List<VersionedHash> versionedHashes) {
    validateInputParameters(blobType, kzgCommitments, blobs, kzgProofs, versionedHashes);
    this.blobProofBundles =
        buildBlobProofBundles(blobType, kzgCommitments, blobs, kzgProofs, versionedHashes);
    this.blobType = blobType;
  }

  private static List<BlobProofBundle> buildBlobProofBundles(
      final BlobType blobType,
      final List<KZGCommitment> kzgCommitments,
      final List<Blob> blobs,
      final List<KZGProof> kzgProofs,
      final List<VersionedHash> versionedHashes) {
    return IntStream.range(0, blobs.size())
        .mapToObj(
            index -> {
              List<KZGProof> kzgProofsForBlob = extractProofsForBlob(blobType, kzgProofs, index);
              return new BlobProofBundle(
                  blobType,
                  blobs.get(index),
                  kzgCommitments.get(index),
                  kzgProofsForBlob,
                  versionedHashes.get(index));
            })
        .toList();
  }

  private static List<KZGProof> extractProofsForBlob(
      final BlobType blobType, final List<KZGProof> kzgProofs, final int index) {
    return switch (blobType) {
      case KZG_PROOF -> List.of(kzgProofs.get(index)); // Single proof per blob
      case KZG_CELL_PROOFS ->
          kzgProofs.subList(
              index * CELL_PROOFS_PER_BLOB,
              (index + 1) * CELL_PROOFS_PER_BLOB); // 128 cell proofs per blob
    };
  }

  private static void validateInputParameters(
      final BlobType blobType,
      final List<KZGCommitment> kzgCommitments,
      final List<Blob> blobs,
      final List<KZGProof> kzgProofs,
      final List<VersionedHash> versionedHashes) {
    checkNotNull(versionedHashes, "versionedHashes must be set before calling kzgBlobs()");
    int blobCount = blobs.size();
    int expectedProofs = blobType == KZG_PROOF ? blobCount : CELL_PROOFS_PER_BLOB * blobCount;
    checkArgument(
        blobCount > 0,
        "There needs to be a minimum of one blob in a blob transaction with commitments");
    checkArgument(
        blobCount == kzgCommitments.size(),
        "Invalid number of kzgCommitments, expected %s, got %s",
        blobCount,
        kzgCommitments.size());
    checkArgument(
        blobCount == versionedHashes.size(),
        "Invalid number of versionedHashes, expected %s, got %s",
        blobCount,
        versionedHashes.size());
    checkArgument(
        kzgProofs.size() == expectedProofs,
        "Invalid number of proofs (%s), expected %s, got %s",
        blobType,
        expectedProofs,
        kzgProofs.size());
  }

  /**
   * Get the blobs.
   *
   * @return the blobs
   */
  @Override
  public List<Blob> getBlobs() {
    return blobProofBundles.stream().map(BlobProofBundle::getBlob).toList();
  }

  /**
   * Get the commitments.
   *
   * @return the commitments
   */
  @Override
  public List<KZGCommitment> getKzgCommitments() {
    return blobProofBundles.stream().map(BlobProofBundle::getKzgCommitment).toList();
  }

  /**
   * Get the proofs.
   *
   * @return the proofs
   */
  @Override
  public List<KZGProof> getKzgProofs() {
    return blobProofBundles.stream().flatMap(bundle -> bundle.getKzgProof().stream()).toList();
  }

  /**
   * Get the hashes.
   *
   * @return the hashes
   */
  @Override
  public List<VersionedHash> getVersionedHashes() {
    return blobProofBundles.stream().map(BlobProofBundle::getVersionedHash).toList();
  }

  /**
   * Get the list of BlobProofBundle.
   *
   * @return blob proof bundles
   */
  public List<BlobProofBundle> getBlobProofBundles() {
    return blobProofBundles;
  }

  /**
   * Get the BlobType
   *
   * @return the type of the blobs
   */
  @Override
  public BlobType getBlobType() {
    return blobType;
  }

  /**
   * Get the KZG proofs as a byte array. Passed to the CKZG4844JNI for proof verification.
   *
   * @return the KZG proofs as a byte array
   */
  byte[] getKzgProofsByteArray() {
    return Bytes.wrap(getKzgProofs().stream().map(kp -> (Bytes) kp.getData()).toList())
        .toArrayUnsafe();
  }

  /**
   * Get the blobs as a byte array. Passed to the CKZG4844JNI for proof verification.
   *
   * @return the blobs as a byte array
   */
  byte[] getBlobsByteArray() {
    return Bytes.wrap(getBlobs().stream().map(Blob::getData).toList()).toArrayUnsafe();
  }

  /**
   * Get the KZG commitments as a byte array. Passed to the CKZG4844JNI for proof verification.
   *
   * @return the KZG commitments as a byte array
   */
  byte[] getKzgCommitmentsByteArray() {
    List<KZGCommitment> commitments =
        (blobType == BlobType.KZG_CELL_PROOFS)
            ? extendCommitments(getKzgCommitments())
            : getKzgCommitments();
    return Bytes.wrap(commitments.stream().map(kc -> (Bytes) kc.getData()).toList())
        .toArrayUnsafe();
  }

  /**
   * Extends the KZG commitments to match the number of cell proofs per blob. This is necessary when
   * the blob type is KZG_CELL_PROOFS, and we want to verify the cell proofs
   *
   * @param commitments the original list of KZG commitments.
   * @return a new list of KZG commitments, extended to match the number of cell proofs per blob.
   */
  private List<KZGCommitment> extendCommitments(final List<KZGCommitment> commitments) {
    int newSize = commitments.size() * CELL_PROOFS_PER_BLOB;
    ArrayList<KZGCommitment> extendedCommitments = new ArrayList<>(newSize);
    for (KZGCommitment kzgCommitment : commitments) {
      for (int i = 0; i < CELL_PROOFS_PER_BLOB; i++) {
        extendedCommitments.add(new KZGCommitment(kzgCommitment.getData()));
      }
    }
    return extendedCommitments;
  }

  /**
   * Get the blob cells as a byte array. Passed to the CKZG4844JNI for proof verification.
   *
   * @return the blob cells as a byte array
   */
  byte[] getBlobCellsByteArray() {
    return Bytes.wrap(
            blobProofBundles.stream().map(cell -> cell.getBlobCellsBytes().orElseThrow()).toList())
        .toArrayUnsafe();
  }

  /**
   * Get the cell indexes for the blobs. Passed to the CKZG4844JNI for proof verification.
   *
   * @return an array of cell indexes
   */
  long[] getCellIndexes() {
    long[] cellIndices = new long[CELL_PROOFS_PER_BLOB * blobProofBundles.size()];
    for (int blobIndex = 0; blobIndex < blobProofBundles.size(); blobIndex++) {
      for (int index = 0; index < CELL_PROOFS_PER_BLOB; index++) {
        cellIndices[blobIndex * CELL_PROOFS_PER_BLOB + index] = index;
      }
    }
    return cellIndices;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BlobsWithCommitments that = (BlobsWithCommitments) o;
    return blobType == that.blobType && Objects.equals(blobProofBundles, that.blobProofBundles);
  }

  @Override
  public int hashCode() {
    return Objects.hash(blobProofBundles, blobType);
  }
}
