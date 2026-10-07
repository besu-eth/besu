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
import static org.hyperledger.besu.datatypes.BlobType.KZG_CELL_PROOFS;
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
   * Private: instances are built through the static factories, each of which validates what its own
   * inputs can get wrong. However it was built, an instance holds at least one bundle and every
   * bundle carries the declared {@link BlobType}.
   *
   * @param blobType the blob type every bundle declares
   * @param blobProofBundles the bundles, one per blob
   */
  private BlobsWithCommitments(
      final BlobType blobType, final List<BlobProofBundle> blobProofBundles) {
    this.blobType = blobType;
    this.blobProofBundles = blobProofBundles;
  }

  /**
   * Assembles an instance from ready-made bundles, whose origin this class cannot see, so the
   * invariants have to be checked here rather than following from how the bundles were built.
   *
   * @param blobProofBundles the bundles, one per blob
   * @return the assembled instance
   */
  public static BlobsWithCommitments createFromBundles(
      final List<BlobProofBundle> blobProofBundles) {
    checkArgument(!blobProofBundles.isEmpty(), "BlobProofBundles list cannot be empty");
    checkArgument(
        blobProofBundles.stream().noneMatch(Objects::isNull),
        "BlobProofBundles must all be non null");
    final BlobType blobType = blobProofBundles.getFirst().getBlobType();
    checkArgument(
        blobProofBundles.stream().allMatch(bundle -> bundle.getBlobType() == blobType),
        "BlobProofBundles must have the same BlobType");
    return new BlobsWithCommitments(blobType, blobProofBundles);
  }

  /**
   * Constructs an instance of {@link BlobType#KZG_PROOF}, which carries one proof per blob.
   *
   * @param kzgCommitments commitments for the blobs
   * @param blobs list of blobs to be committed to
   * @param kzgProofs one proof per blob
   * @param versionedHashes hashes of the commitments
   * @return the instance
   * @throws InvalidParameterException if the input parameters are invalid
   */
  public static BlobsWithCommitments createFromBlobsType0(
      final List<KZGCommitment> kzgCommitments,
      final List<Blob> blobs,
      final List<KZGProof> kzgProofs,
      final List<VersionedHash> versionedHashes) {
    final int blobCount = validateBlobsAndCommitments(kzgCommitments, blobs, versionedHashes);
    checkArgument(
        kzgProofs.size() == blobCount,
        "Invalid number of proofs (%s), expected %s, got %s",
        KZG_PROOF,
        blobCount,
        kzgProofs.size());

    return new BlobsWithCommitments(
        KZG_PROOF,
        IntStream.range(0, blobCount)
            .mapToObj(
                index ->
                    new BlobProofBundle(
                        KZG_PROOF,
                        blobs.get(index),
                        kzgCommitments.get(index),
                        List.of(kzgProofs.get(index)),
                        versionedHashes.get(index)))
            .toList());
  }

  /**
   * Constructs an instance of {@link BlobType#KZG_CELL_PROOFS}, which carries {@link
   * CKZG4844Helper#CELL_PROOFS_PER_BLOB} proofs per blob.
   *
   * @param kzgCommitments commitments for the blobs
   * @param blobs list of blobs to be committed to
   * @param kzgProofs the proofs of each blob, one group per blob and in blob order
   * @param versionedHashes hashes of the commitments
   * @return the instance
   * @throws InvalidParameterException if the input parameters are invalid
   */
  public static BlobsWithCommitments createFromBlobsType1(
      final List<KZGCommitment> kzgCommitments,
      final List<Blob> blobs,
      final List<List<KZGProof>> kzgProofs,
      final List<VersionedHash> versionedHashes) {
    final int blobCount = validateBlobsAndCommitments(kzgCommitments, blobs, versionedHashes);
    checkArgument(
        kzgProofs.size() == blobCount,
        "Invalid number of proof groups (%s), expected %s, got %s",
        KZG_CELL_PROOFS,
        blobCount,
        kzgProofs.size());
    // A group per blob is not enough: each has to hold that blob's full set of cell proofs
    kzgProofs.forEach(
        proofsForBlob -> {
          checkArgument(
              proofsForBlob != null, "Proof groups (%s) must all be non null", KZG_CELL_PROOFS);
          checkArgument(
              proofsForBlob.size() == CELL_PROOFS_PER_BLOB,
              "Invalid number of proofs (%s), expected %s, got %s",
              KZG_CELL_PROOFS,
              CELL_PROOFS_PER_BLOB,
              proofsForBlob.size());
        });

    return new BlobsWithCommitments(
        KZG_CELL_PROOFS,
        IntStream.range(0, blobCount)
            .mapToObj(
                index ->
                    new BlobProofBundle(
                        KZG_CELL_PROOFS,
                        blobs.get(index),
                        kzgCommitments.get(index),
                        kzgProofs.get(index),
                        versionedHashes.get(index)))
            .toList());
  }

  /**
   * Checks what every blob-carrying factory needs, anchored on the blobs: they are the payload
   * being described, so every other list is counted against them.
   *
   * @return the number of blobs
   */
  private static int validateBlobsAndCommitments(
      final List<KZGCommitment> kzgCommitments,
      final List<Blob> blobs,
      final List<VersionedHash> versionedHashes) {
    checkNotNull(versionedHashes, "versionedHashes must be set before calling kzgBlobs()");
    final int blobCount = blobs.size();
    checkArgument(
        blobCount > 0,
        "There needs to be a minimum of one blob in a blob transaction with commitments");
    checkArgument(blobs.stream().noneMatch(Objects::isNull), "Blobs must all be non null");
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
    return blobCount;
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
