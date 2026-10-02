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
import static java.util.Collections.emptyList;
import static org.hyperledger.besu.datatypes.BlobType.KZG_CELL_PROOFS;
import static org.hyperledger.besu.datatypes.BlobType.KZG_PROOF;
import static org.hyperledger.besu.ethereum.core.kzg.CKZG4844Helper.CELL_PROOFS_PER_BLOB;

import org.hyperledger.besu.datatypes.BlobType;
import org.hyperledger.besu.datatypes.VersionedHash;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;

import org.apache.tuweni.bytes.Bytes;

/** A class to hold the blobs, commitments, proofs, and versioned hashes for a set of blobs. */
public class BlobsWithCommitments implements org.hyperledger.besu.datatypes.BlobsWithCommitments {
  private final BlobType blobType;
  private final List<BlobProofBundle> blobProofBundles;

  /**
   * Private: instances are built through the static factories, each of which validates what its own
   * inputs can get wrong. However it was built, an instance satisfies:
   *
   * <ul>
   *   <li>the bundle list is not empty
   *   <li>every bundle carries the declared {@link BlobType}
   *   <li>every bundle shares one cell availability mask, or none of them has cells
   *   <li>every bundle carries its blob payload, or none of them does
   * </ul>
   *
   * <p>The last two are what let {@link #getCellMask()} and {@link #hasBlobData()} answer from the
   * first bundle alone. For the factories that build the bundles themselves they hold by
   * construction; {@link #createFromBundles} checks them.
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
    // These follow from how the other factories build their bundles, but here the bundles arrive
    // already built, so they have to be checked: getCellMask() and hasBlobData() read the first
    // bundle only and rely on them.
    checkSharedCellMask(
        blobProofBundles.stream()
            .map(BlobProofBundle::getCellsWithMask)
            .flatMap(Optional::stream)
            .toList());
    checkArgument(
        blobProofBundles.stream()
                .map(bundle -> bundle.getCellsWithMask().isPresent())
                .distinct()
                .count()
            == 1,
        "BlobProofBundles must either all carry cells or none of them");
    checkArgument(
        blobProofBundles.stream().map(bundle -> bundle.getBlob().isPresent()).distinct().count()
            == 1,
        "BlobProofBundles must either all carry their blob payload or none of them");
    return new BlobsWithCommitments(blobType, List.copyOf(blobProofBundles));
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
    checkArgument(blobs.stream().noneMatch(Objects::isNull), "Blobs must all be non null");
    return validatePayloadCount(blobs.size(), kzgCommitments, versionedHashes);
  }

  /**
   * Checks the counts every factory needs against the number of payload items it was given, whether
   * those are blobs or the cells of blobs.
   *
   * @return the payload count
   */
  private static int validatePayloadCount(
      final int payloadCount,
      final List<KZGCommitment> kzgCommitments,
      final List<VersionedHash> versionedHashes) {
    checkNotNull(versionedHashes, "versionedHashes must be set before calling kzgBlobs()");
    checkArgument(
        payloadCount > 0,
        "There needs to be a minimum of one blob in a blob transaction with commitments");
    checkArgument(
        payloadCount == kzgCommitments.size(),
        "Invalid number of kzgCommitments, expected %s, got %s",
        payloadCount,
        kzgCommitments.size());
    checkArgument(
        payloadCount == versionedHashes.size(),
        "Invalid number of versionedHashes, expected %s, got %s",
        payloadCount,
        versionedHashes.size());
    return payloadCount;
  }

  /**
   * Constructs an instance of {@link BlobType#KZG_CELL_PROOFS} holding cells rather than blobs,
   * which is what a node has after receiving a transaction over eth/72.
   *
   * @param kzgCommitments commitments for the blobs
   * @param cellsWithMaskList the cells held of each blob, one entry per blob
   * @param kzgProofs every blob's cell proofs, one blob after another
   * @param versionedHashes hashes of the commitments
   * @return the instance
   * @throws InvalidParameterException if the input parameters are invalid
   */
  public static BlobsWithCommitments createFromBlobCells(
      final List<KZGCommitment> kzgCommitments,
      final List<CellsWithMask> cellsWithMaskList,
      final List<KZGProof> kzgProofs,
      final List<VersionedHash> versionedHashes) {
    checkArgument(
        cellsWithMaskList.stream().noneMatch(Objects::isNull), "Cells must all be non null");
    final int blobCount =
        validatePayloadCount(cellsWithMaskList.size(), kzgCommitments, versionedHashes);
    final int expectedProofs = CELL_PROOFS_PER_BLOB * blobCount;
    checkArgument(
        kzgProofs.size() == expectedProofs,
        "Invalid number of proofs (%s), expected %s, got %s",
        KZG_CELL_PROOFS,
        expectedProofs,
        kzgProofs.size());
    checkSharedCellMask(cellsWithMaskList);

    return new BlobsWithCommitments(
        KZG_CELL_PROOFS,
        IntStream.range(0, blobCount)
            .mapToObj(
                index ->
                    new BlobProofBundle(
                        KZG_CELL_PROOFS,
                        cellsWithMaskList.get(index),
                        kzgCommitments.get(index),
                        kzgProofs.subList(
                            index * CELL_PROOFS_PER_BLOB, (index + 1) * CELL_PROOFS_PER_BLOB),
                        versionedHashes.get(index)))
            .toList());
  }

  /**
   * Enforces that all blobs of a transaction share one cell availability mask.
   *
   * <p>This is a property of the protocol, not an implementation convenience: an eth/72 cell index
   * is transaction level, referring to the corresponding cell of every blob in the transaction, so
   * per-blob divergence is not representable on the wire.
   *
   * @param cellsWithMasks the cells to check, which may be empty when no bundle holds any
   */
  private static void checkSharedCellMask(final List<CellsWithMask> cellsWithMasks) {
    if (cellsWithMasks.isEmpty()) {
      return;
    }
    final CellMask firstCellMask = cellsWithMasks.getFirst().getCellMask();
    checkArgument(
        cellsWithMasks.stream()
            .skip(1)
            .map(CellsWithMask::getCellMask)
            .allMatch(firstCellMask::equals),
        "Cells must have the same cell mask");
  }

  @Override
  public List<Blob> getBlobs() {
    if (hasBlobData()) {
      return blobProofBundles.stream()
          .map(BlobProofBundle::getBlob)
          .map(Optional::orElseThrow)
          .toList();
    }
    return emptyList();
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
    final List<KZGProof> proofs =
        (blobType == KZG_CELL_PROOFS) ? proofsForHeldCells() : getKzgProofs();
    return Bytes.wrap(proofs.stream().map(kp -> (Bytes) kp.getData()).toList()).toArrayUnsafe();
  }

  /**
   * The cell proofs matching the cells we hold, one per held cell per blob, in the same order as
   * {@link #getBlobCellsByteArray()}.
   *
   * <p>Proofs are never elided on the wire, so a bundle always carries all {@link
   * CKZG4844Helper#CELL_PROOFS_PER_BLOB} of them; only the subset covering the cells we actually
   * have can be verified.
   *
   * @return the proofs for the held cells
   */
  private List<KZGProof> proofsForHeldCells() {
    final int[] heldIndexes = getCellMask().indexes();
    final List<KZGProof> proofs = new ArrayList<>(heldIndexes.length * blobProofBundles.size());
    for (final BlobProofBundle bundle : blobProofBundles) {
      final List<KZGProof> blobProofs = bundle.getKzgProof();
      for (final int heldIndex : heldIndexes) {
        proofs.add(blobProofs.get(heldIndex));
      }
    }
    return proofs;
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
        (blobType == KZG_CELL_PROOFS)
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
    // verifyCellKzgProofBatch takes four parallel arrays, one entry per cell being verified, so a
    // blob's commitment is repeated once per cell we actually hold, not once per possible cell.
    final int cellsPerBlob = getCellMask().cardinality();
    final ArrayList<KZGCommitment> extendedCommitments =
        new ArrayList<>(commitments.size() * cellsPerBlob);
    for (final KZGCommitment kzgCommitment : commitments) {
      for (int i = 0; i < cellsPerBlob; i++) {
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
    // The indexes we actually hold, repeated per blob, parallel to getBlobCellsByteArray().
    final int[] heldIndexes = getCellMask().indexes();
    final long[] cellIndices = new long[heldIndexes.length * blobProofBundles.size()];
    for (int blobIndex = 0; blobIndex < blobProofBundles.size(); blobIndex++) {
      for (int index = 0; index < heldIndexes.length; index++) {
        cellIndices[blobIndex * heldIndexes.length + index] = heldIndexes[index];
      }
    }
    return cellIndices;
  }

  /**
   * The cell availability mask shared by every blob of this transaction. Reading the first bundle
   * is sufficient because the constructor enforces that they all agree.
   *
   * @return the shared mask, or a full mask for blob types that do not carry cells
   */
  public CellMask getCellMask() {
    return blobProofBundles
        .getFirst()
        .getCellsWithMask()
        .map(CellsWithMask::getCellMask)
        .orElse(CellMask.FULL);
  }

  public boolean allCellsPresent() {
    return getCellMask().isFull();
  }

  /**
   * Whether the actual blob payloads are held, as opposed to cells.
   *
   * <p>Distinct from {@link #allCellsPresent()}, which asks about the cell mask. A transaction
   * reassembled from a complete set of cells reports every cell present while holding no {@link
   * Blob} at all, so only this predicate answers whether the pre-eth/72 wire form — which carries
   * the payloads themselves — can be produced.
   *
   * @return true if every blob of this transaction is held in full
   */
  public boolean hasBlobData() {
    // The canonical constructor rejects a mix, so the first bundle answers for all of them.
    return blobProofBundles.getFirst().getBlob().isPresent();
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

  /**
   * A copy sharing no byte array with this one, for holding beyond the lifetime of the message it
   * was decoded from.
   *
   * <p>The versioned hashes are passed in rather than copied: a blob transaction holds the same
   * hashes as its sidecar does, so detaching the whole transaction copies them once and hands them
   * to both, rather than leaving the copy with two sets of identical hashes where the original had
   * one.
   *
   * @param detachedVersionedHashes the hashes the copy should hold, one per blob and in blob order
   * @return the detached copy
   */
  public BlobsWithCommitments detachedCopy(final List<VersionedHash> detachedVersionedHashes) {
    checkArgument(
        detachedVersionedHashes.size() == blobProofBundles.size(),
        "Invalid number of versionedHashes, expected %s, got %s",
        blobProofBundles.size(),
        detachedVersionedHashes.size());

    final List<KZGProof> detachedProofs =
        getKzgProofs().stream().map(proof -> new KZGProof(proof.getData().copy())).toList();
    final int proofsPerBlob = detachedProofs.size() / blobProofBundles.size();

    return new BlobsWithCommitments(
        blobType,
        IntStream.range(0, blobProofBundles.size())
            .mapToObj(
                index ->
                    blobProofBundles
                        .get(index)
                        .detachedCopy(
                            detachedVersionedHashes.get(index),
                            proofsFor(detachedProofs, index, proofsPerBlob)))
            .toList());
  }

  /**
   * One blob's proofs out of the copied list, in the shape a decoded sidecar holds them, which is
   * the shape the blobpool accounts for: a cell-proof sidecar keeps one list for the whole
   * transaction and each blob views its own part of it, where a sidecar with one proof per blob
   * keeps that proof on its own.
   */
  private List<KZGProof> proofsFor(
      final List<KZGProof> detachedProofs, final int index, final int proofsPerBlob) {
    return blobType == KZG_CELL_PROOFS
        ? detachedProofs.subList(index * proofsPerBlob, (index + 1) * proofsPerBlob)
        : List.of(detachedProofs.get(index));
  }
}
