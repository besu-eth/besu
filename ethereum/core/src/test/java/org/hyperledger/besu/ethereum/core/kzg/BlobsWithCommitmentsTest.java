/*
 * Copyright contributors to Hyperledger Besu.
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
import static org.hyperledger.besu.datatypes.BlobType.KZG_CELL_PROOFS;
import static org.hyperledger.besu.datatypes.BlobType.KZG_PROOF;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.BlobType;
import org.hyperledger.besu.datatypes.VersionedHash;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

public class BlobsWithCommitmentsTest {
  List<Blob> blobs = List.of(mock(Blob.class), mock(Blob.class));
  List<KZGCommitment> kzgCommitments =
      List.of(mock(KZGCommitment.class), mock(KZGCommitment.class));
  List<KZGProof> kzgProofs = List.of(mock(KZGProof.class), mock(KZGProof.class));
  List<VersionedHash> versionedHashes =
      List.of(mock(VersionedHash.class), mock(VersionedHash.class));

  @Test
  public void blobsWithCommitmentsMustHaveAtLeastOneBlob() {
    String actualMessage =
        assertThrows(
                IllegalArgumentException.class,
                () ->
                    BlobsWithCommitments.createFromBlobsType0(
                        List.of(), List.of(), List.of(), List.of()))
            .getMessage();
    final String expectedMessage =
        "There needs to be a minimum of one blob in a blob transaction with commitments";
    assertThat(actualMessage).isEqualTo(expectedMessage);
  }

  @Test
  public void shouldThrowExceptionWhenKzgCommitmentsSizeIsInvalid_V0() {
    List<KZGCommitment> wrongCommitments =
        List.of(mock(KZGCommitment.class)); // Only one commitment instead of two
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                BlobsWithCommitments.createFromBlobsType0(
                    wrongCommitments, blobs, kzgProofs, versionedHashes));

    assertEquals("Invalid number of kzgCommitments, expected 2, got 1", exception.getMessage());
  }

  @Test
  public void shouldThrowExceptionWhenVersionedHashSizeIsInvalid_V0() {
    List<VersionedHash> wrongVersionedHashes =
        List.of(mock(VersionedHash.class)); // Only one versioned hash instead of two
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                BlobsWithCommitments.createFromBlobsType0(
                    kzgCommitments, blobs, kzgProofs, wrongVersionedHashes));
    assertEquals("Invalid number of versionedHashes, expected 2, got 1", exception.getMessage());
  }

  @Test
  public void shouldThrowExceptionWhenKzgProofsSizeIsInvalid_V0() {
    List<KZGProof> wrongKzgProofs = List.of(mock(KZGProof.class)); // Only one proof instead of two
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                BlobsWithCommitments.createFromBlobsType0(
                    kzgCommitments, blobs, wrongKzgProofs, versionedHashes));
    String error = String.format("Invalid number of proofs (%s), expected 2, got 1", KZG_PROOF);
    assertEquals(error, exception.getMessage());
  }

  @Test
  public void shouldThrowExceptionWhenKzgCommitmentsSizeIsInvalid_V1() {
    List<KZGCommitment> wrongCommitments = List.of(mock(KZGCommitment.class));
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                BlobsWithCommitments.createFromBlobsType1(
                    wrongCommitments, blobs, cellProofGroups(2), versionedHashes));
    assertEquals("Invalid number of kzgCommitments, expected 2, got 1", exception.getMessage());
  }

  @Test
  public void shouldThrowExceptionWhenVersionedHashSizeIsInvalid_V1() {
    List<VersionedHash> wrongVersionedHashes = List.of(mock(VersionedHash.class));
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                BlobsWithCommitments.createFromBlobsType1(
                    kzgCommitments, blobs, cellProofGroups(2), wrongVersionedHashes));
    assertEquals("Invalid number of versionedHashes, expected 2, got 1", exception.getMessage());
  }

  @Test
  public void shouldThrowExceptionWhenProofGroupCountIsInvalid_V1() {
    // One group of proofs for two blobs: there is one group per blob.
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                BlobsWithCommitments.createFromBlobsType1(
                    kzgCommitments, blobs, cellProofGroups(1), versionedHashes));
    String error =
        String.format("Invalid number of proof groups (%s), expected 2, got 1", KZG_CELL_PROOFS);
    assertEquals(error, exception.getMessage());
  }

  @Test
  public void shouldThrowExceptionWhenProofsSizeIsInvalid_V1() {
    // A group per blob, but each holding two proofs rather than one per cell.
    List<List<KZGProof>> shortGroups =
        Collections.nCopies(2, List.of(mock(KZGProof.class), mock(KZGProof.class)));
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                BlobsWithCommitments.createFromBlobsType1(
                    kzgCommitments, blobs, shortGroups, versionedHashes));
    String error =
        String.format(
            "Invalid number of proofs (%s), expected %s, got 2",
            KZG_CELL_PROOFS, CKZG4844Helper.CELL_PROOFS_PER_BLOB);
    assertEquals(error, exception.getMessage());
  }

  @Test
  public void shouldThrowExceptionWhenAProofGroupIsNull_V1() {
    // The grouped shape is this factory's own, so a null group is its own to reject: without this
    // the size check below dereferences it.
    List<List<KZGProof>> withANull = Arrays.asList(cellProofGroups(1).getFirst(), null);
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                BlobsWithCommitments.createFromBlobsType1(
                    kzgCommitments, blobs, withANull, versionedHashes));
    String error = String.format("Proof groups (%s) must all be non null", KZG_CELL_PROOFS);
    assertEquals(error, exception.getMessage());
  }

  @Test
  public void shouldThrowExceptionWhenABlobIsNull() {
    List<Blob> withANull = Arrays.asList(mock(Blob.class), null);
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                BlobsWithCommitments.createFromBlobsType0(
                    kzgCommitments, withANull, kzgProofs, versionedHashes));
    assertEquals("Blobs must all be non null", exception.getMessage());
  }

  @Test
  public void shouldThrowExceptionWhenBlobProofBundlesHaveDifferentTypes() {
    List<BlobProofBundle> invalidBundles =
        List.of(mockBlobProofBundle(KZG_PROOF), mockBlobProofBundle(KZG_CELL_PROOFS));
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> BlobsWithCommitments.createFromBundles(invalidBundles));
    assertEquals("BlobProofBundles must have the same BlobType", exception.getMessage());
  }

  @Test
  public void shouldThrowExceptionWhenBlobProofBundlesListIsEmpty() {
    List<BlobProofBundle> emptyBundles = List.of();
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> BlobsWithCommitments.createFromBundles(emptyBundles));
    assertEquals("BlobProofBundles list cannot be empty", exception.getMessage());
  }

  @Test
  public void shouldThrowExceptionWhenABlobProofBundleIsNull() {
    List<BlobProofBundle> withANull = Arrays.asList(mockBlobProofBundle(KZG_PROOF), null);
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> BlobsWithCommitments.createFromBundles(withANull));
    assertEquals("BlobProofBundles must all be non null", exception.getMessage());
  }

  @Test
  public void shouldAcceptBlobProofBundlesSharingOneCellMask() {
    final CellMask mask = CellMask.fromBytes(Bytes.fromHexString("0x05" + "00".repeat(15)));
    final BlobsWithCommitments bwc =
        BlobsWithCommitments.createFromBundles(
            List.of(
                mockBlobProofBundle(BlobType.KZG_CELL_PROOFS, mask),
                mockBlobProofBundle(BlobType.KZG_CELL_PROOFS, mask)));

    // With the invariant enforced, the shared mask can be read from the first bundle alone.
    assertThat(bwc.getCellMask()).isEqualTo(mask);
    assertThat(bwc.allCellsPresent()).isFalse();
  }

  @Test
  public void shouldThrowExceptionWhenBlobProofBundlesHaveDifferentCellMasks() {
    // A cell index is transaction level, referring to the same cell of every blob, so per-blob
    // divergence is not representable on the wire and must not be constructible.
    final CellMask indexZero = CellMask.fromBytes(Bytes.fromHexString("0x01" + "00".repeat(15)));
    final CellMask indexTwo = CellMask.fromBytes(Bytes.fromHexString("0x04" + "00".repeat(15)));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                BlobsWithCommitments.createFromBundles(
                    List.of(
                        mockBlobProofBundle(BlobType.KZG_CELL_PROOFS, indexZero),
                        mockBlobProofBundle(BlobType.KZG_CELL_PROOFS, indexTwo))));
    assertEquals("Cells must have the same cell mask", exception.getMessage());
  }

  @Test
  public void shouldThrowExceptionWhenOnlySomeBlobProofBundlesHaveCells() {
    // Mixing a bundle that carries cells with one that does not is just as invalid as two
    // differing masks, in either order.
    final CellMask mask = CellMask.fromBytes(Bytes.fromHexString("0x01" + "00".repeat(15)));

    assertEquals(
        "BlobProofBundles must either all carry cells or none of them",
        assertThrows(
                IllegalArgumentException.class,
                () ->
                    BlobsWithCommitments.createFromBundles(
                        List.of(
                            mockBlobProofBundle(BlobType.KZG_CELL_PROOFS, mask),
                            mockBlobProofBundle(BlobType.KZG_CELL_PROOFS))))
            .getMessage());

    assertEquals(
        "BlobProofBundles must either all carry cells or none of them",
        assertThrows(
                IllegalArgumentException.class,
                () ->
                    BlobsWithCommitments.createFromBundles(
                        List.of(
                            mockBlobProofBundle(BlobType.KZG_CELL_PROOFS),
                            mockBlobProofBundle(BlobType.KZG_CELL_PROOFS, mask))))
            .getMessage());
  }

  @Test
  public void shouldThrowExceptionWhenOnlySomeBlobProofBundlesCarryTheirBlob() {
    // Blob presence reflects how the transaction reached this node, which is the same for all of
    // its blobs, in either order. Both bundles share a mask, so only the payloads differ.
    assertEquals(
        "BlobProofBundles must either all carry their blob payload or none of them",
        assertThrows(
                IllegalArgumentException.class,
                () ->
                    BlobsWithCommitments.createFromBundles(
                        List.of(
                            mockBlobProofBundleWithBlob(),
                            mockBlobProofBundle(BlobType.KZG_CELL_PROOFS, CellMask.FULL))))
            .getMessage());

    assertEquals(
        "BlobProofBundles must either all carry their blob payload or none of them",
        assertThrows(
                IllegalArgumentException.class,
                () ->
                    BlobsWithCommitments.createFromBundles(
                        List.of(
                            mockBlobProofBundle(BlobType.KZG_CELL_PROOFS, CellMask.FULL),
                            mockBlobProofBundleWithBlob())))
            .getMessage());
  }

  @Test
  public void hasBlobDataAnswersForEveryBundle() {
    // With the invariant enforced, the first bundle answers for all of them.
    assertThat(
            BlobsWithCommitments.createFromBundles(
                    List.of(mockBlobProofBundleWithBlob(), mockBlobProofBundleWithBlob()))
                .hasBlobData())
        .isTrue();

    assertThat(
            BlobsWithCommitments.createFromBundles(
                    List.of(
                        mockBlobProofBundle(BlobType.KZG_CELL_PROOFS, CellMask.FULL),
                        mockBlobProofBundle(BlobType.KZG_CELL_PROOFS, CellMask.FULL)))
                .hasBlobData())
        .isFalse();
  }

  /** One full group of cell proofs per blob, as a type 1 sidecar carries them. */
  private List<List<KZGProof>> cellProofGroups(final int blobCount) {
    return Collections.nCopies(
        blobCount, Collections.nCopies(CKZG4844Helper.CELL_PROOFS_PER_BLOB, mock(KZGProof.class)));
  }

  private BlobProofBundle mockBlobProofBundle(final BlobType blobType) {
    BlobProofBundle bundle = mock(BlobProofBundle.class);
    when(bundle.getBlobType()).thenReturn(blobType);
    return bundle;
  }

  /** A bundle holding its blob payload, and the full cell mask a computed bundle would have. */
  private BlobProofBundle mockBlobProofBundleWithBlob() {
    final BlobProofBundle bundle = mockBlobProofBundle(BlobType.KZG_CELL_PROOFS, CellMask.FULL);
    when(bundle.getBlob()).thenReturn(Optional.of(mock(Blob.class)));
    return bundle;
  }

  /** A bundle holding the given cell mask, as a sparsely sampled one does. */
  private BlobProofBundle mockBlobProofBundle(final BlobType blobType, final CellMask cellMask) {
    BlobProofBundle bundle = mockBlobProofBundle(blobType);
    final CellsWithMask cellsWithMask = mock(CellsWithMask.class);
    when(cellsWithMask.getCellMask()).thenReturn(cellMask);
    when(bundle.getCellsWithMask()).thenReturn(Optional.of(cellsWithMask));
    return bundle;
  }
}
