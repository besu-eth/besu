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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.hyperledger.besu.ethereum.core.kzg.CKZG4844Helper.CELLS_PER_EXT_BLOB;
import static org.hyperledger.besu.ethereum.core.kzg.CKZG4844Helper.CELLS_TO_RECOVER_BLOB;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;

import org.hyperledger.besu.ethereum.core.BlobTestFixture;
import org.hyperledger.besu.ethereum.util.TrustedSetupClassLoaderExtension;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * Rebuilding a blob from the cells a node sampled, which is how a transaction received over eth/72
 * becomes something this node can put in a block.
 */
class BlobRecoveryTest extends TrustedSetupClassLoaderExtension {

  private static final int BLOB_COUNT = 2;

  /** A complete, genuinely computed v1 sidecar, as a pre-eth/72 peer would send it. */
  private static BlobsWithCommitments fullBlobs() {
    return CKZG4844Helper.convertToVersion1(
        new BlobTestFixture().createBlobsWithCommitments(BLOB_COUNT));
  }

  /** The same sidecar reduced to the cells {@code mask} names, as a sampling node holds it. */
  private static BlobsWithCommitments narrowTo(
      final BlobsWithCommitments full, final CellMask mask) {
    return narrowTo(full, mask, full.getKzgProofs());
  }

  /** As {@link #narrowTo(BlobsWithCommitments, CellMask)}, with the proofs the sender sent. */
  private static BlobsWithCommitments narrowTo(
      final BlobsWithCommitments full, final CellMask mask, final List<KZGProof> kzgProofs) {
    final List<CellsWithMask> narrowed = new ArrayList<>(full.getBlobProofBundles().size());
    for (final BlobProofBundle bundle : full.getBlobProofBundles()) {
      final CellsWithMask allCells = bundle.getCellsWithMask().orElseThrow();
      narrowed.add(
          new CellsWithMask(mask.streamIndexes().mapToObj(allCells::getCell).toList(), mask));
    }
    return BlobsWithCommitments.createFromBlobCells(
        full.getKzgCommitments(), narrowed, kzgProofs, full.getVersionedHashes());
  }

  /** The flat proof list with the two blobs' groups exchanged, so each blob has the other's. */
  private static List<KZGProof> proofsOfTheWrongBlob(final BlobsWithCommitments full) {
    final List<KZGProof> proofs = full.getKzgProofs();
    final int perBlob = CKZG4844Helper.CELL_PROOFS_PER_BLOB;
    final List<KZGProof> swapped = new ArrayList<>(proofs.size());
    swapped.addAll(proofs.subList(perBlob, 2 * perBlob));
    swapped.addAll(proofs.subList(0, perBlob));
    return swapped;
  }

  private static CellMask maskOf(final int... indexes) {
    final BitSet bits = new BitSet(CELLS_PER_EXT_BLOB);
    for (final int index : indexes) {
      bits.set(index);
    }
    final byte[] bytes = new byte[CellMask.BYTE_LENGTH];
    final byte[] set = bits.toByteArray();
    System.arraycopy(set, 0, bytes, 0, set.length);
    return CellMask.fromBytes(Bytes.wrap(bytes));
  }

  private static CellMask everyOtherCell() {
    final int[] indexes = new int[CELLS_TO_RECOVER_BLOB];
    for (int i = 0; i < indexes.length; i++) {
      indexes[i] = i * 2;
    }
    return maskOf(indexes);
  }

  @Test
  void recoversTheBlobsFromHalfOfTheirCells() {
    final BlobsWithCommitments full = fullBlobs();
    // The scattered half, not the first one: the original data sits in the first half of the
    // extended cells, so recovering from exactly those would not exercise the erasure coding.
    final BlobsWithCommitments sampled = narrowTo(full, everyOtherCell());
    assertThat(sampled.hasBlobData()).isFalse();

    final BlobsWithCommitments recovered = CKZG4844Helper.recoverBlobs(sampled);

    assertThat(recovered.hasBlobData()).isTrue();
    assertThat(recovered.getBlobs()).isEqualTo(full.getBlobs());
    // Recovery also yields the cells the node did not sample, so it can now serve all of them.
    assertThat(recovered.getCellMask()).isEqualTo(CellMask.FULL);
    assertThat(CKZG4844Helper.verify4844Kzg(recovered)).isTrue();
  }

  @Test
  void recoveryPreservesWhatBindsTheTransaction() {
    final BlobsWithCommitments recovered =
        CKZG4844Helper.recoverBlobs(narrowTo(fullBlobs(), everyOtherCell()));

    // The commitments and versioned hashes are the ones the sender signed over, and the proofs are
    // the ones it sent: recovery must not quietly substitute its own.
    final BlobsWithCommitments full = fullBlobs();
    assertThat(recovered.getKzgCommitments()).isEqualTo(full.getKzgCommitments());
    assertThat(recovered.getVersionedHashes()).isEqualTo(full.getVersionedHashes());
    assertThat(recovered.getKzgProofs()).isEqualTo(full.getKzgProofs());
  }

  @Test
  void handsOverTheRecoveredCellsRatherThanExtendingTheBlobAgain() {
    // Recovery produces the whole cell set, so extending the recovered blob to compute the cells
    // would repeat work already done, once per blob, and leave the bundle holding two copies of
    // the same 256 KiB.
    final BlobsWithCommitments sampled = narrowTo(fullBlobs(), everyOtherCell());

    final BlobsWithCommitments recovered;
    try (MockedStatic<CKZG4844Helper> helper =
        mockStatic(CKZG4844Helper.class, CALLS_REAL_METHODS)) {
      recovered = CKZG4844Helper.recoverBlobs(sampled);
      helper.verify(() -> CKZG4844Helper.computeCells(any()), never());
    }

    assertThat(recovered.hasBlobData()).isTrue();
    assertThat(recovered.getCellMask()).isEqualTo(CellMask.FULL);
  }

  @Test
  void keepsTheProofsItWasGivenRatherThanTheOnesRecoveryComputes() {
    // Recovery returns proofs of its own, and they are discarded. For a sidecar whose proofs are
    // the right ones this is invisible, since recovery recomputes the same values; it shows up only
    // when they are not. Substituting them would leave this node holding a transaction that
    // verifies while every other node rejects the one that was actually signed and broadcast.
    final BlobsWithCommitments full = fullBlobs();
    final List<KZGProof> sentProofs = proofsOfTheWrongBlob(full);
    assertThat(sentProofs).isNotEqualTo(full.getKzgProofs());

    final BlobsWithCommitments recovered =
        CKZG4844Helper.recoverBlobs(narrowTo(full, everyOtherCell(), sentProofs));

    assertThat(recovered.getKzgProofs()).isEqualTo(sentProofs);
    // so the sidecar still does not verify, which is the point: nothing was quietly repaired
    assertThat(CKZG4844Helper.verify4844Kzg(recovered)).isFalse();
  }

  @Test
  void refusesToRecoverFromTooFewCells() {
    // One short of half: the erasure coding has nothing to work with.
    final int[] indexes = new int[CELLS_TO_RECOVER_BLOB - 1];
    for (int i = 0; i < indexes.length; i++) {
      indexes[i] = i * 2;
    }
    final BlobsWithCommitments tooFew = narrowTo(fullBlobs(), maskOf(indexes));

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> CKZG4844Helper.recoverBlobs(tooFew))
        .withMessageContaining("at least " + CELLS_TO_RECOVER_BLOB);
  }
}
