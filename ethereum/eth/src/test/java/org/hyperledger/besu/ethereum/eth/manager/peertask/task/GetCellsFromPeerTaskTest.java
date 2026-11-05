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
package org.hyperledger.besu.ethereum.eth.manager.peertask.task;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.core.BlobTestFixture;
import org.hyperledger.besu.ethereum.core.CellsOnlyBlobTransactionFixture;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.kzg.BlobsWithCommitments;
import org.hyperledger.besu.ethereum.core.kzg.CKZG4844Helper;
import org.hyperledger.besu.ethereum.core.kzg.CellMask;
import org.hyperledger.besu.ethereum.core.kzg.CellsWithMask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskValidationResponse;
import org.hyperledger.besu.ethereum.util.TrustedSetupClassLoaderExtension;

import java.util.BitSet;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The cryptographic half of validating a {@code Cells} answer. Its shape is checked while it is
 * decoded; this is where the cells are established to be the cells they claim to be, and the last
 * point at which the peer that sent them can still be named.
 */
class GetCellsFromPeerTaskTest extends TrustedSetupClassLoaderExtension {

  private static final CellMask REQUESTED = CellMask.FULL;

  /** One fixture for the whole test: each call to it yields a different blob. */
  private final BlobTestFixture blobTestFixture = new BlobTestFixture();

  private BlobsWithCommitments fullSidecar;
  private Transaction requestedTx;
  private GetCellsFromPeerTask task;

  @BeforeEach
  void setUp() {
    fullSidecar = CKZG4844Helper.convertToVersion1(blobTestFixture.createBlobsWithCommitments(1));
    // As decoded from an eth/72 PooledTransactions response: commitments and proofs, no cells.
    requestedTx =
        new CellsOnlyBlobTransactionFixture()
            .create(
                BlobsWithCommitments.createFromBlobCells(
                    fullSidecar.getKzgCommitments(),
                    List.of(CellsWithMask.EMPTY),
                    fullSidecar.getKzgProofs(),
                    fullSidecar.getVersionedHashes()));
    task = new GetCellsFromPeerTask(requestedTx, REQUESTED);
  }

  @Test
  void acceptsCellsThatOpenTheirCommitments() {
    assertThat(task.validateResult(cellsOf(fullSidecar, REQUESTED)))
        .isEqualTo(PeerTaskValidationResponse.RESULTS_VALID_AND_GOOD);
  }

  @Test
  void acceptsAPartialAnswer() {
    // A responder may truncate, so holding back cells is not misbehaviour; sending wrong ones is.
    assertThat(task.validateResult(cellsOf(fullSidecar, range(0, 64))))
        .isEqualTo(PeerTaskValidationResponse.RESULTS_VALID_AND_GOOD);
  }

  @Test
  void acceptsAnEmptyAnswer() {
    assertThat(task.validateResult(List.of()))
        .isEqualTo(PeerTaskValidationResponse.RESULTS_VALID_AND_GOOD);
  }

  @Test
  void rejectsCellsOfAnotherBlob() {
    // Every cell well formed, and not one of them opening the commitment it is offered against.
    final BlobsWithCommitments anotherBlob =
        CKZG4844Helper.convertToVersion1(blobTestFixture.createBlobsWithCommitments(1));
    assertThat(anotherBlob.getBlobs()).isNotEqualTo(fullSidecar.getBlobs());

    assertThat(task.validateResult(cellsOf(anotherBlob, REQUESTED)))
        .isEqualTo(PeerTaskValidationResponse.INVALID_CELLS_RETURNED);
  }

  @Test
  void rejectsAnAnswerThatDoesNotFitTheSidecar() {
    // One cell list for a transaction that carries one blob, but two blobs' worth of them.
    final List<CellsWithMask> tooMany =
        List.of(
            cellsOf(fullSidecar, REQUESTED).getFirst(), cellsOf(fullSidecar, REQUESTED).getFirst());

    assertThat(task.validateResult(tooMany))
        .isEqualTo(PeerTaskValidationResponse.INVALID_CELLS_RETURNED);
  }

  @Test
  void invalidCellsCostThePeerItsConnection() {
    assertThat(PeerTaskValidationResponse.INVALID_CELLS_RETURNED.getDisconnectReason()).isPresent();
    assertThat(PeerTaskValidationResponse.INVALID_CELLS_RETURNED.recordUselessResponse()).isTrue();
  }

  private static List<CellsWithMask> cellsOf(
      final BlobsWithCommitments sidecar, final CellMask mask) {
    return sidecar.getBlobProofBundles().stream()
        .map(bundle -> bundle.getCellsWithMask().orElseThrow())
        .map(held -> new CellsWithMask(mask.streamIndexes().mapToObj(held::getCell).toList(), mask))
        .toList();
  }

  private static CellMask range(final int fromInclusive, final int toExclusive) {
    final BitSet bits = new BitSet(CKZG4844Helper.CELLS_PER_EXT_BLOB);
    bits.set(fromInclusive, toExclusive);
    final byte[] bytes = new byte[CellMask.BYTE_LENGTH];
    final byte[] set = bits.toByteArray();
    System.arraycopy(set, 0, bytes, 0, set.length);
    return CellMask.fromBytes(Bytes.wrap(bytes));
  }
}
