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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlobTestFixture;
import org.hyperledger.besu.ethereum.core.CellsOnlyBlobTransactionFixture;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.kzg.BlobsWithCommitments;
import org.hyperledger.besu.ethereum.core.kzg.CKZG4844Helper;
import org.hyperledger.besu.ethereum.core.kzg.Cell;
import org.hyperledger.besu.ethereum.core.kzg.CellMask;
import org.hyperledger.besu.ethereum.core.kzg.CellsWithMask;
import org.hyperledger.besu.ethereum.eth.EthProtocolVersion;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.peertask.InvalidPeerTaskResponseException;
import org.hyperledger.besu.ethereum.eth.manager.peertask.MalformedRlpFromPeerException;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskValidationResponse;
import org.hyperledger.besu.ethereum.eth.messages.CellsMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.util.TrustedSetupClassLoaderExtension;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

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
  void rejectsCellContentThatDoesNotDecode() {
    // Well shaped and well sized, but bytes that are not field elements at all. The KZG boundary
    // answers false for these rather than throwing, so garbage content still reads as a peer
    // sending wrong cells, not as a local failure.
    final List<Cell> garbage =
        Collections.nCopies(
            REQUESTED.cardinality(), new Cell(Bytes.repeat((byte) 0xFF, Cell.SIZE)));

    assertThat(task.validateResult(List.of(new CellsWithMask(garbage, REQUESTED))))
        .isEqualTo(PeerTaskValidationResponse.INVALID_CELLS_RETURNED);
  }

  @Test
  void surfacesAResultThatCouldNotComeFromItsOwnDecoding() {
    // Two blobs' worth of groups for a one-blob transaction cannot leave processResponse, so it
    // is a local bug: it propagates to be seen as one, instead of blaming the peer for it.
    final List<CellsWithMask> tooMany =
        List.of(
            cellsOf(fullSidecar, REQUESTED).getFirst(), cellsOf(fullSidecar, REQUESTED).getFirst());

    assertThatThrownBy(() -> task.validateResult(tooMany))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void returnsNothingForAnAnswerNamingNoTransaction() throws Exception {
    final CellsMessage response = CellsMessage.create(List.of(), List.of(), CellMask.EMPTY);

    assertThat(task.processResponse(response, Set.of())).isEmpty();
  }

  @Test
  void treatsAnEmptyMaskAsAnEmptyAnswer() throws Exception {
    // A responder that truncated to nothing still names the hash. That reads as the allowed empty
    // answer, not as one cell-less result per blob for validateResult to accept as if cells came.
    final CellsMessage response =
        CellsMessage.create(List.of(requestedTx.getHash()), List.of(List.of()), CellMask.EMPTY);

    assertThat(task.processResponse(response, Set.of())).isEmpty();
  }

  @Test
  void rejectsAnAnswerForMoreTransactionsThanTheOneAsked() {
    final CellsMessage response =
        CellsMessage.create(
            List.of(requestedTx.getHash(), otherHash()),
            List.of(fullCells(), fullCells()),
            REQUESTED);

    assertThatThrownBy(() -> task.processResponse(response, Set.of()))
        .isInstanceOf(InvalidPeerTaskResponseException.class);
  }

  @Test
  void rejectsAnAnswerForAnotherTransaction() {
    final CellsMessage response =
        CellsMessage.create(List.of(otherHash()), List.of(fullCells()), REQUESTED);

    assertThatThrownBy(() -> task.processResponse(response, Set.of()))
        .isInstanceOf(InvalidPeerTaskResponseException.class);
  }

  @Test
  void rejectsAMaskBeyondTheRequestedOne() {
    final GetCellsFromPeerTask halfTask = new GetCellsFromPeerTask(requestedTx, range(0, 64));
    final CellsMessage response =
        CellsMessage.create(List.of(requestedTx.getHash()), List.of(fullCells()), REQUESTED);

    assertThatThrownBy(() -> halfTask.processResponse(response, Set.of()))
        .isInstanceOf(InvalidPeerTaskResponseException.class);
  }

  @Test
  void rejectsACellCountThatDoesNotMatchTheBlobCount() {
    // Two blobs' worth of cells for a one-blob transaction. A count that is not a multiple of the
    // mask's cardinality never gets this far: the message decoding already rejects it.
    final List<Cell> doubled = new ArrayList<>(fullCells());
    doubled.addAll(fullCells());
    final CellsMessage response =
        CellsMessage.create(List.of(requestedTx.getHash()), List.of(doubled), REQUESTED);

    assertThatThrownBy(() -> task.processResponse(response, Set.of()))
        .isInstanceOf(InvalidPeerTaskResponseException.class);
  }

  @Test
  void rejectsACellListCountThatDoesNotMatchTheHashes() {
    // One hash with two cell groups cannot be built through CellsMessage.create, so craft the
    // body by hand; such a message is malformed at decode, before the task's own checks see it.
    final BytesValueRLPOutput hashesOut = new BytesValueRLPOutput();
    hashesOut.writeList(List.of(requestedTx.getHash()), (h, w) -> w.writeBytes(h.getBytes()));
    final BytesValueRLPOutput cellsOut = new BytesValueRLPOutput();
    cellsOut.writeList(
        List.of(fullCells(), fullCells()), (cells, w) -> w.writeList(cells, Cell::writeTo));
    final BytesValueRLPOutput maskOut = new BytesValueRLPOutput();
    maskOut.writeBytes(REQUESTED.toBytes());
    final CellsMessage response =
        CellsMessage.createUnsafe(
            Bytes.concatenate(hashesOut.encoded(), cellsOut.encoded(), maskOut.encoded()));

    assertThatThrownBy(() -> task.processResponse(response, Set.of()))
        .isInstanceOf(MalformedRlpFromPeerException.class);
  }

  @Test
  void splitsAMultiBlobAnswerIntoOneGroupPerBlob() throws Exception {
    // A group arrives blob major: for each blob in transaction order, its cells by ascending
    // index. The split must hand each blob its own contiguous run, under the answered mask.
    final BlobsWithCommitments twoBlobs =
        CKZG4844Helper.convertToVersion1(blobTestFixture.createBlobsWithCommitments(2));
    final Transaction twoBlobTx =
        new CellsOnlyBlobTransactionFixture()
            .create(
                BlobsWithCommitments.createFromBlobCells(
                    twoBlobs.getKzgCommitments(),
                    List.of(CellsWithMask.EMPTY, CellsWithMask.EMPTY),
                    twoBlobs.getKzgProofs(),
                    twoBlobs.getVersionedHashes()));
    final GetCellsFromPeerTask twoBlobTask = new GetCellsFromPeerTask(twoBlobTx, REQUESTED);

    final CellMask served = range(0, 64);
    final List<CellsWithMask> perBlob = cellsOf(twoBlobs, served);
    final List<Cell> blobMajor = new ArrayList<>();
    perBlob.forEach(group -> blobMajor.addAll(group.getCells()));
    final CellsMessage response =
        CellsMessage.create(List.of(twoBlobTx.getHash()), List.of(blobMajor), served);

    final List<CellsWithMask> result = twoBlobTask.processResponse(response, Set.of());

    assertThat(result).hasSize(2);
    for (int blob = 0; blob < 2; blob++) {
      assertThat(result.get(blob).getCellMask()).isEqualTo(served);
      assertThat(result.get(blob).getCells()).isEqualTo(perBlob.get(blob).getCells());
    }
    // and the split agrees with the sidecar itself, not merely with how this test built the list
    assertThat(twoBlobTask.validateResult(result))
        .isEqualTo(PeerTaskValidationResponse.RESULTS_VALID_AND_GOOD);
  }

  @Test
  void invalidCellsCostThePeerItsConnection() {
    assertThat(PeerTaskValidationResponse.INVALID_CELLS_RETURNED.getDisconnectReason()).isPresent();
    assertThat(PeerTaskValidationResponse.INVALID_CELLS_RETURNED.recordUselessResponse()).isTrue();
  }

  @Test
  void isOnlyWillingToAskAnEth72Peer() {
    // GetCells has no message code before eth/72, so asking a peer that negotiated anything
    // earlier for cells is an invalid message code for the version agreed with it: the peer
    // disconnects instead of answering.
    assertThat(task.getPeerRequirementFilter().test(peerSpeaking(EthProtocolVersion.V72))).isTrue();
    assertThat(task.getPeerRequirementFilter().test(peerSpeaking(EthProtocolVersion.V71)))
        .isFalse();
    assertThat(task.getPeerRequirementFilter().test(peerSpeaking(EthProtocolVersion.V68)))
        .isFalse();
  }

  private static EthPeerImmutableAttributes peerSpeaking(final EthProtocolVersion version) {
    final EthPeer peer = Mockito.mock(EthPeer.class);
    Mockito.when(peer.getAgreedCapabilities())
        .thenReturn(Set.<Capability>of(version.getCapability()));
    final EthPeerImmutableAttributes attributes = Mockito.mock(EthPeerImmutableAttributes.class);
    Mockito.when(attributes.ethPeer()).thenReturn(peer);
    return attributes;
  }

  /** A full blob's worth of real cells, for answers whose shape, not content, is under test. */
  private List<Cell> fullCells() {
    return cellsOf(fullSidecar, REQUESTED).getFirst().getCells();
  }

  private static Hash otherHash() {
    return Hash.hash(Bytes.of(0x01));
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
