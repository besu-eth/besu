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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.kzg.BlobsWithCommitments;
import org.hyperledger.besu.ethereum.core.kzg.CKZG4844Helper;
import org.hyperledger.besu.ethereum.core.kzg.Cell;
import org.hyperledger.besu.ethereum.core.kzg.CellMask;
import org.hyperledger.besu.ethereum.core.kzg.CellsWithMask;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.peertask.InvalidPeerTaskResponseException;
import org.hyperledger.besu.ethereum.eth.manager.peertask.MalformedRlpFromPeerException;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskValidationResponse;
import org.hyperledger.besu.ethereum.eth.messages.CellsMessage;
import org.hyperledger.besu.ethereum.eth.messages.GetCellsMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.SubProtocol;
import org.hyperledger.besu.ethereum.rlp.RLPException;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetCellsFromPeerTask implements PeerTask<List<CellsWithMask>> {
  private static final Logger LOG = LoggerFactory.getLogger(GetCellsFromPeerTask.class);

  private final Transaction requestedTx;
  private final CellMask requestedCellMask;

  public GetCellsFromPeerTask(final Transaction requestedTx, final CellMask requestedCellMask) {
    this.requestedTx = requestedTx;
    this.requestedCellMask = requestedCellMask;
  }

  @Override
  public SubProtocol getSubProtocol() {
    return EthProtocol.get();
  }

  @Override
  public MessageData getRequestMessage(final Set<Capability> agreedCapabilities) {
    LOG.atTrace()
        .setMessage("Requesting cells for tx {} with mask {}")
        .addArgument(requestedTx::toTraceLog)
        .addArgument(requestedCellMask)
        .log();

    return GetCellsMessage.create(List.of(requestedTx), requestedCellMask);
  }

  @Override
  public List<CellsWithMask> processResponse(
      final MessageData messageData, final Set<Capability> agreedCapabilities)
      throws InvalidPeerTaskResponseException, MalformedRlpFromPeerException {
    final CellsMessage cellsMessage = CellsMessage.readFrom(messageData);
    final List<Hash> resTxHashes;
    final List<List<Cell>> resCellsList;
    final CellMask resCellMask;
    try {
      resTxHashes = cellsMessage.txHashes();
      resCellsList = cellsMessage.cellsList();
      resCellMask = cellsMessage.cellMask();
    } catch (RLPException e) {
      throw new MalformedRlpFromPeerException(e, messageData.getData());
    }

    if (resTxHashes.isEmpty()) {
      LOG.debug("Not received cells for tx hash {}", requestedTx.getHash());
      return List.of();
    }

    if (resTxHashes.size() > 1) {
      throw new InvalidPeerTaskResponseException(
          "Received results for %d txs, more than requested %d".formatted(resTxHashes.size(), 1));
    }

    if (!resTxHashes.contains(requestedTx.getHash())) {
      throw new InvalidPeerTaskResponseException(
          "Received cells for not requested tx hashes: " + resTxHashes);
    }

    if (!requestedCellMask.containsAll(resCellMask)) {
      throw new InvalidPeerTaskResponseException(
          "Received cell mask %s is not contained in requested cell mask %s"
              .formatted(resCellMask.toString(), requestedCellMask.toString()));
    }

    // The responder MAY truncate its response, so receiving fewer cells than requested is valid.
    if (!resCellMask.containsAll(requestedCellMask)) {
      LOG.atTrace()
          .setMessage("Received partial cells for tx {}, requested mask {}, received mask {}")
          .addArgument(requestedTx::toTraceLog)
          .addArgument(requestedCellMask)
          .addArgument(resCellMask)
          .log();
    }

    if (resCellsList.size() != 1) {
      throw new InvalidPeerTaskResponseException(
          "Received %d cell lists, expected 1 for the single requested tx"
              .formatted(resCellsList.size()));
    }

    // A transaction's group arrives blob major: for each blob in transaction order, its cells by
    // ascending index. So blob b owns the contiguous run [b * cellsPerBlob, (b+1) * cellsPerBlob).
    // Split it into one CellsWithMask per blob.
    final int txBlobCount = requestedTx.getBlobCount();
    final int cellsPerBlob = resCellMask.cardinality();
    final List<Cell> txCells = resCellsList.getFirst();

    if (txCells.size() != txBlobCount * cellsPerBlob) {
      throw new InvalidPeerTaskResponseException(
          "Received %d cells, expected %d (%d blobs x %d cells)"
              .formatted(txCells.size(), txBlobCount * cellsPerBlob, txBlobCount, cellsPerBlob));
    }

    final List<CellsWithMask> cellsPerBlobList = new ArrayList<>(txBlobCount);
    for (int blobIndex = 0; blobIndex < txBlobCount; blobIndex++) {
      cellsPerBlobList.add(
          new CellsWithMask(
              txCells.subList(blobIndex * cellsPerBlob, (blobIndex + 1) * cellsPerBlob),
              resCellMask));
    }

    LOG.atTrace()
        .setMessage("Retrieved cells for tx {} with mask {}")
        .addArgument(requestedTx::toTraceLog)
        .addArgument(resCellMask)
        .log();

    return cellsPerBlobList;
  }

  @Override
  public Predicate<EthPeerImmutableAttributes> getPeerRequirementFilter() {
    return _ -> true;
  }

  /**
   * Whether every cell answered with opens the commitment of the blob it belongs to.
   *
   * <p>The checks in {@link #processResponse} establish the shape of an answer — the transaction it
   * names, its mask being a subset of the one requested, the cell count agreeing with that mask.
   * What is left is whether the cells are the cells they claim to be, and this is the last point at
   * which the peer that sent them is still identifiable: once the answers of several peers have
   * been merged, a single bad cell condemns the whole transaction with nothing to say who supplied
   * it. It also keeps cells that open nothing out of the recovery that rebuilds the blobs, which
   * would otherwise produce a blob that is merely wrong.
   */
  @Override
  public PeerTaskValidationResponse validateResult(final List<CellsWithMask> result) {
    if (result.isEmpty()) {
      // answered nothing, which is allowed and leaves nothing to verify
      return PeerTaskValidationResponse.RESULTS_VALID_AND_GOOD;
    }

    final BlobsWithCommitments blobs = requestedTx.getBlobsWithCommitments().orElseThrow();
    final boolean valid;
    try {
      valid =
          CKZG4844Helper.verify4844Kzg(
              BlobsWithCommitments.createFromBlobCells(
                  blobs.getKzgCommitments(),
                  result,
                  blobs.getKzgProofs(),
                  blobs.getVersionedHashes()));
    } catch (final RuntimeException e) {
      // anything that does not fit the sidecar at all, which is no better than rubbish
      LOG.debug("Cells answered for tx {} do not fit its sidecar", requestedTx.getHash(), e);
      return PeerTaskValidationResponse.INVALID_CELLS_RETURNED;
    }

    return valid
        ? PeerTaskValidationResponse.RESULTS_VALID_AND_GOOD
        : PeerTaskValidationResponse.INVALID_CELLS_RETURNED;
  }

  @Override
  public int getRetriesWithOtherPeer() {
    return 0;
  }

  @Override
  public int getRetriesWithSamePeer() {
    return 0;
  }
}
