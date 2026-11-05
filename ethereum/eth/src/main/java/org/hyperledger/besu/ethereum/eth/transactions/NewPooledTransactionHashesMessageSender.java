/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.eth.transactions;

import static org.hyperledger.besu.ethereum.core.Transaction.toHashList;

import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.kzg.CellMask;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.messages.NewPooledTransactionHashesMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection.PeerNotConnected;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NewPooledTransactionHashesMessageSender {
  private static final Logger LOG =
      LoggerFactory.getLogger(NewPooledTransactionHashesMessageSender.class);
  private final PeerTransactionTracker transactionTracker;
  private final int maxAnnouncementsPerMessage;

  public NewPooledTransactionHashesMessageSender(
      final PeerTransactionTracker transactionTracker, final int maxAnnouncementsPerMessage) {
    this.transactionTracker = transactionTracker;
    this.maxAnnouncementsPerMessage = maxAnnouncementsPerMessage;
  }

  public void sendTransactionAnnouncementsToPeer(final EthPeer peer) {
    final Capability capability = peer.getConnection().capability(EthProtocol.NAME);
    final EncodingContext encodingContext = EthProtocol.pooledTransactionContext(capability);

    // An eth/72 announcement carries one cell mask covering every blob tx in the message, so a
    // message may only hold blob txs that agree on it.
    final boolean oneBatchPerCellMask = EthProtocol.isEth72Compatible(capability);
    // Keyed by the mask the batch will announce, empty for the batch of txs that carry none.
    // Insertion ordered, so batches are sent in the order their first tx was queued.
    final Map<Optional<CellMask>, List<Transaction>> openBatches = new LinkedHashMap<>();

    Transaction announcementToSend;
    while ((announcementToSend = transactionTracker.claimAnnouncementToSendToPeer(peer)) != null) {
      if (transactionTracker.hasPeerSeenTransactionOrAnnouncement(
          peer, announcementToSend.getHash())) {
        continue;
      }

      if (!encodingContext.canEncode(announcementToSend)) {
        continue;
      }

      final Optional<CellMask> batchKey =
          oneBatchPerCellMask
              ? TransactionAnnouncement.announcedCellMask(announcementToSend)
              : Optional.empty();
      final List<Transaction> batch = openBatches.computeIfAbsent(batchKey, _ -> new ArrayList<>());
      batch.add(announcementToSend);

      if (batch.size() == maxAnnouncementsPerMessage) {
        openBatches.remove(batchKey);
        // send the full batch, and stop if the peer is no longer connected
        if (!send(peer, batch, capability)) {
          return;
        }
      }
    }

    // send the partial batches left open
    for (final List<Transaction> batch : openBatches.values()) {
      if (!send(peer, batch, capability)) {
        return;
      }
    }
  }

  private boolean send(
      final EthPeer peer, final List<Transaction> txBatch, final Capability capability) {
    transactionTracker.markAnnouncementsAsSeenByTransaction(peer, txBatch);

    try {
      LOG.atTrace()
          .setMessage("Sending transaction hashes to peer={}, hashes={}")
          .addArgument(peer)
          .addArgument(() -> toHashList(txBatch))
          .log();

      final var message = NewPooledTransactionHashesMessage.create(txBatch, capability);

      peer.send(message);
    } catch (final PeerNotConnected unused) {
      LOG.atTrace()
          .setMessage(
              "Peer no more connected while sending transaction hashes: peer={}, message hashes={}")
          .addArgument(peer)
          .addArgument(() -> toHashList(txBatch))
          .log();
      return false;
    }
    return true;
  }
}
