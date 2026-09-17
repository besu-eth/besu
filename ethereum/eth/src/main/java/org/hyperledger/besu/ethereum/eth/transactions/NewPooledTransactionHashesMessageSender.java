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
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.messages.NewPooledTransactionHashesMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection.PeerNotConnected;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NewPooledTransactionHashesMessageSender {
  private static final Logger LOG =
      LoggerFactory.getLogger(NewPooledTransactionHashesMessageSender.class);
  private static final int MAX_TRANSACTIONS_HASHES = 4096;

  private final PeerTransactionTracker transactionTracker;

  public NewPooledTransactionHashesMessageSender(final PeerTransactionTracker transactionTracker) {
    this.transactionTracker = transactionTracker;
  }

  public void sendTransactionAnnouncementsToPeer(final EthPeer peer) {
    final Capability capability = peer.getConnection().capability(EthProtocol.NAME);
    final List<Transaction> txBatch = new ArrayList<>(MAX_TRANSACTIONS_HASHES);
    Transaction announcementToSend;
    while ((announcementToSend = transactionTracker.claimAnnouncementToSendToPeer(peer)) != null) {
      if (!transactionTracker.hasPeerSeenTransactionOrAnnouncement(
          peer, announcementToSend.getHash())) {
        txBatch.add(announcementToSend);
      }

      if (txBatch.size() == MAX_TRANSACTIONS_HASHES) {
        // send current batch and exit loop if peer no more connected
        final boolean connectionLost = !send(peer, txBatch, capability);
        txBatch.clear();
        if (connectionLost) break;
      }
    }

    // send the last partial batch
    if (!txBatch.isEmpty()) {
      send(peer, txBatch, capability);
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
