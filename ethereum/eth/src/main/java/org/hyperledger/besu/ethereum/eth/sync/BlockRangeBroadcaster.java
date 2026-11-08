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
package org.hyperledger.besu.ethereum.eth.sync;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthMessage;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.messages.BlockRangeUpdateMessage;
import org.hyperledger.besu.ethereum.eth.messages.EthProtocolMessages;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage;
import org.hyperledger.besu.ethereum.rlp.RLPException;

import java.time.Duration;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockRangeBroadcaster {
  private static final Logger LOG = LoggerFactory.getLogger(BlockRangeBroadcaster.class);

  // range update block interval
  static final int BLOCK_RANGE_UPDATE_INTERVAL = 120;

  private final EthContext ethContext;
  private final Blockchain blockchain;

  public BlockRangeBroadcaster(final EthContext ethContext, final Blockchain blockchain) {
    this.ethContext = ethContext;
    this.blockchain = blockchain;
    ethContext
        .getEthMessages()
        .subscribe(EthProtocolMessages.BLOCK_RANGE_UPDATE, this::handleBlockRangeUpdateMessage);
    ethContext
        .getScheduler()
        .scheduleFutureTaskWithFixedDelay(
            this::broadcastBlockRange,
            Duration.ofSeconds(BLOCK_RANGE_UPDATE_INTERVAL),
            Duration.ofSeconds(BLOCK_RANGE_UPDATE_INTERVAL));
  }

  /**
   * Handles the block range update message received from a peer.
   *
   * @param message the message received from the peer
   */
  @VisibleForTesting
  void handleBlockRangeUpdateMessage(final EthMessage message) {
    try {
      final BlockRangeUpdateMessage blockRangeUpdateMessage =
          BlockRangeUpdateMessage.readFrom(message.getData());
      final long earliestBlockNumber = blockRangeUpdateMessage.getEarliestBlockNumber();
      final long latestBlockNumber = blockRangeUpdateMessage.getLatestBlockNumber();
      final Hash blockHash = blockRangeUpdateMessage.getBlockHash();
      LOG.debug(
          "Received blockRange=[{}, {}, {}] from peer={}",
          earliestBlockNumber,
          latestBlockNumber,
          blockHash,
          message.getPeer().getLoggableId());
      if (isValid(blockRangeUpdateMessage)) {
        message.getPeer().registerKnownBlock(blockHash);
        message.getPeer().registerBlockRange(blockHash, latestBlockNumber, earliestBlockNumber);
      } else {
        LOG.trace(
            "Invalid block range update message received: earliest={}, latest={}",
            earliestBlockNumber,
            latestBlockNumber);
        handleInvalidMessage(
            message, DisconnectMessage.DisconnectReason.SUBPROTOCOL_TRIGGERED_INVALID_BLOCK_RANGE);
      }
    } catch (final RLPException e) {
      LOG.atTrace()
          .setMessage("Unable to parse BlockRangeUpdateMessage from peer {} {}")
          .addArgument(message.getPeer()::getLoggableId)
          .addArgument(e)
          .log();
      handleInvalidMessage(
          message,
          DisconnectMessage.DisconnectReason.BREACH_OF_PROTOCOL_MALFORMED_MESSAGE_RECEIVED);
    }
  }

  private void handleInvalidMessage(
      final EthMessage message, final DisconnectMessage.DisconnectReason reason) {
    message.getPeer().disconnect(reason);
  }

  private boolean isValid(final BlockRangeUpdateMessage message) {
    return message.getLatestBlockNumber() >= message.getEarliestBlockNumber();
  }

  /**
   * Broadcasts a block range update message to all connected peers that support the
   * BLOCK_RANGE_UPDATE protocol. This method is used to inform peers about the range of blocks
   * available, from the earliest to the latest. If the earliest block number is not available, or
   * above the chain head, the genesis block number is used as a fallback.
   */
  @VisibleForTesting
  protected void broadcastBlockRange() {
    final BlockHeader chainHeader = blockchain.getChainHeadHeader();
    blockchain
        .getEarliestBlockNumber(chainHeader.getNumber())
        .ifPresentOrElse(
            earliestBlockNumber ->
                broadcastBlockRange(
                    earliestBlockNumber, chainHeader.getNumber(), chainHeader.getHash()),
            () -> {
              BlockHeader genesisBlock = blockchain.getGenesisBlockHeader();
              broadcastBlockRange(
                  genesisBlock.getNumber(), genesisBlock.getNumber(), genesisBlock.getHash());
            });
  }

  /**
   * Broadcasts the block range update message to all peers that support it.
   *
   * @param earliestBlockNumber the earliest block number
   * @param latestBlockNumber the latest block number
   * @param blockHash the hash of the block
   */
  @VisibleForTesting
  void broadcastBlockRange(
      final long earliestBlockNumber, final long latestBlockNumber, final Hash blockHash) {
    List<EthPeer> peers = getPeersSupportingBlockRangeUpdate();
    LOG.debug(
        "Broadcasting blockRange=[{}, {}, {}] to {} peers",
        earliestBlockNumber,
        latestBlockNumber,
        blockHash,
        peers.size());
    if (peers.isEmpty()) {
      LOG.debug("No peers available that support BLOCK_RANGE_UPDATE message.");
      return;
    }
    final BlockRangeUpdateMessage blockRangeUpdateMessage =
        BlockRangeUpdateMessage.create(earliestBlockNumber, latestBlockNumber, blockHash);
    peers.forEach(peer -> broadcastToPeer(peer, blockRangeUpdateMessage));
  }

  /**
   * Gets the list of peers that support the block range update message.
   *
   * @return the list of peers that support the block range update message
   */
  private List<EthPeer> getPeersSupportingBlockRangeUpdate() {
    // Only peers with eth/69 support BLOCK_RANGE_UPDATE message
    return ethContext
        .getEthPeers()
        .streamAvailablePeers()
        .map(EthPeerImmutableAttributes::ethPeer)
        .filter(peer -> peer.hasSupportForMessage(EthProtocolMessages.BLOCK_RANGE_UPDATE))
        .toList();
  }

  /**
   * Broadcasts the block range update message to a specific peer.
   *
   * @param peer the peer to send the message to
   * @param message the block range update message
   */
  private void broadcastToPeer(final EthPeer peer, final BlockRangeUpdateMessage message) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Broadcasting blockRange=[{}, {}, {}] to peer={}",
          message.getEarliestBlockNumber(),
          message.getLatestBlockNumber(),
          message.getBlockHash(),
          peer.getLoggableId());
    }
    try {
      peer.send(message);
    } catch (PeerConnection.PeerNotConnected e) {
      LOG.trace("Failed to broadcast blockRange to peer {}", peer.getLoggableId(), e);
    }
  }
}
