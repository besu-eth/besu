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
package org.hyperledger.besu.consensus.qbft;

import org.hyperledger.besu.consensus.common.bft.BftContext;
import org.hyperledger.besu.consensus.common.validator.ValidatorProvider;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.common.NoSyncRequiredException;
import org.hyperledger.besu.ethereum.eth.sync.common.PivotSelectorFromPeers;
import org.hyperledger.besu.ethereum.eth.sync.common.PivotSyncState;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a BFT-specific implementatino of the pivot-block selection used for snap-sync. It
 * makes some pragmatic decisions about cases specific to permissioned chains, e.g. when there is a
 * single validator node at the start of a chain, or a brand new chain has all nodes at block 0. For
 * other cases the behaviour is the same as public-chain pivot selection, namely that the best peer
 * is asked for its candidate pivot block.
 */
public class BFTPivotSelectorFromPeers extends PivotSelectorFromPeers {

  private static final Logger LOG = LoggerFactory.getLogger(BFTPivotSelectorFromPeers.class);

  private final ProtocolContext protocolContext;
  private final BlockHeader blockHeader;
  private final NodeKey nodeKey;

  /**
   * Create a BFT-specific pivot selector
   *
   * @param ethContext the eth context
   * @param syncConfig the sync config
   * @param syncState the sync state
   * @param protocolContext the protocol context
   * @param nodeKey the node key
   * @param blockHeader the block header
   */
  public BFTPivotSelectorFromPeers(
      final EthContext ethContext,
      final SynchronizerConfiguration syncConfig,
      final SyncState syncState,
      final ProtocolContext protocolContext,
      final NodeKey nodeKey,
      final BlockHeader blockHeader) {
    super(ethContext, syncConfig, syncState);
    this.protocolContext = protocolContext;
    this.blockHeader = blockHeader;
    this.nodeKey = nodeKey;
    LOG.info("Creating pivot block selector for BFT node");
  }

  /**
   * Determine if our node is a BFT validator node
   *
   * @param validatorProvider the validator provider
   * @return true if we are a validator
   */
  protected boolean weAreAValidator(final ValidatorProvider validatorProvider) {
    return validatorProvider.nodeIsValidator(nodeKey);
  }

  @Override
  public CompletableFuture<PivotSyncState> selectNewPivotBlock() {

    final BftContext bftContext = protocolContext.getConsensusContext(BftContext.class);
    final ValidatorProvider validatorProvider = bftContext.getValidatorProvider();
    // See if we have a best peer
    Optional<EthPeer> bestPeer = selectBestPeer();

    if (bestPeer.isPresent()) {
      // For a recently created permissioned chain we can skip snap sync until we're past the
      // pivot distance
      if (bestPeer.get().chainState().getEstimatedHeight() <= syncConfig.getSyncPivotDistance()) {
        LOG.info(
            "Best peer for sync found but chain height hasn't reached minimum sync pivot distance {}, exiting sync process",
            syncConfig.getSyncPivotDistance());
        throw new NoSyncRequiredException();
      }

      return bestPeer
          .map(this::fromPeer)
          .orElse(
              CompletableFuture.failedFuture(
                  new RuntimeException("No peers with sufficient height")));
    } else {
      // Treat us being the only validator as a special case. We are the only node that can produce
      // blocks so we won't wait to sync with a non-validator node that may or may not exist
      if (weAreAValidator(validatorProvider)
          && validatorProvider.getValidatorsAtHead().size() == 1) {
        LOG.info("This node is the only BFT validator, exiting sync process");
        throw new NoSyncRequiredException();
      }

      // Treat the case where we are at block 0 and don't yet have any validator peers with a chain
      // height estimate as potentially a new QBFT chain. Check if any of the other peers have the
      // same block hash as our genesis block.
      if (blockHeader.getNumber() == 0) {
        final AtomicInteger peerValidatorCount = new AtomicInteger();
        final AtomicBoolean peerAtOurGenesisBlock = new AtomicBoolean();
        ethContext
            .getEthPeers()
            .streamAllPeers()
            .map(EthPeerImmutableAttributes::ethPeer)
            .forEach(
                peer -> {
                  // If we are at block 0 and our block hash matches at least one of our peers we
                  // assume we're all at block 0 and therefore won't try to snap sync.
                  if (peer.chainState()
                      .getBestBlock()
                      .getHash()
                      .equals(blockHeader.getBlockHash())) {
                    peerAtOurGenesisBlock.set(true);
                  }
                  if (!peer.getConnection().isDisconnected()
                      && validatorProvider
                          .getValidatorsAtHead()
                          .contains(peer.getConnection().getPeerInfo().getAddress())) {
                    peerValidatorCount.getAndIncrement();
                  }
                });

        if (weAreAValidator(validatorProvider)
            && peerValidatorCount.get() >= syncConfig.getSyncMinimumPeerCount()
            && peerAtOurGenesisBlock.get()) {
          // We have sync-min-peers x validators connected, all of whom have no head estimate. We'll
          // assume this is a new chain and skip waiting for any more peers to sync with. The worst
          // case is this puts us into full sync mode.
          LOG.info(
              "Peered with {} validators but no best peer found to sync from and their current block hash matches our genesis block. Assuming new BFT chain, exiting snap-sync",
              peerValidatorCount.get());
          throw new NoSyncRequiredException();
        }
      }
    }

    return CompletableFuture.failedFuture(
        new RuntimeException("Not enough peers to select pivot block"));
  }
}
