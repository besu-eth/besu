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
package org.hyperledger.besu.ethereum.eth.sync.common;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.sync.PivotBlockSelector;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.TrailingPeerLimiter;
import org.hyperledger.besu.ethereum.eth.sync.TrailingPeerRequirements;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PivotSelectorFromPeers implements PivotBlockSelector {

  private static final Logger LOG = LoggerFactory.getLogger(PivotSelectorFromPeers.class);

  protected final EthContext ethContext;
  protected final SynchronizerConfiguration syncConfig;
  private final SyncState syncState;

  public PivotSelectorFromPeers(
      final EthContext ethContext,
      final SynchronizerConfiguration syncConfig,
      final SyncState syncState) {
    this.ethContext = ethContext;
    this.syncConfig = syncConfig;
    this.syncState = syncState;
  }

  @Override
  public CompletableFuture<PivotSyncState> selectNewPivotBlock() {
    return selectBestPeer()
        .map(this::fromPeer)
        .orElse(
            CompletableFuture.failedFuture(
                new RuntimeException("Not enough peers to select pivot block")));
  }

  @Override
  public CompletableFuture<Void> prepareRetry() {
    final long estimatedPivotBlock = conservativelyEstimatedPivotBlock();
    final TrailingPeerLimiter trailingPeerLimiter =
        new TrailingPeerLimiter(
            ethContext.getEthPeers(),
            () ->
                new TrailingPeerRequirements(
                    estimatedPivotBlock, syncConfig.getMaxTrailingPeers()));
    trailingPeerLimiter.enforceTrailingPeerLimit();

    return ethContext
        .getEthPeers()
        .waitForPeer((peer) -> peer.estimatedChainHeight() >= estimatedPivotBlock)
        .thenRun(() -> {});
  }

  @Override
  public long getBestChainHeight() {
    return syncState.bestChainHeight();
  }

  protected CompletableFuture<PivotSyncState> fromPeer(final EthPeer peer) {
    final long pivotBlockNumber =
        peer.chainState().getEstimatedHeight() - syncConfig.getSyncPivotDistance();
    if (pivotBlockNumber <= BlockHeader.GENESIS_BLOCK_NUMBER) {
      // Peer's chain isn't long enough, return an empty value, so we can try again.
      LOG.info("Waiting for peers with sufficient chain height");
      return CompletableFuture.failedFuture(
          new RuntimeException("No peers with sufficient height"));
    }
    LOG.info("Selecting block number {} as fast sync pivot block.", pivotBlockNumber);
    return CompletableFuture.completedFuture(new PivotSyncState(pivotBlockNumber, false));
  }

  protected Optional<EthPeer> selectBestPeer() {
    List<EthPeerImmutableAttributes> peers =
        ethContext
            .getEthPeers()
            .streamAvailablePeers()
            .filter((peer) -> peer.hasEstimatedChainHeight() && peer.isFullyValidated())
            .toList();

    // Only select a pivot block number when we have a minimum number of height estimates
    final int minPeerCount = syncConfig.getSyncMinimumPeerCount();
    if (peers.size() < minPeerCount) {
      LOG.info(
          "Waiting for valid peers with chain height information.  {} / {} required peers currently available.",
          peers.size(),
          minPeerCount);
      return Optional.empty();
    } else {
      return peers.stream()
          .max(ethContext.getEthPeers().getBestPeerComparator())
          .map(EthPeerImmutableAttributes::ethPeer);
    }
  }

  private long conservativelyEstimatedPivotBlock() {
    final long estimatedNextPivot =
        syncState.getLocalChainHeight() + syncConfig.getSyncPivotDistance();
    return Math.min(syncState.bestChainHeight(), estimatedNextPivot);
  }
}
