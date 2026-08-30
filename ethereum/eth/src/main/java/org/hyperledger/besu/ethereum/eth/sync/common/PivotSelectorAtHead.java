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
package org.hyperledger.besu.ethereum.eth.sync.common;

import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Selects the snap/2 pivot a small constant distance (default {@value #DEFAULT_BLOCKS_BEHIND_HEAD}
 * block) behind the chain head. The pivot is reused while the head stays less than {@code
 * pivotBlockWindowValidity} blocks ahead, and refreshes to head - {@code blocksBehindHead} once it
 * exits that window.
 *
 * <p>Normally the head is the FCU head; selection fails when no FCU has been received or when the
 * last FCU is older than {@code pivotBlockWindowValidity} slots (CL offline). But the FCU head can
 * lag the network arbitrarily far while the CL syncs, and such a pivot is unservable (peers serve
 * only ~128 recent state roots) — so when the FCU head is absent or lags the best fully-validated
 * peers by more than {@link #PEER_HEAD_LAG_THRESHOLD} blocks, the pivot is anchored at {@code
 * peerBestHeight - blocksBehindHead} instead (number-only; confirmed with {@code
 * syncMinimumPeerCount} peers downstream by {@code PivotBlockRetriever}). The pivot never moves
 * backwards.
 */
public class PivotSelectorAtHead extends AbstractForkchoicePivotSelector {

  private static final Logger LOG = LoggerFactory.getLogger(PivotSelectorAtHead.class);

  /** Default number of blocks behind the FCU head to anchor the pivot. */
  public static final long DEFAULT_BLOCKS_BEHIND_HEAD = 1;

  /**
   * FCU-head to best peer height lag, beyond which the peer-anchored branch is used; keeps an
   * FCU-anchored pivot well inside the ~128 state roots peers can serve.
   */
  static final long PEER_HEAD_LAG_THRESHOLD = 100;

  private final EthContext ethContext;
  private final int syncMinimumPeerCount;
  private final long blocksBehindHead;
  private volatile long lastInsufficientPeerInfoLog;

  public PivotSelectorAtHead(
      final ProtocolContext protocolContext,
      final GenesisConfigOptions genesisConfig,
      final SingleBlockHeaderDownloader headerDownloader,
      final ProtocolSchedule protocolSchedule,
      final EthContext ethContext,
      final int syncMinimumPeerCount,
      final Clock clock,
      final int pivotBlockWindowValidity,
      final Runnable cleanupAction) {
    this(
        protocolContext,
        genesisConfig,
        headerDownloader,
        protocolSchedule,
        ethContext,
        syncMinimumPeerCount,
        clock,
        pivotBlockWindowValidity,
        cleanupAction,
        DEFAULT_BLOCKS_BEHIND_HEAD);
  }

  /**
   * @param blocksBehindHead blocks behind the head to anchor the pivot; 0 selects the head itself
   */
  public PivotSelectorAtHead(
      final ProtocolContext protocolContext,
      final GenesisConfigOptions genesisConfig,
      final SingleBlockHeaderDownloader headerDownloader,
      final ProtocolSchedule protocolSchedule,
      final EthContext ethContext,
      final int syncMinimumPeerCount,
      final Clock clock,
      final int pivotBlockWindowValidity,
      final Runnable cleanupAction,
      final long blocksBehindHead) {
    super(
        protocolContext,
        genesisConfig,
        headerDownloader,
        protocolSchedule,
        clock,
        pivotBlockWindowValidity,
        cleanupAction);
    if (blocksBehindHead < 0 || blocksBehindHead > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          "blocksBehindHead must be in [0, " + Integer.MAX_VALUE + "]: " + blocksBehindHead);
    }
    this.ethContext = ethContext;
    this.syncMinimumPeerCount = syncMinimumPeerCount;
    this.blocksBehindHead = blocksBehindHead;
    this.lastInsufficientPeerInfoLog = clock.millis();
  }

  @Override
  public CompletableFuture<SnapSyncProcessState> selectNewPivotBlock() {
    final Hash headHash = getLatestHeadHash();
    final OptionalLong peerBest = peerBestHeight();

    if (Hash.ZERO.equals(headHash)) {
      // No FCU yet: anchor at the peer head if possible, so sync progresses while the CL syncs.
      if (peerBest.isPresent()) {
        LOG.debug(
            "No forkchoice update received yet; selecting pivot from peer best height {}",
            peerBest.getAsLong());
        return peerAnchoredPivot(peerBest.getAsLong()).thenApply(this::recordLastPivot);
      }
      return logAndFailNoFcu();
    }

    final long sinceLastFcu = millisSinceLastFcu();

    return getOrDownload(headHash)
        .thenCompose(
            head -> {
              LOG.debug("Head block {} is at {}", head.getNumber(), head.getHash());
              return selectPivotFromFcuHead(head, sinceLastFcu, peerBest);
            })
        .thenApply(this::recordLastPivot);
  }

  @Override
  public long getBestChainHeight() {
    return Math.max(super.getBestChainHeight(), peerBestHeight().orElse(0L));
  }

  private CompletableFuture<SnapSyncProcessState> selectPivotFromFcuHead(
      final BlockHeader head, final long sinceLastFcu, final OptionalLong peerBest) {
    // FCU head lags the network beyond the snap-serving window; an FCU-anchored pivot
    // would be unservable.
    if (isFcuHeadLaggingPeers(head, peerBest)) {
      LOG.info(
          "FCU head {} lags peer best height {} by more than {} blocks; selecting pivot"
              + " from peer height",
          head.getNumber(),
          peerBest.getAsLong(),
          PEER_HEAD_LAG_THRESHOLD);
      return peerAnchoredPivot(peerBest.getAsLong());
    }

    if (consensusClientAppearsOffline(head, sinceLastFcu)) {
      return CompletableFuture.failedFuture(
          new RuntimeException(
              "Consensus client appears offline: last FCU was "
                  + (sinceLastFcu / 1000)
                  + "s ago; pivot block would be outside the snap-serving window"));
    }

    final long pivotLag = head.getNumber() - getLastReturnedPivotNumber();
    if (hasLastPivot() && pivotLag < pivotBlockWindowValidity) {
      LOG.debug(
          "Reusing existing pivot block {} — head has only advanced {} blocks",
          getLastReturnedPivotNumber(),
          pivotLag);
      return CompletableFuture.completedFuture(lastPivotState());
    }

    return fcuAnchoredPivot(head);
  }

  private boolean isFcuHeadLaggingPeers(final BlockHeader head, final OptionalLong peerBest) {
    return peerBest.isPresent()
        && peerBest.getAsLong() - head.getNumber() > PEER_HEAD_LAG_THRESHOLD;
  }

  private boolean consensusClientAppearsOffline(final BlockHeader head, final long sinceLastFcu) {
    return remainingOfflineWindowBlocks(head, sinceLastFcu) <= 0;
  }

  /**
   * Number-only pivot {@code blocksBehindHead} behind the best peer height; the header is confirmed
   * with {@code syncMinimumPeerCount} peers downstream ({@code PivotBlockRetriever}). Never moves
   * backwards.
   */
  private CompletableFuture<SnapSyncProcessState> peerAnchoredPivot(final long peerBestHeight) {
    if (hasLastPivot()
        && peerBestHeight - getLastReturnedPivotNumber() < pivotBlockWindowValidity) {
      LOG.debug("Reusing existing peer-anchored pivot {}", getLastReturnedPivotNumber());
      return CompletableFuture.completedFuture(lastPivotState());
    }
    final long pivotNumber = peerBestHeight - blocksBehindHead;
    if (pivotNumber <= BlockHeader.GENESIS_BLOCK_NUMBER) {
      final long now = clock.millis();
      if (lastInsufficientPeerInfoLog + Duration.ofMinutes(1).toMillis() < now) {
        lastInsufficientPeerInfoLog = now;
        LOG.info("Waiting for peers with sufficient chain height");
      }
      return CompletableFuture.failedFuture(
          new RuntimeException("No peers with sufficient height"));
    }
    final long effectivePivotNumber = Math.max(pivotNumber, getLastReturnedPivotNumber());
    LOG.info(
        "Selecting block number {} as snap/2 pivot block ({} blocks behind peer best height {})",
        effectivePivotNumber,
        blocksBehindHead,
        peerBestHeight);
    return CompletableFuture.completedFuture(new SnapSyncProcessState(effectivePivotNumber));
  }

  private CompletableFuture<SnapSyncProcessState> fcuAnchoredPivot(final BlockHeader head) {
    final long pivotNumber = head.getNumber() - blocksBehindHead;
    if (pivotNumber <= BlockHeader.GENESIS_BLOCK_NUMBER) {
      LOG.debug("Using head block {} as pivot", head.getNumber());
      return CompletableFuture.completedFuture(new SnapSyncProcessState(head));
    }
    LOG.debug("Walking back {} blocks from head {} for pivot", blocksBehindHead, head.getNumber());
    return walkBackParents(head, (int) blocksBehindHead)
        .thenApply(
            pivotHeader -> {
              if (hasLastPivot() && pivotHeader.getNumber() < getLastReturnedPivotNumber()) {
                LOG.debug(
                    "FCU-anchored pivot {} is behind last returned pivot {}; reusing last pivot",
                    pivotHeader.getNumber(),
                    getLastReturnedPivotNumber());
                return lastPivotState();
              }
              return new SnapSyncProcessState(pivotHeader);
            });
  }

  /**
   * Best height advertised by fully-validated peers, or empty if fewer than {@code
   * syncMinimumPeerCount} qualify.
   */
  private OptionalLong peerBestHeight() {
    final List<EthPeerImmutableAttributes> peers =
        ethContext
            .getEthPeers()
            .streamAvailablePeers()
            .filter(peer -> peer.hasEstimatedChainHeight() && peer.isFullyValidated())
            .toList();
    if (peers.size() < syncMinimumPeerCount) {
      return OptionalLong.empty();
    }
    return peers.stream().mapToLong(EthPeerImmutableAttributes::estimatedChainHeight).max();
  }
}
