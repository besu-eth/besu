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

import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.time.Clock;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Selects the pivot block for snap sync using the FCU safe and head header.
 *
 * <p>The pivot is reused across calls until the head has advanced at least {@code
 * pivotBlockWindowValidity} blocks past it, ensuring the pivot stays within the 128-block
 * snap-serving window. The effective threshold shrinks by one per estimated missed slot since the
 * last FCU; when it reaches zero the method fails so the caller knows the consensus client appears
 * offline.
 */
public class PivotSelectorFromSafeBlock extends AbstractForkchoicePivotSelector {

  private static final Logger LOG = LoggerFactory.getLogger(PivotSelectorFromSafeBlock.class);

  /**
   * Number of blocks behind the FCU head to anchor the pivot if no safe block is available. Chosen
   * to match the typical distance of the safe block (≈ 2 epochs = 64 slots) to provide reorg
   * protection.
   */
  private static final int PIVOT_DISTANCE = 64;

  public PivotSelectorFromSafeBlock(
      final ProtocolContext protocolContext,
      final GenesisConfigOptions genesisConfig,
      final SingleBlockHeaderDownloader headerDownloader,
      final ProtocolSchedule protocolSchedule,
      final Clock clock,
      final int pivotBlockWindowValidity,
      final Runnable cleanupAction) {
    super(
        protocolContext,
        genesisConfig,
        headerDownloader,
        protocolSchedule,
        clock,
        pivotBlockWindowValidity,
        cleanupAction);
  }

  @Override
  public CompletableFuture<SnapSyncProcessState> selectNewPivotBlock() {
    final Hash headHash = getLatestHeadHash();
    if (Hash.ZERO.equals(headHash)) {
      return logAndFailNoFcu();
    }

    final long sinceLastFcu = millisSinceLastFcu();

    return getOrDownload(headHash)
        .thenCompose(
            head -> {
              LOG.debug("Head block {} is at {}", head.getNumber(), head.getHash());
              final long effectiveThreshold = remainingOfflineWindowBlocks(head, sinceLastFcu);

              if (effectiveThreshold <= 0) {
                return CompletableFuture.failedFuture(
                    new RuntimeException(
                        "Consensus client appears offline: last FCU was "
                            + (sinceLastFcu / 1000)
                            + "s ago; pivot block would be outside the snap-serving window"));
              }

              if (hasLastPivot()) {
                final long distanceFromHead = head.getNumber() - getLastReturnedPivotNumber();
                if (distanceFromHead < effectiveThreshold) {
                  LOG.debug(
                      "Reusing existing pivot block {} — head has only advanced {} blocks (threshold {})",
                      getLastReturnedPivotNumber(),
                      distanceFromHead,
                      effectiveThreshold);
                  return CompletableFuture.completedFuture(lastPivotState());
                }
              }

              final BlockHeader cachedSafe = getCachedHeader(getLatestSafeHash());
              if (cachedSafe != null
                  && head.getNumber() - cachedSafe.getNumber() < effectiveThreshold) {
                LOG.debug("Using safe block {} as pivot", cachedSafe.getNumber());
                return CompletableFuture.completedFuture(new SnapSyncProcessState(cachedSafe));
              }

              final int blocksToWalk = (int) Math.min(PIVOT_DISTANCE, head.getNumber());
              LOG.debug(
                  "Walking back {} blocks from head {} for pivot", blocksToWalk, head.getNumber());
              return walkBackParents(head, blocksToWalk).thenApply(SnapSyncProcessState::new);
            })
        .thenApply(this::recordLastPivot);
  }
}
