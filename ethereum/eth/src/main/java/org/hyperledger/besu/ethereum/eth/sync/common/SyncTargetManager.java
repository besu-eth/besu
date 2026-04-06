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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hyperledger.besu.util.log.LogUtil.throttledLog;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask;
import org.hyperledger.besu.ethereum.eth.sync.AbstractSyncTargetManager;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncTargetManager extends AbstractSyncTargetManager {
  private static final Logger LOG = LoggerFactory.getLogger(SyncTargetManager.class);

  private static final int LOG_DEBUG_REPEAT_DELAY = 15;
  private static final int LOG_INFO_REPEAT_DELAY = 120;

  private final SynchronizerConfiguration config;
  private final WorldStateStorageCoordinator worldStateStorageCoordinator;
  private final ProtocolSchedule protocolSchedule;
  private final ProtocolContext protocolContext;
  private final EthContext ethContext;
  private final PivotSyncState fastSyncState;
  private final AtomicBoolean logDebug = new AtomicBoolean(true);
  private final AtomicBoolean logInfo = new AtomicBoolean(true);

  public SyncTargetManager(
      final SynchronizerConfiguration config,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final EthContext ethContext,
      final MetricsSystem metricsSystem,
      final PivotSyncState fastSyncState) {
    super(config, protocolSchedule, protocolContext, ethContext, metricsSystem);
    this.config = config;
    this.worldStateStorageCoordinator = worldStateStorageCoordinator;
    this.protocolSchedule = protocolSchedule;
    this.protocolContext = protocolContext;
    this.ethContext = ethContext;
    this.fastSyncState = fastSyncState;
  }

  @Override
  protected CompletableFuture<Optional<EthPeer>> selectBestAvailableSyncTarget() {
    final BlockHeader pivotBlockHeader = fastSyncState.getPivotBlockHeader().get();
    final EthPeers ethPeers = ethContext.getEthPeers();
    final Optional<EthPeer> maybeBestPeer = ethPeers.bestPeerWithHeightEstimate();
    if (maybeBestPeer.isEmpty()) {
      throttledLog(
          LOG::debug,
          String.format(
              "Unable to find sync target. Waiting for %d peers minimum. Currently checking %d peers for usefulness. Pivot block: %d",
              config.getSyncMinimumPeerCount(),
              ethContext.getEthPeers().peerCount(),
              pivotBlockHeader.getNumber()),
          logDebug,
          LOG_DEBUG_REPEAT_DELAY);
      throttledLog(
          LOG::info,
          String.format(
              "Unable to find sync target. Waiting for %d peers minimum. Currently checking %d peers for usefulness.",
              config.getSyncMinimumPeerCount(), ethContext.getEthPeers().peerCount()),
          logInfo,
          LOG_INFO_REPEAT_DELAY);
      return completedFuture(Optional.empty());
    } else {
      final EthPeer bestPeer = maybeBestPeer.get();
      // Do not check the best peers estimated height if we are doing PoS
      if (!protocolSchedule.getByBlockHeader(pivotBlockHeader).isPoS()
          && bestPeer.chainState().getEstimatedHeight() < pivotBlockHeader.getNumber()) {
        LOG.info(
            "Best peer {} has chain height {} below pivotBlock height {}. Waiting for better peers. Current {} of max {}",
            maybeBestPeer.map(EthPeer::getLoggableId).orElse("none"),
            maybeBestPeer.map(p -> p.chainState().getEstimatedHeight()).orElse(-1L),
            pivotBlockHeader.getNumber(),
            ethPeers.peerCount(),
            ethPeers.getMaxPeers());
        ethPeers.disconnectWorstUselessPeer();
        return completedFuture(Optional.empty());
      } else {
        return confirmPivotBlockHeader(bestPeer);
      }
    }
  }

  private CompletableFuture<Optional<EthPeer>> confirmPivotBlockHeader(final EthPeer bestPeer) {
    final BlockHeader pivotBlockHeader = fastSyncState.getPivotBlockHeader().get();
    return ethContext
        .getScheduler()
        .scheduleServiceTask(
            () -> {
              GetHeadersFromPeerTask task =
                  new GetHeadersFromPeerTask(
                      pivotBlockHeader.getNumber(),
                      1,
                      0,
                      GetHeadersFromPeerTask.Direction.FORWARD,
                      PivotBlockRetriever.MAX_QUERY_RETRIES_PER_PEER,
                      protocolSchedule);
              PeerTaskExecutorResult<List<BlockHeader>> taskResult =
                  ethContext.getPeerTaskExecutor().executeAgainstPeer(task, bestPeer);
              if (taskResult.responseCode() != PeerTaskExecutorResponseCode.SUCCESS
                  || taskResult.result().isEmpty()) {
                return CompletableFuture.failedFuture(
                    new RuntimeException("Unable to retrieve requested header from peer"));
              }
              return CompletableFuture.completedFuture(taskResult.result().get());
            })
        .thenCompose(
            result -> {
              if (peerHasDifferentPivotBlock(result)) {
                if (!hasPivotChanged(pivotBlockHeader)) {
                  // if the pivot block has not changed, then warn and disconnect this peer
                  LOG.warn(
                      "Best peer has wrong pivot block (#{}) expecting {} but received {}.  Disconnect: {}",
                      pivotBlockHeader.getNumber(),
                      pivotBlockHeader.getHash(),
                      result.size() == 1 ? result.get(0).getHash() : "invalid response",
                      bestPeer);
                  bestPeer.disconnect(DisconnectReason.USELESS_PEER_MISMATCHED_PIVOT_BLOCK);
                  return CompletableFuture.completedFuture(Optional.<EthPeer>empty());
                }
                LOG.debug(
                    "Retrying best peer {} with new pivot block {}",
                    bestPeer.getLoggableId(),
                    pivotBlockHeader.toLogString());
                return confirmPivotBlockHeader(bestPeer);
              } else {
                return CompletableFuture.completedFuture(Optional.of(bestPeer));
              }
            })
        .exceptionally(
            error -> {
              LOG.atDebug()
                  .setMessage("Could not confirm best peer {} had pivot block {}, {}")
                  .addArgument(bestPeer.getLoggableId())
                  .addArgument(pivotBlockHeader.getNumber())
                  .addArgument(error)
                  .log();
              bestPeer.disconnect(DisconnectReason.USELESS_PEER_CANNOT_CONFIRM_PIVOT_BLOCK);
              return Optional.empty();
            });
  }

  private boolean hasPivotChanged(final BlockHeader requestedPivot) {
    return fastSyncState
        .getPivotBlockHash()
        .filter(currentPivotHash -> requestedPivot.getBlockHash().equals(currentPivotHash))
        .isEmpty();
  }

  private boolean peerHasDifferentPivotBlock(final List<BlockHeader> result) {
    final BlockHeader pivotBlockHeader = fastSyncState.getPivotBlockHeader().get();
    return result.size() != 1 || !result.get(0).equals(pivotBlockHeader);
  }

  @Override
  public boolean shouldContinueDownloading() {
    final BlockHeader pivotBlockHeader = fastSyncState.getPivotBlockHeader().get();
    boolean isValidChainHead =
        protocolContext.getBlockchain().getChainHeadHash().equals(pivotBlockHeader.getHash());
    if (!isValidChainHead) {
      if (protocolContext.getBlockchain().contains(pivotBlockHeader.getHash())) {
        protocolContext.getBlockchain().rewindToBlock(pivotBlockHeader.getHash());
      } else {
        return true;
      }
    }
    return !worldStateStorageCoordinator.isWorldStateAvailable(
        Bytes32.wrap(pivotBlockHeader.getStateRoot().getBytes()), pivotBlockHeader.getBlockHash());
  }
}
