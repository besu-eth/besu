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
package org.hyperledger.besu.ethereum.eth.sync.fullsync;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.sync.ChainDownloader;
import org.hyperledger.besu.ethereum.eth.sync.SyncMode;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.TrailingPeerRequirements;
import org.hyperledger.besu.ethereum.eth.sync.fullsync.era1prepipeline.Era1ImportPrepipelineFactory;
import org.hyperledger.besu.ethereum.eth.sync.fullsync.era1prepipeline.FileImportChainDownloader;
import org.hyperledger.besu.ethereum.eth.sync.fullsync.ereprepipeline.EreImportPrepipelineFactory;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FullSyncDownloader {

  private static final Logger LOG = LoggerFactory.getLogger(FullSyncDownloader.class);
  private final ChainDownloader chainDownloader;
  private final Optional<ChainDownloader> fileImportPrepipelineChainDownloader;
  private final SynchronizerConfiguration syncConfig;
  private final ProtocolContext protocolContext;
  private final SyncState syncState;
  private final SyncTerminationCondition terminationCondition;

  public FullSyncDownloader(
      final SynchronizerConfiguration syncConfig,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final EthContext ethContext,
      final SyncState syncState,
      final MetricsSystem metricsSystem,
      final SyncTerminationCondition terminationCondition,
      final PeerTaskExecutor peerTaskExecutor,
      final SyncDurationMetrics syncDurationMetrics) {
    this.syncConfig = syncConfig;
    this.protocolContext = protocolContext;
    this.syncState = syncState;
    this.terminationCondition = terminationCondition;

    if (syncConfig.ereImportPrepipelineEnabled()) {
      this.fileImportPrepipelineChainDownloader =
          Optional.of(
              new FileImportChainDownloader(
                  new EreImportPrepipelineFactory(
                      metricsSystem,
                      syncConfig.ereDataUri().orElseThrow(),
                      syncConfig.era1ImportPrepipelineConcurrency(),
                      protocolSchedule,
                      protocolContext,
                      ethContext,
                      terminationCondition),
                  protocolContext.getBlockchain(),
                  ethContext.getScheduler()));
    } else if (syncConfig.getSyncMode() == SyncMode.FULL
        && syncConfig.era1ImportPrepipelineEnabled()) {
      this.fileImportPrepipelineChainDownloader =
          Optional.of(
              new FileImportChainDownloader(
                  new Era1ImportPrepipelineFactory(
                      metricsSystem,
                      syncConfig.era1DataUri(),
                      syncConfig.era1ImportPrepipelineConcurrency(),
                      protocolSchedule,
                      protocolContext,
                      ethContext,
                      terminationCondition),
                  protocolContext.getBlockchain(),
                  ethContext.getScheduler()));
    } else {
      this.fileImportPrepipelineChainDownloader = Optional.empty();
    }

    this.chainDownloader =
        FullSyncChainDownloader.create(
            syncConfig,
            protocolSchedule,
            protocolContext,
            ethContext,
            syncState,
            metricsSystem,
            terminationCondition,
            syncDurationMetrics,
            peerTaskExecutor);
  }

  public CompletableFuture<Void> start() {
    if (fileImportPrepipelineChainDownloader.isPresent()) {
      if (syncConfig.ereImportPrepipelineEnabled()) {
        syncState.markInitialSyncRestart();
      }
      LOG.info(
          "Starting {} file import prepipeline. Full sync will start after prepipeline completion",
          syncConfig.ereImportPrepipelineEnabled() ? "Ere" : "ERA1");
      CompletableFuture<Void> pipelineFuture = fileImportPrepipelineChainDownloader.get().start();
      return pipelineFuture.thenCompose(
          (v) -> {
            if (syncConfig.ereImportPrepipelineEnabled()) {
              syncState.markInitialSyncPhaseAsDone();
              if (terminationCondition.shouldStopDownload()) {
                LOG.info("Ere import complete; waiting for consensus-client forkchoice.");
                return CompletableFuture.completedFuture(null);
              }
            }
            LOG.info("Starting full sync.");
            return chainDownloader.start();
          });

    } else {
      LOG.info("Starting full sync.");
      return chainDownloader.start();
    }
  }

  public void stop() {
    fileImportPrepipelineChainDownloader.ifPresent((p) -> p.cancel());
    chainDownloader.cancel();
  }

  public TrailingPeerRequirements calculateTrailingPeerRequirements() {
    return syncState.isInSync()
        ? TrailingPeerRequirements.UNRESTRICTED
        : new TrailingPeerRequirements(
            protocolContext.getBlockchain().getChainHeadBlockNumber(),
            syncConfig.getMaxTrailingPeers());
  }
}
