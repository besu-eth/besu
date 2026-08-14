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
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.sync.PivotBlockSelector;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.common.ChainSyncState;
import org.hyperledger.besu.ethereum.eth.sync.common.ChainSyncStateStorage;
import org.hyperledger.besu.ethereum.eth.sync.common.PivotSyncActions;
import org.hyperledger.besu.ethereum.eth.sync.common.checkpoint.Checkpoint;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.context.SnapSyncStatePersistenceManager;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.v2.SnapV2WorldStateDownloader;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldStateDownloader;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ScheduleBasedBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.services.tasks.InMemoryTasksPriorityQueues;

import java.nio.file.Path;
import java.time.Clock;
import java.util.Optional;
import java.util.OptionalLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapDownloaderFactory {

  private static final Logger LOG = LoggerFactory.getLogger(SnapDownloaderFactory.class);
  protected static final String SYNC_FOLDER = "syncFolder";

  public static Optional<SnapSyncController> createSnapDownloader(
      final SnapSyncStatePersistenceManager snapContext,
      final PivotBlockSelector pivotBlockSelector,
      final SynchronizerConfiguration syncConfig,
      final Path dataDirectory,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final MetricsSystem metricsSystem,
      final EthContext ethContext,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final SyncState syncState,
      final Clock clock,
      final SyncDurationMetrics syncDurationMetrics) {
    if (Boolean.TRUE.equals(syncConfig.getSnapSyncConfiguration().isSnap2Enabled())) {
      // The snap/2 controller will be created here; until then v2 uses v1 behavior.
    }

    return createSnapDownloaderV1(
        snapContext,
        pivotBlockSelector,
        syncConfig,
        dataDirectory,
        protocolSchedule,
        protocolContext,
        metricsSystem,
        ethContext,
        worldStateStorageCoordinator,
        syncState,
        clock,
        syncDurationMetrics);
  }

  public static Optional<SnapSyncController> createSnapDownloaderV1(
      final SnapSyncStatePersistenceManager snapContext,
      final PivotBlockSelector pivotBlockSelector,
      final SynchronizerConfiguration syncConfig,
      final Path dataDirectory,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final MetricsSystem metricsSystem,
      final EthContext ethContext,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final SyncState syncState,
      final Clock clock,
      final SyncDurationMetrics syncDurationMetrics) {
    final boolean snap2Enabled =
        Boolean.TRUE.equals(syncConfig.getSnapSyncConfiguration().isSnap2Enabled());

    final Path syncDataDirectory = dataDirectory.resolve(SYNC_FOLDER);

    ensureDirectoryExists(syncDataDirectory.toFile());

    final ChainSyncState chainSyncState =
        new ChainSyncStateStorage(syncDataDirectory)
            .loadState(
                rlpInput ->
                    BlockHeader.readFrom(
                        rlpInput, ScheduleBasedBlockHeaderFunctions.create(protocolSchedule)));
    if (syncState.isResyncNeeded()) {
      snapContext.clear();
      if (!snap2Enabled) {
        syncState
            .getAccountToRepair()
            .ifPresent(
                address ->
                    snapContext.addAccountToHealingList(
                        CompactEncoding.bytesToPath(address.addressHash().getBytes())));
      }
    } else if (chainSyncState == null
        && !holdsNothingButTheTrustAnchor(protocolContext.getBlockchain(), syncState)) {
      LOG.info(
          "Snap sync was requested, but cannot be enabled because the local blockchain is not empty.");
      return Optional.empty();
    }

    final SnapSyncProcessState snapSyncState =
        chainSyncState != null
            ? new SnapSyncProcessState(chainSyncState.pivotBlockHeader())
            : new SnapSyncProcessState();

    final InMemoryTasksPriorityQueues<SnapDataRequest> snapTaskCollection =
        createSnapWorldStateDownloaderTaskCollection();
    final WorldStateDownloader snapWorldStateDownloader;
    if (snap2Enabled) {
      if (!worldStateStorageCoordinator.getDataStorageFormat().isBonsaiFormat()) {
        throw new IllegalStateException(
            "Snap/2 synchronization requires a Bonsai data storage format, but "
                + worldStateStorageCoordinator.getDataStorageFormat()
                + " is configured");
      }
      snapWorldStateDownloader =
          new SnapV2WorldStateDownloader(
              ethContext,
              snapContext,
              protocolContext.getBlockchain(),
              worldStateStorageCoordinator,
              protocolSchedule,
              snapTaskCollection,
              syncConfig.getSnapSyncConfiguration(),
              syncConfig.getWorldStateRequestParallelism(),
              syncConfig.getWorldStateMaxRequestsWithoutProgress(),
              syncConfig.getWorldStateMinMillisBeforeStalling(),
              clock,
              metricsSystem,
              syncDurationMetrics);
    } else {
      snapWorldStateDownloader =
          new SnapWorldStateDownloader(
              ethContext,
              snapContext,
              protocolContext,
              worldStateStorageCoordinator,
              snapTaskCollection,
              syncConfig.getSnapSyncConfiguration(),
              syncConfig.getWorldStateRequestParallelism(),
              syncConfig.getWorldStateMaxRequestsWithoutProgress(),
              syncConfig.getWorldStateMinMillisBeforeStalling(),
              clock,
              metricsSystem,
              syncDurationMetrics);
    }
    final SnapSyncDownloader fastSyncDownloader =
        new SnapSyncDownloader(
            new PivotSyncActions(
                syncConfig,
                worldStateStorageCoordinator,
                protocolSchedule,
                protocolContext,
                ethContext,
                syncState,
                pivotBlockSelector,
                metricsSystem,
                syncDataDirectory),
            snapWorldStateDownloader,
            syncDataDirectory,
            snapSyncState,
            syncDurationMetrics,
            syncState
                .getCheckpoint()
                .map(checkpoint -> OptionalLong.of(checkpoint.blockNumber()))
                .orElse(OptionalLong.empty()));
    syncState.setWorldStateDownloadStatus(snapWorldStateDownloader);
    return Optional.of(fastSyncDownloader);
  }

  /**
   * Whether the local blockchain holds nothing but the lower trust anchor, so a snap sync may still
   * start from scratch. That anchor is genesis, or — with checkpoint sync — the trusted checkpoint
   * header on its own: {@code SnapSyncChainDownloader} stores that header and moves the chain head
   * to it before it persists its {@code ChainSyncState}, so a crash in between leaves a database
   * whose only content is the checkpoint header. Treating that as "not empty" would permanently
   * disable snap sync for the data directory and silently fall back to full sync over a chain with
   * a gap below the checkpoint.
   *
   * @param blockchain the local blockchain
   * @param syncState the sync state holding the configured checkpoint, if any
   * @return true when nothing but the trust anchor is stored
   */
  static boolean holdsNothingButTheTrustAnchor(
      final Blockchain blockchain, final SyncState syncState) {
    final BlockHeader chainHead = blockchain.getChainHeadHeader();
    if (chainHead.getNumber() == BlockHeader.GENESIS_BLOCK_NUMBER) {
      return true;
    }
    final Optional<Checkpoint> maybeCheckpoint = syncState.getCheckpoint();
    // A body at the chain head means real block data was imported, not just the checkpoint header.
    if (maybeCheckpoint
            .map(checkpoint -> chainHead.getHash().equals(checkpoint.blockHash()))
            .orElse(false)
        && blockchain.getBlockBody(chainHead.getHash()).isEmpty()) {
      LOG.info(
          "Local blockchain holds only the trusted checkpoint header {}, most likely from an interrupted snap sync; restarting snap sync.",
          chainHead.getNumber());
      return true;
    }
    return false;
  }

  protected static InMemoryTasksPriorityQueues<SnapDataRequest>
      createSnapWorldStateDownloaderTaskCollection() {
    return new InMemoryTasksPriorityQueues<>();
  }

  protected static void ensureDirectoryExists(final java.io.File dir) {
    if (!dir.mkdirs() && !dir.isDirectory()) {
      throw new IllegalStateException("Unable to create directory: " + dir.getAbsolutePath());
    }
  }
}
