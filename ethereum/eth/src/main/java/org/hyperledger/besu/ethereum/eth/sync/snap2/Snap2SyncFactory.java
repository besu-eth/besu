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
package org.hyperledger.besu.ethereum.eth.sync.snap2;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.sync.PivotBlockSelector;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.common.checkpoint.Checkpoint;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.BalFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.ChainHeadSource;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.HeaderFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestScheduler;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.StateSync;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.BonsaiSyncStateStorage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.ForestSyncStateStorage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStateStorage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.trie.WorldStateTrieBuilder;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapDownloaderFactory;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncController;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates the SNAP2 sync, or nothing when the node already holds a chain. */
public final class Snap2SyncFactory {

  private static final Logger LOG = LoggerFactory.getLogger(Snap2SyncFactory.class);
  // a list asked in batches of 32 blocks, 2 MiB an answer, given up after 60 rounds without one
  private static final int BAL_BATCH_SIZE = 32;
  private static final int BAL_RESPONSE_BYTES = 2 * 1024 * 1024;
  private static final int BAL_ROUNDS = 60;

  private Snap2SyncFactory() {}

  /**
   * Creates the sync when the node holds nothing but genesis or the bare checkpoint header, when a
   * sync did not finish, or on a resync.
   *
   * @param storageProvider gives the segments a sync on Forest stages its flat state in
   * @param pivotBlockSelector the source of the chain head, when it is one
   * @param terminalTotalDifficulty where proof of stake starts, none on a network that does not
   *     merge
   * @param handOver takes the scheduler of the sync once it completes
   * @return the sync, or empty when none is needed
   */
  public static Optional<SnapSyncController> create(
      final SynchronizerConfiguration syncConfig,
      final Path dataDirectory,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final MetricsSystem metricsSystem,
      final EthPeers ethPeers,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final StorageProvider storageProvider,
      final SyncState syncState,
      final PivotBlockSelector pivotBlockSelector,
      final Optional<Difficulty> terminalTotalDifficulty,
      final Consumer<RequestSender> handOver) {
    final MutableBlockchain blockchain = protocolContext.getBlockchain();
    final Path marker = Snap2SyncController.markerFile(dataDirectory);
    final Optional<Checkpoint> checkpoint = syncState.getCheckpoint();
    final long floor = checkpoint.map(Checkpoint::blockNumber).orElse(0L);
    if (!needsSync(marker, syncState, blockchain)) {
      return Optional.empty();
    }
    final SyncStateStorage storage = syncStorage(worldStateStorageCoordinator, storageProvider);
    final Snap2Configuration config =
        Snap2Configuration.create(
            syncConfig.getDownloaderHeaderRequestSize(),
            syncConfig.isSnapSyncHeadersToCheckpointOnly(),
            syncConfig.getSnapSyncConfiguration().isSnapSyncTransactionIndexingEnabled(),
            terminalTotalDifficulty);
    final RequestScheduler scheduler =
        RequestScheduler.forSync(ethPeers, metricsSystem, config.workerThreads());
    // without a consensus client, the head the best peer announced
    final ChainHeadSource heads =
        pivotBlockSelector instanceof ChainHeadSource source
            ? source
            : () ->
                ethPeers
                    .bestPeerWithHeightEstimate()
                    .map(peer -> peer.chainState().getBestBlock().getHash());
    final HeaderFetcher fetcher =
        new HeaderFetcher(scheduler, protocolSchedule, heads, config.chain().headersPerRequest());
    final ExecutorService executor =
        Executors.newCachedThreadPool(
            Thread.ofPlatform().name("snap2-sync-", 0).daemon().factory());
    final SyncAttempt.Context context =
        new SyncAttempt.Context(
            blockchain,
            storage,
            heads,
            fetcher,
            new BalFetcher(scheduler, BAL_BATCH_SIZE, BAL_RESPONSE_BYTES, BAL_ROUNDS),
            config.moves(),
            () ->
                new StateSync(
                    storage,
                    scheduler,
                    config.state(),
                    config.workerThreads(),
                    config.trieBuildThreads()),
            () ->
                new SyncChain(
                    blockchain,
                    protocolSchedule,
                    scheduler,
                    fetcher,
                    checkpoint,
                    config.chain(),
                    metricsSystem,
                    executor),
            trieLeft -> new WorldStateTrieBuilder(storage, config.trieBuildThreads(), trieLeft),
            executor,
            delay ->
                CompletableFuture.delayedExecutor(
                    delay.toMillis(), TimeUnit.MILLISECONDS, executor));
    return Optional.of(
        new Snap2SyncController(
            context,
            scheduler,
            floor,
            config.pollInterval(),
            marker,
            handOver,
            executor,
            Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().name("snap2-control").daemon().factory()),
            metricsSystem));
  }

  // a resync, a sync left unfinished, or nothing but the trust anchor, as for the snap sync
  static boolean needsSync(
      final Path marker, final SyncState syncState, final Blockchain blockchain) {
    if (syncState.isResyncNeeded()) {
      LOG.info("World state resync requested, syncing it again with SNAP2");
      return true;
    }
    if (Files.exists(marker)) {
      LOG.info("A previous SNAP2 sync did not finish, syncing again");
      return true;
    }
    // a chain is kept, even below a checkpoint a later release raised
    if (!SnapDownloaderFactory.holdsNothingButTheTrustAnchor(blockchain, syncState)) {
      LOG.info(
          "The node holds a chain up to block {}, no SNAP2 sync needed",
          blockchain.getChainHeadBlockNumber());
      return false;
    }
    return true;
  }

  private static SyncStateStorage syncStorage(
      final WorldStateStorageCoordinator coordinator, final StorageProvider storageProvider) {
    return switch (coordinator.getDataStorageFormat()) {
      case BONSAI ->
          new BonsaiSyncStateStorage(
              coordinator.getStrategy(BonsaiWorldStateKeyValueStorage.class));
      case FOREST ->
          new ForestSyncStateStorage(
              coordinator.getStrategy(ForestWorldStateKeyValueStorage.class), storageProvider);
      case X_BONSAI_ARCHIVE ->
          throw new IllegalStateException(
              "SNAP2 sync needs the BONSAI or FOREST data storage format, not X_BONSAI_ARCHIVE");
    };
  }
}
