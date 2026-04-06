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

import static org.hyperledger.besu.util.FutureUtils.exceptionallyCompose;

import org.hyperledger.besu.ethereum.eth.manager.exceptions.MaxRetriesReachedException;
import org.hyperledger.besu.ethereum.eth.manager.exceptions.NoAvailablePeersException;
import org.hyperledger.besu.ethereum.eth.sync.ChainDownloader;
import org.hyperledger.besu.ethereum.eth.sync.TrailingPeerRequirements;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncChainDownloader;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapWorldStateDownloader;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.StalledDownloadException;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldStateDownloader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.util.ExceptionUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PivotSyncDownloader {

  private static final Duration FAST_SYNC_RETRY_DELAY = Duration.ofSeconds(5);

  @SuppressWarnings("PrivateStaticFinalLoggers")
  protected final Logger LOG = LoggerFactory.getLogger(getClass());

  private final WorldStateStorageCoordinator worldStateStorageCoordinator;
  private final WorldStateDownloader worldStateDownloader;
  private final Path fastSyncDataDirectory;
  private final SyncDurationMetrics syncDurationMetrics;
  private volatile Optional<TrailingPeerRequirements> trailingPeerRequirements = Optional.empty();
  private final AtomicBoolean running = new AtomicBoolean(false);

  protected final PivotSyncActions fastSyncActions;
  protected PivotSyncState initialPivotSyncState;

  public PivotSyncDownloader(
      final PivotSyncActions fastSyncActions,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final WorldStateDownloader worldStateDownloader,
      final Path fastSyncDataDirectory,
      final PivotSyncState initialPivotSyncState,
      final SyncDurationMetrics syncDurationMetrics) {
    this.fastSyncActions = fastSyncActions;
    this.worldStateStorageCoordinator = worldStateStorageCoordinator;
    this.worldStateDownloader = worldStateDownloader;
    this.fastSyncDataDirectory = fastSyncDataDirectory;
    this.initialPivotSyncState = initialPivotSyncState;
    this.syncDurationMetrics = syncDurationMetrics;
  }

  public CompletableFuture<PivotSyncState> start() {
    if (!running.compareAndSet(false, true)) {
      throw new IllegalStateException("SyncDownloader already running");
    }
    LOG.info("Starting pivot-based sync");

    return start(initialPivotSyncState);
  }

  protected CompletableFuture<PivotSyncState> start(final PivotSyncState fastSyncState) {
    worldStateStorageCoordinator.applyOnMatchingStrategies(
        List.of(DataStorageFormat.BONSAI, DataStorageFormat.X_BONSAI_ARCHIVE),
        worldStateKeyValueStorage -> {
          BonsaiWorldStateKeyValueStorage onBonsai =
              (BonsaiWorldStateKeyValueStorage) worldStateKeyValueStorage;
          LOG.info("Clearing bonsai flat account db");
          onBonsai.clearFlatDatabase();
          onBonsai.clearTrieLog();
        });
    LOG.info("Start fast sync with initial sync state {}", fastSyncState);
    return findPivotBlock(fastSyncState, fss -> downloadChainAndWorldState(fastSyncActions, fss));
  }

  public CompletableFuture<PivotSyncState> findPivotBlock(
      final PivotSyncState fastSyncState,
      final Function<PivotSyncState, CompletableFuture<PivotSyncState>> onNewPivotBlock) {
    return exceptionallyCompose(
        CompletableFuture.completedFuture(fastSyncState)
            .thenCompose(fastSyncActions::selectPivotBlock)
            .thenCompose(fastSyncActions::downloadPivotBlockHeader)
            .thenApply(this::updateMaxTrailingPeers)
            .thenApply(this::storeState)
            .thenCompose(onNewPivotBlock),
        this::handleFailure);
  }

  protected CompletableFuture<PivotSyncState> handleFailure(final Throwable error) {
    trailingPeerRequirements = Optional.empty();
    Throwable rootCause = ExceptionUtils.rootCause(error);
    if (rootCause instanceof NoSyncRequiredException) {
      return CompletableFuture.completedFuture(new NoSyncRequiredState());
    } else if (rootCause instanceof SyncException) {
      return CompletableFuture.failedFuture(error);
    } else if (rootCause instanceof StalledDownloadException) {
      LOG.debug("Stalled sync re-pivoting to newer block.");
      return start(PivotSyncState.EMPTY_SYNC_STATE);
    } else if (rootCause instanceof CancellationException) {
      return CompletableFuture.failedFuture(error);
    } else if (rootCause instanceof MaxRetriesReachedException) {
      LOG.debug(
          "A download operation reached the max number of retries, re-pivoting to newer block");
      return start(PivotSyncState.EMPTY_SYNC_STATE);
    } else if (rootCause instanceof NoAvailablePeersException) {
      LOG.debug(
          "No peers available for sync. Restarting sync in {} seconds",
          FAST_SYNC_RETRY_DELAY.toSeconds());
      return fastSyncActions.scheduleFutureTask(
          () -> start(PivotSyncState.EMPTY_SYNC_STATE), FAST_SYNC_RETRY_DELAY);
    } else {
      LOG.error(
          "Encountered an unexpected error during sync. Restarting sync in "
              + FAST_SYNC_RETRY_DELAY.toSeconds()
              + " seconds.",
          error);
      return fastSyncActions.scheduleFutureTask(
          () -> start(PivotSyncState.EMPTY_SYNC_STATE), FAST_SYNC_RETRY_DELAY);
    }
  }

  public void stop() {
    synchronized (this) {
      if (running.compareAndSet(true, false)) {
        LOG.info("Stopping sync");
        // Cancelling the world state download will also cause the chain download to be cancelled.
        worldStateDownloader.cancel();
      }
    }
  }

  public void deletePivotSyncState() {
    // Make sure downloader is stopped before we start cleaning up its dependencies
    worldStateDownloader.cancel();
    try {
      if (fastSyncDataDirectory.toFile().exists()) {
        // Clean up this data for now (until fast sync resume functionality is in place)
        MoreFiles.deleteRecursively(fastSyncDataDirectory, RecursiveDeleteOption.ALLOW_INSECURE);
      }
    } catch (final IOException e) {
      LOG.error("Unable to clean up sync state", e);
    }
  }

  protected PivotSyncState updateMaxTrailingPeers(final PivotSyncState state) {
    if (state.getPivotBlockNumber().isPresent()) {
      trailingPeerRequirements =
          Optional.of(new TrailingPeerRequirements(state.getPivotBlockNumber().getAsLong(), 0));
    } else {
      trailingPeerRequirements = Optional.empty();
    }
    return state;
  }

  /**
   * Wires up the chain downloader by registering it for callbacks and establishing bidirectional
   * references with the world state downloader for SnapSync.
   *
   * @param chainDownloader the chain downloader to wire up
   */
  private void wireSnapSyncBidirectionalReferences(final ChainDownloader chainDownloader) {
    // Register chain downloader for pivot update callbacks
    if (chainDownloader instanceof PivotUpdateListener) {
      fastSyncActions.setChainDownloaderListener((PivotUpdateListener) chainDownloader);
      LOG.debug("Registered chain downloader as pivot update listener");
    }

    // Wire bidirectional references for SnapSync
    if (worldStateDownloader instanceof SnapWorldStateDownloader
        && chainDownloader instanceof SnapSyncChainDownloader) {
      ((SnapWorldStateDownloader) worldStateDownloader)
          .setChainDownloader((SnapSyncChainDownloader) chainDownloader);
      LOG.debug("Wired bidirectional references between chain and world state downloaders");
    }
  }

  protected PivotSyncState storeState(final PivotSyncState state) {
    return state;
  }

  protected CompletableFuture<PivotSyncState> downloadChainAndWorldState(
      final PivotSyncActions fastSyncActions, final PivotSyncState currentState) {
    // Synchronized ensures that stop isn't called while we're in the process of starting a
    // world state and chain download. If it did we might wind up starting a new download
    // after the stop method had called cancel.
    synchronized (this) {
      if (!running.get()) {
        return CompletableFuture.failedFuture(
            new CancellationException("PivotSyncDownloader stopped"));
      }
      final ChainDownloader chainDownloader =
          fastSyncActions.createChainDownloader(currentState, syncDurationMetrics);

      // Wire up chain downloader callbacks and bidirectional references
      wireSnapSyncBidirectionalReferences(chainDownloader);

      final CompletableFuture<Void> worldStateFuture =
          worldStateDownloader.run(fastSyncActions, currentState);

      final CompletableFuture<Void> chainFuture = chainDownloader.start();

      // If either download fails, cancel the other one.
      chainFuture.exceptionally(
          error -> {
            worldStateFuture.cancel(true);
            return null;
          });
      worldStateFuture.exceptionally(
          error -> {
            chainDownloader.cancel();
            return null;
          });

      return CompletableFuture.allOf(worldStateFuture, chainFuture)
          .thenApply(
              complete -> {
                trailingPeerRequirements = Optional.empty();
                return currentState;
              });
    }
  }

  public Optional<TrailingPeerRequirements> calculateTrailingPeerRequirements() {
    return trailingPeerRequirements;
  }
}
