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

import static org.hyperledger.besu.util.FutureUtils.exceptionallyCompose;
import static org.hyperledger.besu.util.log.LogUtil.throttledLog;

import org.hyperledger.besu.ethereum.eth.manager.exceptions.MaxRetriesReachedException;
import org.hyperledger.besu.ethereum.eth.manager.exceptions.NoAvailablePeersException;
import org.hyperledger.besu.ethereum.eth.sync.ChainDownloader;
import org.hyperledger.besu.ethereum.eth.sync.TrailingPeerRequirements;
import org.hyperledger.besu.ethereum.eth.sync.common.NoSyncRequiredException;
import org.hyperledger.besu.ethereum.eth.sync.common.NoSyncRequiredState;
import org.hyperledger.besu.ethereum.eth.sync.common.PivotAtOrBelowCheckpointException;
import org.hyperledger.besu.ethereum.eth.sync.common.PivotSyncActions;
import org.hyperledger.besu.ethereum.eth.sync.common.PivotUpdateListener;
import org.hyperledger.besu.ethereum.eth.sync.common.SyncException;
import org.hyperledger.besu.ethereum.eth.sync.common.WrongChainException;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.StalledDownloadException;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldStateDownloader;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.util.ExceptionUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapSyncDownloader implements SnapSyncController {

  private static final Duration FAST_SYNC_RETRY_DELAY = Duration.ofSeconds(5);
  private static final int PIVOT_BELOW_CHECKPOINT_LOG_DELAY_SECONDS = 30;
  private static final int WRONG_CHAIN_LOG_DELAY_SECONDS = 30;
  private static final int WRONG_CHAIN_REPIVOT_WARN_THRESHOLD = 3;

  private static final Logger LOG = LoggerFactory.getLogger(SnapSyncDownloader.class);

  private final PivotSyncActions fastSyncActions;
  private final WorldStateDownloader worldStateDownloader;
  private final Path fastSyncDataDirectory;
  private final SyncDurationMetrics syncDurationMetrics;
  private final OptionalLong checkpointBlockNumber;
  private volatile Optional<TrailingPeerRequirements> trailingPeerRequirements = Optional.empty();
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicBoolean shouldLogPivotBelowCheckpoint = new AtomicBoolean(true);
  private final AtomicBoolean shouldLogWrongChain = new AtomicBoolean(true);
  private final AtomicInteger consecutiveWrongChainRePivots = new AtomicInteger(0);
  private SnapSyncProcessState initialPivotSyncState;

  public SnapSyncDownloader(
      final PivotSyncActions fastSyncActions,
      final WorldStateDownloader worldStateDownloader,
      final Path fastSyncDataDirectory,
      final SnapSyncProcessState initialPivotSyncState,
      final SyncDurationMetrics syncDurationMetrics,
      final OptionalLong checkpointBlockNumber) {
    this.fastSyncActions = fastSyncActions;
    this.worldStateDownloader = worldStateDownloader;
    this.fastSyncDataDirectory = fastSyncDataDirectory;
    this.initialPivotSyncState = initialPivotSyncState;
    this.syncDurationMetrics = syncDurationMetrics;
    this.checkpointBlockNumber = checkpointBlockNumber;
  }

  @Override
  public CompletableFuture<SnapSyncProcessState> start() {
    if (!running.compareAndSet(false, true)) {
      throw new IllegalStateException("SyncDownloader already running");
    }
    LOG.info("Starting pivot-based sync");

    return start(initialPivotSyncState);
  }

  private CompletableFuture<SnapSyncProcessState> start(final SnapSyncProcessState fastSyncState) {
    LOG.debug("Start snap sync with initial sync state {}", fastSyncState);
    return findPivotBlock(fastSyncState, this::downloadChainAndWorldState);
  }

  private CompletableFuture<SnapSyncProcessState> findPivotBlock(
      final SnapSyncProcessState fastSyncState,
      final Function<SnapSyncProcessState, CompletableFuture<SnapSyncProcessState>>
          onNewPivotBlock) {
    return exceptionallyCompose(
        CompletableFuture.completedFuture(fastSyncState)
            .thenCompose(fastSyncActions::selectPivotBlock)
            .thenCompose(fastSyncActions::resolvePivotBlockHeader)
            .thenApply(this::updateMaxTrailingPeers)
            .thenApply(this::storeState)
            .thenCompose(onNewPivotBlock),
        this::handleFailure);
  }

  private CompletableFuture<SnapSyncProcessState> handleFailure(final Throwable error) {
    trailingPeerRequirements = Optional.empty();
    Throwable rootCause = ExceptionUtils.rootCause(error);
    if (!(rootCause instanceof WrongChainException)) {
      consecutiveWrongChainRePivots.set(0);
    }
    if (rootCause instanceof NoSyncRequiredException) {
      return CompletableFuture.completedFuture(new NoSyncRequiredState());
    } else if (rootCause instanceof SyncException syncEx) {
      // Pivot block header mismatch is caused by bad peers — re-pivot to recover.
      LOG.debug("Sync error ({}), re-pivoting.", syncEx.getError());
      return start(new SnapSyncProcessState());
    } else if (rootCause instanceof WrongChainException) {
      // A genuinely wrong chain — a mis-configured checkpoint hash, say — shows up as repeated
      // re-pivots rather than a hard stop. Re-pivoting cannot recover from that, so escalate to a
      // throttled WARN once the re-pivots stop looking like a transient reorg.
      final int rePivots = consecutiveWrongChainRePivots.incrementAndGet();
      LOG.atDebug()
          .setMessage(
              "Snap sync pivot is not on the chain we trust, re-pivoting to a new block: {}")
          .addArgument(rootCause.getMessage())
          .log();
      if (rePivots >= WRONG_CHAIN_REPIVOT_WARN_THRESHOLD) {
        throttledLog(
            LOG::warn,
            String.format(
                "Snap sync has re-pivoted %d times in a row because the downloaded headers do not "
                    + "connect to the chain we trust. Re-pivoting cannot recover from a trusted "
                    + "checkpoint that is not on the canonical chain: if one is configured (in the "
                    + "genesis file or via --checkpoint), verify its hash and number against "
                    + "another node or a block explorer. Last failure: %s",
                rePivots, rootCause.getMessage()),
            shouldLogWrongChain,
            WRONG_CHAIN_LOG_DELAY_SECONDS);
      }
      return start(new SnapSyncProcessState());
    } else if (rootCause instanceof PivotAtOrBelowCheckpointException) {
      // Recoverable by waiting: re-pivot after a delay rather than immediately, because the pivot
      // selector reuses its previous pivot until the chain head has advanced far enough, so an
      // immediate retry would spin on the same block.
      LOG.debug("{} Waiting before selecting a new pivot.", rootCause.getMessage());
      return fastSyncActions.scheduleFutureTask(
          () -> start(new SnapSyncProcessState()), FAST_SYNC_RETRY_DELAY);
    } else if (rootCause instanceof StalledDownloadException) {
      LOG.debug("Stalled sync re-pivoting to newer block.");
      return start(new SnapSyncProcessState());
    } else if (rootCause instanceof CancellationException) {
      if (!running.get()) {
        return CompletableFuture.failedFuture(error);
      }
      LOG.debug("Sync cancelled internally, re-pivoting.");
      return start(new SnapSyncProcessState());
    } else if (rootCause instanceof MaxRetriesReachedException) {
      LOG.debug(
          "A download operation reached the max number of retries, re-pivoting to newer block");
      return start(new SnapSyncProcessState());
    } else if (rootCause instanceof NoAvailablePeersException) {
      LOG.debug(
          "No peers available for sync. Restarting sync in {} seconds",
          FAST_SYNC_RETRY_DELAY.toSeconds());
      return fastSyncActions.scheduleFutureTask(
          () -> start(new SnapSyncProcessState()), FAST_SYNC_RETRY_DELAY);
    } else {
      LOG.error(
          "Encountered an unexpected error during sync. Restarting sync in "
              + FAST_SYNC_RETRY_DELAY.toSeconds()
              + " seconds.",
          error);
      return fastSyncActions.scheduleFutureTask(
          () -> start(new SnapSyncProcessState()), FAST_SYNC_RETRY_DELAY);
    }
  }

  @Override
  public void stop() {
    synchronized (this) {
      if (running.compareAndSet(true, false)) {
        LOG.info("Stopping sync");
        // Cancelling the world state download will also cause the chain download to be cancelled.
        worldStateDownloader.cancel();
      }
    }
  }

  @Override
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

  private SnapSyncProcessState updateMaxTrailingPeers(final SnapSyncProcessState state) {
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
    if (chainDownloader instanceof PivotUpdateListener pivotUpdateListener) {
      fastSyncActions.setChainDownloaderListener(pivotUpdateListener);
      LOG.debug("Registered chain downloader as pivot update listener");
    }

    worldStateDownloader.setChainDownloader(chainDownloader);
  }

  private SnapSyncProcessState storeState(final SnapSyncProcessState fastSyncState) {
    initialPivotSyncState = fastSyncState;
    return fastSyncState;
  }

  /**
   * Rejects a pivot that is at or below the trusted checkpoint. Stage 1 stops at the checkpoint (or
   * validates against it) and Stage 2 only downloads bodies above it, so such a pivot cannot be
   * synced: in headers-to-checkpoint-only mode the backward driver has no range to walk, and with
   * all headers it would silently skip Stage 2 and heal a world state the checkpoint never covers.
   *
   * @param currentState the sync state holding the resolved pivot block header
   * @return the failure to propagate, or empty when the pivot is usable
   */
  private Optional<PivotAtOrBelowCheckpointException> checkPivotIsAboveCheckpoint(
      final SnapSyncProcessState currentState) {
    if (checkpointBlockNumber.isEmpty()) {
      return Optional.empty();
    }
    final long checkpointNumber = checkpointBlockNumber.getAsLong();
    return currentState
        .getPivotBlockHeader()
        .filter(pivot -> pivot.getNumber() <= checkpointNumber)
        .map(
            pivot -> {
              throttledLog(
                  LOG::warn,
                  String.format(
                      "Selected pivot block %d is at or below the trusted checkpoint %d; "
                          + "the consensus client has not caught up to the checkpoint yet. "
                          + "Waiting for a higher pivot, retrying every %d seconds.",
                      pivot.getNumber(), checkpointNumber, FAST_SYNC_RETRY_DELAY.toSeconds()),
                  shouldLogPivotBelowCheckpoint,
                  PIVOT_BELOW_CHECKPOINT_LOG_DELAY_SECONDS);
              return new PivotAtOrBelowCheckpointException(
                  "Pivot block "
                      + pivot.getNumber()
                      + " is at or below the trusted checkpoint "
                      + checkpointNumber);
            });
  }

  private CompletableFuture<SnapSyncProcessState> downloadChainAndWorldState(
      final SnapSyncProcessState currentState) {
    // Synchronized ensures that stop isn't called while we're in the process of starting a
    // world state and chain download. If it did we might wind up starting a new download
    // after the stop method had called cancel.
    synchronized (this) {
      if (!running.get()) {
        return CompletableFuture.failedFuture(
            new CancellationException("SnapSyncDownloader stopped"));
      }

      // A genesis pivot means there is nothing to snap-sync (we already hold genesis state). Skip
      // all downloading and hand off to full/backward sync via the NoSyncRequired path.
      if (currentState.getPivotBlockHeader().map(h -> h.getNumber() == 0L).orElse(false)) {
        LOG.info("Pivot is genesis; no snap sync required, proceeding to full/backward sync.");
        return CompletableFuture.failedFuture(new NoSyncRequiredException());
      }

      // Unlike a genesis pivot, a pivot at or below the trusted checkpoint is not a "nothing to do"
      // case: the operator asked us to sync from the checkpoint, so we wait for the consensus
      // client to catch up.
      final Optional<PivotAtOrBelowCheckpointException> belowCheckpoint =
          checkPivotIsAboveCheckpoint(currentState);
      if (belowCheckpoint.isPresent()) {
        return CompletableFuture.failedFuture(belowCheckpoint.get());
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

  @Override
  public Optional<TrailingPeerRequirements> calculateTrailingPeerRequirements() {
    return trailingPeerRequirements;
  }
}
