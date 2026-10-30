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

import static org.hyperledger.besu.ethereum.eth.sync.snap2.ProgressFormat.block;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.ProgressFormat.duration;
import static org.hyperledger.besu.util.ExceptionUtils.rootCause;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.sync.TrailingPeerRequirements;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.HeaderFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Priority;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Protocol;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncController;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The SNAP2 sync: the chain and the state downloaded together, the state flat at a pivot that
 * follows the chain head through block access lists, the tries built once it is complete. One run
 * is a {@link SyncAttempt}; the sync starts over when the state cannot be recovered.
 */
final class Snap2SyncController implements SnapSyncController {

  private static final Logger LOG = LoggerFactory.getLogger(Snap2SyncController.class);
  private static final int MAX_RESTARTS = 3;
  private static final Duration RETRY_DELAY = Duration.ofSeconds(5);
  // until the fork that adds block access lists, which is far
  private static final Duration FORK_DELAY = Duration.ofSeconds(30);
  private static final Duration PROGRESS_PERIOD = Duration.ofSeconds(30);

  private final SyncAttempt.Context context;
  private final RequestSender scheduler;
  // the checkpoint, or genesis, below which no pivot is chosen
  private final long floor;
  private final Duration pollInterval;
  // marks a sync in progress: a node restarted while it exists syncs again from scratch
  private final Path marker;
  private final Consumer<RequestSender> handOver;
  private final ExecutorService executor;
  private final ScheduledExecutorService control;
  private final MetricsSystem metricsSystem;
  private final CompletableFuture<SnapSyncProcessState> result = new CompletableFuture<>();
  private final AtomicInteger restarts = new AtomicInteger();
  // guarded by this: the moves of the attempts that started over
  private long earlierMoves;
  private volatile SyncAttempt current;
  // the figures of the last attempt, kept once the sync released it
  private volatile Optional<SyncAttempt.Figures> lastFigures = Optional.empty();
  // set under the lock with the attempts it cancels
  private volatile boolean stopped;

  /**
   * Creates the sync.
   *
   * @param scheduler sends the requests of the sync, until it ends
   * @param handOver takes the scheduler once the sync completes
   * @param executor runs the attempts, owned by the sync
   * @param control moves the pivot when due, owned by the sync
   * @param metricsSystem shows the progress of the sync
   */
  Snap2SyncController(
      final SyncAttempt.Context context,
      final RequestSender scheduler,
      final long floor,
      final Duration pollInterval,
      final Path marker,
      final Consumer<RequestSender> handOver,
      final ExecutorService executor,
      final ScheduledExecutorService control,
      final MetricsSystem metricsSystem) {
    this.context = context;
    this.scheduler = scheduler;
    this.floor = floor;
    this.pollInterval = pollInterval;
    this.marker = marker;
    this.handOver = handOver;
    this.executor = executor;
    this.control = control;
    this.metricsSystem = metricsSystem;
  }

  /** Returns the file that marks a SNAP2 sync in progress. */
  static Path markerFile(final Path dataDirectory) {
    return dataDirectory.resolve("snap2-sync-in-progress");
  }

  @Override
  public CompletableFuture<SnapSyncProcessState> start() {
    LOG.info("SNAP2 sync started");
    final Instant started = Instant.now();
    CompletableFuture.runAsync(this::prepare, executor)
        .thenCompose(unused -> run())
        .whenComplete((pivot, error) -> end(pivot, error, started));
    return result;
  }

  @Override
  public void stop() {
    final SyncAttempt attempt;
    synchronized (this) {
      if (stopped) {
        return;
      }
      stopped = true;
      attempt = current;
    }
    LOG.info("Stopping SNAP2 sync");
    if (attempt != null) {
      attempt.cancel();
    }
    result.cancel(false);
  }

  /** Waits for the reads and writes of the stopped sync to end, before the storage closes. */
  @Override
  public void awaitStop() throws InterruptedException {
    final SyncAttempt attempt = current;
    if (attempt != null) {
      attempt.awaitStop();
    }
  }

  @Override
  public void deletePivotSyncState() {
    try {
      Files.deleteIfExists(marker);
    } catch (final IOException e) {
      throw new UncheckedIOException("cannot delete " + marker, e);
    }
  }

  @Override
  public Optional<TrailingPeerRequirements> calculateTrailingPeerRequirements() {
    return Optional.empty();
  }

  /** Returns the attempt in progress. */
  Optional<SyncAttempt> current() {
    return Optional.ofNullable(current);
  }

  /**
   * Returns the figures of the attempt in progress, or those of the last one once the sync ended.
   */
  Optional<SyncAttempt.Figures> figures() {
    final SyncAttempt attempt = current;
    return attempt == null ? lastFigures : Optional.of(attempt.figures());
  }

  /** Returns the number of attempts that started over. */
  int restarts() {
    return restarts.get();
  }

  /** Returns the number of pivot moves of every attempt. */
  synchronized long pivotMoves() {
    return earlierMoves + (current == null ? 0 : current.pivotMoves());
  }

  private void prepare() {
    try {
      Files.createDirectories(marker.getParent());
      if (!Files.exists(marker)) {
        Files.createFile(marker);
      }
    } catch (final IOException e) {
      throw new UncheckedIOException("cannot create " + marker, e);
    }
    Snap2Metrics.register(metricsSystem, this);
    control.scheduleWithFixedDelay(
        this::maybeMove, pollInterval.toMillis(), pollInterval.toMillis(), TimeUnit.MILLISECONDS);
    control.scheduleWithFixedDelay(
        this::logProgress,
        PROGRESS_PERIOD.toMillis(),
        PROGRESS_PERIOD.toMillis(),
        TimeUnit.MILLISECONDS);
  }

  // an attempt from scratch, started over while the state cannot be recovered
  private CompletableFuture<BlockHeader> run() {
    if (!prepareForSync()) {
      return stoppedFuture();
    }
    return waitForPeers()
        .thenCompose(unused -> selectPivot())
        .thenComposeAsync(pivot -> newAttempt(pivot).start(), executor)
        .exceptionallyComposeAsync(this::startOver, executor);
  }

  // under the lock, so that a stop either waits for the wipe or keeps it from starting
  private synchronized boolean prepareForSync() {
    if (stopped) {
      return false;
    }
    context.storage().prepareForSync();
    return true;
  }

  // under the lock, so that a stop either finds it or keeps it from starting
  private synchronized SyncAttempt newAttempt(final BlockHeader pivot) {
    if (stopped) {
      throw new CancellationException("SNAP2 sync stopped");
    }
    current = new SyncAttempt(context, pivot);
    return current;
  }

  // once the failed attempt ended, however long it takes, as the next one wipes the state it wrote
  private CompletableFuture<BlockHeader> startOver(final Throwable error) {
    final SyncAttempt failed = current;
    if (failed != null) {
      failed.cancel();
      try {
        failed.awaitEnd();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return CompletableFuture.failedFuture(e);
      }
      endAttempt(failed);
    }
    final Optional<RestartException> restart = RestartException.in(error);
    if (stopped || restart.isEmpty() || restarts.get() >= MAX_RESTARTS) {
      return CompletableFuture.failedFuture(error);
    }
    LOG.warn(
        "SNAP2 restart {} of {}: {}",
        restarts.incrementAndGet(),
        MAX_RESTARTS,
        restart.get().getMessage());
    return run();
  }

  // its moves counted with the earlier ones, none current until the next attempt
  private synchronized void endAttempt(final SyncAttempt ended) {
    earlierMoves += ended.pivotMoves();
    current = null;
  }

  private CompletableFuture<Void> waitForPeers() {
    if (stopped) {
      return stoppedFuture();
    }
    if (scheduler.peerCount(Protocol.SNAP) > 0) {
      return CompletableFuture.completedFuture(null);
    }
    LOG.info("SNAP2 waiting for a snap peer");
    return later(RETRY_DELAY).thenCompose(unused -> waitForPeers());
  }

  // the pivot trails the head, above the floor, and the block after it has a block access list
  private CompletableFuture<BlockHeader> selectPivot() {
    if (stopped) {
      return stoppedFuture();
    }
    final Optional<Hash> head = context.heads().headHash();
    if (head.isEmpty()) {
      LOG.info("SNAP2 waiting for a chain head from the consensus client");
      return later(RETRY_DELAY).thenCompose(unused -> selectPivot());
    }
    final HeaderFetcher fetcher = context.fetcher();
    return fetcher
        .byHash(head.get(), Priority.PIVOT)
        .thenCompose(this::pivotChain)
        .thenCompose(this::pivotOf)
        .exceptionallyCompose(
            error -> {
              if (stopped) {
                return CompletableFuture.failedFuture(error);
              }
              LOG.info(
                  "SNAP2 pivot selection failed, trying again: {}", rootCause(error).getMessage());
              return later(RETRY_DELAY).thenCompose(unused -> selectPivot());
            });
  }

  // the headers from the pivot up to the head, none while the head is not above the floor
  private CompletableFuture<List<BlockHeader>> pivotChain(final BlockHeader head) {
    final long pivot =
        Math.max(floor + 1, head.getNumber() - context.moveSettings().pivotDistance());
    return context.fetcher().chainDown(head, pivot, Priority.PIVOT);
  }

  private CompletableFuture<BlockHeader> pivotOf(final List<BlockHeader> chain) {
    if (chain.isEmpty()) {
      LOG.info("SNAP2 waiting for the chain head to pass block {}", block(floor));
      return later(RETRY_DELAY).thenCompose(unused -> selectPivot());
    }
    final BlockHeader pivot = chain.getFirst();
    final BlockHeader next = chain.size() > 1 ? chain.get(1) : pivot;
    if (next.getBalHash().isEmpty()) {
      LOG.info(
          "SNAP2 waiting for the fork that adds block access lists, block {} has none",
          block(next.getNumber()));
      return later(FORK_DELAY).thenCompose(unused -> selectPivot());
    }
    LOG.info(
        "SNAP2 pivot selected: {} ({}), {} blocks behind the head",
        block(pivot.getNumber()),
        pivot.getHash(),
        chain.size() - 1);
    return CompletableFuture.completedFuture(pivot);
  }

  // on the control thread, which a failure would stop
  private void maybeMove() {
    try {
      final SyncAttempt attempt = current;
      if (attempt != null) {
        attempt.maybeMove();
      }
    } catch (final RuntimeException e) {
      LOG.warn("SNAP2 pivot move check failed", e);
    }
  }

  // one line a period, the figures behind it for debugging
  private void logProgress() {
    try {
      current()
          .ifPresent(
              attempt -> {
                LOG.info("{}", attempt.progressLine());
                LOG.debug("{}", attempt.details());
              });
    } catch (final RuntimeException e) {
      LOG.debug("SNAP2 progress not logged", e);
    }
  }

  private void end(final BlockHeader pivot, final Throwable error, final Instant started) {
    control.shutdownNow();
    // released with what it downloaded with, its figures kept for the metrics
    final SyncAttempt last = current;
    if (last != null) {
      lastFigures = Optional.of(last.figures());
      endAttempt(last);
    }
    if (error == null) {
      LOG.info(
          "SNAP2 sync complete at {} in {} | {} pivot moves, {} restarts",
          block(pivot.getNumber()),
          duration(Duration.between(started, Instant.now())),
          pivotMoves(),
          restarts.get());
      handOver.accept(scheduler);
      result.complete(new SnapSyncProcessState(pivot));
    } else {
      if (!stopped) {
        LOG.error("SNAP2 sync failed", error);
      }
      scheduler.stop();
      result.completeExceptionally(error);
    }
    // the dependents of the result ran on this thread, interrupted last
    executor.shutdownNow();
  }

  private CompletableFuture<Void> later(final Duration delay) {
    return CompletableFuture.runAsync(() -> {}, context.delays().apply(delay));
  }

  private static <T> CompletableFuture<T> stoppedFuture() {
    return CompletableFuture.failedFuture(new CancellationException("SNAP2 sync stopped"));
  }
}
