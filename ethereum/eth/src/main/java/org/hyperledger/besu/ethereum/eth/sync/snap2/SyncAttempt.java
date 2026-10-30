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
import static org.hyperledger.besu.ethereum.eth.sync.snap2.ProgressFormat.compact;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.ProgressFormat.count;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.ProgressFormat.duration;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.ProgressFormat.eta;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.ProgressFormat.percent;

import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.BalFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.ChainHeadSource;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.HeaderFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.Pivot;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.StateSync;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStateStorage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.trie.WorldStateTrieBuilder;

import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One attempt of the SNAP2 sync from a first pivot: the headers down to the floor, the state at a
 * pivot that follows the chain head, the final build of the tries, and the chain head set at the
 * final pivot once the bodies of the window below it are stored. A cancelled attempt writes nothing
 * once {@link #awaitStop} returns.
 */
final class SyncAttempt {

  private static final Logger LOG = LoggerFactory.getLogger(SyncAttempt.class);
  // a cancelled build ends at its next check, after a few thousand slots at most
  private static final Duration BUILD_TIMEOUT = Duration.ofSeconds(30);

  /**
   * What the attempts of a sync share.
   *
   * @param stateSyncs creates the state download of an attempt
   * @param chains creates the chain of an attempt
   * @param trieBuilders creates the final build, given whether the download left the trie of a
   *     storage to it
   * @param executor runs the final build
   * @param delays runs a task once a delay passed, as a retry
   */
  record Context(
      MutableBlockchain blockchain,
      SyncStateStorage storage,
      ChainHeadSource heads,
      HeaderFetcher fetcher,
      BalFetcher balFetcher,
      PivotMoves.Settings moveSettings,
      Supplier<StateSync> stateSyncs,
      Supplier<SyncChain> chains,
      Function<Predicate<Bytes32>, WorldStateTrieBuilder> trieBuilders,
      Executor executor,
      Function<Duration, Executor> delays) {}

  /** What an attempt is busy with. */
  enum Stage {
    STATE,
    TRIES,
    CHAIN,
    DONE;

    /** Returns the label of the stage in the metrics. */
    String label() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  private final Context context;
  private final BlockHeader firstPivot;
  private final StateSync stateSync;
  private final SyncChain chain;
  private final PivotMoves moves;
  // fails when the attempt ends early: a restart, or a header download that ended for good
  private final CompletableFuture<BlockHeader> aborted = new CompletableFuture<>();
  private final CountDownLatch buildEnded = new CountDownLatch(1);
  private volatile boolean cancelled;
  private volatile Stage stage = Stage.STATE;
  // guarded by this: the final build, once started
  private WorldStateTrieBuilder trieBuilder;

  SyncAttempt(final Context context, final BlockHeader firstPivot) {
    this.context = context;
    this.firstPivot = firstPivot;
    this.stateSync = context.stateSyncs().get();
    this.chain = context.chains().get();
    this.moves =
        new PivotMoves(
            stateSync,
            chain::storeHeaders,
            context.blockchain(),
            context.heads(),
            context.fetcher(),
            context.balFetcher(),
            context.moveSettings(),
            aborted::completeExceptionally,
            context.delays());
  }

  /**
   * Starts the attempt, on a thread that may wait for the checkpoint. It completes with the final
   * pivot, made the chain head, or fails: with a {@link RestartException} when the sync must start
   * over.
   */
  CompletableFuture<BlockHeader> start() {
    chain.start(firstPivot);
    // a header download that ended for good ends the attempt now, not after the state
    chain
        .headersDone()
        .exceptionally(
            error -> {
              // a cancel fails it too
              if (!cancelled) {
                aborted.completeExceptionally(error);
              }
              return null;
            });
    final CompletableFuture<BlockHeader> synced =
        stateSync
            .start(firstPivot)
            .exceptionallyCompose(this::stateFailed)
            .thenCompose(unused -> moves.settle())
            .thenCompose(unused -> moves.catchUp())
            .thenComposeAsync(unused -> buildTriesWithWindow(), context.executor())
            .thenApply(
                pivot -> {
                  chain.setChainHead();
                  stage = Stage.DONE;
                  return pivot;
                });
    return synced.applyToEither(aborted, Function.identity());
  }

  /** Moves the pivot when the head went far enough, or when peers stopped serving its state. */
  void maybeMove() {
    moves.maybeMove();
  }

  /** Stops every part of the attempt; see {@link #awaitStop}. */
  void cancel() {
    cancelled = true;
    moves.stop();
    stateSync.stop();
    chain.cancel();
    final WorldStateTrieBuilder builder;
    synchronized (this) {
      builder = trieBuilder;
    }
    if (builder == null) {
      // none starts once cancelled
      buildEnded.countDown();
    } else {
      builder.cancel();
    }
  }

  /** Waits for the reads and writes of a cancelled attempt to end, before the storage is wiped. */
  void awaitStop() throws InterruptedException {
    chain.awaitStop();
    if (!buildEnded.await(BUILD_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
      LOG.warn("SNAP2 trie build still running after {}", BUILD_TIMEOUT);
    }
  }

  /**
   * As {@link #awaitStop}, however long the build takes, before the next attempt wipes the state.
   */
  void awaitEnd() throws InterruptedException {
    awaitStop();
    buildEnded.await();
  }

  /** Returns the number of pivot moves applied. */
  long pivotMoves() {
    return moves.moves();
  }

  /**
   * What the attempt did of each phase, from 0 to 1.
   *
   * @param state the share of the account key space downloaded
   * @param tries the share of the accounts the final build added
   * @param headers the share of the headers stored
   * @param bodies the share of the bodies of the window stored
   */
  record Progress(double state, double tries, double headers, double bodies) {}

  /** Returns what the attempt did of each phase. */
  Progress progress() {
    final Stage current = stage;
    final SyncChain.HeaderProgress headers = chain.headerProgress();
    return new Progress(
        current == Stage.STATE ? stateSync.accountCoverage() : 1,
        switch (current) {
          case STATE -> 0;
          case TRIES ->
              trieBuilder()
                  .map(
                      build ->
                          share(build.accountsProcessed(), stateSync.downloadedAccounts(), false))
                  .orElse(0.0);
          case CHAIN, DONE -> 1;
        },
        share(headers.stored(), headers.stored() + headers.left(), chain.headersDone().isDone()),
        current == Stage.DONE ? 1 : chain.windowProgress());
  }

  /**
   * The figures the metrics show: the pivot, the stage and the progress, the counts of the state
   * download, then those of the final build.
   */
  record Figures(
      long pivot,
      Stage stage,
      Progress progress,
      long accounts,
      long slots,
      long codes,
      long pendingStorages,
      long peersWithoutState,
      long storageTriesBuilt,
      long storageTriesUpdated,
      long storageTriesLeft,
      long storageTrieUpdatesPending,
      long trieAccounts,
      long trieSlots,
      long keptStorageTries) {}

  /** Returns the figures of the attempt, read at once. */
  Figures figures() {
    final Optional<WorldStateTrieBuilder> build = trieBuilder();
    return new Figures(
        pivotNumber(),
        stage,
        progress(),
        stateSync.downloadedAccounts(),
        stateSync.downloadedSlots(),
        stateSync.downloadedCodes(),
        stateSync.pendingStorageCount(),
        stateSync.peersWithoutState(),
        stateSync.storageTriesBuilt(),
        stateSync.storageTriesUpdated(),
        stateSync.storageTriesLeft(),
        stateSync.storageTrieUpdatesPending(),
        build.map(WorldStateTrieBuilder::accountsProcessed).orElse(0L),
        build.map(WorldStateTrieBuilder::slotsProcessed).orElse(0L),
        build.map(WorldStateTrieBuilder::keptStorageTries).orElse(0L));
  }

  /** Returns the line of a progress log: the share done of each phase. */
  String progressLine() {
    final Progress progress = progress();
    final SyncChain.HeaderProgress headers = chain.headerProgress();
    final StringBuilder line =
        new StringBuilder("SNAP2 sync | state ")
            .append(percent(progress.state()))
            .append(" | tries ")
            .append(percent(progress.tries()))
            .append(" | headers ")
            .append(percent(progress.headers()));
    eta(headers.stored(), headers.left(), headers.elapsed())
        .ifPresent(left -> line.append(", ETA ").append(left));
    return line.append(" | bodies ").append(percent(progress.bodies())).toString();
  }

  /** Returns the figures behind the progress, for debugging. */
  String details() {
    return "SNAP2 details | pivot "
        + block(pivotNumber())
        + " | accounts "
        + count(stateSync.downloadedAccounts())
        + " | slots "
        + compact(stateSync.downloadedSlots())
        + " | bytecodes "
        + compact(stateSync.downloadedCodes())
        + " | storages pending "
        + count(stateSync.pendingStorageCount())
        + " | storage tries "
        + compact(stateSync.storageTriesBuilt())
        + " built, "
        + compact(stateSync.storageTriesUpdated())
        + " updated, "
        + compact(stateSync.storageTriesLeft())
        + " left | peers left out "
        + stateSync.peersWithoutState();
  }

  /** Returns what the attempt is busy with. */
  Stage stage() {
    return stage;
  }

  /** Returns the current pivot, 0 before the state download starts. */
  long pivotNumber() {
    final Pivot pivot = stateSync.pivot();
    return pivot == null ? 0 : pivot.header().getNumber();
  }

  /** Returns the final build, once started. */
  synchronized Optional<WorldStateTrieBuilder> trieBuilder() {
    return Optional.ofNullable(trieBuilder);
  }

  // complete once over, none when there is nothing to do
  private static double share(final long done, final long total, final boolean over) {
    if (total <= 0) {
      return over ? 1 : 0;
    }
    return Math.min(1, (double) done / total);
  }

  // a download that failed, with a diff partly applied for one, cannot go on from its state
  private CompletableFuture<Void> stateFailed(final Throwable error) {
    return cancelled
        ? CompletableFuture.failedFuture(error)
        : CompletableFuture.failedFuture(new RestartException("the state download failed", error));
  }

  // the bodies of the window below the final pivot download while the tries build
  private CompletableFuture<BlockHeader> buildTriesWithWindow() {
    final BlockHeader pivot = stateSync.pivot().header();
    final CompletableFuture<Void> window = chain.downloadWindow(pivot);
    buildTries(pivot);
    if (!chain.headersDone().isDone()) {
      LOG.info("SNAP2 tries built, waiting for the header download");
    }
    return window.thenApply(unused -> pivot);
  }

  private void buildTries(final BlockHeader pivot) {
    try {
      // first, so that a cancel goes to the build while the storage tries finish on this thread
      final WorldStateTrieBuilder builder = newTrieBuilder();
      // the tries the last moves changed are kept by the build
      stateSync.finishStorageTries();
      stateSync.stop();
      final Instant started = Instant.now();
      LOG.info(
          "SNAP2 trie build started at {} | accounts {} | storage tries built while downloading {}",
          block(pivot.getNumber()),
          compact(stateSync.downloadedAccounts()),
          compact(stateSync.storageTriesBuilt()));
      final WorldStateTrieBuilder.Result built = builder.build();
      if (!built.root().equals(pivot.getStateRoot().getBytes())) {
        throw new RestartException(
            "built state root "
                + built.root()
                + " does not match the root "
                + pivot.getStateRoot()
                + " of block "
                + pivot.getNumber());
      }
      context.storage().saveWorldState(pivot, built.rootNode());
      context.storage().completeSync();
      stage = Stage.CHAIN;
      LOG.info(
          "SNAP2 trie build complete in {} | root verified | accounts {} | storage tries kept {}"
              + " | slots read {}",
          duration(Duration.between(started, Instant.now())),
          compact(built.accounts()),
          compact(built.keptStorageTries()),
          compact(built.slots()));
    } finally {
      buildEnded.countDown();
    }
  }

  // under the lock, so that a cancel either finds it or stops it from starting
  private synchronized WorldStateTrieBuilder newTrieBuilder() {
    if (cancelled) {
      throw new CancellationException("attempt abandoned");
    }
    trieBuilder = context.trieBuilders().apply(stateSync::storageTrieLeft);
    stage = Stage.TRIES;
    return trieBuilder;
  }
}
