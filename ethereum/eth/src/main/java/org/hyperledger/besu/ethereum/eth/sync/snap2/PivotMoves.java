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
import static org.hyperledger.besu.ethereum.eth.sync.snap2.ProgressFormat.count;
import static org.hyperledger.besu.util.ExceptionUtils.rootCause;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.SyncBlockAccessList;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.BalFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.ChainHeadSource;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.HeaderFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Priority;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.StateDiff;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.StateDiffApplier;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.StateSync;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.base.Throwables;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Moves the pivot of an attempt as the chain head moves on. The headers of the blocks it passes are
 * stored, and their block access lists folded into the diffs the state download applies.
 */
final class PivotMoves {

  private static final Logger LOG = LoggerFactory.getLogger(PivotMoves.class);
  // blocks folded into one diff, so that it stays small
  private static final int MAX_BLOCKS_PER_MOVE = 128;
  private static final Duration RETRY_DELAY = Duration.ofSeconds(5);

  /**
   * When the pivot moves.
   *
   * @param pivotDistance blocks between the chain head and the block a move goes to
   * @param moveThreshold blocks the head may go past the pivot distance before the pivot moves
   */
  record Settings(int pivotDistance, int moveThreshold) {}

  private final StateSync stateSync;
  private final Consumer<List<BlockHeader>> storeHeaders;
  private final MutableBlockchain blockchain;
  private final ChainHeadSource heads;
  private final HeaderFetcher fetcher;
  private final BalFetcher balFetcher;
  private final Settings settings;
  private final Consumer<RestartException> restart;
  private final Function<Duration, Executor> delays;
  private final AtomicLong moves = new AtomicLong();
  private volatile boolean stopped;
  // guarded by this: the move in progress or the last one, and whether the periodic moves ended
  private CompletableFuture<Void> move = CompletableFuture.completedFuture(null);
  private boolean settled;

  /**
   * Creates the moves of an attempt.
   *
   * @param storeHeaders stores the headers of the blocks a move passes
   * @param restart ends the attempt, whose state cannot follow the chain any more
   * @param delays runs a task once a delay passed, as a final catch-up tried again
   */
  PivotMoves(
      final StateSync stateSync,
      final Consumer<List<BlockHeader>> storeHeaders,
      final MutableBlockchain blockchain,
      final ChainHeadSource heads,
      final HeaderFetcher fetcher,
      final BalFetcher balFetcher,
      final Settings settings,
      final Consumer<RestartException> restart,
      final Function<Duration, Executor> delays) {
    this.stateSync = stateSync;
    this.storeHeaders = storeHeaders;
    this.blockchain = blockchain;
    this.heads = heads;
    this.fetcher = fetcher;
    this.balFetcher = balFetcher;
    this.settings = settings;
    this.restart = restart;
    this.delays = delays;
  }

  /**
   * Moves the pivot when the head went far enough from it, or when peers stopped serving its state,
   * unless a move is in progress.
   */
  synchronized void maybeMove() {
    // nothing to move before the state download starts
    if (stopped || settled || !move.isDone() || stateSync.pivot() == null) {
      return;
    }
    final Optional<Hash> head = heads.headHash();
    if (head.isEmpty()) {
      return;
    }
    move =
        fetcher
            .byHash(head.get(), Priority.PIVOT)
            .thenCompose(header -> moveTowards(header, false))
            .whenComplete((unused, error) -> logFailure(error));
  }

  /** Ends the periodic moves; completes once a move in progress ends, whether it fails or not. */
  synchronized CompletableFuture<Void> settle() {
    settled = true;
    return move.exceptionally(error -> null);
  }

  /**
   * Moves the pivot next to the head, checking that the head descends from it, so that the tries
   * are built at a recent block of the chain. A failure is tried again later, unless the attempt
   * must start over or stopped.
   */
  CompletableFuture<Void> catchUp() {
    if (stopped) {
      return stoppedFuture();
    }
    final Optional<Hash> head = heads.headHash();
    if (head.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    return fetcher
        .byHash(head.get(), Priority.PIVOT)
        .thenCompose(header -> moveTowards(header, true))
        .exceptionallyCompose(
            error -> {
              if (stopped || endsTheAttempt(error)) {
                return CompletableFuture.failedFuture(error);
              }
              LOG.info(
                  "SNAP2 final pivot check failed, trying again: {}",
                  rootCause(error).getMessage());
              return CompletableFuture.runAsync(() -> {}, delays.apply(RETRY_DELAY))
                  .thenCompose(unused -> catchUp());
            });
  }

  /** Stops moving the pivot: nothing is stored once it returns. */
  synchronized void stop() {
    stopped = true;
  }

  /** Returns the number of moves applied. */
  long moves() {
    return moves.get();
  }

  // forced, it also checks that the head descends from the pivot, however close they are
  private CompletableFuture<Void> moveTowards(final BlockHeader head, final boolean force) {
    final BlockHeader pivot = stateSync.pivot().header();
    final long distance = head.getNumber() - pivot.getNumber();
    final boolean due = distance >= settings.pivotDistance() + settings.moveThreshold();
    if (distance <= 0 || !(force || due || stateSync.isPivotStale())) {
      return CompletableFuture.completedFuture(null);
    }
    return fetcher
        .chainDown(head, pivot.getNumber() + 1, Priority.PIVOT)
        .thenCompose(
            chain -> {
              if (!chain.getFirst().getParentHash().equals(pivot.getHash())) {
                // past finality: the state cannot go back to a block below the pivot
                return restart(
                    new RestartException(
                        "the chain reorganised below the pivot " + pivot.getNumber()));
              }
              final long passed = head.getNumber() - settings.pivotDistance() - pivot.getNumber();
              return passed <= 0
                  ? CompletableFuture.completedFuture(null)
                  : applyMoves(chain.subList(0, (int) passed));
            });
  }

  // a move over many blocks goes in steps, while the download sends no request
  private CompletableFuture<Void> applyMoves(final List<BlockHeader> headers) {
    final boolean steps = headers.size() > MAX_BLOCKS_PER_MOVE;
    if (steps) {
      stateSync.pause();
    }
    return applyStep(headers, 0)
        .whenComplete(
            (unused, error) -> {
              if (steps) {
                stateSync.resume();
              }
            });
  }

  private CompletableFuture<Void> applyStep(final List<BlockHeader> headers, final int from) {
    if (from >= headers.size()) {
      return CompletableFuture.completedFuture(null);
    }
    final List<BlockHeader> step =
        headers.subList(from, Math.min(headers.size(), from + MAX_BLOCKS_PER_MOVE));
    return balFetcher
        .fetch(step)
        .thenCompose(lists -> moveTo(step, lists))
        .thenCompose(unused -> applyStep(headers, from + MAX_BLOCKS_PER_MOVE));
  }

  // stores the blocks of a step, then moves the download to the last one
  private CompletableFuture<Void> moveTo(final List<BlockHeader> step, final List<Bytes> lists) {
    if (!store(step, lists)) {
      return stoppedFuture();
    }
    final BlockHeader target = step.getLast();
    return stateSync
        .moveTo(target, fold(lists))
        .exceptionallyCompose(
            error ->
                stopped
                    ? CompletableFuture.failedFuture(error)
                    // the download stopped with the diff partly applied
                    : restart(
                        new RestartException(
                            "the move to block " + target.getNumber() + " failed", error)))
        .thenAccept(outcome -> moved(target, outcome));
  }

  private void moved(final BlockHeader target, final StateDiffApplier.Outcome outcome) {
    moves.incrementAndGet();
    LOG.info(
        "SNAP2 pivot moved to {} | {} accounts updated, {} outside the downloaded ranges,"
            + " {} removed, {} slots, {} storages to refresh",
        block(target.getNumber()),
        count(outcome.applied()),
        count(outcome.skipped()),
        count(outcome.deleted()),
        count(outcome.slots()),
        count(outcome.staleStorage()));
  }

  // under the lock, so that a stop either waits for the writes or keeps them from starting
  private synchronized boolean store(final List<BlockHeader> step, final List<Bytes> lists) {
    if (stopped) {
      return false;
    }
    // the lists first, so that a stored header always has its list
    storeBlockAccessLists(step, lists);
    storeHeaders.accept(step);
    return true;
  }

  // kept to serve them once the node follows the chain
  private void storeBlockAccessLists(final List<BlockHeader> headers, final List<Bytes> lists) {
    final Map<Hash, SyncBlockAccessList> accessLists = new HashMap<>();
    for (int i = 0; i < headers.size(); i++) {
      accessLists.put(headers.get(i).getHash(), new SyncBlockAccessList(lists.get(i)));
    }
    blockchain.unsafeStoreSyncBlockAccessLists(accessLists);
  }

  private <T> CompletableFuture<T> restart(final RestartException exception) {
    restart.accept(exception);
    return CompletableFuture.failedFuture(exception);
  }

  // a failed move is tried again at the next check, a restart is logged where the attempt ends
  private void logFailure(final Throwable error) {
    if (error != null && !stopped && !endsTheAttempt(error)) {
      LOG.warn("SNAP2 pivot move failed, trying again: {}", rootCause(error).getMessage());
    }
  }

  private static boolean endsTheAttempt(final Throwable error) {
    return RestartException.in(error).isPresent()
        || Throwables.getCausalChain(error).stream()
            .anyMatch(CancellationException.class::isInstance);
  }

  private static <T> CompletableFuture<T> stoppedFuture() {
    return CompletableFuture.failedFuture(new CancellationException("pivot moves stopped"));
  }

  private static StateDiff fold(final List<Bytes> lists) {
    final StateDiff diff = new StateDiff();
    lists.forEach(list -> diff.add(BlockAccessList.fromBytes(list)));
    return diff;
  }
}
