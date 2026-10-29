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

import static com.google.common.base.Preconditions.checkState;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.ProgressFormat.block;
import static org.hyperledger.besu.util.ExceptionUtils.rootCause;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.eth.sync.common.WrongChainException;
import org.hyperledger.besu.ethereum.eth.sync.common.checkpoint.Checkpoint;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.BodyBackfill;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.HeaderBackfill;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.HeaderFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.WriteRetry;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestFailedException;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Priority;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The chain of an attempt of the SNAP2 sync: the floor below which no body is needed, the headers
 * down to it or to genesis, the bodies and receipts of a window below the last pivot, and the chain
 * head, set once they are stored.
 */
final class SyncChain {

  private static final Logger LOG = LoggerFactory.getLogger(SyncChain.class);
  private static final Duration RESUME_DELAY = Duration.ofSeconds(5);
  // a stopped window writes nothing, its workers only end the reads they started
  private static final Duration WORKERS_TIMEOUT = Duration.ofSeconds(30);
  private static final int TOTAL_DIFFICULTIES_PER_COMMIT = 10_000;

  /**
   * Sizes of the downloads, and where proof of stake starts.
   *
   * @param headersPerRequest block headers per request
   * @param headerParallelism header requests at once
   * @param bodyWindow blocks up to the final pivot whose bodies and receipts are stored before the
   *     chain head is set
   * @param headersToCheckpointOnly whether the headers stop at the checkpoint rather than genesis
   * @param indexTransactions whether the transactions of the window are indexed
   * @param terminalTotalDifficulty every block above one whose total difficulty reaches it is proof
   *     of stake, none on a network that does not merge
   */
  record Settings(
      int headersPerRequest,
      int headerParallelism,
      int bodyWindow,
      boolean headersToCheckpointOnly,
      boolean indexTransactions,
      Optional<Difficulty> terminalTotalDifficulty) {}

  private final MutableBlockchain blockchain;
  private final ProtocolSchedule protocolSchedule;
  private final RequestSender sender;
  private final HeaderFetcher fetcher;
  // none for genesis, the floor of the sync without a checkpoint
  private final Optional<Checkpoint> checkpoint;
  private final Settings settings;
  private final MetricsSystem metricsSystem;
  private final ExecutorService executor;
  private final ExecutorService windowWorkers;
  private final Executor resumes;
  private final WriteRetry.Pause pause;
  private final int totalDifficultiesPerCommit;
  private final CompletableFuture<Void> headersDone = new CompletableFuture<>();

  private volatile BlockHeader floor;
  private volatile Difficulty floorTotal;
  // the headers above the floor then share its total difficulty
  private volatile boolean proofOfStakeAboveFloor;
  private volatile boolean cancelled;
  // guarded by this
  private CompletableFuture<BlockHeader> checkpointFetch;
  private HeaderBackfill headerDownload;
  private BodyBackfill window;
  private BlockHeader windowPivot;

  /**
   * Creates the chain of an attempt.
   *
   * @param checkpoint the trusted block below which no body is needed, genesis without one
   * @param executor runs the header download
   */
  SyncChain(
      final MutableBlockchain blockchain,
      final ProtocolSchedule protocolSchedule,
      final RequestSender sender,
      final HeaderFetcher fetcher,
      final Optional<Checkpoint> checkpoint,
      final Settings settings,
      final MetricsSystem metricsSystem,
      final ExecutorService executor) {
    this(
        blockchain,
        protocolSchedule,
        sender,
        fetcher,
        checkpoint,
        settings,
        metricsSystem,
        executor,
        Executors.newFixedThreadPool(
            BodyBackfill.Settings.forWindow(settings.indexTransactions()).parallelism(),
            Thread.ofPlatform().name("snap2-window-", 0).daemon().factory()),
        CompletableFuture.delayedExecutor(RESUME_DELAY.toMillis(), TimeUnit.MILLISECONDS),
        Thread::sleep,
        TOTAL_DIFFICULTIES_PER_COMMIT);
  }

  /**
   * Creates the chain of an attempt whose window downloads on {@code windowWorkers}, whose failed
   * header downloads start again on {@code resumes}, whose chain head writes wait {@code pause}
   * between two tries, and whose total difficulties are committed {@code
   * totalDifficultiesPerCommit} at a time.
   */
  @VisibleForTesting
  SyncChain(
      final MutableBlockchain blockchain,
      final ProtocolSchedule protocolSchedule,
      final RequestSender sender,
      final HeaderFetcher fetcher,
      final Optional<Checkpoint> checkpoint,
      final Settings settings,
      final MetricsSystem metricsSystem,
      final ExecutorService executor,
      final ExecutorService windowWorkers,
      final Executor resumes,
      final WriteRetry.Pause pause,
      final int totalDifficultiesPerCommit) {
    this.blockchain = blockchain;
    this.protocolSchedule = protocolSchedule;
    this.sender = sender;
    this.fetcher = fetcher;
    this.checkpoint = checkpoint.filter(trusted -> trusted.blockNumber() > 0);
    this.settings = settings;
    this.metricsSystem = metricsSystem;
    this.executor = executor;
    this.windowWorkers = windowWorkers;
    this.resumes = resumes;
    this.pause = pause;
    this.totalDifficultiesPerCommit = totalDifficultiesPerCommit;
  }

  /**
   * Sets the chain head on the floor, removes the canonical entries above the first pivot, stores
   * it, and starts downloading the headers below it; an attempt starts once.
   *
   * @throws RestartException when peers do not serve the checkpoint
   * @throws IllegalStateException when the checkpoint is at another height, or not below the pivot
   * @throws WrongChainException when the pivot is of proof of work above a floor that reaches the
   *     terminal total difficulty, once the head is on the floor
   * @throws StorageException when a write still fails after its tries
   * @throws CancellationException once cancelled
   */
  void start(final BlockHeader firstPivot) {
    final BlockHeader floorHeader = floorHeader();
    // the window stores the bodies above the floor only, the one of the pivot included
    if (firstPivot.getNumber() <= floorHeader.getNumber()) {
      throw new IllegalStateException(
          "pivot " + firstPivot.getNumber() + " is not above the floor " + floorHeader.getNumber());
    }
    synchronized (this) {
      throwIfCancelled();
      checkState(floor == null, "an attempt starts once");
      floor = floorHeader;
      // the chain head stays on the floor until every body above it is stored
      if (checkpoint.isPresent()) {
        // stored as the chain head
        floorTotal = checkpoint.get().totalDifficulty();
        retryWrite(() -> blockchain.unsafeStoreHeader(floorHeader, floorTotal));
      } else {
        floorTotal =
            blockchain
                .getTotalDifficultyByHash(floorHeader.getHash())
                .orElseThrow(() -> new IllegalStateException("no total difficulty for genesis"));
        retryWrite(() -> blockchain.unsafeSetChainHead(floorHeader, floorTotal));
      }
      removeIndexAbove(firstPivot.getNumber());
      // only proof of stake follows the terminal total difficulty, which Hoodi's genesis reaches
      proofOfStakeAboveFloor =
          settings.terminalTotalDifficulty().map(floorTotal::greaterOrEqualThan).orElse(false);
      writeHeaders(List.of(firstPivot));
      downloadHeaders(firstPivot);
    }
  }

  /**
   * Completes once the headers are stored down to the checkpoint, or down to genesis unless they
   * stop at the checkpoint; fails when they lead to another chain or cannot be stored.
   */
  CompletableFuture<Void> headersDone() {
    return headersDone;
  }

  /** Stores headers of the chain of the pivot, which must lead to the floor. */
  synchronized void storeHeaders(final List<BlockHeader> headers) {
    throwIfCancelled();
    checkState(floor != null, "the attempt has not started");
    writeHeaders(headers);
  }

  /**
   * Downloads the bodies and receipts of the window up to the final pivot, which must be canonical,
   * once every header is stored; completes once they are stored and recorded.
   */
  CompletableFuture<Void> downloadWindow(final BlockHeader pivot) {
    return headersDone.thenComposeAsync(unused -> startWindow(pivot), executor);
  }

  /**
   * Makes the pivot of the window the chain head, once the window is stored, and removes the
   * canonical entries above it.
   */
  synchronized void setChainHead() {
    throwIfCancelled();
    checkState(
        window != null && window.done().state() == Future.State.SUCCESS,
        "the window is not stored");
    final BlockHeader head = windowPivot;
    final Difficulty total = proofOfStakeAboveFloor ? floorTotal : storeTotalDifficulties(head);
    throwIfCancelled();
    retryWrite(() -> blockchain.unsafeSetChainHead(head, total));
    removeIndexAbove(head.getNumber());
    LOG.info(
        "SNAP2 chain head set to {} | bodies and receipts from {}",
        block(head.getNumber()),
        block(windowStart(head.getNumber())));
  }

  /** Stops the downloads and fails {@link #headersDone}; nothing is written once it returns. */
  void cancel() {
    // before the lock, which a long call holds until its next check
    cancelled = true;
    final CompletableFuture<BlockHeader> fetch;
    final HeaderBackfill runningHeaders;
    final BodyBackfill runningWindow;
    synchronized (this) {
      fetch = checkpointFetch;
      runningHeaders = headerDownload;
      runningWindow = window;
    }
    // outside the lock: the downloads run callbacks under their own locks
    if (fetch != null) {
      fetch.cancel(false);
    }
    if (runningHeaders != null) {
      runningHeaders.stop();
    }
    if (runningWindow != null) {
      runningWindow.stop();
    }
    windowWorkers.shutdownNow();
    headersDone.completeExceptionally(new CancellationException("attempt abandoned"));
  }

  /** Waits for the workers of the window to end their reads, before the storage closes. */
  void awaitStop() throws InterruptedException {
    if (!windowWorkers.awaitTermination(WORKERS_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
      LOG.warn("SNAP2 window workers still running after {}", WORKERS_TIMEOUT);
    }
  }

  private void throwIfCancelled() {
    if (cancelled) {
      throw new CancellationException("attempt abandoned");
    }
  }

  // genesis, or the checkpoint, downloaded when the node lacks it
  private BlockHeader floorHeader() {
    if (checkpoint.isEmpty()) {
      return blockchain.getGenesisBlockHeader();
    }
    final Checkpoint trusted = checkpoint.get();
    final BlockHeader header =
        blockchain
            .getBlockHeader(trusted.blockHash())
            .orElseGet(() -> fetchCheckpoint(trusted.blockHash()));
    if (header.getNumber() != trusted.blockNumber()) {
      throw new IllegalStateException(
          "checkpoint " + trusted.blockHash() + " is not at block " + trusted.blockNumber());
    }
    return header;
  }

  // peers may not serve it yet: the sync starts over rather than fails
  private BlockHeader fetchCheckpoint(final Hash hash) {
    final CompletableFuture<BlockHeader> fetch;
    synchronized (this) {
      throwIfCancelled();
      fetch = fetcher.byHash(hash, Priority.HEADERS);
      checkpointFetch = fetch;
    }
    try {
      return fetch.get();
    } catch (final InterruptedException e) {
      // the scheduler retries the request until it is cancelled
      fetch.cancel(false);
      Thread.currentThread().interrupt();
      throw new CancellationException("interrupted while downloading the checkpoint");
    } catch (final ExecutionException e) {
      throw new RestartException(
          "checkpoint " + hash + " unavailable: " + rootCause(e).getMessage(), e);
    }
  }

  // left by a failed attempt or an earlier run; those above a gap stay until a pivot passes them
  private void removeIndexAbove(final long number) {
    long highest = number;
    while (blockchain.getBlockHashByNumber(highest + 1).isPresent()) {
      highest++;
    }
    if (highest > number) {
      final long last = highest;
      retryWrite(() -> blockchain.unsafeRemoveCanonicalIndexRange(number, last));
    }
  }

  // in batches, which have no conflict check to retry
  private void writeHeaders(final List<BlockHeader> headers) {
    // headers down to genesis pass the checkpoint, which must be on their chain
    if (headers.stream()
        .anyMatch(
            header ->
                header.getNumber() == floor.getNumber()
                    && !header.getHash().equals(floor.getHash()))) {
      throw new WrongChainException(
          "checkpoint " + floor.getHash() + " is not on the chain of the pivot");
    }
    if (!proofOfStakeAboveFloor) {
      blockchain.storeBlockHeaders(headers);
      return;
    }
    // the headers above the floor share its total difficulty, those below have none
    final Map<Boolean, List<BlockHeader>> aboveFloor =
        headers.stream()
            .collect(Collectors.partitioningBy(header -> header.getNumber() > floor.getNumber()));
    if (aboveFloor.get(true).stream().anyMatch(header -> !header.getDifficulty().isZero())) {
      throw new WrongChainException(
          "proof-of-work blocks above the terminal total difficulty lead to the pivot");
    }
    if (!aboveFloor.get(false).isEmpty()) {
      blockchain.storeBlockHeaders(aboveFloor.get(false));
    }
    if (!aboveFloor.get(true).isEmpty()) {
      blockchain.storeBlockHeaders(aboveFloor.get(true), floorTotal);
    }
  }

  // above a floor followed by proof of work, the total difficulty of every header up to the head
  private Difficulty storeTotalDifficulties(final BlockHeader head) {
    Difficulty total = floorTotal;
    for (long parent = floor.getNumber();
        parent < head.getNumber();
        parent += totalDifficultiesPerCommit) {
      // a cancel waits for one commit at most
      throwIfCancelled();
      total =
          blockchain.unsafeStoreSyncTotalDifficulties(
              parent, total, Math.min(head.getNumber(), parent + totalDifficultiesPerCommit));
    }
    return total;
  }

  // the headers below a stored one, down to the checkpoint or genesis
  private synchronized void downloadHeaders(final BlockHeader top) {
    final HeaderBackfill download =
        new HeaderBackfill(
            sender,
            protocolSchedule,
            this::writeHeaders,
            fetcher,
            settings.headersToCheckpointOnly() ? floor : blockchain.getGenesisBlockHeader(),
            top,
            settings.headersPerRequest(),
            settings.headerParallelism(),
            metricsSystem);
    headerDownload = download;
    download.run(executor).whenComplete((unused, error) -> downloaded(download, error));
  }

  // runs under the lock of the pipeline of the download, so it takes no lock of its own
  private void downloaded(final HeaderBackfill download, final Throwable error) {
    if (error == null) {
      headersDone.complete(null);
      return;
    }
    // its requests in flight are no longer awaited
    download.stop();
    final Optional<Throwable> lasting =
        Throwables.getCausalChain(error).stream().filter(SyncChain::endsTheDownload).findFirst();
    if (lasting.isPresent()) {
      headersDone.completeExceptionally(lasting.get());
    } else if (!cancelled) {
      LOG.warn("SNAP2 header download failed, resuming: {}", rootCause(error).getMessage());
      final long lowestStored = download.lowestStored();
      CompletableFuture.runAsync(() -> resumeBelow(lowestStored), resumes)
          .exceptionally(
              failure -> {
                headersDone.completeExceptionally(failure);
                return null;
              });
    }
  }

  // another chain, the storage, or a stopped scheduler: a resume would end the same way
  private static boolean endsTheDownload(final Throwable failure) {
    return failure instanceof WrongChainException
        || failure instanceof StorageException
        || (failure instanceof RequestFailedException request
            && request.reason() == RequestFailedException.Reason.STOPPED);
  }

  // starts again below the lowest header the failed download stored
  private synchronized void resumeBelow(final long lowestStored) {
    if (cancelled) {
      return;
    }
    downloadHeaders(
        blockchain
            .getBlockHeader(lowestStored)
            .orElseThrow(() -> new IllegalStateException("lost the lowest stored header")));
  }

  private synchronized CompletableFuture<Void> startWindow(final BlockHeader pivot) {
    throwIfCancelled();
    checkState(window == null, "the window downloads once");
    checkState(
        blockchain.blockIsOnCanonicalChain(pivot.getHash()),
        "pivot %s is not canonical",
        pivot.getNumber());
    final long from = windowStart(pivot.getNumber());
    window =
        new BodyBackfill(
            blockchain,
            protocolSchedule,
            sender,
            () -> true,
            pivot.getNumber() + 1,
            head -> from,
            BodyBackfill.Settings.forWindow(settings.indexTransactions()),
            metricsSystem);
    windowPivot = pivot;
    window.start(windowWorkers);
    return window.done();
  }

  // the lowest block whose body and receipts are needed before the head is set at this pivot
  private long windowStart(final long pivot) {
    return Math.max(floor.getNumber() + 1, pivot - settings.bodyWindow() + 1);
  }

  // a write in a transaction that RocksDB checks for conflicts
  private void retryWrite(final Runnable write) {
    WriteRetry.run(write, pause);
  }
}
