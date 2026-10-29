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
package org.hyperledger.besu.ethereum.eth.sync.snap2.state;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.sync.StorageExceptionManager;
import org.hyperledger.besu.ethereum.eth.sync.snap2.ProgressFormat;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestFailedException;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Protocol;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStateStorage;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Downloads the flat state at the pivot: accounts, storages and bytecodes in parallel, each
 * response checked against its root before it is written.
 */
public final class StateSync {

  private static final Logger LOG = LoggerFactory.getLogger(StateSync.class);
  // pending storages or bytecodes above which no account range is asked
  private static final int STORAGE_BACKLOG = 50_000;
  private static final int NO_EPOCH = -1;
  // a slot in an answer, its hash and its value with their encoding
  private static final int BYTES_PER_SLOT = 64;
  private static final long DISPATCH_PERIOD_MILLIS = 500;

  /**
   * Sizes of the requests.
   *
   * @param accountChunks ranges the account key space is split into, downloaded lowest first
   * @param responseBytes soft response size of the account, storage and bytecode requests
   * @param storageBatchSize storages asked in one request
   * @param maxStorageChunks slot ranges a storage too large for one response is split into
   * @param codeBatchSize bytecodes asked in one request
   */
  public record Settings(
      int accountChunks,
      int responseBytes,
      int storageBatchSize,
      int maxStorageChunks,
      int codeBatchSize) {

    /** Checks the sizes: a zero would leave nothing to ask, or a queue that never drains. */
    @SuppressWarnings("MethodInputParametersMustBeFinal") // none is final in a compact constructor
    public Settings {
      checkArgument(accountChunks > 0, "accountChunks must be positive");
      checkArgument(responseBytes > 0, "responseBytes must be positive");
      checkArgument(storageBatchSize > 0, "storageBatchSize must be positive");
      checkArgument(maxStorageChunks > 0, "maxStorageChunks must be positive");
      checkArgument(codeBatchSize > 0, "codeBatchSize must be positive");
    }
  }

  private final RequestSender scheduler;
  private final Settings settings;
  private final StateCoverage coverage = new StateCoverage();
  private final StateWriter writer;
  // writes and sends hold it shared, a stop exclusively: after the stop, nothing is sent or written
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final ScheduledExecutorService loop;
  // writes the verified responses, off the threads that verify them
  private final ExecutorService persister;
  private final LongSupplier nanoTime;
  private final CompletableFuture<Void> completion = new CompletableFuture<>();
  // the requests the scheduler holds, which a stop withdraws
  private final Set<CompletableFuture<?>> submitted = ConcurrentHashMap.newKeySet();
  private volatile Pivot pivot;
  private volatile Instant started = Instant.now();
  private volatile boolean stopped;
  // peers left out of state requests, a new set on each dispatch
  private volatile Set<EthPeer> excluded = Set.of();

  // bookkeeping, loop thread only
  private final List<RangeTask> accountTasks = new ArrayList<>();
  // the lowest account range not done yet
  private int firstAccountTask;
  // the storages to ask in batches, each once, in the order they came
  private final LinkedHashSet<StorageTask> storageQueue = new LinkedHashSet<>();
  private final Set<StorageTask> chunked = new LinkedHashSet<>();
  private final CodeQueue codeQueue;
  private final StateServers servers = new StateServers();
  // no peer took a bytecode request, so none would take any: no request until the periodic dispatch
  private boolean waitingForPeers;
  // no peer took a state request, as when only peers left out serve: only bytecodes until then
  private boolean waitingForStatePeers;
  private int accountsInFlight;
  private int storageInFlight;
  private int codesRequestsInFlight;

  /**
   * Creates a download.
   *
   * @param workerThreads the threads that verify the responses; as many write them, two at least
   */
  public StateSync(
      final SyncStateStorage storage,
      final RequestSender scheduler,
      final Settings settings,
      final int workerThreads) {
    this(
        storage,
        scheduler,
        settings,
        Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().name("snap2-state").daemon().factory()),
        Executors.newFixedThreadPool(
            Math.max(2, workerThreads),
            Thread.ofPlatform().name("snap2-persist-", 0).daemon().factory()),
        System::nanoTime);
  }

  /** Creates a download whose bookkeeping runs on {@code loop}. */
  @VisibleForTesting
  StateSync(
      final SyncStateStorage storage,
      final RequestSender scheduler,
      final Settings settings,
      final ScheduledExecutorService loop,
      final ExecutorService persister,
      final LongSupplier nanoTime) {
    this.scheduler = scheduler;
    this.settings = settings;
    this.loop = loop;
    this.persister = persister;
    this.nanoTime = nanoTime;
    this.writer = new StateWriter(storage, coverage);
    this.codeQueue = new CodeQueue(storage);
  }

  /** Starts downloading the state of the given pivot, nothing once stopped. */
  public CompletableFuture<Void> start(final BlockHeader pivotHeader) {
    checkState(pivot == null, "the state download starts once");
    if (stopped) {
      // its completion failed with the stop
      return completion;
    }
    pivot = new Pivot(0, pivotHeader);
    started = Instant.now();
    execute(
        () -> {
          RangeManager.generateAllRanges(settings.accountChunks())
              .forEach((from, to) -> accountTasks.add(new RangeTask(from, to)));
          LOG.info(
              "SNAP2 state download started at {} | {} account ranges",
              ProgressFormat.block(pivotHeader.getNumber()),
              ProgressFormat.count(accountTasks.size()));
          dispatch();
        });
    try {
      loop.scheduleWithFixedDelay(
          this::dispatchPeriodically,
          DISPATCH_PERIOD_MILLIS,
          DISPATCH_PERIOD_MILLIS,
          TimeUnit.MILLISECONDS);
    } catch (final RejectedExecutionException e) {
      // a stop on another thread failed the completion meanwhile
      LOG.trace("State loop rejected the periodic dispatch", e);
    }
    return completion;
  }

  /**
   * Stops downloading; once it returns, nothing more is sent or written. It also releases the
   * threads of a download that completed.
   */
  public void stop() {
    // waits for the writes and the sends under way
    lock.writeLock().lock();
    try {
      stopped = true;
    } finally {
      lock.writeLock().unlock();
    }
    // no interrupt: a completion callback that stops the download runs on these threads
    loop.shutdown();
    persister.shutdown();
    // the requests the scheduler still holds no longer go out
    submitted.forEach(request -> request.cancel(false));
    completion.completeExceptionally(new CancellationException("state download stopped"));
  }

  /** Returns the current pivot. */
  public Pivot pivot() {
    return pivot;
  }

  /** Returns the time since the download started. */
  public Duration elapsed() {
    return Duration.between(started, Instant.now());
  }

  /** Returns the share of the account key space downloaded, from 0 to 1. */
  public double accountCoverage() {
    return coverage.accounts().coverage();
  }

  /** Returns the number of accounts whose storage is still to download. */
  public int pendingStorageCount() {
    return coverage.pendingStorageCount();
  }

  /** Returns the number of peers left out of state requests because they serve no state. */
  public int peersWithoutState() {
    return excluded.size();
  }

  /** Returns the number of accounts written. */
  public long downloadedAccounts() {
    return writer.accountsWritten();
  }

  /** Returns the number of slots written. */
  public long downloadedSlots() {
    return writer.slotsWritten();
  }

  /** Returns the number of bytecodes written. */
  public long downloadedCodes() {
    return writer.codesWritten();
  }

  /** Returns the number of storage tries written while downloading. */
  public long storageTriesBuilt() {
    return writer.storageTriesWritten();
  }

  /** Returns the number of slots in the storage tries written while downloading. */
  public long storageTrieSlots() {
    return writer.storageTrieSlots();
  }

  /** Returns the number of storages whose trie the download left to the final build. */
  public long storageTriesLeft() {
    return writer.storageTriesLeft();
  }

  /** Returns whether the download completed the storage of an account without writing its trie. */
  public boolean storageTrieLeft(final Bytes32 accountHash) {
    return writer.isTrieLeft(accountHash);
  }

  // the result of a write, or null once the download stopped
  private <T> T unlessStopped(final Supplier<T> write) {
    lock.readLock().lock();
    try {
      // a stopped download must not touch a storage that may already be wiped for a new sync
      return stopped ? null : write.get();
    } finally {
      lock.readLock().unlock();
    }
  }

  // nothing runs once the loop is stopped
  private void execute(final Runnable task) {
    try {
      loop.execute(() -> runGuarded(task));
    } catch (final RejectedExecutionException e) {
      LOG.trace("State loop rejected a task", e);
    }
  }

  // on a timer, for what no response triggers: peers back from being left out, more capacity
  private void dispatchPeriodically() {
    runGuarded(
        () -> {
          waitingForPeers = false;
          waitingForStatePeers = false;
          dispatch();
        });
  }

  // a failed task leaves the bookkeeping unsure, so it fails the download; a scheduled executor
  // would keep the failure to itself, and stop the periodic dispatch
  private void runGuarded(final Runnable task) {
    try {
      task.run();
    } catch (final Throwable e) {
      completion.completeExceptionally(e);
    }
  }

  private int inFlight() {
    return accountsInFlight + storageInFlight + codesRequestsInFlight;
  }

  private void dispatch() {
    if (stopped || completion.isDone()) {
      return;
    }
    if (!waitingForPeers) {
      // a stop waits for the sends under way, which read the storage too
      lock.readLock().lock();
      try {
        if (!stopped) {
          sendRequests();
        }
      } finally {
        lock.readLock().unlock();
      }
    }
    // after the sends, which may find every queued bytecode stored; outside the lock, as a
    // completion callback may stop the download
    if (isDownloaded()) {
      complete();
    }
  }

  private void sendRequests() {
    excluded = servers.excluded(nanoTime.getAsLong());
    final Pivot current = pivot;
    final int capacity = Math.max(1, scheduler.capacity(Protocol.SNAP));
    // more than the peers take now, so that a peer gets its next request at once
    final int target = capacity + capacity / 4;

    if (!waitingForStatePeers) {
      sendAccountRanges(current, target);
      // bytecodes flow alongside the state
      sendCodes(target, Math.max(1, target / 8));
      sendChunks(current, target);
      sendStorageBatches(current, target);
    }
    // idle capacity goes to bytecodes
    sendCodes(target, target);
  }

  // lowest ranges first: the state written in key order is not rewritten at each database level
  private void sendAccountRanges(final Pivot current, final int target) {
    // accounts feed storages and bytecodes: they wait while those lag, to bound memory
    final boolean backlogged =
        coverage.pendingStorageCount() > STORAGE_BACKLOG || codeQueue.size() > STORAGE_BACKLOG;
    final int share = backlogged ? 0 : Math.max(1, target / 3);
    while (firstAccountTask < accountTasks.size() && accountTasks.get(firstAccountTask).done) {
      firstAccountTask++;
    }
    for (int i = firstAccountTask;
        i < accountTasks.size() && inFlight() < target && accountsInFlight < share;
        i++) {
      final RangeTask task = accountTasks.get(i);
      if (!task.done && !task.inFlight) {
        sendAccounts(task, current);
      }
    }
  }

  // sends bytecode requests while the target and the share leave room, until the queue is empty
  private void sendCodes(final int target, final int share) {
    while (inFlight() < target && codesRequestsInFlight < share && !codeQueue.isEmpty()) {
      final List<Bytes32> batch = codeQueue.take(settings.codeBatchSize());
      if (!batch.isEmpty()) {
        sendCodeBatch(batch);
      }
    }
  }

  // the slot ranges of the storages too large for one response
  private void sendChunks(final Pivot current, final int target) {
    final Iterator<StorageTask> tasks = chunked.iterator();
    while (tasks.hasNext() && inFlight() < target) {
      final StorageTask task = tasks.next();
      if (!coverage.isPending(task)) {
        tasks.remove();
      } else {
        for (int i = 0; i < task.chunks.size() && inFlight() < target; i++) {
          final RangeTask chunk = task.chunks.get(i);
          if (!chunk.done && !chunk.inFlight) {
            sendStorageChunk(task, chunk, current);
          }
        }
      }
    }
  }

  private void sendStorageBatches(final Pivot current, final int target) {
    while (inFlight() < target && !storageQueue.isEmpty()) {
      final List<StorageTask> batch = takeStorageBatch();
      if (!batch.isEmpty()) {
        sendStorageBatch(batch, current);
      }
    }
  }

  // the next storages to ask together
  private List<StorageTask> takeStorageBatch() {
    final List<StorageTask> batch = new ArrayList<>();
    while (batch.size() < settings.storageBatchSize() && !storageQueue.isEmpty()) {
      final StorageTask task = storageQueue.removeFirst();
      if (coverage.isPending(task)) {
        batch.add(task);
      }
    }
    return batch;
  }

  // a storage already queued keeps its place
  private void queueStorage(final StorageTask task) {
    storageQueue.add(task);
  }

  private void sendAccounts(final RangeTask task, final Pivot current) {
    task.inFlight = true;
    accountsInFlight++;
    final AccountRangeRequest request =
        new AccountRangeRequest(
            current, task.next, task.limit, settings.responseBytes(), "account_range");
    send(
        request,
        current.epoch(),
        result -> unlessStopped(() -> writer.accounts(request, result)),
        written -> {
          task.inFlight = false;
          accountsInFlight--;
          if (written != null) {
            if (written.done()) {
              task.done = true;
            } else {
              task.next = KeyRangeSet.next(written.end()).orElseThrow();
            }
            written.storageTasks().forEach(this::queueStorage);
            written.codeHashes().forEach(codeQueue::add);
          }
        });
  }

  private void sendStorageBatch(final List<StorageTask> batch, final Pivot current) {
    final StorageRangesRequest request =
        new StorageRangesRequest(
            current,
            batch,
            RangeManager.MIN_RANGE,
            RangeManager.MAX_RANGE,
            settings.responseBytes(),
            "storage_ranges");
    sendStorage(request, written -> afterBatch(request, written));
  }

  private void sendStorageChunk(
      final StorageTask task, final RangeTask chunk, final Pivot current) {
    chunk.inFlight = true;
    final StorageRangesRequest request =
        new StorageRangesRequest(
            current,
            List.of(task),
            chunk.next,
            chunk.limit,
            settings.responseBytes(),
            "storage_chunk");
    sendStorage(request, written -> afterChunk(chunk, written));
  }

  private void sendStorage(
      final StorageRangesRequest request, final Consumer<StateWriter.StorageWritten> after) {
    storageInFlight++;
    send(
        request,
        request.pivot.epoch(),
        result -> unlessStopped(() -> writer.storages(request, result)),
        written -> {
          storageInFlight--;
          after.accept(written);
        });
  }

  // a response serves the first storages asked, in order, the last one possibly cut short
  private void afterBatch(
      final StorageRangesRequest request, final StateWriter.StorageWritten written) {
    final int served = written == null ? 0 : written.served();
    for (int i = 0; i < request.tasks.size(); i++) {
      final StorageTask task = request.tasks.get(i);
      if (!coverage.isPending(task)) {
        continue;
      }
      if (i >= served) {
        // not served, asked again
        queueStorage(task);
      } else if (i == served - 1 && !written.lastComplete()) {
        // the last one served, cut short
        splitIntoChunks(task, written.lastSlot(), written.lastReturned());
      }
    }
  }

  private void afterChunk(final RangeTask chunk, final StateWriter.StorageWritten written) {
    chunk.inFlight = false;
    if (written == null) {
      return;
    }
    if (written.lastComplete()) {
      chunk.done = true;
    } else {
      chunk.next = KeyRangeSet.next(written.lastSlot()).orElseThrow();
    }
  }

  // slot ranges downloaded in parallel, as many as the density of the slots so far suggests
  private void splitIntoChunks(final StorageTask task, final Bytes32 lastSlot, final int returned) {
    final Bytes32 start = KeyRangeSet.next(lastSlot).orElseThrow();
    final double covered = KeyRangeSet.fraction(RangeManager.MIN_RANGE, lastSlot);
    final double remainingSlots = returned * (1 - covered) / covered;
    final double requests = remainingSlots * BYTES_PER_SLOT / settings.responseBytes();
    // a range for every four requests left
    final int parts = (int) Math.clamp(Math.ceil(requests / 4), 1, settings.maxStorageChunks());
    RangeManager.generateRanges(start, RangeManager.MAX_RANGE, parts)
        .forEach((from, to) -> task.chunks.add(new RangeTask(from, to)));
    chunked.add(task);
  }

  private void sendCodeBatch(final List<Bytes32> batch) {
    codesRequestsInFlight++;
    send(
        new ByteCodesRequest(batch, settings.responseBytes()),
        NO_EPOCH,
        this::writeCodes,
        stored -> {
          codesRequestsInFlight--;
          codeQueue.answered(batch, stored == null ? Set.of() : stored);
        });
  }

  private Set<Bytes32> writeCodes(final Map<Bytes32, Bytes> received) {
    return unlessStopped(
        () -> {
          writer.codes(received);
          // a copy: the keys of the map keep its codes, which slice the response
          return Set.copyOf(received.keySet());
        });
  }

  /**
   * Sends a request, writes its response off the threads that verify it, then runs the bookkeeping
   * on the loop with what was written, or null if nothing was.
   *
   * @param epoch the pivot the request is about, or {@link #NO_EPOCH}
   */
  private <T, R> void send(
      final SyncRequest<T> request,
      final int epoch,
      final Function<T, R> write,
      final Consumer<R> bookkeeping) {
    final CompletableFuture<RequestSender.Response<T>> sent =
        scheduler.submit(request, epoch == NO_EPOCH ? Set.of() : excluded);
    submitted.add(sent);
    sent.whenCompleteAsync(
        (response, error) -> {
          submitted.remove(sent);
          // the peer alone, so that the loop does not keep the response
          final EthPeer peer = error == null ? response.peer() : null;
          final R written =
              error == null ? writeSafely(() -> write.apply(response.value()), request) : null;
          execute(
              () -> {
                if (error != null) {
                  onFailure(error, epoch);
                } else if (epoch != NO_EPOCH) {
                  servers.served(peer, epoch);
                }
                bookkeeping.accept(written);
                dispatch();
              });
        },
        persister);
  }

  // a failed write leaves its request to be asked again, unless no retry can fix it
  private <R> R writeSafely(final Supplier<R> write, final SyncRequest<?> request) {
    try {
      return write.get();
    } catch (final Throwable e) {
      if (e instanceof StorageException failure
          && StorageExceptionManager.canRetryOnError(failure)) {
        LOG.debug("Failed to persist {}, asking it again: {}", request.name(), e.getMessage());
      } else {
        // it would come back with every answer
        completion.completeExceptionally(e);
      }
      return null;
    }
  }

  private void onFailure(final Throwable error, final int epoch) {
    final Optional<RequestFailedException> failure = RequestFailedException.from(error);
    if (failure.isEmpty()) {
      // the code of the request failed, it would on any peer
      completion.completeExceptionally(error);
      return;
    }
    LOG.trace("State request failed: {}", failure.get().getMessage());
    final RequestFailedException.Reason reason = failure.get().reason();
    if (reason == RequestFailedException.Reason.STOPPED) {
      // no request goes out any more
      completion.completeExceptionally(failure.get());
    } else if (reason == RequestFailedException.Reason.NO_PEER) {
      // the next requests would fail too: they wait for the periodic dispatch
      if (epoch == NO_EPOCH) {
        waitingForPeers = true;
      } else {
        waitingForStatePeers = true;
      }
    } else if (reason == RequestFailedException.Reason.EMPTY && epoch != NO_EPOCH) {
      // an empty bytecode answer says nothing about the state of the pivot
      failure.get().peer().ifPresent(peer -> onEmpty(peer, epoch));
    }
  }

  // a peer that answers nothing for a pivot others serve is left out of state requests a while
  private void onEmpty(final EthPeer peer, final int epoch) {
    if (servers.empty(peer, epoch, nanoTime.getAsLong())) {
      LOG.debug(
          "{} does not serve the state of block {}, leaving it out for a while",
          peer.getLoggableId(),
          pivot.header().getNumber());
    }
  }

  // every account, slot and bytecode of the pivot is persisted
  private boolean isDownloaded() {
    return coverage.accounts().isFull()
        && coverage.pendingStorageCount() == 0
        && codeQueue.isEmpty()
        && inFlight() == 0;
  }

  private void complete() {
    LOG.info(
        "SNAP2 state download complete at {} in {} | accounts {} | slots {} | bytecodes {}"
            + " | storage tries {} built, {} left to the final build",
        ProgressFormat.block(pivot.header().getNumber()),
        ProgressFormat.duration(elapsed()),
        ProgressFormat.compact(writer.accountsWritten()),
        ProgressFormat.compact(writer.slotsWritten()),
        ProgressFormat.compact(writer.codesWritten()),
        ProgressFormat.compact(writer.storageTriesWritten()),
        ProgressFormat.compact(writer.storageTriesLeft()));
    completion.complete(null);
  }
}
