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
import org.hyperledger.besu.ethereum.eth.sync.snap2.trie.StorageTrieUpdater;
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
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Downloads the flat state at the pivot: accounts, storages and bytecodes in parallel, each
 * response checked against its root and persisted only if the pivot did not move meanwhile.
 */
public final class StateSync {

  private static final Logger LOG = LoggerFactory.getLogger(StateSync.class);
  // response size of a request for one account
  private static final int SINGLE_ACCOUNT_BYTES = 4096;
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

  private final SyncStateStorage storage;
  private final RequestSender scheduler;
  private final Settings settings;
  private final StateCoverage coverage = new StateCoverage();
  private final StateWriter writer;
  // applies the pivot moves to the storage tries written while downloading
  private final StorageTrieUpdater trieUpdater;
  // writes and sends hold it shared, a pivot move or a stop exclusively
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
  // the epoch of the pivot that peers stopped serving, so that a move leaves its successor fresh
  private volatile int staleEpoch = NO_EPOCH;
  private volatile boolean stopped;
  private volatile boolean paused;
  // peers left out of state requests, a new set on each dispatch
  private volatile Set<EthPeer> excluded = Set.of();

  // bookkeeping, loop thread only
  private final List<RangeTask> accountTasks = new ArrayList<>();
  // the lowest account range not done yet
  private int firstAccountTask;
  // the storages to ask in batches, each once, in the order they came
  private final LinkedHashSet<StorageTask> storageQueue = new LinkedHashSet<>();
  // the storages whose account to ask again for its new root, each once
  private final LinkedHashSet<StorageTask> refreshQueue = new LinkedHashSet<>();
  private final Set<StorageTask> chunked = new LinkedHashSet<>();
  private final CodeQueue codeQueue;
  private final StateServers servers = new StateServers();
  private final EmptyAnswers emptyAnswers = new EmptyAnswers();
  // no peer took a bytecode request, so none would take any: no request until the periodic dispatch
  private boolean waitingForPeers;
  // no peer took a state request, as when only peers left out serve: only bytecodes until then
  private boolean waitingForStatePeers;
  private int accountsInFlight;
  private int storageInFlight;
  private int codesRequestsInFlight;
  private int refreshesInFlight;

  /**
   * Creates a download.
   *
   * @param workerThreads the threads that verify the responses; as many write them, two at least
   * @param trieBuildThreads the threads of the final trie build; a quarter of them, one to four,
   *     apply the pivot moves to the storage tries written
   */
  public StateSync(
      final SyncStateStorage storage,
      final RequestSender scheduler,
      final Settings settings,
      final int workerThreads,
      final int trieBuildThreads) {
    this(
        storage,
        scheduler,
        settings,
        Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().name("snap2-state").daemon().factory()),
        Executors.newFixedThreadPool(
            Math.max(2, workerThreads),
            Thread.ofPlatform().name("snap2-persist-", 0).daemon().factory()),
        System::nanoTime,
        (withoutMoves, trieLeft) ->
            new StorageTrieUpdater(
                storage, withoutMoves, trieLeft, Math.max(1, Math.min(4, trieBuildThreads / 4))));
  }

  /**
   * Creates a download whose bookkeeping runs on {@code loop}, and whose storage trie updater
   * {@code newTrieUpdater} makes from the guard of the writes and the tries left.
   */
  @VisibleForTesting
  StateSync(
      final SyncStateStorage storage,
      final RequestSender scheduler,
      final Settings settings,
      final ScheduledExecutorService loop,
      final ExecutorService persister,
      final LongSupplier nanoTime,
      final BiFunction<Predicate<Runnable>, Predicate<Bytes32>, StorageTrieUpdater>
          newTrieUpdater) {
    this.storage = storage;
    this.scheduler = scheduler;
    this.settings = settings;
    this.loop = loop;
    this.persister = persister;
    this.nanoTime = nanoTime;
    this.writer = new StateWriter(storage, coverage);
    this.codeQueue = new CodeQueue(storage);
    this.trieUpdater = newTrieUpdater.apply(this::withoutMoves, writer::isTrieLeft);
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
   * Moves the download to a newer pivot, applying the state changes of the blocks in between to
   * what is already persisted. A diff that fails to apply stops the download and fails it.
   */
  public CompletableFuture<StateDiffApplier.Outcome> moveTo(
      final BlockHeader newPivot, final StateDiff diff) {
    checkState(pivot != null, "the state download moves once started");
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            return exclusively(
                () -> {
                  final StateDiffApplier.Outcome outcome =
                      StateDiffApplier.apply(diff, storage, coverage, trieUpdater::changed);
                  switchPivot(newPivot);
                  return outcome;
                });
          } catch (final Throwable e) {
            // the diff is partly applied, the download cannot go on from it
            stop(e);
            throw e;
          } finally {
            execute(this::dispatch);
          }
        },
        scheduler.workers());
  }

  /** Stops asking for state while several pivot moves are applied in a row; bytecodes go on. */
  public void pause() {
    paused = true;
  }

  /** Resumes sending requests. */
  public void resume() {
    paused = false;
    execute(this::dispatch);
  }

  /** Applies the storage changes of the last moves to the tries written. */
  public void finishStorageTries() {
    trieUpdater.finish();
  }

  /**
   * Stops downloading; once it returns, nothing more is sent or written. It also releases the
   * threads of a download that completed.
   */
  public void stop() {
    stop(new CancellationException("state download stopped"));
  }

  /** Returns the current pivot. */
  public Pivot pivot() {
    return pivot;
  }

  /** Returns whether peers stopped serving the state of the pivot. */
  public boolean isPivotStale() {
    final Pivot current = pivot;
    return current != null && staleEpoch == current.epoch();
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

  /** Returns the number of storage tries written while downloading that pivot moves changed. */
  public long storageTriesUpdated() {
    return trieUpdater.updated();
  }

  /** Returns the number of storage tries whose changes from pivot moves wait to be applied. */
  public int storageTrieUpdatesPending() {
    return trieUpdater.pendingStorages();
  }

  /** Returns the number of changed slots waiting to be applied to storage tries. */
  public long storageTrieUpdateSlotsPending() {
    return trieUpdater.pendingSlots();
  }

  // the result of a write at a known pivot, or null if the pivot moved
  private <T> T atPivot(final int epoch, final Supplier<T> write) {
    lock.readLock().lock();
    try {
      // a stopped download must not touch a storage that may already be wiped for a new sync
      return stopped || pivot.epoch() != epoch ? null : write.get();
    } finally {
      lock.readLock().unlock();
    }
  }

  // runs an action exclusive with pivot moves, unless the download stopped
  private boolean withoutMoves(final Runnable action) {
    lock.readLock().lock();
    try {
      if (stopped) {
        return false;
      }
      action.run();
      return true;
    } finally {
      lock.readLock().unlock();
    }
  }

  // runs a write on the flat state exclusive with every download write
  private <T> T exclusively(final Supplier<T> write) {
    lock.writeLock().lock();
    try {
      if (stopped) {
        throw new IllegalStateException("state download stopped");
      }
      return write.get();
    } finally {
      lock.writeLock().unlock();
    }
  }

  // callers hold the exclusive lock
  private void switchPivot(final BlockHeader newPivot) {
    pivot = new Pivot(pivot.epoch() + 1, newPivot);
    execute(emptyAnswers::clear);
  }

  // fails the download with the cause, unless it ended before; the caller must not hold the lock
  private void stop(final Throwable cause) {
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
    // the requests the scheduler still holds no longer go out, before the wait for the updates
    submitted.forEach(request -> request.cancel(false));
    trieUpdater.stop();
    // last, as a callback may start a new download
    completion.completeExceptionally(cause);
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
    return accountsInFlight + storageInFlight + codesRequestsInFlight + refreshesInFlight;
  }

  private void dispatch() {
    if (stopped || completion.isDone()) {
      return;
    }
    if (!waitingForPeers) {
      // a pivot move or a stop waits for the sends under way, which read the storage too
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

    // bytecodes do not depend on the pivot: they go on while it waits for a move
    if (!isPivotStale() && !paused && !waitingForStatePeers) {
      sendRefreshes(current, target);
      sendAccountRanges(current, target);
      // bytecodes flow alongside the state
      sendCodes(target, Math.max(1, target / 8));
      sendChunks(current, target);
      sendStorageBatches(current, target);
      // the refreshes these storages asked for, rather than at the next dispatch
      sendRefreshes(current, target);
    }
    // idle capacity goes to bytecodes
    sendCodes(target, target);
  }

  private void sendRefreshes(final Pivot current, final int target) {
    while (inFlight() < target && !refreshQueue.isEmpty()) {
      final StorageTask task = refreshQueue.removeFirst();
      if (coverage.isPending(task) && !task.refreshing) {
        sendRefresh(task, current);
      }
    }
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
      } else if (task.isRootStale()) {
        // refreshed once the requests at its old root are back
        if (task.inFlight == 0) {
          requestRefresh(task);
        }
      } else if (!task.refreshing) {
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

  // the next storages to ask together, those whose root is stale going to the refresh queue
  private List<StorageTask> takeStorageBatch() {
    final List<StorageTask> batch = new ArrayList<>();
    while (batch.size() < settings.storageBatchSize() && !storageQueue.isEmpty()) {
      final StorageTask task = storageQueue.removeFirst();
      if (coverage.isPending(task)) {
        if (task.isRootStale()) {
          requestRefresh(task);
        } else {
          batch.add(task);
        }
      }
    }
    return batch;
  }

  // a storage already queued keeps its place
  private void queueStorage(final StorageTask task) {
    storageQueue.add(task);
  }

  // a storage already queued keeps its place
  private void requestRefresh(final StorageTask task) {
    if (!task.refreshing) {
      refreshQueue.add(task);
    }
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
        result -> atPivot(current.epoch(), () -> writer.accounts(request, result)),
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
    batch.forEach(task -> task.inFlight++);
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
    task.inFlight++;
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
        result -> atPivot(request.pivot.epoch(), () -> writer.storages(request, result)),
        written -> {
          storageInFlight--;
          request.tasks.forEach(task -> task.inFlight--);
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

  private void sendRefresh(final StorageTask task, final Pivot current) {
    task.refreshing = true;
    refreshesInFlight++;
    final AccountRangeRequest request =
        new AccountRangeRequest(
            current,
            task.accountHash(),
            task.accountHash(),
            SINGLE_ACCOUNT_BYTES,
            "account_refresh");
    send(
        request,
        current.epoch(),
        result -> atPivot(current.epoch(), () -> writer.refresh(task, result)),
        unused -> {
          task.refreshing = false;
          refreshesInFlight--;
          if (coverage.isPending(task) && task.chunks.isEmpty()) {
            queueStorage(task);
          }
        });
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
    // a copy: the keys of the map keep its codes, which slice the response
    return withoutMoves(() -> writer.codes(received)) ? Set.copyOf(received.keySet()) : Set.of();
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
                  onServed(peer, epoch);
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

  private void onServed(final EthPeer peer, final int epoch) {
    servers.served(peer, epoch);
    if (epoch == pivot.epoch()) {
      emptyAnswers.clear();
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
    } else if (reason == RequestFailedException.Reason.EMPTY && epoch == pivot.epoch()) {
      // only empty answers about the current pivot tell about its state, bytecode ones never do
      failure.get().peer().ifPresent(peer -> onEmpty(peer, epoch));
    }
  }

  // empty answers of peers that serve the state tell that the pivot is too old
  private void onEmpty(final EthPeer peer, final int epoch) {
    if (servers.empty(peer, epoch, nanoTime.getAsLong())) {
      LOG.debug(
          "{} does not serve the state of block {}, leaving it out for a while",
          peer.getLoggableId(),
          pivot.header().getNumber());
      emptyAnswers.forget(peer);
      return;
    }
    // only the peers that served the state and are not left out can answer empty
    final int candidates = Math.max(1, servers.serving(nanoTime.getAsLong()));
    // a peer that never served state says nothing about the age of the pivot
    if (servers.hasServed(peer) && emptyAnswers.add(peer, candidates) && staleEpoch != epoch) {
      LOG.info(
          "Peers stopped serving the state of block {}, the pivot must move",
          pivot.header().getNumber());
      staleEpoch = epoch;
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
