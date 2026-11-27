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
package org.hyperledger.besu.ethereum.eth.sync.snap2.storage;

import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SortedWriter;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes the accounts and slots of a SNAP2 download as sorted files.
 *
 * <p>The account ranges are downloaded lowest first, many at a time. The accounts of a range wait
 * in memory until every range before it is complete, and then go into the account file in key
 * order. The accounts with storage are noted in that order, and the slots of an account go into the
 * storage file once its storage is complete and every account before it is written. A storage that
 * grows too large to wait gets files of its own, one per stream of requests, and the storage file
 * ends before it so that the files do not overlap. When too much waits altogether, the account that
 * is waited for is given up: what it still gets goes through the regular write path. The runs of a
 * storage with files of its own count as waiting until they are worth a file, and go through the
 * regular write path first when they hold more than the storages waiting for their turn.
 *
 * <p>Before anything reads or overwrites the state, {@link #flush()} writes out what waits through
 * the regular write path and stores every file: a file stored after a write to one of its keys
 * would hide that write.
 */
public final class SortedStateIngest implements SyncStateStorage.SortedWrites {

  private static final Logger LOG = LoggerFactory.getLogger(SortedStateIngest.class);
  private static final long MEGABYTE = 1024L * 1024;
  // what is less than a file is worth goes through the regular write path
  static final long MIN_FILE_BYTES = MEGABYTE;
  // a file is ended at this size, so that later compactions work on files of a usual size
  static final long MAX_FILE_BYTES = 128 * MEGABYTE;
  private static final long MIN_MEMORY_LIMIT = 64 * MEGABYTE;
  private static final long MAX_MEMORY_LIMIT = 2048 * MEGABYTE;

  private enum Mode {
    // waits for its turn to go into the storage file
    WAIT,
    // has files of its own
    OWN_FILES,
    // goes through the regular write path
    REGULAR
  }

  private record Piece(Object stream, byte[][] keys, byte[][] values, long bytes) {}

  private static final class AccountRange {
    private final List<byte[]> keys = new ArrayList<>();
    private final List<byte[]> values = new ArrayList<>();
    private final List<Bytes32> withStorage = new ArrayList<>();
    private long bytes;
    private boolean done;
    private boolean regular;
  }

  private final class AccountStorage {
    private final byte[] prefix;
    private Mode mode = Mode.WAIT;
    private final List<Piece> pieces = new ArrayList<>();
    private final IdentityHashMap<Object, SortedRun> streams = new IdentityHashMap<>();
    private long bytes;
    private boolean complete;
    private boolean passed;

    private AccountStorage(final Bytes32 accountHash) {
      this.prefix = accountHash.toArray();
    }

    private SortedRun stream(final Object stream) {
      return streams.computeIfAbsent(
          stream,
          unused ->
              new SortedRun(
                  storage, storageSegment, MIN_FILE_BYTES, stats, change -> ownRunBytes += change));
    }

    private void endStreams() {
      streams.values().forEach(SortedStateIngest.this::end);
      streams.clear();
    }
  }

  private final SegmentedKeyValueStorage storage;
  private final SegmentIdentifier accountSegment;
  private final SegmentIdentifier storageSegment;
  private final long memoryLimit;
  private final long accountLimit;
  private final SortedIngestStats stats;
  private final ExecutorService finisher =
      Executors.newSingleThreadExecutor(
          Thread.ofPlatform().name("snap2-sorted-files").daemon().factory());

  // guarded by this
  private final TreeMap<Bytes32, AccountRange> ranges = new TreeMap<>();
  private final ArrayDeque<Bytes32> storageOrder = new ArrayDeque<>();
  private final Map<Bytes32, AccountStorage> storages = new HashMap<>();
  private Predicate<Bytes32> storagePending = unused -> false;
  // the last account whose turn in the storage file has passed
  private Bytes32 lastPassed;
  private SortedRun accountRun;
  private SortedRun storageRun;
  // bytes waiting in memory, of accounts and of slots, and held by the runs of storages with files
  // of their own until they are worth a file
  private long waitingAccounts;
  private long waitingStorage;
  private long ownRunBytes;
  private boolean closed;
  private volatile Throwable failure;

  SortedStateIngest(
      final SegmentedKeyValueStorage storage,
      final SegmentIdentifier accountSegment,
      final SegmentIdentifier storageSegment,
      final SortedIngestStats stats) {
    this(storage, accountSegment, storageSegment, memoryLimit(), stats);
  }

  @VisibleForTesting
  SortedStateIngest(
      final SegmentedKeyValueStorage storage,
      final SegmentIdentifier accountSegment,
      final SegmentIdentifier storageSegment,
      final long memoryLimit,
      final SortedIngestStats stats) {
    this.storage = storage;
    this.accountSegment = accountSegment;
    this.storageSegment = storageSegment;
    this.memoryLimit = memoryLimit;
    this.accountLimit = memoryLimit / 8;
    this.stats = stats;
  }

  // what waits takes about twice its size on the heap: a twelfth of the heap may wait
  private static long memoryLimit() {
    return Math.clamp(Runtime.getRuntime().maxMemory() / 12, MIN_MEMORY_LIMIT, MAX_MEMORY_LIMIT);
  }

  @Override
  public synchronized void start(
      final List<Bytes32> rangeLimits, final Predicate<Bytes32> storagePending) {
    rangeLimits.forEach(limit -> ranges.put(limit.copy(), new AccountRange()));
    this.storagePending = storagePending;
    LOG.info(
        "Writing the SNAP2 state as sorted files, with up to {} MB waiting for its turn",
        memoryLimit / MEGABYTE);
  }

  @Override
  public synchronized void accounts(
      final Bytes32 rangeLimit,
      final List<Bytes32> hashes,
      final List<Bytes> accounts,
      final List<Bytes32> withStorage,
      final boolean rangeDone) {
    if (closed) {
      return;
    }
    final AccountRange range = ranges.get(rangeLimit);
    if (range == null) {
      // the range was passed already, which only a complete range is
      writeRegular(accountSegment, keys(hashes), values(accounts));
      return;
    }
    if (range.regular) {
      writeRegular(accountSegment, keys(hashes), values(accounts));
    } else {
      for (int i = 0; i < hashes.size(); i++) {
        final byte[] key = hashes.get(i).toArray();
        final byte[] value = accounts.get(i).toArray();
        range.keys.add(key);
        range.values.add(value);
        range.bytes += key.length + value.length;
        waitingAccounts += key.length + value.length;
      }
    }
    withStorage.forEach(hash -> range.withStorage.add(hash.copy()));
    range.done |= rangeDone;
    advanceAccounts();
    advanceStorage();
    limitMemory();
  }

  @Override
  public synchronized void storage(
      final Bytes32 accountHash,
      final Object stream,
      final List<Bytes32> slotHashes,
      final List<Bytes32> values) {
    if (closed || slotHashes.isEmpty()) {
      return;
    }
    if (isPassed(accountHash)) {
      // its turn is over, what it still gets goes through the regular write path
      toRegular(accountHash);
    }
    final AccountStorage account =
        storages.computeIfAbsent(accountHash.copy(), AccountStorage::new);
    final byte[][] keys = new byte[slotHashes.size()][];
    final byte[][] slotValues = new byte[slotHashes.size()][];
    long bytes = 0;
    for (int i = 0; i < keys.length; i++) {
      keys[i] =
          com.google.common.primitives.Bytes.concat(
              account.prefix, slotHashes.get(i).toArrayUnsafe());
      slotValues[i] = values.get(i).toArray();
      bytes += keys[i].length + slotValues[i].length;
    }
    switch (account.mode) {
      case REGULAR -> writeRegular(storageSegment, Arrays.asList(keys), Arrays.asList(slotValues));
      case OWN_FILES -> putOwn(account, stream, keys, slotValues);
      case WAIT -> {
        account.pieces.add(new Piece(stream, keys, slotValues, bytes));
        account.bytes += bytes;
        waitingStorage += bytes;
        if (account.bytes > accountLimit) {
          toOwnFiles(account);
        }
      }
    }
    advanceStorage();
    limitMemory();
  }

  @Override
  public synchronized void storageComplete(final Bytes32 accountHash) {
    if (closed) {
      return;
    }
    if (isPassed(accountHash)) {
      toRegular(accountHash);
      storages.remove(accountHash);
      return;
    }
    final AccountStorage account =
        storages.computeIfAbsent(accountHash.copy(), AccountStorage::new);
    account.complete = true;
    if (account.mode == Mode.OWN_FILES) {
      account.endStreams();
    }
    if (account.passed) {
      storages.remove(accountHash);
    }
    advanceStorage();
  }

  @Override
  public void storageRemoved(final Bytes32 accountHash) {
    final boolean inFiles;
    synchronized (this) {
      if (closed) {
        return;
      }
      // the slots of a passed account may be in the storage file
      final boolean passed = lastPassed != null && accountHash.compareTo(lastPassed) <= 0;
      final AccountStorage account = storages.get(accountHash);
      inFiles = passed || (account != null && account.mode == Mode.OWN_FILES);
      if (account != null) {
        drainPieces(account, piece -> {});
        account.streams.values().forEach(SortedRun::discard);
        account.streams.clear();
        // its slots are gone, nothing is waited for any more
        account.complete = true;
        account.mode = Mode.REGULAR;
        if (account.passed) {
          storages.remove(accountHash);
        }
      }
      if (passed) {
        storageRun = end(storageRun);
      }
      advanceStorage();
    }
    if (inFiles) {
      // its slots in files are removed through reads, which see a file once it is stored
      awaitFiles();
    }
  }

  @Override
  public void flush() {
    synchronized (this) {
      if (closed) {
        return;
      }
      writeWaitingRanges();
      accountRun = end(accountRun);
      for (final AccountStorage account : storages.values()) {
        switch (account.mode) {
          case WAIT -> {
            if (!account.pieces.isEmpty()) {
              giveUp(account);
            }
          }
          case OWN_FILES -> account.endStreams();
          case REGULAR -> {}
        }
      }
      storageRun = end(storageRun);
      advanceStorage();
    }
    awaitFiles();
    LOG.debug("Sorted SNAP2 state: {}", stats);
  }

  @Override
  public void close() {
    final boolean first;
    synchronized (this) {
      first = !closed;
      if (first) {
        closed = true;
        if (accountRun != null) {
          accountRun.discard();
        }
        if (storageRun != null) {
          storageRun.discard();
        }
        storages.values().forEach(account -> account.streams.values().forEach(SortedRun::discard));
        storages.clear();
        ranges.clear();
        storageOrder.clear();
      }
    }
    if (first) {
      for (final Runnable notStarted : finisher.shutdownNow()) {
        if (notStarted instanceof StoreFile file) {
          // its writer still holds native memory and a file on disk
          file.writer.rollback();
        } else if (notStarted instanceof Future<?> await) {
          // a flush waiting for it would wait forever
          await.cancel(false);
        }
      }
    }
    // a file stored after this returns could land in the state of the next sync attempt, which
    // wipes the state first: every call waits, not only the first
    try {
      while (!finisher.awaitTermination(1, TimeUnit.MINUTES)) {
        LOG.warn("Waiting for a sorted SNAP2 state file still being stored after the sync stopped");
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  // ranges in key order go into the account file once every range before them is complete
  private void advanceAccounts() {
    while (!ranges.isEmpty() && ranges.firstEntry().getValue().done) {
      final AccountRange range = ranges.pollFirstEntry().getValue();
      if (range.regular) {
        // its accounts were written already, the file ends before them
        accountRun = end(accountRun);
      } else {
        for (int i = 0; i < range.keys.size(); i++) {
          if (accountRun == null) {
            accountRun = newRun(accountSegment);
          }
          accountRun.put(range.keys.get(i), range.values.get(i));
          if (accountRun.size() >= MAX_FILE_BYTES) {
            accountRun = end(accountRun);
          }
        }
        waitingAccounts -= range.bytes;
      }
      storageOrder.addAll(range.withStorage);
    }
  }

  // storages in account order go into the storage file once complete
  private void advanceStorage() {
    while (!storageOrder.isEmpty()) {
      final Bytes32 accountHash = storageOrder.peekFirst();
      final AccountStorage account = storages.get(accountHash);
      if (account == null) {
        if (storagePending.test(accountHash)) {
          return;
        }
        // nothing was downloaded and nothing will be
        lastPassed = storageOrder.pollFirst();
        continue;
      }
      if (account.mode == Mode.WAIT) {
        if (!account.complete) {
          return;
        }
        account.pieces.sort(Comparator.comparing(piece -> Bytes.wrap(piece.keys[0])));
        drainPieces(account, this::putInStorageFile);
        storages.remove(accountHash);
        lastPassed = storageOrder.pollFirst();
        continue;
      }
      // its slots are elsewhere: the storage file ends before them
      storageRun = end(storageRun);
      account.passed = true;
      if (account.complete) {
        // a piece that came after its storage completed opened a run of its own
        account.endStreams();
        storages.remove(accountHash);
      }
      lastPassed = storageOrder.pollFirst();
    }
  }

  // gives up waiting until less waits than allowed
  private void limitMemory() {
    if (waiting() <= memoryLimit) {
      return;
    }
    if (ownRunBytes > waitingStorage) {
      // written out each time, the runs would seldom grow into files: they only go first when
      // they hold more than the storages waiting for their turn
      writeOwnRuns();
    }
    if (waiting() > memoryLimit && waitingAccounts > waitingStorage) {
      // the account file waits for a range while later ones complete
      writeWaitingRanges();
    }
    // the storage file waits for an account while later ones complete
    while (waiting() > memoryLimit && !storageOrder.isEmpty()) {
      final Bytes32 accountHash = storageOrder.peekFirst();
      final AccountStorage account = storages.get(accountHash);
      if (account == null) {
        final AccountStorage given = new AccountStorage(accountHash);
        given.mode = Mode.REGULAR;
        storages.put(accountHash, given);
      } else if (account.mode == Mode.WAIT) {
        release(account);
      }
      advanceStorage();
    }
    // storages whose accounts are not in turn yet
    final Iterator<AccountStorage> largest =
        storages.values().stream()
            .filter(account -> account.mode == Mode.WAIT && account.bytes > 0)
            .sorted(Comparator.comparingLong((AccountStorage account) -> account.bytes).reversed())
            .toList()
            .iterator();
    while (waiting() > memoryLimit && largest.hasNext()) {
      release(largest.next());
    }
    if (waiting() > memoryLimit) {
      writeOwnRuns();
    }
    if (waiting() > memoryLimit) {
      writeWaitingRanges();
    }
  }

  private void writeOwnRuns() {
    storages.values().stream()
        .filter(account -> account.mode == Mode.OWN_FILES)
        .forEach(this::writeRuns);
  }

  private long waiting() {
    return waitingAccounts + waitingStorage + ownRunBytes;
  }

  // gives up waiting for the storage, and nothing of what it got stays in memory
  private void release(final AccountStorage account) {
    toOwnFiles(account);
    writeRuns(account);
  }

  // the runs not worth a file yet go through the regular write path
  private void writeRuns(final AccountStorage account) {
    final Iterator<SortedRun> runs = account.streams.values().iterator();
    while (runs.hasNext()) {
      final SortedRun run = runs.next();
      if (run.buffered() > 0) {
        end(run);
        runs.remove();
      }
    }
  }

  // the accounts waiting for their turn go through the regular write path, and so does the rest of
  // their ranges
  private void writeWaitingRanges() {
    for (final AccountRange range : ranges.values()) {
      if (!range.keys.isEmpty()) {
        writeRegular(accountSegment, range.keys, range.values);
        waitingAccounts -= range.bytes;
        range.keys.clear();
        range.values.clear();
        range.bytes = 0;
        range.regular = true;
      }
    }
  }

  // an account at or before the last one passed has had its turn, or never had storage
  private boolean isPassed(final Bytes32 accountHash) {
    final AccountStorage account = storages.get(accountHash);
    return lastPassed != null
        && accountHash.compareTo(lastPassed) <= 0
        && (account == null || account.mode == Mode.WAIT);
  }

  private void toRegular(final Bytes32 accountHash) {
    final AccountStorage account = storages.get(accountHash);
    if (account != null) {
      giveUp(account);
      account.passed = true;
    } else {
      final AccountStorage given = new AccountStorage(accountHash);
      given.mode = Mode.REGULAR;
      given.passed = true;
      storages.put(accountHash.copy(), given);
    }
  }

  private void toOwnFiles(final AccountStorage account) {
    account.mode = Mode.OWN_FILES;
    drainPieces(account, piece -> putOwn(account, piece.stream, piece.keys, piece.values));
    if (account.complete) {
      account.endStreams();
    }
  }

  // what waits goes through the regular write path, as does what is still to come
  private void giveUp(final AccountStorage account) {
    drainPieces(
        account,
        piece ->
            writeRegular(storageSegment, Arrays.asList(piece.keys), Arrays.asList(piece.values)));
    account.mode = Mode.REGULAR;
  }

  // the pieces no longer wait in memory once written
  private void drainPieces(final AccountStorage account, final Consumer<Piece> write) {
    account.pieces.forEach(write);
    waitingStorage -= account.bytes;
    account.bytes = 0;
    account.pieces.clear();
  }

  private void putInStorageFile(final Piece piece) {
    if (storageRun == null) {
      storageRun = newRun(storageSegment);
    }
    putAll(storageRun, piece.keys, piece.values);
    if (storageRun.size() >= MAX_FILE_BYTES) {
      storageRun = end(storageRun);
    }
  }

  // each stream has a run of its own, ended at the size of a file
  private void putOwn(
      final AccountStorage account,
      final Object stream,
      final byte[][] keys,
      final byte[][] values) {
    final SortedRun run = account.stream(stream);
    putAll(run, keys, values);
    if (run.size() >= MAX_FILE_BYTES) {
      end(run);
      account.streams.remove(stream);
    }
  }

  private void putAll(final SortedRun run, final byte[][] keys, final byte[][] values) {
    for (int i = 0; i < keys.length; i++) {
      run.put(keys[i], values[i]);
    }
  }

  private SortedRun newRun(final SegmentIdentifier segment) {
    return new SortedRun(storage, segment, MIN_FILE_BYTES, stats);
  }

  // ends a run, its file is stored in the background
  private SortedRun end(final SortedRun run) {
    if (run != null) {
      run.end().ifPresent(this::store);
    }
    return null;
  }

  private void store(final SortedWriter writer) {
    finisher.execute(new StoreFile(writer));
  }

  // a task of its own type, so that close finds the files it never got to
  private final class StoreFile implements Runnable {
    private final SortedWriter writer;

    private StoreFile(final SortedWriter writer) {
      this.writer = writer;
    }

    @Override
    public void run() {
      try {
        writer.commit();
      } catch (final Throwable e) {
        // an error as well, or the next flush would not know that the file is missing
        failure = e;
        LOG.error("Failed to store a sorted SNAP2 state file", e);
      }
    }
  }

  // returns once every file ended so far is stored
  private void awaitFiles() {
    try {
      finisher.submit(() -> {}).get();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new StorageException("Interrupted while storing the sorted SNAP2 state files", e);
    } catch (final ExecutionException e) {
      throw new StorageException("Failed to store the sorted SNAP2 state files", e.getCause());
    }
    final Throwable failed = failure;
    if (failed != null) {
      throw new StorageException("Failed to store a sorted SNAP2 state file", failed);
    }
  }

  private void writeRegular(
      final SegmentIdentifier segment, final List<byte[]> keys, final List<byte[]> values) {
    RegularWriter.writeAll(storage, segment, stats, keys, values);
  }

  private static List<byte[]> keys(final List<Bytes32> hashes) {
    return hashes.stream().map(Bytes32::toArray).toList();
  }

  private static List<byte[]> values(final List<Bytes> accounts) {
    return accounts.stream().map(Bytes::toArray).toList();
  }
}
