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
package org.hyperledger.besu.plugin.services.storage.rocksdb.segmented;

import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBTransaction;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.CompactionOptions;
import org.rocksdb.LevelMetaData;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.SstFileMetaData;
import org.rocksdb.Status;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Merges the small files a bulk load leaves in the bottom level of a column family, in the
 * background once its level 0 is merged down.
 *
 * <p>RocksDB never rewrites a file of the bottom level on its own, and a load that ends many sorted
 * files early leaves thousands of them there, which a regular write of the same entries would not
 * have. Neighbouring small files are merged together, and a small file that has no small neighbour
 * is merged with the smaller of its neighbours. A small file stays only when neither neighbour can
 * be merged: when they are being compacted, or larger than the target size, as rewriting such a
 * neighbour costs far more than the small file does.
 *
 * <p>The merges run one at a time, on a thread of their own. RocksDB's limit on background jobs
 * does not hold back a compaction started by hand, so more of them would compete with block import
 * and with RocksDB's own compactions right after a sync.
 *
 * <p>The merges wait until RocksDB has no compaction of its own pending for the column family,
 * which would take files of the plan, or rewrite the small files anyway. A plan that RocksDB turned
 * down in part is made again, a few times at most.
 *
 * <p>A marker in the default column family records each column family whose small files are still
 * to be merged, from the start of its bulk load until its merge is done. A merge that a stop cut
 * short, or that never started because the load was not closed, is done when the database opens
 * again.
 *
 * <p>A merge keeps its column family for seconds, so it does not hold the lock of the storage on
 * its column families, which the merge of level 0 and the bulk loads take. It holds a lock of its
 * column family for each step instead, which {@code clear()} and {@code close()} take before they
 * drop or close it, and a bulk load while it starts. The lock is taken before the one of the
 * storage, never after it.
 */
final class RocksDBSmallFileMerge {

  private static final Logger LOG = LoggerFactory.getLogger(RocksDBSmallFileMerge.class);

  // a file of less than this share of the target size counts as small
  private static final int SMALL_FILE_SHARE = 4;
  // the most target sizes of small files merged in one go
  private static final int MAX_MERGE_TARGETS = 8;
  private static final int MAX_PLANS = 3;
  private static final long POLL_MILLIS = 500;
  private static final String COMPACTION_PENDING_PROPERTY = "rocksdb.compaction-pending";
  // marker keys are the prefix followed by the segment name
  private static final byte[] MARKER_PREFIX = "merge-small-files:".getBytes(StandardCharsets.UTF_8);

  private final RocksDBColumnarKeyValueStorage storage;
  // RocksJava does not check its handles, so a dropped or closed one would crash the JVM; fair, so
  // that a merge between two steps queues behind clear() or close() instead of taking it again
  private final Map<SegmentIdentifier, ReentrantLock> handlesInUse = new ConcurrentHashMap<>();
  private final ThreadPoolExecutor worker =
      new ThreadPoolExecutor(
          1,
          1,
          1,
          TimeUnit.MINUTES,
          new LinkedBlockingQueue<>(),
          Thread.ofPlatform().daemon().name("rocksdb-small-file-merge").factory());
  private final Map<SegmentIdentifier, CompletableFuture<Void>> lastMergeBySegment =
      new ConcurrentHashMap<>();

  /** Files of the bottom level merged in one go, and the size of the files written. */
  @VisibleForTesting
  record Merge(List<SstFileMetaData> files, long outputFileBytes) {}

  // the storage closed, or the column family was dropped or is in a bulk load
  private static final class MergeStopped extends RuntimeException {
    MergeStopped() {
      super(null, null, false, false);
    }
  }

  RocksDBSmallFileMerge(final RocksDBColumnarKeyValueStorage storage) {
    this.storage = storage;
    worker.allowCoreThreadTimeOut(true);
  }

  /**
   * Records durably that the small files of the column families are to be merged. The caller holds
   * their locks, as the last step of a merge removes the marker.
   */
  void markToMerge(final Collection<SegmentIdentifier> segments) {
    storage.throwIfClosed();
    final RocksDB db = storage.getDB();
    try (final WriteBatch batch = new WriteBatch();
        final WriteOptions durable = new WriteOptions().setSync(true)) {
      for (final SegmentIdentifier segment : segments) {
        batch.put(db.getDefaultColumnFamily(), markerKey(segment), new byte[0]);
      }
      db.write(durable, batch);
    } catch (final RocksDBException e) {
      throw RocksDBTransaction.storageException(e);
    }
  }

  /** Merges the small files of the column families whose merge a stop cut short or prevented. */
  void mergeMarked() {
    final Set<String> marked = markedSegments();
    storage.getColumnHandlesBySegmentIdentifier().keySet().stream()
        .filter(segment -> marked.contains(segment.getName()))
        .forEach(
            segment -> {
              LOG.info(
                  "The small files of the {} segment were not merged before the database closed: merging them",
                  segment.getName());
              mergeWhenDrained(segment, storage.level0Drain.whenDrained(segment));
            });
  }

  @VisibleForTesting
  Set<String> markedSegments() {
    final RocksDB db = storage.getDB();
    final Set<String> names = new HashSet<>();
    try (final RocksIterator iterator = db.newIterator(db.getDefaultColumnFamily())) {
      for (iterator.seek(MARKER_PREFIX); iterator.isValid(); iterator.next()) {
        final byte[] key = iterator.key();
        if (!Arrays.equals(
            key,
            0,
            Math.min(key.length, MARKER_PREFIX.length),
            MARKER_PREFIX,
            0,
            MARKER_PREFIX.length)) {
          break;
        }
        names.add(
            new String(
                key,
                MARKER_PREFIX.length,
                key.length - MARKER_PREFIX.length,
                StandardCharsets.UTF_8));
      }
    }
    return names;
  }

  /**
   * Merges the small files of the bottom level of a column family once its level 0 is drained, and
   * its merge before is done.
   */
  void mergeWhenDrained(final SegmentIdentifier segment, final CompletableFuture<Void> drained) {
    lastMergeBySegment.compute(
        segment,
        (unused, previous) ->
            (previous == null
                    ? drained
                    : CompletableFuture.allOf(previous.exceptionally(error -> null), drained))
                .thenRunAsync(() -> mergeSmallFiles(segment), worker));
  }

  /** Completes once the last merge of the column family is done, or at once if it has none. */
  @VisibleForTesting
  CompletableFuture<Void> whenMerged(final SegmentIdentifier segment) {
    return Optional.ofNullable(lastMergeBySegment.get(segment))
        .map(CompletableFuture::copy)
        .orElseGet(() -> CompletableFuture.completedFuture(null));
  }

  @VisibleForTesting
  ReentrantLock handlesInUse(final SegmentIdentifier segment) {
    return handlesInUse.computeIfAbsent(segment, unused -> new ReentrantLock(true));
  }

  /** Runs what drops or closes a column family once the step of a merge under way is done. */
  void excludingMerges(final SegmentIdentifier segment, final Runnable work) {
    excludingMerges(List.of(segment), work);
  }

  /**
   * Runs work on column families once the steps of their merges under way are done, with no merge
   * step running meanwhile. Their locks are taken in the order of their names, so two callers never
   * wait for each other.
   */
  void excludingMerges(final Collection<SegmentIdentifier> segments, final Runnable work) {
    final List<ReentrantLock> locks =
        segments.stream()
            .distinct()
            .sorted(Comparator.comparing(SegmentIdentifier::getName))
            .map(this::handlesInUse)
            .toList();
    locks.forEach(ReentrantLock::lock);
    try {
      work.run();
    } finally {
      locks.forEach(ReentrantLock::unlock);
    }
  }

  /** Waits for the merges under way, as the storage closes, and starts no others. */
  void close() {
    // the merges find the storage closed from now on
    List.copyOf(handlesInUse.keySet()).forEach(segment -> excludingMerges(segment, () -> {}));
    worker.shutdown();
  }

  private void mergeSmallFiles(final SegmentIdentifier segment) {
    try {
      mergeSmallFilesOf(segment);
    } catch (final MergeStopped e) {
      // the marker stays for the merge after the bulk load or the next open
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (final RuntimeException e) {
      if (!storage.isClosed()) {
        LOG.warn("Merging the small files of the {} segment failed", segment.getName(), e);
      }
    }
  }

  private void mergeSmallFilesOf(final SegmentIdentifier segment) throws InterruptedException {
    final ColumnFamilyHandle handle;
    final long targetBytes;
    final ReentrantLock lock = handlesInUse(segment);
    lock.lock();
    try {
      if (storage.isClosed() || storage.segmentsInBulkLoad.contains(segment)) {
        throw new MergeStopped();
      }
      handle = storage.safeColumnHandle(segment);
      targetBytes = storage.columnFamilyOptions(segment).targetFileSizeBase();
    } finally {
      lock.unlock();
    }
    final long start = System.nanoTime();
    int files = 0;
    long bytes = 0;
    int written = 0;
    int turnedDown = 0;
    for (int round = 0; round < MAX_PLANS; round++) {
      awaitNoPendingCompaction(segment, handle);
      final Optional<LevelMetaData> bottom =
          whileMergeable(segment, handle, () -> bottomLevel(handle));
      if (bottom.isEmpty()) {
        break;
      }
      turnedDown = 0;
      for (final Merge merge : plan(bottom.get().files(), targetBytes)) {
        final Optional<Integer> outputs =
            whileMergeable(segment, handle, () -> compact(handle, merge, bottom.get().level()));
        if (outputs.isEmpty()) {
          turnedDown++;
        } else {
          files += merge.files().size();
          bytes += merge.files().stream().mapToLong(SstFileMetaData::size).sum();
          written += outputs.get();
        }
      }
      if (turnedDown == 0) {
        break;
      }
    }
    // done as far as RocksDB let it, so the next open does not try again
    final long left =
        whileMergeable(
            segment,
            handle,
            () -> {
              unmark(segment);
              return smallFiles(handle, targetBytes);
            });
    if (files == 0 && turnedDown == 0) {
      return;
    }
    LOG.info(
        "Merged {} files ({} MB) of the {} segment into {} files in {} s, {} merges turned down by RocksDB, {} small files left",
        files,
        bytes >> 20,
        segment.getName(),
        written,
        TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start),
        turnedDown,
        left);
  }

  // runs the work under the lock of the column family, or throws MergeStopped
  private <T> T whileMergeable(
      final SegmentIdentifier segment, final ColumnFamilyHandle handle, final Supplier<T> work) {
    final ReentrantLock lock = handlesInUse(segment);
    lock.lock();
    try {
      if (!isMergeable(segment, handle)) {
        throw new MergeStopped();
      }
      return work.get();
    } finally {
      lock.unlock();
    }
  }

  // the storage is open, no bulk load started on the column family, and it was not dropped since
  // the merge started; equals() would read the handles, of which the old one is closed then
  @SuppressWarnings("ReferenceEquality")
  private boolean isMergeable(final SegmentIdentifier segment, final ColumnFamilyHandle handle) {
    return !storage.isClosed()
        && !storage.segmentsInBulkLoad.contains(segment)
        && storage.safeColumnHandle(segment) == handle;
  }

  private void awaitNoPendingCompaction(
      final SegmentIdentifier segment, final ColumnFamilyHandle handle)
      throws InterruptedException {
    while (whileMergeable(segment, handle, () -> compactionPending(handle))) {
      Thread.sleep(POLL_MILLIS);
    }
  }

  private boolean compactionPending(final ColumnFamilyHandle handle) {
    try {
      return storage.getDB().getLongProperty(handle, COMPACTION_PENDING_PROPERTY) > 0;
    } catch (final RocksDBException e) {
      throw new StorageException(e);
    }
  }

  // the files of level 0 overlap and are not merged by key, so a column family with nothing below
  // level 0 has no bottom level to merge
  private Optional<LevelMetaData> bottomLevel(final ColumnFamilyHandle handle) {
    return storage.getDB().getColumnFamilyMetaData(handle).levels().stream()
        .filter(level -> !level.files().isEmpty())
        .reduce((a, b) -> b)
        .filter(level -> level.level() > 0);
  }

  /**
   * The merges that leave hardly any file of less than a quarter of the target size in the bottom
   * level.
   *
   * <p>Neighbouring small files are merged together, in pieces of at most eight target sizes. A
   * piece too small to stand alone, under a quarter of the target size, joins the merge that took
   * the file right before it, as the tail of a long run does. Otherwise it takes the smaller of its
   * neighbours along if that is no larger than the target size, or else is merged on its own if it
   * has more than one file. A file that is being compacted stays as it is and is no neighbour.
   */
  @VisibleForTesting
  static List<Merge> plan(final List<SstFileMetaData> bottomFiles, final long targetBytes) {
    final long smallBytes = targetBytes / SMALL_FILE_SHARE;
    final List<SstFileMetaData> files = new ArrayList<>(bottomFiles);
    files.sort(Comparator.comparing(SstFileMetaData::smallestKey, Arrays::compareUnsigned));
    // the indexes of the files of each merge, in key order
    final List<List<Integer>> merges = new ArrayList<>();
    int i = 0;
    while (i < files.size()) {
      if (!isSmall(files.get(i), smallBytes)) {
        i++;
        continue;
      }
      List<Integer> piece = new ArrayList<>();
      long pieceBytes = 0;
      while (i < files.size() && isSmall(files.get(i), smallBytes)) {
        piece.add(i);
        pieceBytes += files.get(i).size();
        i++;
        if (pieceBytes >= MAX_MERGE_TARGETS * targetBytes) {
          merges.add(piece);
          piece = new ArrayList<>();
          pieceBytes = 0;
        }
      }
      if (piece.isEmpty()) {
        continue;
      }
      if (pieceBytes >= smallBytes) {
        merges.add(piece);
        continue;
      }
      // going from left to right, a file before the piece can only be taken by the last merge,
      // and the file after it by none
      final int before = piece.getFirst() - 1;
      if (!merges.isEmpty() && merges.getLast().getLast() == before) {
        merges.getLast().addAll(piece);
        continue;
      }
      final boolean beforeFree = before >= 0 && isFree(files.get(before), targetBytes);
      final boolean afterFree = i < files.size() && isFree(files.get(i), targetBytes);
      if (beforeFree && (!afterFree || files.get(before).size() <= files.get(i).size())) {
        piece.addFirst(before);
        merges.add(piece);
      } else if (afterFree) {
        piece.add(i);
        merges.add(piece);
      } else if (piece.size() > 1) {
        merges.add(piece);
      }
    }
    return merges.stream()
        .map(
            indexes ->
                new Merge(
                    indexes.stream().map(files::get).toList(),
                    outputFileBytes(bytes(files, indexes), targetBytes, smallBytes)))
        .toList();
  }

  private static boolean isSmall(final SstFileMetaData file, final long smallBytes) {
    return !file.beingCompacted() && file.size() < smallBytes;
  }

  private static boolean isFree(final SstFileMetaData file, final long targetBytes) {
    return !file.beingCompacted() && file.size() <= targetBytes;
  }

  // the files written are as even in size as the count closest to the target size allows, so that
  // the last one is not small: halfway between the even share and the size that would leave
  // exactly a small file last
  @VisibleForTesting
  static long outputFileBytes(final long total, final long targetBytes, final long smallBytes) {
    final long count = Math.max(1, Math.round((double) total / targetBytes));
    if (count == 1) {
      return 2 * total;
    }
    final long even = total / count;
    final long mostThatLeavesASmallFile = (total - smallBytes) / (count - 1);
    return (even + mostThatLeavesASmallFile) / 2;
  }

  private static long bytes(final List<SstFileMetaData> files, final List<Integer> indexes) {
    return indexes.stream().mapToLong(index -> files.get(index).size()).sum();
  }

  // the number of files written, or empty when another compaction took one of the files
  private Optional<Integer> compact(
      final ColumnFamilyHandle handle, final Merge merge, final int level) {
    try (CompactionOptions options =
        new CompactionOptions().setOutputFileSizeLimit(merge.outputFileBytes())) {
      return Optional.of(
          storage
              .getDB()
              .compactFiles(
                  options,
                  handle,
                  merge.files().stream().map(SstFileMetaData::fileName).toList(),
                  level,
                  -1,
                  null)
              .size());
    } catch (final RocksDBException e) {
      if (isTurnedDown(e)) {
        LOG.debug("Could not merge {} files: {}", merge.files().size(), e.getMessage());
        return Optional.empty();
      }
      // stops the node on a full disk
      throw RocksDBTransaction.storageException(e);
    }
  }

  // a file of the plan is being compacted or gone, or a compaction writes to the same key range
  @VisibleForTesting
  static boolean isTurnedDown(final RocksDBException e) {
    final Status status = e.getStatus();
    return status != null
        && (status.getCode() == Status.Code.Aborted
            || status.getCode() == Status.Code.InvalidArgument);
  }

  private long smallFiles(final ColumnFamilyHandle handle, final long targetBytes) {
    return bottomLevel(handle)
        .map(
            bottom ->
                bottom.files().stream()
                    .filter(file -> file.size() < targetBytes / SMALL_FILE_SHARE)
                    .count())
        .orElse(0L);
  }

  private void unmark(final SegmentIdentifier segment) {
    final RocksDB db = storage.getDB();
    try {
      db.delete(db.getDefaultColumnFamily(), markerKey(segment));
    } catch (final RocksDBException e) {
      throw RocksDBTransaction.storageException(e);
    }
  }

  private static byte[] markerKey(final SegmentIdentifier segment) {
    final byte[] name = segment.getName().getBytes(StandardCharsets.UTF_8);
    final byte[] key = Arrays.copyOf(MARKER_PREFIX, MARKER_PREFIX.length + name.length);
    System.arraycopy(name, 0, key, MARKER_PREFIX.length, name.length);
    return key;
  }
}
