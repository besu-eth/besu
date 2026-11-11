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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.CompactionOptions;
import org.rocksdb.LevelMetaData;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileMetaData;
import org.rocksdb.Status;
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
 * neighbour costs far more than the small file does. The merges of four column families run at
 * once, those of one column family one after the other.
 *
 * <p>The merges wait until RocksDB has no compaction of its own pending for the column family,
 * which would take files of the plan, or rewrite the small files anyway. A plan that RocksDB turned
 * down in part is made again, a few times at most.
 *
 * <p>A merge keeps its column family for seconds, so it does not hold the lock of the storage on
 * its column families, which the merge of level 0 and the bulk loads take. It holds a lock of its
 * column family for reading instead, which {@code clear()} and {@code close()} take for writing
 * before they drop or close it, and a bulk load before it starts. The lock is taken before the one
 * of the storage, never after it.
 */
final class RocksDBSmallFileMerge {

  private static final Logger LOG = LoggerFactory.getLogger(RocksDBSmallFileMerge.class);

  // a file of less than this share of the target size counts as small
  private static final int SMALL_FILE_SHARE = 4;
  // the most target sizes of small files merged in one go
  private static final int MAX_MERGE_TARGETS = 8;
  private static final int MERGE_THREADS = 4;
  private static final int MAX_PLANS = 3;
  private static final long POLL_MILLIS = 500;
  private static final String COMPACTION_PENDING_PROPERTY = "rocksdb.compaction-pending";
  private static final int TURNED_DOWN = -1;

  private final RocksDBColumnarKeyValueStorage storage;
  // RocksJava does not check its handles, so a dropped or closed one would crash the JVM
  private final Map<SegmentIdentifier, ReentrantReadWriteLock> handlesInUse =
      new ConcurrentHashMap<>();
  private final ThreadPoolExecutor workers =
      new ThreadPoolExecutor(
          MERGE_THREADS,
          MERGE_THREADS,
          1,
          TimeUnit.MINUTES,
          new LinkedBlockingQueue<>(),
          Thread.ofPlatform().daemon().name("rocksdb-small-file-merge-", 0).factory());
  private final Map<SegmentIdentifier, CompletableFuture<Void>> lastMergeBySegment =
      new ConcurrentHashMap<>();

  /** Files of the bottom level merged in one go, and the size of the files written. */
  @VisibleForTesting
  record Merge(List<SstFileMetaData> files, long outputFileBytes) {}

  RocksDBSmallFileMerge(final RocksDBColumnarKeyValueStorage storage) {
    this.storage = storage;
    workers.allowCoreThreadTimeOut(true);
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
                .thenRunAsync(() -> mergeSmallFiles(segment), workers));
  }

  /** Completes once the last merge of the column family is done, or at once if it has none. */
  @VisibleForTesting
  CompletableFuture<Void> whenMerged(final SegmentIdentifier segment) {
    return Optional.ofNullable(lastMergeBySegment.get(segment))
        .map(CompletableFuture::copy)
        .orElseGet(() -> CompletableFuture.completedFuture(null));
  }

  @VisibleForTesting
  ReentrantReadWriteLock handlesInUse(final SegmentIdentifier segment) {
    return handlesInUse.computeIfAbsent(segment, unused -> new ReentrantReadWriteLock());
  }

  /** Runs what drops or closes a column family once its merge under way is done. */
  void excludingMerges(final SegmentIdentifier segment, final Runnable work) {
    final Lock lock = handlesInUse(segment).writeLock();
    lock.lock();
    try {
      work.run();
    } finally {
      lock.unlock();
    }
  }

  /** Waits for the merges under way of column families that a bulk load is about to start on. */
  void awaitMergesUnderWay(final Collection<SegmentIdentifier> segments) {
    // the merges after it find the column family in the bulk load
    segments.forEach(segment -> excludingMerges(segment, () -> {}));
  }

  /** Waits for the merges under way, as the storage closes, and starts no others. */
  void close() {
    // the merges find the storage closed from now on
    awaitMergesUnderWay(List.copyOf(handlesInUse.keySet()));
    workers.shutdown();
  }

  private void mergeSmallFiles(final SegmentIdentifier segment) {
    try {
      mergeSmallFilesOf(segment);
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
    final Lock lock = handlesInUse(segment).readLock();
    lock.lock();
    try {
      if (storage.isClosed() || storage.segmentsInBulkLoad.contains(segment)) {
        return;
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
      if (!awaitNoPendingCompaction(segment, handle)) {
        return;
      }
      final Optional<LevelMetaData> bottom =
          whileMergeable(segment, handle, () -> bottomLevel(handle));
      if (bottom.isEmpty()) {
        break;
      }
      final List<Merge> plan = plan(bottom.get().files(), targetBytes);
      turnedDown = 0;
      for (final Merge merge : plan) {
        final Optional<Integer> outputs =
            whileMergeable(segment, handle, () -> compact(handle, merge, bottom.get().level()));
        if (outputs.isEmpty()) {
          return;
        }
        if (outputs.get() == TURNED_DOWN) {
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
    if (files == 0 && turnedDown == 0) {
      return;
    }
    final Optional<Long> left =
        whileMergeable(segment, handle, () -> Optional.of(smallFiles(handle, targetBytes)));
    if (left.isEmpty()) {
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
        left.get());
  }

  // runs the work under the lock of the column family, unless the merge has to stop
  private <T> Optional<T> whileMergeable(
      final SegmentIdentifier segment,
      final ColumnFamilyHandle handle,
      final Supplier<Optional<T>> work) {
    final Lock lock = handlesInUse(segment).readLock();
    lock.lock();
    try {
      return isMergeable(segment, handle) ? work.get() : Optional.empty();
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

  // false when the merge has to stop
  private boolean awaitNoPendingCompaction(
      final SegmentIdentifier segment, final ColumnFamilyHandle handle)
      throws InterruptedException {
    while (true) {
      final Optional<Boolean> pending =
          whileMergeable(segment, handle, () -> Optional.of(compactionPending(handle)));
      if (pending.isEmpty()) {
        return false;
      }
      if (!pending.get()) {
        return true;
      }
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
   * <p>Neighbouring small files are merged together. Those that are too few to make up a quarter of
   * the target size, or a small file on its own, take the smaller of their neighbours along, if it
   * is no larger than the target size, or else join the merge that took the file before them. A
   * file that is being compacted stays as it is and is no neighbour.
   */
  @VisibleForTesting
  static List<Merge> plan(final List<SstFileMetaData> bottomFiles, final long targetBytes) {
    final long smallBytes = targetBytes / SMALL_FILE_SHARE;
    final List<SstFileMetaData> files = new ArrayList<>(bottomFiles);
    files.sort(Comparator.comparing(SstFileMetaData::smallestKey, Arrays::compareUnsigned));
    final boolean[] taken = new boolean[files.size()];
    // the indexes of the files of each merge, in key order
    final List<List<Integer>> merges = new ArrayList<>();
    int i = 0;
    while (i < files.size()) {
      if (!isSmall(files.get(i), smallBytes)) {
        i++;
        continue;
      }
      int end = i;
      while (end + 1 < files.size() && isSmall(files.get(end + 1), smallBytes)) {
        end++;
      }
      for (final List<Integer> chunk : chunks(files, i, end, targetBytes, smallBytes)) {
        final List<Integer> merged = new ArrayList<>(chunk);
        if (bytes(files, merged) < smallBytes || merged.size() == 1) {
          final int neighbour =
              smallerNeighbour(files, taken, chunk.getFirst(), chunk.getLast(), targetBytes);
          if (neighbour >= 0) {
            merged.add(neighbour < chunk.getFirst() ? 0 : merged.size(), neighbour);
          } else if (!merges.isEmpty() && merges.getLast().getLast() == chunk.getFirst() - 1) {
            merges.getLast().addAll(merged);
            merged.forEach(index -> taken[index] = true);
            continue;
          }
        }
        if (merged.size() > 1) {
          merged.forEach(index -> taken[index] = true);
          merges.add(merged);
        }
      }
      i = end + 1;
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

  // a run of small files in pieces of at most the most merged in one go, the last one large enough
  // to stand alone unless the run is not
  private static List<List<Integer>> chunks(
      final List<SstFileMetaData> files,
      final int first,
      final int last,
      final long targetBytes,
      final long smallBytes) {
    final List<List<Integer>> chunks = new ArrayList<>();
    List<Integer> chunk = new ArrayList<>();
    long chunkBytes = 0;
    for (int index = first; index <= last; index++) {
      chunk.add(index);
      chunkBytes += files.get(index).size();
      if (chunkBytes >= MAX_MERGE_TARGETS * targetBytes) {
        chunks.add(chunk);
        chunk = new ArrayList<>();
        chunkBytes = 0;
      }
    }
    if (!chunk.isEmpty()) {
      if (chunkBytes < smallBytes && !chunks.isEmpty()) {
        chunks.getLast().addAll(chunk);
      } else {
        chunks.add(chunk);
      }
    }
    return chunks;
  }

  // -1 when neither neighbour can be merged
  private static int smallerNeighbour(
      final List<SstFileMetaData> files,
      final boolean[] taken,
      final int first,
      final int last,
      final long targetBytes) {
    final int before = first - 1;
    final int after = last + 1;
    final boolean beforeFree = before >= 0 && isFree(files.get(before), taken[before], targetBytes);
    final boolean afterFree =
        after < files.size() && isFree(files.get(after), taken[after], targetBytes);
    if (beforeFree && afterFree) {
      return files.get(before).size() <= files.get(after).size() ? before : after;
    }
    return beforeFree ? before : afterFree ? after : -1;
  }

  private static boolean isFree(
      final SstFileMetaData file, final boolean taken, final long targetBytes) {
    return !taken && !file.beingCompacted() && file.size() <= targetBytes;
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

  // the number of files written, or TURNED_DOWN when another compaction took one of the files
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
        return Optional.of(TURNED_DOWN);
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
}
