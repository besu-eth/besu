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
import org.hyperledger.besu.plugin.services.storage.SegmentBulkLoad;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionOptions;
import org.rocksdb.LevelMetaData;
import org.rocksdb.MutableColumnFamilyOptions;
import org.rocksdb.MutableDBOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Switches the automatic compaction of column families off for the time of a bulk load.
 *
 * <p>A sorted file that cannot go to the bottom level, because its key range holds other keys or
 * overlaps another file, is placed higher up. With automatic compaction on, RocksDB starts merging
 * such files down while the load is still adding files. A running compaction keeps every file that
 * overlaps its output out of the levels below, so those files pile up in level 0, the next
 * compaction has more to merge, and the entries end up being written many times over, which is what
 * the sorted files were meant to avoid. With it off the files stay where they were placed.
 *
 * <p>When the load is closed the column families go back to compacting on their own, with the
 * limits on level 0 and on the bytes waiting to be compacted lifted until level 0 is merged down,
 * in the background. Nothing is compacted by hand: a manual compaction of a whole column family
 * merges every level into the next and so rewrites the bottom level, which holds nearly all of the
 * load, for the sake of the few files above it. While level 0 is merged down the compactions read
 * ahead less: such a merge reads every file of level 0 at once, each with a buffer of its own. They
 * also get more threads, as the merge holds one for as long as it runs.
 *
 * <p>A column family can be let go before the others, when it is read much before the load ends and
 * its regular writes piled up in level 0, where a read checks every file.
 *
 * <p>Only the small files of the bottom level are merged, in the background once level 0 of their
 * column family is merged down. RocksDB never rewrites a file there on its own, and a load that
 * ends many files early leaves thousands of them, which a regular write of the same entries would
 * not have. A small file that has no small neighbour is merged with the smaller of its neighbours,
 * so that no file of less than a quarter of the target size stays behind.
 *
 * <p>The column families of large values written in no particular key order keep compacting, but
 * put their values in blob files until the load is closed. Each compaction on the way to the bottom
 * level would otherwise rewrite the values along with their keys. Their compaction is not held back
 * instead: a lookup checks every file of level 0, and such keys overlap every file there.
 */
final class RocksDBSegmentBulkLoad implements SegmentBulkLoad {

  private static final Logger LOG = LoggerFactory.getLogger(RocksDBSegmentBulkLoad.class);

  private static final String LEVEL0_FILES_PROPERTY = "rocksdb.num-files-at-level0";
  private static final String PENDING_COMPACTION_BYTES_PROPERTY =
      "rocksdb.estimate-pending-compaction-bytes";
  private static final int NO_LEVEL0_LIMIT = Integer.MAX_VALUE;
  private static final long NO_PENDING_COMPACTION_LIMIT = 0;
  private static final long LEVEL0_POLL_MILLIS = 500;
  // a file of less than this share of the target size counts as small
  private static final int SMALL_FILE_SHARE = 4;
  // the most target sizes of small files merged in one go
  private static final int MAX_MERGE_TARGETS = 8;
  private static final int MERGE_THREADS = 4;
  // the buffers of a thousand files in level 0 then take 256 MB, against 2 GB at RocksDB's default
  private static final long DRAIN_READAHEAD_BYTES = 256 * 1024;
  // the merge of a level 0 is one compaction that keeps its thread until it is done, which can take
  // most of an hour; without more threads the other column families stop compacting meanwhile and
  // their writes slow down once their own level 0 fills up
  private static final int DRAIN_EXTRA_BACKGROUND_JOBS = 2;

  private final RocksDBColumnarKeyValueStorage storage;
  private final List<SegmentIdentifier> segments;
  // the segments whose values go to blob files during the load
  private final List<SegmentIdentifier> separateValues;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  // the segments whose level 0 is still merged down with its limits lifted
  private final List<Draining> draining = new ArrayList<>();
  // the segments whose small files are merged once their level 0 is merged down
  private final List<SegmentIdentifier> toMerge = new ArrayList<>();
  // the segments let go before the load was closed
  private final Set<SegmentIdentifier> ended = ConcurrentHashMap.newKeySet();
  private final AtomicInteger drainsRunning = new AtomicInteger();
  private final CountDownLatch done = new CountDownLatch(1);
  // set once the storage starts closing
  private volatile boolean stopping;

  private record Draining(
      SegmentIdentifier segment,
      int slowdownTrigger,
      int stopTrigger,
      long softPendingLimit,
      long hardPendingLimit,
      long startNanos) {}

  /** Files of the bottom level merged in one go, and the size of the files written. */
  @VisibleForTesting
  record Merge(List<SstFileMetaData> files, long outputFileBytes) {}

  RocksDBSegmentBulkLoad(
      final RocksDBColumnarKeyValueStorage storage,
      final List<SegmentIdentifier> segments,
      final List<SegmentIdentifier> separateValues) {
    if (segments.stream().anyMatch(separateValues::contains)) {
      throw new IllegalArgumentException("A segment is either held back or has separate values");
    }
    this.storage = storage;
    this.segments = List.copyOf(segments);
    this.separateValues = List.copyOf(separateValues);
    for (final SegmentIdentifier segment : this.segments) {
      setOptions(segment, MutableColumnFamilyOptions.builder().setDisableAutoCompactions(true));
    }
    // the blob files are compressed as the column family is configured when the database opens
    for (final SegmentIdentifier segment : this.separateValues) {
      setOptions(
          segment,
          MutableColumnFamilyOptions.builder()
              .setEnableBlobFiles(true)
              .setMinBlobSize(RocksDBColumnarKeyValueStorage.MIN_BLOB_SIZE));
    }
  }

  @Override
  public synchronized void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    for (final SegmentIdentifier segment : separateValues) {
      if (storage.isClosed()) {
        return;
      }
      if (!ended.contains(segment)) {
        restoreValues(segment);
      }
    }
    for (final SegmentIdentifier segment : segments) {
      if (storage.isClosed()) {
        // the column families come back with automatic compaction on when the database is opened
        return;
      }
      if (ended.contains(segment)) {
        continue;
      }
      try {
        resumeCompaction(segment).ifPresent(draining::add);
        toMerge.add(segment);
      } catch (final RuntimeException e) {
        if (storage.isClosed()) {
          return;
        }
        // the other segments still have to get theirs back
        LOG.warn(
            "Automatic compaction of the {} segment could not be switched back on after its bulk load, it stays off until the database is opened again",
            segment.getName(),
            e);
      }
    }
    startBackgroundWork();
  }

  /**
   * Lets one segment go before the load is closed, in the way {@link #close()} does.
   *
   * <p>The merge of its level 0 that follows is not stopped by cancelling the returned future, and
   * the limits it lifted are put back once it is done whatever happened to the future.
   */
  @Override
  public synchronized CompletableFuture<Void> end(final SegmentIdentifier segment) {
    if (separateValues.contains(segment)) {
      if (!closed.get() && ended.add(segment)) {
        restoreValues(segment);
      }
      return CompletableFuture.completedFuture(null);
    }
    if (closed.get() || !segments.contains(segment) || !ended.add(segment)) {
      return CompletableFuture.completedFuture(null);
    }
    final Optional<Draining> drain;
    try {
      drain = resumeCompaction(segment);
    } catch (final RuntimeException e) {
      return CompletableFuture.failedFuture(e);
    }
    if (drain.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    final CompletableFuture<Void> drained = new CompletableFuture<>();
    final CountDownLatch watched = new CountDownLatch(1);
    storage.onClose(
        () -> {
          stopping = true;
          awaitQuietly(watched);
        });
    Thread.ofPlatform()
        .daemon()
        .name("rocksdb-bulk-load-end")
        .start(() -> watchLevel0(drain.get(), drained, watched));
    return drained;
  }

  private void watchLevel0(
      final Draining segment, final CompletableFuture<Void> drained, final CountDownLatch watched) {
    try {
      while (!stopping && !storage.isClosed()) {
        if (restoreIfDrained(segment)) {
          drained.complete(null);
          return;
        }
        Thread.sleep(LEVEL0_POLL_MILLIS);
      }
      drained.completeExceptionally(new StorageException("storage closed"));
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      drained.completeExceptionally(e);
    } catch (final RuntimeException e) {
      if (!storage.isClosed()) {
        LOG.warn("Watching level 0 after the bulk load failed, its limits stay lifted", e);
      }
      drained.completeExceptionally(e);
    } finally {
      watched.countDown();
    }
  }

  private static void awaitQuietly(final CountDownLatch latch) {
    try {
      if (!latch.await(1, TimeUnit.MINUTES)) {
        LOG.warn("Bulk load work still running a minute after the storage started closing");
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  // the values written so far stay in their blob files, which reads find as they find the others
  private void restoreValues(final SegmentIdentifier segment) {
    final ColumnFamilyOptions configured = storage.columnFamilyOptions(segment);
    try {
      setOptions(
          segment,
          MutableColumnFamilyOptions.builder()
              .setEnableBlobFiles(configured.enableBlobFiles())
              .setMinBlobSize(configured.minBlobSize()));
    } catch (final RuntimeException e) {
      if (!storage.isClosed()) {
        LOG.warn(
            "The {} segment keeps writing its values to blob files after its bulk load, until the database is opened again",
            segment.getName(),
            e);
      }
    }
  }

  // the segment to watch until its level 0 is merged down, if it is not already
  private Optional<Draining> resumeCompaction(final SegmentIdentifier segment) {
    final ColumnFamilyOptions configured = storage.columnFamilyOptions(segment);
    final int slowdownTrigger = configured.level0SlowdownWritesTrigger();
    final int stopTrigger = configured.level0StopWritesTrigger();
    final long softPendingLimit = configured.softPendingCompactionBytesLimit();
    final long hardPendingLimit = configured.hardPendingCompactionBytesLimit();
    LOG.info("Bulk load of the {} segment ended with {}", segment.getName(), levels(segment));
    if (isDrained(segment, slowdownTrigger, softPendingLimit)) {
      setOptions(segment, MutableColumnFamilyOptions.builder().setDisableAutoCompactions(false));
      return Optional.empty();
    }
    // before the merge starts, which takes the readahead it finds
    drainStarted();
    // RocksDB does not hold back writes for the files in level 0 while automatic compaction is
    // off, but does so the moment it is switched on: with this many files, or this many bytes
    // waiting to be compacted, it would slow down or stop every write to the database, not only
    // the ones to this column family, and every flush, until it has merged them down. The limits
    // are lifted until it has, which nothing waits for.
    setOptions(
        segment,
        MutableColumnFamilyOptions.builder()
            .setLevel0SlowdownWritesTrigger(NO_LEVEL0_LIMIT)
            .setLevel0StopWritesTrigger(NO_LEVEL0_LIMIT)
            .setSoftPendingCompactionBytesLimit(NO_PENDING_COMPACTION_LIMIT)
            .setHardPendingCompactionBytesLimit(NO_PENDING_COMPACTION_LIMIT)
            .setDisableAutoCompactions(false));
    return Optional.of(
        new Draining(
            segment,
            slowdownTrigger,
            stopTrigger,
            softPendingLimit,
            hardPendingLimit,
            System.nanoTime()));
  }

  private void drainStarted() {
    if (drainsRunning.getAndIncrement() == 0) {
      setDrainOptions(
          DRAIN_READAHEAD_BYTES, storage.options.maxBackgroundJobs() + DRAIN_EXTRA_BACKGROUND_JOBS);
    }
  }

  private void drainEnded() {
    if (drainsRunning.decrementAndGet() == 0) {
      setDrainOptions(
          storage.options.compactionReadaheadSize(), storage.options.maxBackgroundJobs());
    }
  }

  // best effort: the merge works with any readahead and any number of threads
  private void setDrainOptions(final long readaheadBytes, final int backgroundJobs) {
    try {
      storage
          .getDB()
          .setDBOptions(
              MutableDBOptions.builder()
                  .setCompactionReadaheadSize(readaheadBytes)
                  .setMaxBackgroundJobs(backgroundJobs)
                  .build());
    } catch (final RocksDBException e) {
      LOG.warn(
          "Could not set the compaction readahead to {} bytes and the background jobs to {}",
          readaheadBytes,
          backgroundJobs,
          e);
    }
  }

  private boolean isDrained(
      final SegmentIdentifier segment, final int slowdownTrigger, final long softPendingLimit) {
    return level0Files(segment) < slowdownTrigger
        && (softPendingLimit == NO_PENDING_COMPACTION_LIMIT
            || pendingCompactionBytes(segment) < softPendingLimit);
  }

  // puts the limits of each segment back once its level 0 is merged down, and merges its small
  // files then
  private void startBackgroundWork() {
    if (toMerge.isEmpty()) {
      done.countDown();
      return;
    }
    storage.onClose(this::stopBackgroundWork);
    if (storage.isClosed()) {
      done.countDown();
      return;
    }
    Thread.ofPlatform().daemon().name("rocksdb-bulk-load-end").start(this::runBackgroundWork);
  }

  private void runBackgroundWork() {
    final ExecutorService workers =
        Executors.newFixedThreadPool(
            MERGE_THREADS,
            Thread.ofPlatform().daemon().name("rocksdb-small-file-merge-", 0).factory());
    final List<Future<?>> merges = new ArrayList<>();
    try {
      final List<SegmentIdentifier> drainingSegments =
          draining.stream().map(Draining::segment).toList();
      for (final SegmentIdentifier segment : toMerge) {
        if (!drainingSegments.contains(segment)) {
          merges.add(workers.submit(() -> mergeSmallFiles(segment)));
        }
      }
      while (!draining.isEmpty() && !stopping && !storage.isClosed()) {
        for (final Draining segment : List.copyOf(draining)) {
          if (restoreIfDrained(segment)) {
            draining.remove(segment);
            merges.add(workers.submit(() -> mergeSmallFiles(segment.segment())));
          }
        }
        if (!draining.isEmpty()) {
          Thread.sleep(LEVEL0_POLL_MILLIS);
        }
      }
      awaitAll(merges);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (final RuntimeException e) {
      if (!storage.isClosed()) {
        LOG.warn("Watching level 0 after the bulk load failed, its limits stay lifted", e);
      }
    } finally {
      workers.shutdown();
      done.countDown();
    }
  }

  private boolean restoreIfDrained(final Draining segment) {
    if (!isDrained(segment.segment(), segment.slowdownTrigger(), segment.softPendingLimit())) {
      return false;
    }
    setOptions(
        segment.segment(),
        MutableColumnFamilyOptions.builder()
            .setLevel0SlowdownWritesTrigger(segment.slowdownTrigger())
            .setLevel0StopWritesTrigger(segment.stopTrigger())
            .setSoftPendingCompactionBytesLimit(segment.softPendingLimit())
            .setHardPendingCompactionBytesLimit(segment.hardPendingLimit()));
    drainEnded();
    LOG.info(
        "Level 0 of the {} segment was merged down {} s after its bulk load",
        segment.segment().getName(),
        TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - segment.startNanos()));
    return true;
  }

  // the database waits for the merges under way, and no other starts
  private void stopBackgroundWork() {
    stopping = true;
    try {
      if (!done.await(1, TimeUnit.MINUTES)) {
        LOG.warn("Bulk load work still running a minute after the storage started closing");
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Returns once the limits of every segment are back and its small files are merged, or the
   * storage closed.
   */
  void awaitBackgroundWork() throws InterruptedException {
    done.await();
  }

  private void mergeSmallFiles(final SegmentIdentifier segment) {
    if (stopping || storage.isClosed()) {
      return;
    }
    final ColumnFamilyHandle handle = storage.safeColumnHandle(segment);
    final LevelMetaData bottom =
        storage.getDB().getColumnFamilyMetaData(handle).levels().stream()
            .filter(level -> !level.files().isEmpty())
            .reduce((a, b) -> b)
            .orElse(null);
    if (bottom == null) {
      return;
    }
    final long targetBytes = storage.columnFamilyOptions(segment).targetFileSizeBase();
    final List<Merge> merges = merges(bottom.files(), targetBytes);
    if (merges.isEmpty()) {
      return;
    }
    final long start = System.nanoTime();
    int files = 0;
    long bytes = 0;
    int written = 0;
    for (final Merge merge : merges) {
      if (stopping || storage.isClosed()) {
        return;
      }
      final int outputs = merge(handle, merge, bottom.level());
      if (outputs > 0) {
        files += merge.files().size();
        bytes += merge.files().stream().mapToLong(SstFileMetaData::size).sum();
        written += outputs;
      }
    }
    LOG.info(
        "Merged {} files ({} MB) of the {} segment into {} files in {} s, {} small files left",
        files,
        bytes >> 20,
        segment.getName(),
        written,
        TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start),
        smallFiles(segment, targetBytes));
  }

  /**
   * The merges that leave no file of less than a quarter of the target size in the bottom level.
   *
   * <p>Neighbouring small files are merged together. Those that are too few to make up a quarter of
   * the target size, or a small file on its own, take the smaller of their neighbours along. A file
   * that is being compacted stays as it is and is no neighbour.
   */
  @VisibleForTesting
  static List<Merge> merges(final List<SstFileMetaData> bottomFiles, final long targetBytes) {
    final long smallBytes = targetBytes / SMALL_FILE_SHARE;
    final List<SstFileMetaData> files = new ArrayList<>(bottomFiles);
    files.sort(Comparator.comparing(SstFileMetaData::smallestKey, Arrays::compareUnsigned));
    final boolean[] taken = new boolean[files.size()];
    final List<Merge> merges = new ArrayList<>();
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
          final int neighbour = smallerNeighbour(files, taken, chunk.getFirst(), chunk.getLast());
          if (neighbour >= 0) {
            merged.add(neighbour < chunk.getFirst() ? 0 : merged.size(), neighbour);
          }
        }
        if (merged.size() > 1) {
          merged.forEach(index -> taken[index] = true);
          final long total = bytes(files, merged);
          merges.add(
              new Merge(
                  merged.stream().map(files::get).toList(),
                  outputFileBytes(total, targetBytes, smallBytes)));
        }
      }
      i = end + 1;
    }
    return merges;
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
      final List<SstFileMetaData> files, final boolean[] taken, final int first, final int last) {
    final int before = first - 1;
    final int after = last + 1;
    final boolean beforeFree = before >= 0 && !taken[before] && !files.get(before).beingCompacted();
    final boolean afterFree =
        after < files.size() && !taken[after] && !files.get(after).beingCompacted();
    if (beforeFree && afterFree) {
      return files.get(before).size() <= files.get(after).size() ? before : after;
    }
    return beforeFree ? before : afterFree ? after : -1;
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

  // returns the number of files written, 0 when RocksDB turned the merge down
  private int merge(final ColumnFamilyHandle handle, final Merge merge, final int level) {
    try (CompactionOptions options =
        new CompactionOptions().setOutputFileSizeLimit(merge.outputFileBytes())) {
      return storage
          .getDB()
          .compactFiles(
              options,
              handle,
              merge.files().stream().map(SstFileMetaData::fileName).toList(),
              level,
              -1,
              null)
          .size();
    } catch (final RocksDBException e) {
      // a compaction that started meanwhile took one of the files
      LOG.debug("Could not merge {} files: {}", merge.files().size(), e.getMessage());
      return 0;
    }
  }

  private void awaitAll(final List<Future<?>> futures) throws InterruptedException {
    for (final Future<?> future : futures) {
      try {
        future.get();
      } catch (final ExecutionException e) {
        if (!storage.isClosed()) {
          LOG.warn("Merging the small files left by the bulk load failed", e.getCause());
        }
      }
    }
  }

  private long smallFiles(final SegmentIdentifier segment, final long targetBytes) {
    return storage
        .getDB()
        .getColumnFamilyMetaData(storage.safeColumnHandle(segment))
        .levels()
        .stream()
        .filter(level -> !level.files().isEmpty())
        .reduce((a, b) -> b)
        .map(
            bottom ->
                bottom.files().stream()
                    .filter(file -> file.size() < targetBytes / SMALL_FILE_SHARE)
                    .count())
        .orElse(0L);
  }

  private long level0Files(final SegmentIdentifier segment) {
    try {
      return Long.parseLong(
          storage.getDB().getProperty(storage.safeColumnHandle(segment), LEVEL0_FILES_PROPERTY));
    } catch (final RocksDBException e) {
      throw new StorageException(e);
    }
  }

  private long pendingCompactionBytes(final SegmentIdentifier segment) {
    try {
      return storage
          .getDB()
          .getLongProperty(storage.safeColumnHandle(segment), PENDING_COMPACTION_BYTES_PROPERTY);
    } catch (final RocksDBException e) {
      throw new StorageException(e);
    }
  }

  private String levels(final SegmentIdentifier segment) {
    return storage
        .getDB()
        .getColumnFamilyMetaData(storage.safeColumnHandle(segment))
        .levels()
        .stream()
        .filter(level -> !level.files().isEmpty())
        .map(
            level ->
                String.format(
                    "%d files (%d MB) in level %d",
                    level.files().size(), level.size() >> 20, level.level()))
        .collect(Collectors.joining(", "));
  }

  private void setOptions(
      final SegmentIdentifier segment,
      final MutableColumnFamilyOptions.MutableColumnFamilyOptionsBuilder options) {
    final ColumnFamilyHandle handle = storage.safeColumnHandle(segment);
    try {
      storage.getDB().setOptions(handle, options.build());
    } catch (final RocksDBException e) {
      throw new StorageException(e);
    }
  }
}
