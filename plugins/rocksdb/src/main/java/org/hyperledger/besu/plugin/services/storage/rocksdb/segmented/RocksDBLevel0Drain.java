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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.MutableColumnFamilyOptions;
import org.rocksdb.MutableDBOptions;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lets RocksDB merge level 0 of column families down after a bulk load held their compaction back,
 * with the limits on level 0 and on the bytes waiting to be compacted lifted until it has.
 *
 * <p>RocksDB does not hold back writes for the files in level 0 while automatic compaction is off,
 * but does so the moment it is switched on: with thousands of files, or that many bytes waiting to
 * be compacted, it would slow down or stop every write to the database, not only the ones to that
 * column family, and every flush, until it has merged them down. Nothing waits for the merge.
 *
 * <p>While level 0 is merged down the compactions read ahead less: such a merge reads every file of
 * level 0 at once, each with a buffer of its own. They also get more threads, as the merge holds
 * one for as long as it runs. Both are options of the whole database, so they stay until the last
 * column family has been merged down.
 *
 * <p>RocksDB keeps the options set at runtime in its options file, and the database opens with the
 * configured ones. A column family saved with its compaction off or its limits lifted had its bulk
 * load or its merge cut short, and goes through the merge again when the database opens.
 *
 * <p>All of it runs under the lock of the storage on its column families.
 */
final class RocksDBLevel0Drain {

  private static final Logger LOG = LoggerFactory.getLogger(RocksDBLevel0Drain.class);

  static final int NO_LEVEL0_LIMIT = Integer.MAX_VALUE;
  private static final long NO_PENDING_COMPACTION_LIMIT = 0;
  private static final String LEVEL0_FILES_PROPERTY = "rocksdb.num-files-at-level0";
  private static final String PENDING_COMPACTION_BYTES_PROPERTY =
      "rocksdb.estimate-pending-compaction-bytes";
  private static final long POLL_MILLIS = 500;
  // the buffers of a thousand files in level 0 then take 256 MB, against 2 GB at RocksDB's default
  private static final long DRAIN_READAHEAD_BYTES = 256 * 1024;
  // the merge of a level 0 is one compaction that keeps its thread until it is done, which can take
  // most of an hour; without more threads the other column families stop compacting meanwhile and
  // their writes slow down once their own level 0 fills up
  private static final int DRAIN_EXTRA_BACKGROUND_JOBS = 2;

  private final RocksDBColumnarKeyValueStorage storage;
  // the column families whose level 0 is still merged down, by when they started
  private final Map<SegmentIdentifier, Draining> draining = new LinkedHashMap<>();
  private boolean watching;

  private static final class Draining {
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private final long startNanos = System.nanoTime();
    private boolean warned;
  }

  RocksDBLevel0Drain(final RocksDBColumnarKeyValueStorage storage) {
    this.storage = storage;
  }

  /** Sets the options of a merge on a database about to be opened, which starts merging at once. */
  static void useDrainOptions(final DBOptions options) {
    options
        .setCompactionReadaheadSize(DRAIN_READAHEAD_BYTES)
        .setMaxBackgroundJobs(options.maxBackgroundJobs() + DRAIN_EXTRA_BACKGROUND_JOBS);
  }

  /** Whether the saved options of a column family show a bulk load or its merge cut short. */
  static boolean isCutShort(final ColumnFamilyOptions saved) {
    return saved.disableAutoCompactions() || saved.level0StopWritesTrigger() == NO_LEVEL0_LIMIT;
  }

  /**
   * Switches the automatic compaction of a column family back on, with its limits lifted while its
   * level 0 is behind.
   *
   * @return completes once level 0 is merged down
   */
  CompletableFuture<Void> resume(final SegmentIdentifier segment) {
    storage.columnFamilyWork.lock();
    try {
      storage.throwIfClosed();
      if (isDrained(segment)) {
        storage.setOptions(
            segment, MutableColumnFamilyOptions.builder().setDisableAutoCompactions(false));
        return CompletableFuture.completedFuture(null);
      }
      return lift(segment);
    } finally {
      storage.columnFamilyWork.unlock();
    }
  }

  /** Merges level 0 of the column families whose bulk load or merge was cut short. */
  void resumeCutShort(final Collection<SegmentIdentifier> segments) {
    if (segments.isEmpty()) {
      return;
    }
    storage.columnFamilyWork.lock();
    try {
      for (final SegmentIdentifier segment : segments) {
        try {
          if (!isDrained(segment)) {
            LOG.info(
                "The bulk load of the {} segment, or the merge after it, was cut short: merging level 0 down again",
                segment.getName());
            lift(segment);
          }
        } catch (final RuntimeException e) {
          LOG.warn(
              "Could not lift the limits of the {} segment, writes slow down or stop until its level 0 is merged down",
              segment.getName(),
              e);
        }
      }
      if (draining.isEmpty()) {
        // the database was opened for a merge
        setDrainOptions(false);
      }
    } finally {
      storage.columnFamilyWork.unlock();
    }
  }

  /** Completes once level 0 of the column family is merged down, or at once if it is not behind. */
  CompletableFuture<Void> whenDrained(final SegmentIdentifier segment) {
    storage.columnFamilyWork.lock();
    try {
      final Draining entry = draining.get(segment);
      return entry == null ? CompletableFuture.completedFuture(null) : entry.drained;
    } finally {
      storage.columnFamilyWork.unlock();
    }
  }

  /** Stops watching, as the storage closes. */
  void close() {
    storage.columnFamilyWork.lock();
    try {
      draining
          .values()
          .forEach(
              entry -> entry.drained.completeExceptionally(new StorageException("Storage closed")));
      draining.clear();
    } finally {
      storage.columnFamilyWork.unlock();
    }
  }

  private CompletableFuture<Void> lift(final SegmentIdentifier segment) {
    final boolean first = draining.isEmpty();
    if (first) {
      // before the merge starts, which takes the readahead it finds
      setDrainOptions(true);
    }
    try {
      storage.setOptions(
          segment,
          MutableColumnFamilyOptions.builder()
              .setLevel0SlowdownWritesTrigger(NO_LEVEL0_LIMIT)
              .setLevel0StopWritesTrigger(NO_LEVEL0_LIMIT)
              .setSoftPendingCompactionBytesLimit(NO_PENDING_COMPACTION_LIMIT)
              .setHardPendingCompactionBytesLimit(NO_PENDING_COMPACTION_LIMIT)
              .setDisableAutoCompactions(false));
    } catch (final RuntimeException e) {
      if (first) {
        setDrainOptions(false);
      }
      throw e;
    }
    final Draining entry = draining.computeIfAbsent(segment, unused -> new Draining());
    if (!watching) {
      watching = true;
      Thread.ofPlatform().daemon().name("rocksdb-level0-drain").start(this::watch);
    }
    return entry.drained;
  }

  private void watch() {
    while (true) {
      storage.columnFamilyWork.lock();
      try {
        if (storage.isClosed()) {
          watching = false;
          return;
        }
        draining.entrySet().removeIf(entry -> restoreIfDrained(entry.getKey(), entry.getValue()));
        if (draining.isEmpty()) {
          setDrainOptions(false);
          watching = false;
          return;
        }
      } finally {
        storage.columnFamilyWork.unlock();
      }
      try {
        Thread.sleep(POLL_MILLIS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        storage.columnFamilyWork.lock();
        try {
          watching = false;
        } finally {
          storage.columnFamilyWork.unlock();
        }
        return;
      }
    }
  }

  private boolean restoreIfDrained(final SegmentIdentifier segment, final Draining entry) {
    try {
      if (!isDrained(segment)) {
        return false;
      }
      final ColumnFamilyOptions configured = storage.columnFamilyOptions(segment);
      storage.setOptions(
          segment,
          MutableColumnFamilyOptions.builder()
              .setLevel0SlowdownWritesTrigger(configured.level0SlowdownWritesTrigger())
              .setLevel0StopWritesTrigger(configured.level0StopWritesTrigger())
              .setSoftPendingCompactionBytesLimit(configured.softPendingCompactionBytesLimit())
              .setHardPendingCompactionBytesLimit(configured.hardPendingCompactionBytesLimit()));
    } catch (final RuntimeException e) {
      // tried again at the next poll, the other column families go on meanwhile
      if (!entry.warned) {
        entry.warned = true;
        LOG.warn(
            "Watching level 0 of the {} segment failed, its limits stay lifted until it succeeds",
            segment.getName(),
            e);
      }
      return false;
    }
    LOG.info(
        "Level 0 of the {} segment was merged down {} s after its bulk load",
        segment.getName(),
        TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - entry.startNanos));
    entry.drained.complete(null);
    return true;
  }

  // best effort: the merge works with any readahead and any number of threads
  private void setDrainOptions(final boolean merging) {
    final long readaheadBytes =
        merging ? DRAIN_READAHEAD_BYTES : storage.configuredCompactionReadahead;
    final int backgroundJobs =
        storage.configuredBackgroundJobs + (merging ? DRAIN_EXTRA_BACKGROUND_JOBS : 0);
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

  private boolean isDrained(final SegmentIdentifier segment) {
    final ColumnFamilyOptions configured = storage.columnFamilyOptions(segment);
    final long softPendingLimit = configured.softPendingCompactionBytesLimit();
    return level0Files(segment) < configured.level0SlowdownWritesTrigger()
        && (softPendingLimit == NO_PENDING_COMPACTION_LIMIT
            || pendingCompactionBytes(segment) < softPendingLimit);
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
}
