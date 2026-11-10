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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.MutableColumnFamilyOptions;
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
 * <p>When the load is closed the column families go back to compacting on their own, and {@link
 * RocksDBLevel0Drain} lets RocksDB merge their level 0 down in the background. Nothing is compacted
 * by hand: a manual compaction of a whole column family merges every level into the next and so
 * rewrites the bottom level, which holds nearly all of the load, for the sake of the few files
 * above it.
 *
 * <p>A column family can be let go before the others, when it is read much before the load ends and
 * its regular writes piled up in level 0, where a read checks every file.
 *
 * <p>The column families of large values written in no particular key order keep compacting, but
 * put their values in blob files until the load is closed. Each compaction on the way to the bottom
 * level would otherwise rewrite the values along with their keys. Their compaction is not held back
 * instead: a lookup checks every file of level 0, and such keys overlap every file there.
 */
final class RocksDBSegmentBulkLoad implements SegmentBulkLoad {

  private static final Logger LOG = LoggerFactory.getLogger(RocksDBSegmentBulkLoad.class);

  private final RocksDBColumnarKeyValueStorage storage;
  private final List<SegmentIdentifier> segments;
  // the segments whose values go to blob files during the load
  private final List<SegmentIdentifier> separateValues;
  private boolean closed;
  // the segments let go before the load was closed
  private final Set<SegmentIdentifier> ended = new HashSet<>();

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
    storage.columnFamilyWork.lock();
    try {
      storage.throwIfClosed();
      // all of them before any is changed
      this.segments.forEach(storage::safeColumnHandle);
      this.separateValues.forEach(storage::safeColumnHandle);
      start();
    } finally {
      storage.columnFamilyWork.unlock();
    }
  }

  private void start() {
    final List<SegmentIdentifier> started = new ArrayList<>();
    try {
      for (final SegmentIdentifier segment : segments) {
        storage.setOptions(
            segment, MutableColumnFamilyOptions.builder().setDisableAutoCompactions(true));
        started.add(segment);
      }
      // the blob files are compressed as the column family is configured when the database opens
      for (final SegmentIdentifier segment : separateValues) {
        storage.setOptions(
            segment,
            MutableColumnFamilyOptions.builder()
                .setEnableBlobFiles(true)
                .setMinBlobSize(RocksDBColumnarKeyValueStorage.MIN_BLOB_SIZE));
        started.add(segment);
      }
    } catch (final RuntimeException e) {
      // the caller gets no load to close, which would put them back
      for (final SegmentIdentifier segment : started) {
        try {
          if (separateValues.contains(segment)) {
            restoreValues(segment);
          } else {
            storage.setOptions(
                segment, MutableColumnFamilyOptions.builder().setDisableAutoCompactions(false));
          }
        } catch (final RuntimeException undo) {
          e.addSuppressed(undo);
        }
      }
      throw e;
    }
  }

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;
    storage.columnFamilyWork.lock();
    try {
      if (storage.isClosed()) {
        // the column families come back as configured when the database is opened
        return;
      }
      for (final SegmentIdentifier segment : separateValues) {
        if (!ended.contains(segment)) {
          restoreValues(segment);
        }
      }
      for (final SegmentIdentifier segment : segments) {
        if (ended.contains(segment)) {
          continue;
        }
        try {
          resumeCompaction(segment);
        } catch (final RuntimeException e) {
          // the other segments still have to get theirs back
          LOG.warn(
              "Automatic compaction of the {} segment could not be switched back on after its bulk load, it stays off until the database is opened again",
              segment.getName(),
              e);
        }
      }
    } finally {
      storage.columnFamilyWork.unlock();
    }
  }

  /**
   * Lets one segment go before the load is closed, in the way {@link #close()} does.
   *
   * <p>The merge of its level 0 that follows is not stopped by cancelling the returned future, and
   * the limits it lifted are put back once it is done whatever happened to the future.
   */
  @Override
  public synchronized CompletableFuture<Void> end(final SegmentIdentifier segment) {
    storage.columnFamilyWork.lock();
    try {
      if (storage.isClosed()) {
        return CompletableFuture.failedFuture(new StorageException("Storage closed"));
      }
      if (separateValues.contains(segment)) {
        if (!closed && ended.add(segment)) {
          restoreValues(segment);
        }
        return CompletableFuture.completedFuture(null);
      }
      if (!segments.contains(segment)) {
        return CompletableFuture.completedFuture(null);
      }
      if (closed || ended.contains(segment)) {
        return storage.level0Drain.whenDrained(segment);
      }
      final CompletableFuture<Void> drained = resumeCompaction(segment);
      // not before: close() switches its compaction on if this failed
      ended.add(segment);
      return drained;
    } catch (final RuntimeException e) {
      return CompletableFuture.failedFuture(e);
    } finally {
      storage.columnFamilyWork.unlock();
    }
  }

  // the values written so far stay in their blob files, which reads find as they find the others
  private void restoreValues(final SegmentIdentifier segment) {
    final ColumnFamilyOptions configured = storage.columnFamilyOptions(segment);
    try {
      storage.setOptions(
          segment,
          MutableColumnFamilyOptions.builder()
              .setEnableBlobFiles(configured.enableBlobFiles())
              .setMinBlobSize(configured.minBlobSize()));
    } catch (final RuntimeException e) {
      LOG.warn(
          "The {} segment keeps writing its values to blob files after its bulk load, until the database is opened again",
          segment.getName(),
          e);
    }
  }

  private CompletableFuture<Void> resumeCompaction(final SegmentIdentifier segment) {
    LOG.info("Bulk load of the {} segment ended with {}", segment.getName(), levels(segment));
    return storage.level0Drain.resume(segment);
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
}
