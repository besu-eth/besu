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
import org.hyperledger.besu.plugin.services.storage.SortedWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes entries in key order to an SST file, then moves it into its column family, where it is
 * placed as low as its keys allow instead of going through the memtable and every compaction.
 */
final class RocksDBSortedWriter implements SortedWriter {

  private static final Logger LOG = LoggerFactory.getLogger(RocksDBSortedWriter.class);

  private final RocksDBColumnarKeyValueStorage storage;
  private final SegmentIdentifier segment;
  private final Path file;
  private final Options options;
  private final EnvOptions envOptions;
  private final SstFileWriter writer;
  private boolean empty = true;
  // the native writer is released once done, and using it after that would crash the JVM
  private boolean done;

  RocksDBSortedWriter(
      final RocksDBColumnarKeyValueStorage storage,
      final SegmentIdentifier segment,
      final Path directory) {
    this.storage = storage;
    this.segment = segment;
    this.file = directory.resolve(UUID.randomUUID() + ".sst");
    // first, as it throws for a segment that is not open, before anything needs releasing
    this.options = storage.sstFileOptions(segment);
    this.envOptions = new EnvOptions();
    this.writer = new SstFileWriter(envOptions, options);
    try {
      Files.createDirectories(directory);
      writer.open(file.toString());
    } catch (final IOException | RocksDBException e) {
      close();
      throw new StorageException(e);
    }
  }

  /**
   * The files are written beside the database, on the same file system, so that adding them is a
   * rename; those of a writer stopped by a crash are deleted when the database opens.
   */
  static Path directory(final Path databaseDir) {
    return databaseDir.resolveSibling(databaseDir.getFileName() + "-sorted");
  }

  static void deleteLeftovers(final Path databaseDir) {
    final Path directory = directory(databaseDir);
    try {
      RocksDBSegmentRewrite.deleteTree(directory);
    } catch (final IOException e) {
      // they only take up space, which is no reason to keep the database closed
      LOG.warn("The sorted files left in {} could not be removed", directory, e);
    }
  }

  @Override
  public void put(final byte[] key, final byte[] value) {
    throwIfDone();
    try {
      writer.put(key, value);
      empty = false;
    } catch (final RocksDBException e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void commit() {
    throwIfDone();
    try {
      // RocksDB refuses to finish a file without entries, and there is nothing to add anyway
      if (!empty) {
        writer.finish();
        storage.ingest(segment, List.of(file.toString()), true);
      }
    } catch (final RocksDBException e) {
      throw new StorageException(e);
    } finally {
      close();
    }
  }

  @Override
  public void rollback() {
    if (!done) {
      close();
    }
  }

  private void throwIfDone() {
    if (done) {
      throw new IllegalStateException("The sorted writer was already committed or rolled back");
    }
  }

  private void close() {
    done = true;
    writer.close();
    options.close();
    envOptions.close();
    try {
      Files.deleteIfExists(file);
    } catch (final IOException e) {
      // the entries are added or dropped either way, and the file goes when the database opens
      LOG.warn("The sorted file {} could not be deleted", file, e);
    }
  }
}
