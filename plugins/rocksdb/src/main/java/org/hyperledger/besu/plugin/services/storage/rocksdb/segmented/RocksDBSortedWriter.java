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
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;

/**
 * Writes entries in key order to an SST file, then moves it into its column family, where it is
 * placed as low as its keys allow instead of going through the memtable and every compaction.
 */
final class RocksDBSortedWriter implements SortedWriter {

  private final RocksDBColumnarKeyValueStorage storage;
  private final SegmentIdentifier segment;
  private final Path file;
  private final Options options;
  private final EnvOptions envOptions = new EnvOptions();
  private final SstFileWriter writer;
  private boolean empty = true;

  RocksDBSortedWriter(
      final RocksDBColumnarKeyValueStorage storage,
      final SegmentIdentifier segment,
      final Path directory) {
    this.storage = storage;
    this.segment = segment;
    this.file = directory.resolve(UUID.randomUUID() + ".sst");
    this.options = storage.sstFileOptions(segment);
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
    if (!Files.isDirectory(directory)) {
      return;
    }
    try (final Stream<Path> files = Files.list(directory)) {
      for (final Path leftover : files.toList()) {
        Files.deleteIfExists(leftover);
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void put(final byte[] key, final byte[] value) {
    try {
      writer.put(key, value);
      empty = false;
    } catch (final RocksDBException e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void commit() {
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
    close();
  }

  private void close() {
    writer.close();
    options.close();
    envOptions.close();
    try {
      Files.deleteIfExists(file);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
