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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.EnvOptions;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.SstFileWriter;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rewrites a column family without writing through it. Writing the new entries into the live column
 * family would push every byte through the write-ahead log, a memtable flush and a compaction of
 * every level, several times the size of the data. Instead the new entries are written to sorted
 * SST files beside the database, then swapped in by dropping the column family and ingesting the
 * files, which places them in the bottom level as they are.
 *
 * <p>The swap is recorded under a marker in the default column family before it starts, together
 * with the entries to add, so that it can be finished from the marker when the database is opened
 * again after an interruption. Until the swap the column family is untouched, so a rewrite that is
 * interrupted before it can simply be run again.
 */
final class RocksDBSegmentRewrite {
  private static final Logger LOG = LoggerFactory.getLogger(RocksDBSegmentRewrite.class);

  private static final byte[] MARKER_PREFIX = "rewrite:".getBytes(StandardCharsets.UTF_8);

  private static final long FILE_BYTES = 64L << 20;

  private static final int WRITER_THREADS = 4;

  private static final long REPORT_INTERVAL_MILLIS = 30_000;

  private final RocksDBColumnarKeyValueStorage storage;
  private final SegmentIdentifier segment;
  private final Path directory;

  RocksDBSegmentRewrite(
      final RocksDBColumnarKeyValueStorage storage, final SegmentIdentifier segment) {
    this.storage = storage;
    this.segment = segment;
    this.directory = stagingDirectory(storage.configuration.getDatabaseDir(), segment.getName());
  }

  /**
   * The SST files are staged beside the database, on the same file system, so they can be moved.
   */
  static Path stagingDirectory(final Path databaseDir, final String segmentName) {
    return databaseDir.resolveSibling(databaseDir.getFileName() + "-rewrite").resolve(segmentName);
  }

  void run(
      final BiFunction<byte[], byte[], byte[]> transform,
      final List<Pair<byte[], byte[]>> additions) {
    writeFiles(transform);
    markPending(additions);
    complete(additions);
  }

  /** Finishes every swap that was interrupted, before the storage is handed out. */
  static void completeInterrupted(final RocksDBColumnarKeyValueStorage storage) {
    final RocksDB db = storage.getDB();
    final List<Pair<byte[], byte[]>> markers = new ArrayList<>();
    try (final RocksIterator iterator = db.newIterator(db.getDefaultColumnFamily())) {
      for (iterator.seek(MARKER_PREFIX);
          iterator.isValid() && startsWith(iterator.key(), MARKER_PREFIX);
          iterator.next()) {
        markers.add(Pair.of(iterator.key(), iterator.value()));
      }
    }
    for (final Pair<byte[], byte[]> marker : markers) {
      final String name =
          new String(
              Arrays.copyOfRange(marker.getKey(), MARKER_PREFIX.length, marker.getKey().length),
              StandardCharsets.UTF_8);
      final SegmentIdentifier segment =
          storage.columnHandlesBySegmentIdentifier.keySet().stream()
              .filter(candidate -> candidate.getName().equals(name))
              .findFirst()
              .orElseThrow(
                  () ->
                      new StorageException(
                          "An interrupted rewrite of the "
                              + name
                              + " segment cannot be finished, the segment is not open"));
      LOG.info("Finishing the interrupted rewrite of the {} segment", name);
      new RocksDBSegmentRewrite(storage, segment).complete(decodeAdditions(marker.getValue()));
    }
  }

  /**
   * Writes the transformed entries to files of about {@link #FILE_BYTES} of input each, on a few
   * threads. Consecutive batches of the key ordered stream never overlap, which is all the
   * ingestion asks of the files.
   */
  void writeFiles(final BiFunction<byte[], byte[], byte[]> transform) {
    final ExecutorService writers = Executors.newFixedThreadPool(WRITER_THREADS);
    final Semaphore inFlight = new Semaphore(WRITER_THREADS + 2);
    final List<Future<?>> files = new ArrayList<>();
    final Progress progress = new Progress(segment.getName());
    try (final Options sstOptions =
            new Options(storage.options, storage.columnFamilyOptions(segment));
        final EnvOptions envOptions = new EnvOptions();
        final Stream<Pair<byte[], byte[]>> entries = storage.stream(segment)) {
      deleteDirectory(directory);
      Files.createDirectories(directory);
      final Iterator<Pair<byte[], byte[]>> iterator = entries.iterator();
      List<Pair<byte[], byte[]>> batch = new ArrayList<>();
      long batchBytes = 0;
      while (iterator.hasNext()) {
        final Pair<byte[], byte[]> entry = iterator.next();
        batch.add(entry);
        batchBytes += entry.getValue().length;
        progress.advance(entry.getValue().length);
        if (batchBytes >= FILE_BYTES || !iterator.hasNext()) {
          final int index = files.size();
          final List<Pair<byte[], byte[]>> ready = batch;
          inFlight.acquire();
          files.add(
              writers.submit(
                  () -> {
                    try {
                      writeFile(index, ready, transform, envOptions, sstOptions);
                    } finally {
                      inFlight.release();
                    }
                  }));
          batch = new ArrayList<>();
          batchBytes = 0;
          for (final Future<?> file : files) {
            if (file.isDone()) {
              file.get();
            }
          }
        }
      }
      for (final Future<?> file : files) {
        file.get();
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new StorageException(
          "Interrupted while rewriting the " + segment.getName() + " segment");
    } catch (final ExecutionException e) {
      throw new StorageException(e.getCause());
    } catch (final IOException e) {
      throw new StorageException(e);
    } finally {
      writers.shutdownNow();
    }
    progress.finish();
  }

  private void writeFile(
      final int index,
      final List<Pair<byte[], byte[]>> batch,
      final BiFunction<byte[], byte[], byte[]> transform,
      final EnvOptions envOptions,
      final Options sstOptions) {
    final Path file = directory.resolve(String.format("%06d.sst", index));
    final Path partial = directory.resolve(String.format("%06d.partial", index));
    int written = 0;
    try (final SstFileWriter writer = new SstFileWriter(envOptions, sstOptions)) {
      writer.open(partial.toString());
      for (final Pair<byte[], byte[]> entry : batch) {
        final byte[] value = transform.apply(entry.getKey(), entry.getValue());
        if (value != null) {
          writer.put(entry.getKey(), value);
          written++;
        }
      }
      // a file without entries cannot be finished, nor is it needed
      if (written > 0) {
        writer.finish();
      }
    } catch (final RocksDBException e) {
      throw new StorageException(e);
    }
    try {
      if (written == 0) {
        Files.deleteIfExists(partial);
      } else {
        Files.move(partial, file, StandardCopyOption.ATOMIC_MOVE);
      }
    } catch (final IOException e) {
      throw new StorageException(e);
    }
  }

  /** Records the swap durably, with the entries it has to add, before anything is dropped. */
  void markPending(final List<Pair<byte[], byte[]>> additions) {
    final RocksDB db = storage.getDB();
    try (final WriteOptions durable = new WriteOptions().setSync(true)) {
      db.put(db.getDefaultColumnFamily(), durable, markerKey(), encodeAdditions(additions));
    } catch (final RocksDBException e) {
      throw new StorageException(e);
    }
  }

  /**
   * Drops the column family and ingests the staged files into the empty one, then adds the extra
   * entries and removes the marker together. Every step can be repeated: files that are gone were
   * ingested already, and a column family that was dropped without its files being ingested is
   * dropped again before they are.
   */
  void complete(final List<Pair<byte[], byte[]>> additions) {
    final RocksDB db = storage.getDB();
    try {
      final List<String> files = stagedFiles();
      if (!files.isEmpty()) {
        storage.clear(segment);
        try (final IngestExternalFileOptions options =
            new IngestExternalFileOptions().setMoveFiles(true)) {
          db.ingestExternalFile(storage.safeColumnHandle(segment), files, options);
        }
        LOG.info("Ingested {} files into the {} segment", files.size(), segment.getName());
      }
      deleteDirectory(directory);
      final ColumnFamilyHandle handle = storage.safeColumnHandle(segment);
      try (final WriteBatch batch = new WriteBatch();
          final WriteOptions durable = new WriteOptions().setSync(true)) {
        for (final Pair<byte[], byte[]> addition : additions) {
          batch.put(handle, addition.getKey(), addition.getValue());
        }
        batch.delete(db.getDefaultColumnFamily(), markerKey());
        db.write(durable, batch);
      }
    } catch (final RocksDBException | IOException e) {
      throw new StorageException(e);
    }
  }

  private List<String> stagedFiles() throws IOException {
    if (!Files.isDirectory(directory)) {
      return List.of();
    }
    try (final Stream<Path> files = Files.list(directory)) {
      return files
          .filter(file -> file.getFileName().toString().endsWith(".sst"))
          .sorted()
          .map(Path::toString)
          .toList();
    }
  }

  private byte[] markerKey() {
    final byte[] name = segment.getName().getBytes(StandardCharsets.UTF_8);
    final byte[] key = Arrays.copyOf(MARKER_PREFIX, MARKER_PREFIX.length + name.length);
    System.arraycopy(name, 0, key, MARKER_PREFIX.length, name.length);
    return key;
  }

  private static boolean startsWith(final byte[] key, final byte[] prefix) {
    return key.length >= prefix.length
        && Arrays.equals(key, 0, prefix.length, prefix, 0, prefix.length);
  }

  private static byte[] encodeAdditions(final List<Pair<byte[], byte[]>> additions) {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (final DataOutputStream out = new DataOutputStream(bytes)) {
      out.writeInt(additions.size());
      for (final Pair<byte[], byte[]> addition : additions) {
        out.writeInt(addition.getKey().length);
        out.write(addition.getKey());
        out.writeInt(addition.getValue().length);
        out.write(addition.getValue());
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    return bytes.toByteArray();
  }

  private static List<Pair<byte[], byte[]>> decodeAdditions(final byte[] encoded) {
    final List<Pair<byte[], byte[]>> additions = new ArrayList<>();
    try (final DataInputStream in = new DataInputStream(new ByteArrayInputStream(encoded))) {
      final int count = in.readInt();
      for (int i = 0; i < count; i++) {
        final byte[] key = in.readNBytes(in.readInt());
        final byte[] value = in.readNBytes(in.readInt());
        additions.add(Pair.of(key, value));
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    return additions;
  }

  private static void deleteDirectory(final Path directory) throws IOException {
    if (!Files.isDirectory(directory)) {
      return;
    }
    try (final Stream<Path> files = Files.list(directory)) {
      for (final Path file : files.toList()) {
        Files.delete(file);
      }
    }
    Files.delete(directory);
    // the parent only ever holds staging directories, so it goes as soon as it is empty
    try {
      Files.delete(directory.getParent());
    } catch (final IOException ignored) {
      // another segment is still being rewritten
    }
  }

  /** Reports on the entries read at a steady pace, but not at all when there are none. */
  private static final class Progress {
    private final String segment;
    private final long start = System.currentTimeMillis();
    private long lastReport = start;
    private long entries = 0;
    private long bytes = 0;

    Progress(final String segment) {
      this.segment = segment;
    }

    void advance(final int valueBytes) {
      if (entries == 0) {
        LOG.info("Rewriting the {} segment", segment);
      }
      entries++;
      bytes += valueBytes;
      final long now = System.currentTimeMillis();
      if (now - lastReport >= REPORT_INTERVAL_MILLIS) {
        lastReport = now;
        final long seconds = Math.max(1, (now - start) / 1000);
        LOG.info(
            "Rewriting the {} segment: {} entries, {} MB in {} s ({} MB/s)",
            segment,
            entries,
            bytes >> 20,
            seconds,
            (bytes >> 20) / seconds);
      }
    }

    void finish() {
      if (entries > 0) {
        LOG.info(
            "Rewrote the {} segment: {} entries, {} MB in {} s",
            segment,
            entries,
            bytes >> 20,
            (System.currentTimeMillis() - start) / 1000);
      }
    }
  }
}
