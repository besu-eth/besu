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
 * Rewrites a column family without writing through it.
 *
 * <p>Writing the new entries into the live column family would push every byte through the
 * write-ahead log, a memtable flush and a compaction of every level, several times the size of the
 * data. Instead the rewrite runs in three steps, each a method below:
 *
 * <ol>
 *   <li>{@link #writeFiles stage}: the transformed entries are written to sorted SST files in a
 *       directory beside the database. The column family is only read, so an interruption here
 *       leaves nothing to repair and the rewrite is simply run again from the start.
 *   <li>{@link #markPending mark}: a marker holding the entries to add is written durably to the
 *       default column family. From here on the rewrite has to finish, and the marker is how the
 *       next open of the database knows to finish it.
 *   <li>{@link #complete swap}: the column family is dropped, the staged files are ingested into
 *       the empty one, which places them in the bottom level as they are, and the additions are
 *       written in the same batch that removes the marker.
 * </ol>
 */
final class RocksDBSegmentRewrite {
  private static final Logger LOG = LoggerFactory.getLogger(RocksDBSegmentRewrite.class);

  /** Marker keys are the prefix followed by the segment name, one per segment being swapped. */
  private static final byte[] MARKER_PREFIX = "rewrite:".getBytes(StandardCharsets.UTF_8);

  /**
   * Input bytes per staged file. A batch is held in memory until its file is written, so this
   * bounds memory per writer, while still giving files large enough that the ingestion of a few
   * hundred of them is quick.
   */
  private static final long FILE_BYTES = 64L << 20;

  /** The transform is the expensive part of writing a file; a few threads saturate the disk. */
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
   * The staging directory sits beside the database, on the same file system, so the ingestion can
   * move the files in with a rename instead of copying them.
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

  /**
   * Finishes every swap that was interrupted. Runs before the storage is handed out, so nothing can
   * read a segment that is half swapped.
   */
  static void completeInterrupted(final RocksDBColumnarKeyValueStorage storage) {
    for (final Pair<byte[], byte[]> marker : pendingMarkers(storage.getDB())) {
      final String name = segmentName(marker.getKey());
      LOG.info("Finishing the interrupted rewrite of the {} segment", name);
      new RocksDBSegmentRewrite(storage, openSegment(storage, name))
          .complete(decodeAdditions(marker.getValue()));
    }
  }

  private static List<Pair<byte[], byte[]>> pendingMarkers(final RocksDB db) {
    final List<Pair<byte[], byte[]>> markers = new ArrayList<>();
    try (final RocksIterator iterator = db.newIterator(db.getDefaultColumnFamily())) {
      for (iterator.seek(MARKER_PREFIX);
          iterator.isValid() && startsWith(iterator.key(), MARKER_PREFIX);
          iterator.next()) {
        markers.add(Pair.of(iterator.key(), iterator.value()));
      }
    }
    return markers;
  }

  private static SegmentIdentifier openSegment(
      final RocksDBColumnarKeyValueStorage storage, final String name) {
    return storage.columnHandlesBySegmentIdentifier.keySet().stream()
        .filter(candidate -> candidate.getName().equals(name))
        .findFirst()
        .orElseThrow(
            () ->
                new StorageException(
                    "An interrupted rewrite of the "
                        + name
                        + " segment cannot be finished, the segment is not open"));
  }

  /**
   * Stages the transformed entries as SST files. The segment is read in key order and cut into
   * batches of about {@link #FILE_BYTES} of input, each written to its own file by a writer thread.
   * Consecutive batches of a key ordered stream cover disjoint key ranges, which is all the
   * ingestion asks of the files.
   */
  void writeFiles(final BiFunction<byte[], byte[], byte[]> transform) {
    final Progress progress = new Progress(segment.getName());
    try (final FileWriters writers = new FileWriters(transform);
        final Stream<Pair<byte[], byte[]>> entries = storage.stream(segment)) {
      // files left by an earlier attempt would be ingested alongside the new ones
      deleteDirectory(directory);
      Files.createDirectories(directory);

      List<Pair<byte[], byte[]>> batch = new ArrayList<>();
      long batchBytes = 0;
      for (final Iterator<Pair<byte[], byte[]>> iterator = entries.iterator();
          iterator.hasNext(); ) {
        final Pair<byte[], byte[]> entry = iterator.next();
        batch.add(entry);
        batchBytes += entry.getValue().length;
        progress.advance(entry.getValue().length);
        if (batchBytes >= FILE_BYTES || !iterator.hasNext()) {
          writers.submit(batch);
          batch = new ArrayList<>();
          batchBytes = 0;
        }
      }
      writers.awaitAll();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new StorageException(
          "Interrupted while rewriting the " + segment.getName() + " segment");
    } catch (final ExecutionException e) {
      throw new StorageException(e.getCause());
    } catch (final IOException e) {
      throw new StorageException(e);
    }
    progress.finish();
  }

  /**
   * The threads writing the staged files, and the bound on how far the reader may run ahead of
   * them. The reader is much faster than the writers, so without the bound every batch of the
   * segment would end up in memory waiting for a thread.
   */
  private final class FileWriters implements AutoCloseable {
    private final BiFunction<byte[], byte[], byte[]> transform;
    private final ExecutorService executor = Executors.newFixedThreadPool(WRITER_THREADS);
    private final Semaphore inFlight = new Semaphore(WRITER_THREADS + 2);
    private final List<Future<?>> files = new ArrayList<>();
    // the ingestion only accepts files written with the column family's own comparator and
    // compression, which the column family options carry
    private final Options sstOptions;
    private final EnvOptions envOptions = new EnvOptions();

    FileWriters(final BiFunction<byte[], byte[], byte[]> transform) {
      this.transform = transform;
      this.sstOptions = new Options(storage.options, storage.columnFamilyOptions(segment));
    }

    /**
     * Blocks while too many batches are in flight, and rethrows the failure of any earlier file.
     */
    void submit(final List<Pair<byte[], byte[]>> batch)
        throws InterruptedException, ExecutionException {
      inFlight.acquire();
      final int index = files.size();
      files.add(
          executor.submit(
              () -> {
                try {
                  writeFile(index, batch, transform, envOptions, sstOptions);
                } finally {
                  inFlight.release();
                }
              }));
      // a failed file surfaces here rather than after the rest of the segment has been read for
      // nothing
      for (final Future<?> file : files) {
        if (file.isDone()) {
          file.get();
        }
      }
    }

    void awaitAll() throws InterruptedException, ExecutionException {
      for (final Future<?> file : files) {
        file.get();
      }
    }

    @Override
    public void close() {
      executor.shutdownNow();
      envOptions.close();
      sstOptions.close();
    }
  }

  /**
   * Writes one staged file. The file is built under a temporary name and renamed once it is
   * finished, because {@link #complete} ingests every {@code .sst} file it finds and must not pick
   * up one that was cut short.
   */
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
        // a null value drops the entry from the segment
        final byte[] value = transform.apply(entry.getKey(), entry.getValue());
        if (value != null) {
          writer.put(entry.getKey(), value);
          written++;
        }
      }
      // RocksDB refuses to finish a file without entries, and there is nothing to ingest anyway
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

  /**
   * Records the swap durably before anything is dropped. The additions travel with the marker
   * because the caller that knows them is gone when the swap is finished after a restart.
   */
  void markPending(final List<Pair<byte[], byte[]>> additions) {
    final RocksDB db = storage.getDB();
    try (final WriteOptions durable = new WriteOptions().setSync(true)) {
      db.put(db.getDefaultColumnFamily(), durable, markerKey(), encodeAdditions(additions));
    } catch (final RocksDBException e) {
      throw new StorageException(e);
    }
  }

  /**
   * Drops the column family, ingests the staged files into the empty one, then writes the additions
   * and removes the marker together. Every step is safe to repeat after an interruption:
   *
   * <ul>
   *   <li>The ingestion moves the files, so staged files that are gone were ingested already. In
   *       that case the column family is not dropped again, which would throw the ingested data
   *       away.
   *   <li>A column family that was dropped before its files were ingested is empty, and dropping it
   *       again costs nothing.
   *   <li>The additions and the marker's removal are one atomic batch, so either the marker is
   *       still there and the additions are written on the next attempt, or both are done.
   * </ul>
   */
  void complete(final List<Pair<byte[], byte[]>> additions) {
    final RocksDB db = storage.getDB();
    try {
      final List<String> files = stagedFiles();
      if (!files.isEmpty()) {
        // dropping and recreating the column family is what lets the files land in the bottom
        // level: an ingestion into a column family with overlapping keys would have to go higher
        storage.clear(segment);
        try (final IngestExternalFileOptions options =
            new IngestExternalFileOptions().setMoveFiles(true)) {
          db.ingestExternalFile(storage.safeColumnHandle(segment), files, options);
        }
        LOG.info("Ingested {} files into the {} segment", files.size(), segment.getName());
      }
      // only files cut short by an interruption can still be here
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

  /**
   * The marker lives in the default column family because the segment's own column family is
   * dropped during the swap, and the default one never is.
   */
  private byte[] markerKey() {
    final byte[] name = segment.getName().getBytes(StandardCharsets.UTF_8);
    final byte[] key = Arrays.copyOf(MARKER_PREFIX, MARKER_PREFIX.length + name.length);
    System.arraycopy(name, 0, key, MARKER_PREFIX.length, name.length);
    return key;
  }

  private static String segmentName(final byte[] markerKey) {
    return new String(
        Arrays.copyOfRange(markerKey, MARKER_PREFIX.length, markerKey.length),
        StandardCharsets.UTF_8);
  }

  private static boolean startsWith(final byte[] key, final byte[] prefix) {
    return key.length >= prefix.length
        && Arrays.equals(key, 0, prefix.length, prefix, 0, prefix.length);
  }

  /** Length prefixed key value pairs; the additions are few and small, so nothing fancier. */
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
