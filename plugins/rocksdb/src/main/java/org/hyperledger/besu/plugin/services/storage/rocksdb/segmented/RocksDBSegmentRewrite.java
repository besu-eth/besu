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
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.EnvOptions;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
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
 *   <li>{@link #markPending mark}: a marker holding the number of staged files and the entries to
 *       add is written durably to the default column family. From here on the rewrite has to
 *       finish, and the marker is how the next open of the database, or the next call, knows to
 *       finish it.
 *   <li>{@link #complete swap}: the column family is dropped, the staged files are ingested into
 *       the empty one, which places them in the bottom level as they are, and the additions are
 *       written in the same batch that removes the marker.
 * </ol>
 *
 * <p>The staged files stay complete for as long as the marker exists, so a swap that was
 * interrupted anywhere is finished by running it again from the drop.
 */
final class RocksDBSegmentRewrite {
  private static final Logger LOG = LoggerFactory.getLogger(RocksDBSegmentRewrite.class);

  /** Marker keys are the prefix followed by the segment name, one per segment being swapped. */
  private static final byte[] MARKER_PREFIX = "rewrite:".getBytes(StandardCharsets.UTF_8);

  /**
   * Heap taken by the entries of one staged file. A batch is held in memory until its file is
   * written, so this bounds memory per writer, while still giving files large enough that the
   * ingestion of a few hundred of them is quick.
   */
  static final long FILE_BYTES = 64L << 20;

  /** What an entry holds on the heap beyond its key and value: a pair and two array headers. */
  private static final int ENTRY_OVERHEAD_BYTES = 64;

  /**
   * The one reader keeps about eight writers busy, more of them would only wait for it. Half the
   * processors leaves the others to the reader and to the database.
   */
  static final int WRITER_THREADS =
      Math.max(1, Math.min(Runtime.getRuntime().availableProcessors() / 2, 8));

  /** Where the staged files are linked for the ingestion, inside the staging directory. */
  private static final String LINKS = "ingest";

  private static final long WRITERS_STOP_SECONDS = 60;

  private static final long REPORT_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(30);

  private final RocksDBColumnarKeyValueStorage storage;
  private final SegmentIdentifier segment;
  private final Path directory;
  private final long fileBytes;
  private final int writerThreads;

  RocksDBSegmentRewrite(
      final RocksDBColumnarKeyValueStorage storage, final SegmentIdentifier segment) {
    this(storage, segment, FILE_BYTES, WRITER_THREADS);
  }

  RocksDBSegmentRewrite(
      final RocksDBColumnarKeyValueStorage storage,
      final SegmentIdentifier segment,
      final long fileBytes,
      final int writerThreads) {
    this.storage = storage;
    this.segment = segment;
    this.directory = stagingDirectory(storage.configuration.getDatabaseDir(), segment.getName());
    this.fileBytes = fileBytes;
    this.writerThreads = writerThreads;
  }

  /**
   * The staging directory sits beside the database, on the same file system, so the ingestion can
   * move the files in with a rename instead of copying them.
   */
  static Path stagingDirectory(final Path databaseDir, final String segmentName) {
    return stagingRoot(databaseDir).resolve(segmentName);
  }

  private static Path stagingRoot(final Path databaseDir) {
    return databaseDir.resolveSibling(databaseDir.getFileName() + "-rewrite");
  }

  /** What the marker of a swap holds. */
  record Pending(int files, List<Pair<byte[], byte[]>> additions) {}

  void run(
      final BiFunction<byte[], byte[], byte[]> transform,
      final List<Pair<byte[], byte[]>> additions) {
    if (Arrays.equals(segment.getId(), RocksDB.DEFAULT_COLUMN_FAMILY)) {
      throw new IllegalArgumentException(
          "The default segment holds the rewrite markers and cannot be rewritten");
    }
    final Optional<Pending> unfinished = pending();
    if (unfinished.isPresent()) {
      // staging again would transform a segment that is already dropped or already rewritten
      complete(unfinished.get());
      return;
    }
    writeFiles(transform);
    complete(markPending(additions));
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
          .complete(decodePending(marker.getValue()));
    }
    // files without a marker were staged by a rewrite that never reached its swap
    deleteLeftovers(stagingRoot(storage.configuration.getDatabaseDir()));
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
    return storage.getColumnHandlesBySegmentIdentifier().keySet().stream()
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
   * batches of about {@link #FILE_BYTES}, each written to its own file by a writer thread.
   * Consecutive batches of a key ordered read cover disjoint key ranges, which is all the ingestion
   * asks of the files.
   */
  void writeFiles(final BiFunction<byte[], byte[], byte[]> transform) {
    final Progress progress = new Progress(segment.getName());
    // the blocks are read once, so caching them would only push out blocks that are read again
    try (final FileWriters writers = new FileWriters(transform);
        final ReadOptions readOptions = new ReadOptions().setFillCache(false);
        final RocksIterator iterator =
            storage.getDB().newIterator(storage.safeColumnHandle(segment), readOptions)) {
      // files left by an earlier attempt would be ingested alongside the new ones
      deleteTree(directory);
      Files.createDirectories(directory);

      List<Pair<byte[], byte[]>> batch = new ArrayList<>();
      long batchBytes = 0;
      for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
        final byte[] key = iterator.key();
        final byte[] value = iterator.value();
        final int entryBytes = key.length + value.length;
        batch.add(Pair.of(key, value));
        batchBytes += entryBytes + ENTRY_OVERHEAD_BYTES;
        progress.advance(entryBytes);
        if (batchBytes >= fileBytes) {
          writers.submit(batch);
          batch = new ArrayList<>();
          batchBytes = 0;
        }
      }
      // an iterator that failed looks like one that reached the end
      iterator.status();
      if (!batch.isEmpty()) {
        writers.submit(batch);
      }
      writers.awaitAll();
      // the marker promises complete files, so their names have to survive a power failure too
      syncDirectory(directory);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new StorageException(
          "Interrupted while rewriting the " + segment.getName() + " segment");
    } catch (final ExecutionException e) {
      throw new StorageException(e.getCause());
    } catch (final RocksDBException | IOException e) {
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
    private final ExecutorService executor = Executors.newFixedThreadPool(writerThreads);
    private final Semaphore inFlight = new Semaphore(writerThreads + 2);
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
      // a writer that is still running uses the native options, freeing them under it would crash
      if (writersStopped()) {
        envOptions.close();
        sstOptions.close();
      } else {
        LOG.warn(
            "The writers of the {} segment rewrite did not stop, their options are not released",
            segment.getName());
      }
    }

    private boolean writersStopped() {
      try {
        return executor.awaitTermination(WRITERS_STOP_SECONDS, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
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
        // a rewrite that failed waits for its writers, which have no reason to finish their files
        if (Thread.currentThread().isInterrupted()) {
          throw new StorageException(
              "The rewrite of the " + segment.getName() + " segment was stopped");
        }
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
   * because the caller that knows them is gone when the swap is finished after a restart, and the
   * number of staged files because a swap that found fewer would silently lose their entries.
   */
  Pending markPending(final List<Pair<byte[], byte[]>> additions) {
    final RocksDB db = storage.getDB();
    try (final WriteOptions durable = new WriteOptions().setSync(true)) {
      final Pending pending = new Pending(stagedFiles().size(), additions);
      db.put(db.getDefaultColumnFamily(), durable, markerKey(), encodePending(pending));
      return pending;
    } catch (final RocksDBException | IOException e) {
      throw new StorageException(e);
    }
  }

  private Optional<Pending> pending() {
    final RocksDB db = storage.getDB();
    try {
      return Optional.ofNullable(db.get(db.getDefaultColumnFamily(), markerKey()))
          .map(RocksDBSegmentRewrite::decodePending);
    } catch (final RocksDBException e) {
      throw new StorageException(e);
    }
  }

  void complete(final Pending pending) {
    swap(pending.files());
    finish(pending.additions());
  }

  /**
   * Drops the column family and ingests the staged files into the empty one. The staged files are
   * left as they are, so this is safe to repeat however far an earlier attempt got: the column
   * family is dropped again, with whatever was ingested into it, and filled from the same files.
   */
  void swap(final int expectedFiles) {
    try {
      final List<Path> files = stagedFiles();
      if (files.size() != expectedFiles) {
        throw new StorageException(
            "The rewrite of the "
                + segment.getName()
                + " segment cannot be finished, "
                + directory
                + " holds "
                + files.size()
                + " of its "
                + expectedFiles
                + " staged files");
      }
      // dropping and recreating the column family is what lets the files land in the bottom
      // level: an ingestion into a column family with overlapping keys would have to go higher
      storage.clear(segment);
      if (files.isEmpty()) {
        return;
      }
      ingest(files);
      LOG.info("Ingested {} files into the {} segment", files.size(), segment.getName());
    } catch (final RocksDBException | IOException e) {
      throw new StorageException(e);
    }
  }

  /**
   * An ingestion that moves files in removes the ones it is given, so it is given links to the
   * staged files, which share their content without taking up space.
   */
  private void ingest(final List<Path> files) throws RocksDBException, IOException {
    final Path links = directory.resolve(LINKS);
    deleteTree(links);
    Files.createDirectories(links);
    List<Path> ingested = new ArrayList<>();
    boolean linked = true;
    try {
      for (final Path file : files) {
        ingested.add(Files.createLink(links.resolve(file.getFileName()), file));
      }
    } catch (final IOException | UnsupportedOperationException e) {
      // a file system without hard links has the staged files copied in, which keeps them too
      LOG.debug("Staged files cannot be linked, the ingestion copies them", e);
      ingested = files;
      linked = false;
    }
    try (final IngestExternalFileOptions options =
        new IngestExternalFileOptions().setMoveFiles(linked)) {
      storage
          .getDB()
          .ingestExternalFile(
              storage.safeColumnHandle(segment),
              ingested.stream().map(Path::toString).toList(),
              options);
    }
  }

  /**
   * Writes the additions and removes the marker in one atomic batch, so either the marker is still
   * there and the swap is run again, or both are done.
   */
  private void finish(final List<Pair<byte[], byte[]>> additions) {
    final RocksDB db = storage.getDB();
    final ColumnFamilyHandle handle = storage.safeColumnHandle(segment);
    try (final WriteBatch batch = new WriteBatch();
        final WriteOptions durable = new WriteOptions().setSync(true)) {
      for (final Pair<byte[], byte[]> addition : additions) {
        batch.put(handle, addition.getKey(), addition.getValue());
      }
      batch.delete(db.getDefaultColumnFamily(), markerKey());
      db.write(durable, batch);
    } catch (final RocksDBException e) {
      throw new StorageException(e);
    }
    deleteLeftovers(directory);
    try {
      Files.deleteIfExists(directory.getParent());
    } catch (final IOException ignored) {
      // another segment is still being rewritten
    }
  }

  /**
   * Removes staged files that no marker refers to. A failure is only logged: the rewrite is done,
   * and a caller told otherwise would run it a second time on the rewritten segment.
   */
  private static void deleteLeftovers(final Path directory) {
    try {
      deleteTree(directory);
    } catch (final IOException e) {
      LOG.warn("The staged files in {} could not be removed", directory, e);
    }
  }

  private List<Path> stagedFiles() throws IOException {
    if (!Files.isDirectory(directory)) {
      return List.of();
    }
    try (final Stream<Path> files = Files.list(directory)) {
      return files.filter(file -> file.getFileName().toString().endsWith(".sst")).sorted().toList();
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

  /**
   * The file count, then length prefixed key value pairs; the additions are few and small, so
   * nothing fancier.
   */
  private static byte[] encodePending(final Pending pending) {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (final DataOutputStream out = new DataOutputStream(bytes)) {
      out.writeInt(pending.files());
      out.writeInt(pending.additions().size());
      for (final Pair<byte[], byte[]> addition : pending.additions()) {
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

  private static Pending decodePending(final byte[] encoded) {
    final List<Pair<byte[], byte[]>> additions = new ArrayList<>();
    try (final DataInputStream in = new DataInputStream(new ByteArrayInputStream(encoded))) {
      final int files = in.readInt();
      final int count = in.readInt();
      for (int i = 0; i < count; i++) {
        final byte[] key = in.readNBytes(in.readInt());
        final byte[] value = in.readNBytes(in.readInt());
        additions.add(Pair.of(key, value));
      }
      return new Pending(files, additions);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void syncDirectory(final Path directory) {
    try (final FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
      channel.force(true);
    } catch (final IOException e) {
      // not every platform lets a directory be opened, and those keep its entries on their own
      LOG.debug("The directory {} could not be synced", directory, e);
    }
  }

  private static void deleteTree(final Path directory) throws IOException {
    if (!Files.isDirectory(directory)) {
      return;
    }
    try (final Stream<Path> files = Files.walk(directory)) {
      for (final Path file : files.sorted(Comparator.reverseOrder()).toList()) {
        Files.delete(file);
      }
    }
  }

  /** Reports on the entries read at a steady pace, but not at all when there are none. */
  private static final class Progress {
    /** Reading the clock for every entry would cost more than reading a small entry does. */
    private static final int ENTRIES_PER_CLOCK_READ = 1024;

    private final String segment;
    private final long start;
    private long lastReport;
    private long entries = 0;
    private long bytes = 0;

    Progress(final String segment) {
      this.segment = segment;
      this.start = System.nanoTime();
      this.lastReport = start;
    }

    void advance(final int entryBytes) {
      if (entries == 0) {
        LOG.info("Rewriting the {} segment", segment);
      }
      entries++;
      bytes += entryBytes;
      if (entries % ENTRIES_PER_CLOCK_READ != 0) {
        return;
      }
      final long now = System.nanoTime();
      if (now - lastReport >= REPORT_INTERVAL_NANOS) {
        lastReport = now;
        final long seconds = Math.max(1, TimeUnit.NANOSECONDS.toSeconds(now - start));
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
            TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start));
      }
    }
  }
}
