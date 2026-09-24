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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.code;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;

import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Brings a code column family written before the format byte existed into the {@link
 * CodeStorageFormat}.
 *
 * <p>The column family records the format all its entries have reached under a reserved key, so
 * that the migration runs exactly once, whatever else has been cleared around it. A legacy value is
 * the bare code and cannot be told apart from a new one by inspection.
 */
public final class CodeStorageMigration {
  private static final Logger LOG = LoggerFactory.getLogger(CodeStorageMigration.class);

  /** Reserved key holding the format every other entry in the column family has reached. */
  public static final byte[] FORMAT_KEY = "codeStorageFormat".getBytes(StandardCharsets.UTF_8);

  /** Reserved key holding the last key migrated, only present while a migration is under way. */
  public static final byte[] MIGRATION_KEY =
      "codeStorageMigration".getBytes(StandardCharsets.UTF_8);

  private static final long BATCH_BYTES = 64L << 20;

  private static final long REPORT_INTERVAL_MILLIS = 30_000;

  private CodeStorageMigration() {}

  public static boolean isReservedKey(final byte[] key) {
    return Arrays.equals(key, FORMAT_KEY) || Arrays.equals(key, MIGRATION_KEY);
  }

  /** Marks an empty or freshly cleared column family as being in the current format. */
  public static void markCurrent(final SegmentedKeyValueStorage storage) {
    final SegmentedKeyValueStorageTransaction transaction = storage.startTransaction();
    markCurrent(transaction);
    transaction.commit();
  }

  private static void markCurrent(final SegmentedKeyValueStorageTransaction transaction) {
    transaction.put(CODE_STORAGE, FORMAT_KEY, new byte[] {CodeStorageFormat.CURRENT.version});
    transaction.remove(CODE_STORAGE, MIGRATION_KEY);
  }

  /**
   * Rewrites every entry of a column family written before the format byte existed, unless that has
   * been done already. Runs before the storage is handed out, so nothing writes code concurrently.
   * The entries are read in key order, encoded in parallel and committed in batches, with the last
   * key of every batch recorded next to it, so an interrupted migration carries on where it stopped
   * instead of encoding entries twice. The commit of one batch overlaps with the read and encoding
   * of the next.
   */
  public static void migrate(final SegmentedKeyValueStorage storage) {
    migrate(storage, BATCH_BYTES);
  }

  static void migrate(final SegmentedKeyValueStorage storage, final long batchBytes) {
    if (storage.get(CODE_STORAGE, FORMAT_KEY).isPresent()) {
      return;
    }
    final Optional<byte[]> lastMigrated = storage.get(CODE_STORAGE, MIGRATION_KEY);
    final Progress progress = new Progress(lastMigrated.isPresent());
    final ExecutorService committer = Executors.newSingleThreadExecutor();
    Future<?> lastCommit = CompletableFuture.completedFuture(null);
    try (final Stream<Pair<byte[], byte[]>> stream =
        lastMigrated
            .map(key -> storage.streamFromKey(CODE_STORAGE, key))
            .orElseGet(() -> storage.stream(CODE_STORAGE))) {
      final Iterator<Pair<byte[], byte[]>> entries =
          stream
              .filter(
                  entry ->
                      !isReservedKey(entry.getKey())
                          && lastMigrated.map(k -> !Arrays.equals(k, entry.getKey())).orElse(true))
              .iterator();
      while (entries.hasNext()) {
        final List<Pair<byte[], byte[]>> batch = nextBatch(entries, batchBytes);
        final List<byte[]> encoded =
            batch.parallelStream()
                .map(entry -> CodeStorageFormat.CURRENT.encode(Bytes.wrap(entry.getValue())))
                .toList();
        // the previous commit ran while this batch was read and encoded; a failure stops here
        lastCommit.get();
        lastCommit = committer.submit(() -> commit(storage, batch, encoded));
        progress.advance(batch);
      }
      lastCommit.get();
      final SegmentedKeyValueStorageTransaction transaction = storage.startTransaction();
      markCurrent(transaction);
      transaction.commit();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while migrating the code storage", e);
    } catch (final ExecutionException e) {
      throw new IllegalStateException("Failed to migrate the code storage", e.getCause());
    } finally {
      committer.shutdownNow();
    }
    progress.finish();
  }

  private static List<Pair<byte[], byte[]>> nextBatch(
      final Iterator<Pair<byte[], byte[]>> entries, final long batchBytes) {
    final List<Pair<byte[], byte[]>> batch = new ArrayList<>();
    long bytes = 0;
    while (entries.hasNext() && bytes < batchBytes) {
      final Pair<byte[], byte[]> entry = entries.next();
      batch.add(entry);
      bytes += entry.getValue().length;
    }
    return batch;
  }

  private static void commit(
      final SegmentedKeyValueStorage storage,
      final List<Pair<byte[], byte[]>> batch,
      final List<byte[]> encoded) {
    final SegmentedKeyValueStorageTransaction transaction = storage.startTransaction();
    for (int i = 0; i < batch.size(); i++) {
      transaction.put(CODE_STORAGE, batch.get(i).getKey(), encoded.get(i));
    }
    transaction.put(CODE_STORAGE, MIGRATION_KEY, batch.getLast().getKey());
    transaction.commit();
  }

  /** Reports on the migration at a steady pace, but not at all when there is nothing to do. */
  private static final class Progress {
    private final boolean resumed;
    private final long start = System.currentTimeMillis();
    private long lastReport = start;
    private long entries = 0;
    private long codeBytes = 0;

    Progress(final boolean resumed) {
      this.resumed = resumed;
    }

    void advance(final List<Pair<byte[], byte[]>> batch) {
      if (entries == 0) {
        LOG.info(
            "Migrating the code storage format, {}",
            resumed ? "resuming where it stopped" : "starting");
      }
      entries += batch.size();
      for (final Pair<byte[], byte[]> entry : batch) {
        codeBytes += entry.getValue().length;
      }
      final long now = System.currentTimeMillis();
      if (now - lastReport >= REPORT_INTERVAL_MILLIS) {
        lastReport = now;
        LOG.info(
            "Code storage migration: {} entries, {} MB of code in {} s ({} MB/s)",
            entries,
            codeBytes >> 20,
            (now - start) / 1000,
            String.format("%.1f", codeBytes / 1048576.0 / Math.max(1, (now - start) / 1000)));
      }
    }

    void finish() {
      if (entries > 0) {
        LOG.info(
            "Migrated {} code entries, {} MB of code, in {} s",
            entries,
            codeBytes >> 20,
            (System.currentTimeMillis() - start) / 1000);
      }
    }
  }
}
