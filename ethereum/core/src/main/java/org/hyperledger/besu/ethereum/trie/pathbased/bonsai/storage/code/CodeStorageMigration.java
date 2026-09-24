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
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Moves a code column family between the bare code older Besu versions wrote and the {@link
 * CodeStorageFormat}, in either direction.
 *
 * <p>The column family records the format all its entries have reached under a reserved key, so
 * that the migration runs exactly once, whatever else has been cleared around it. A bare value
 * cannot be told apart from an encoded one by inspection, so a rewrite under way records the last
 * key it committed under a second reserved key and carries on from there when it is interrupted.
 */
public final class CodeStorageMigration {
  private static final Logger LOG = LoggerFactory.getLogger(CodeStorageMigration.class);

  /** Reserved key holding the format every other entry in the column family has reached. */
  public static final byte[] FORMAT_KEY = "codeStorageFormat".getBytes(StandardCharsets.UTF_8);

  /** Reserved key holding the last key migrated, only present while a migration is under way. */
  public static final byte[] MIGRATION_KEY =
      "codeStorageMigration".getBytes(StandardCharsets.UTF_8);

  /** Reserved key holding the last key reverted, only present while a revert is under way. */
  public static final byte[] REVERT_KEY = "codeStorageRevert".getBytes(StandardCharsets.UTF_8);

  private static final long BATCH_BYTES = 64L << 20;

  private static final long REPORT_INTERVAL_MILLIS = 30_000;

  private CodeStorageMigration() {}

  public static boolean isReservedKey(final byte[] key) {
    return Arrays.equals(key, FORMAT_KEY)
        || Arrays.equals(key, MIGRATION_KEY)
        || Arrays.equals(key, REVERT_KEY);
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
    transaction.remove(CODE_STORAGE, REVERT_KEY);
  }

  /**
   * Rewrites every bare entry of a column family into the current format, unless that has been done
   * already. Runs before the storage is handed out, so nothing writes code concurrently. A revert
   * that was interrupted is finished first, so that every entry is bare when the migration starts.
   */
  public static void migrate(final SegmentedKeyValueStorage storage) {
    migrate(storage, BATCH_BYTES);
  }

  static void migrate(final SegmentedKeyValueStorage storage, final long batchBytes) {
    if (storage.get(CODE_STORAGE, REVERT_KEY).isPresent()) {
      LOG.info("Finishing an interrupted revert of the code storage format before migrating it");
      revert(storage, batchBytes);
    }
    if (storage.get(CODE_STORAGE, FORMAT_KEY).isPresent()) {
      return;
    }
    rewrite(
        storage,
        "Migrating",
        MIGRATION_KEY,
        value -> CodeStorageFormat.CURRENT.encode(Bytes.wrap(value)),
        batchBytes);
    markCurrent(storage);
  }

  /**
   * Rewrites every entry of a column family back into the bare code older Besu versions read,
   * unless it holds bare code already. A migration that was interrupted leaves only the entries up
   * to its last key encoded, and only those are reverted.
   *
   * @param storage the storage holding the code column family
   */
  public static void revert(final SegmentedKeyValueStorage storage) {
    revert(storage, BATCH_BYTES);
  }

  static void revert(final SegmentedKeyValueStorage storage, final long batchBytes) {
    final Optional<byte[]> lastMigrated = storage.get(CODE_STORAGE, MIGRATION_KEY);
    if (storage.get(CODE_STORAGE, FORMAT_KEY).isEmpty() && lastMigrated.isEmpty()) {
      return;
    }
    rewrite(
        storage,
        "Reverting",
        REVERT_KEY,
        value -> CodeStorageFormat.of(value).decode(value, null).getBytes().toArrayUnsafe(),
        batchBytes,
        lastMigrated);
    final SegmentedKeyValueStorageTransaction transaction = storage.startTransaction();
    transaction.remove(CODE_STORAGE, FORMAT_KEY);
    transaction.remove(CODE_STORAGE, MIGRATION_KEY);
    transaction.remove(CODE_STORAGE, REVERT_KEY);
    transaction.commit();
  }

  private static void rewrite(
      final SegmentedKeyValueStorage storage,
      final String action,
      final byte[] progressKey,
      final Function<byte[], byte[]> transform,
      final long batchBytes) {
    rewrite(storage, action, progressKey, transform, batchBytes, Optional.empty());
  }

  /**
   * Rewrites the entries in key order, from the one after the recorded progress up to the last key
   * (inclusive) when there is one. The entries are transformed in parallel and committed in
   * batches, with the last key of every batch recorded under the progress key, so an interrupted
   * rewrite carries on where it stopped instead of transforming entries twice. The commit of one
   * batch overlaps with the read and transformation of the next.
   */
  private static void rewrite(
      final SegmentedKeyValueStorage storage,
      final String action,
      final byte[] progressKey,
      final Function<byte[], byte[]> transform,
      final long batchBytes,
      final Optional<byte[]> lastKey) {
    final Optional<byte[]> resumeAfter = storage.get(CODE_STORAGE, progressKey);
    final Progress progress = new Progress(action, resumeAfter.isPresent());
    final ExecutorService committer = Executors.newSingleThreadExecutor();
    Future<?> lastCommit = CompletableFuture.completedFuture(null);
    try (final Stream<Pair<byte[], byte[]>> stream =
        resumeAfter
            .map(key -> storage.streamFromKey(CODE_STORAGE, key))
            .orElseGet(() -> storage.stream(CODE_STORAGE))) {
      final Iterator<Pair<byte[], byte[]>> entries =
          stream
              .filter(
                  entry ->
                      !isReservedKey(entry.getKey())
                          && resumeAfter.map(k -> !Arrays.equals(k, entry.getKey())).orElse(true))
              .takeWhile(
                  entry ->
                      lastKey.map(k -> Arrays.compareUnsigned(entry.getKey(), k) <= 0).orElse(true))
              .iterator();
      while (entries.hasNext()) {
        final List<Pair<byte[], byte[]>> batch = nextBatch(entries, batchBytes);
        final List<byte[]> rewritten =
            batch.parallelStream().map(entry -> transform.apply(entry.getValue())).toList();
        // the previous commit ran while this batch was read and rewritten; a failure stops here
        lastCommit.get();
        lastCommit = committer.submit(() -> commit(storage, progressKey, batch, rewritten));
        progress.advance(batch);
      }
      lastCommit.get();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while rewriting the code storage", e);
    } catch (final ExecutionException e) {
      throw new IllegalStateException("Failed to rewrite the code storage", e.getCause());
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
      final byte[] progressKey,
      final List<Pair<byte[], byte[]>> batch,
      final List<byte[]> rewritten) {
    final SegmentedKeyValueStorageTransaction transaction = storage.startTransaction();
    for (int i = 0; i < batch.size(); i++) {
      transaction.put(CODE_STORAGE, batch.get(i).getKey(), rewritten.get(i));
    }
    transaction.put(CODE_STORAGE, progressKey, batch.getLast().getKey());
    transaction.commit();
  }

  /** Reports on a rewrite at a steady pace, but not at all when there is nothing to do. */
  private static final class Progress {
    private final String action;
    private final boolean resumed;
    private final long start = System.currentTimeMillis();
    private long lastReport = start;
    private long entries = 0;
    private long valueBytes = 0;

    Progress(final String action, final boolean resumed) {
      this.action = action;
      this.resumed = resumed;
    }

    void advance(final List<Pair<byte[], byte[]>> batch) {
      if (entries == 0) {
        LOG.info(
            "{} the code storage format, {}",
            action,
            resumed ? "resuming where it stopped" : "starting");
      }
      entries += batch.size();
      for (final Pair<byte[], byte[]> entry : batch) {
        valueBytes += entry.getValue().length;
      }
      final long now = System.currentTimeMillis();
      if (now - lastReport >= REPORT_INTERVAL_MILLIS) {
        lastReport = now;
        LOG.info(
            "{} the code storage format: {} entries, {} MB in {} s ({} MB/s)",
            action,
            entries,
            valueBytes >> 20,
            (now - start) / 1000,
            String.format("%.1f", valueBytes / 1048576.0 / Math.max(1, (now - start) / 1000)));
      }
    }

    void finish() {
      if (entries > 0) {
        LOG.info(
            "{} the code storage format done: {} entries, {} MB in {} s",
            action,
            entries,
            valueBytes >> 20,
            (System.currentTimeMillis() - start) / 1000);
      }
    }
  }
}
