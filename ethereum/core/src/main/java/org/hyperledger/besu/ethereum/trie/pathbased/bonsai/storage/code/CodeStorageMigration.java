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
import java.util.Arrays;
import java.util.Optional;
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
   * The progress is committed with every batch, so an interrupted migration carries on where it
   * stopped instead of encoding entries twice.
   */
  public static void migrate(final SegmentedKeyValueStorage storage) {
    if (storage.get(CODE_STORAGE, FORMAT_KEY).isPresent()) {
      return;
    }
    final Optional<byte[]> lastMigrated = storage.get(CODE_STORAGE, MIGRATION_KEY);
    final long start = System.currentTimeMillis();
    long entries = 0;
    long batchBytes = 0;
    SegmentedKeyValueStorageTransaction transaction = storage.startTransaction();
    try (final Stream<Pair<byte[], byte[]>> stream =
        lastMigrated
            .map(key -> storage.streamFromKey(CODE_STORAGE, key))
            .orElseGet(() -> storage.stream(CODE_STORAGE))) {
      for (final var iterator = stream.iterator(); iterator.hasNext(); ) {
        final Pair<byte[], byte[]> entry = iterator.next();
        final byte[] key = entry.getKey();
        if (isReservedKey(key) || lastMigrated.map(k -> Arrays.equals(k, key)).orElse(false)) {
          continue;
        }
        if (entries == 0) {
          // In-memory and fresh databases have nothing to migrate and stay quiet
          LOG.info(
              "Migrating the code storage format, {}",
              lastMigrated.isPresent() ? "resuming where it stopped" : "starting");
        }
        final byte[] value = CodeStorageFormat.CURRENT.encode(Bytes.wrap(entry.getValue()));
        transaction.put(CODE_STORAGE, key, value);
        transaction.put(CODE_STORAGE, MIGRATION_KEY, key);
        entries++;
        batchBytes += value.length;
        if (batchBytes >= BATCH_BYTES) {
          transaction.commit();
          transaction = storage.startTransaction();
          batchBytes = 0;
        }
      }
    }
    markCurrent(transaction);
    transaction.commit();
    if (entries > 0) {
      LOG.info(
          "Migrated {} code entries in {} s", entries, (System.currentTimeMillis() - start) / 1000);
    }
  }
}
