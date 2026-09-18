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
 * The value layout of the code column family: a format byte followed by the code. The byte lets
 * later formats add data next to the code, entry by entry, without another pass over the column
 * family.
 *
 * <p>The column family records its own format under a reserved key, so that code written before the
 * format byte existed is migrated exactly once, whatever else has been cleared around it. A legacy
 * value is the bare code and cannot be told apart from a new one by inspection.
 */
public final class CodeStorageFormat {
  private static final Logger LOG = LoggerFactory.getLogger(CodeStorageFormat.class);

  /** Reserved key holding the format of every other entry in the column family. */
  public static final byte[] FORMAT_KEY = "codeStorageFormat".getBytes(StandardCharsets.UTF_8);

  /** Reserved key holding the last key migrated, only present while a migration is under way. */
  public static final byte[] MIGRATION_KEY =
      "codeStorageMigration".getBytes(StandardCharsets.UTF_8);

  static final byte CODE = 1;

  private static final long MIGRATION_BATCH_BYTES = 64L << 20;

  private CodeStorageFormat() {}

  public static boolean isReservedKey(final byte[] key) {
    return Arrays.equals(key, FORMAT_KEY) || Arrays.equals(key, MIGRATION_KEY);
  }

  public static byte[] encode(final Bytes code) {
    final byte[] value = new byte[1 + code.size()];
    value[0] = CODE;
    System.arraycopy(code.toArrayUnsafe(), 0, value, 1, code.size());
    return value;
  }

  public static Bytes decode(final byte[] value) {
    if (value.length == 0 || value[0] != CODE) {
      throw new IllegalStateException(
          "Unknown code storage format " + (value.length == 0 ? "<empty>" : value[0]));
    }
    return Bytes.wrap(value, 1, value.length - 1);
  }

  /** Marks an empty or freshly cleared column family as being in the current format. */
  public static void markCurrent(final SegmentedKeyValueStorageTransaction transaction) {
    transaction.put(CODE_STORAGE, FORMAT_KEY, new byte[] {CODE});
    transaction.remove(CODE_STORAGE, MIGRATION_KEY);
  }

  /**
   * Rewrites every legacy entry of the column family in the current format, unless that has been
   * done already. Runs before the storage is handed out, so nothing writes code concurrently. The
   * progress is committed with every batch, so an interrupted migration carries on where it stopped
   * instead of encoding entries twice.
   */
  public static void migrate(final SegmentedKeyValueStorage storage) {
    if (storage.get(CODE_STORAGE, FORMAT_KEY).isPresent()) {
      return;
    }
    final Optional<byte[]> lastMigrated = storage.get(CODE_STORAGE, MIGRATION_KEY);
    LOG.info(
        "Migrating the code storage format, {}",
        lastMigrated.isPresent() ? "resuming where it stopped" : "starting");
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
        final byte[] value = encode(Bytes.wrap(entry.getValue()));
        transaction.put(CODE_STORAGE, key, value);
        transaction.put(CODE_STORAGE, MIGRATION_KEY, key);
        entries++;
        batchBytes += value.length;
        if (batchBytes >= MIGRATION_BATCH_BYTES) {
          transaction.commit();
          transaction = storage.startTransaction();
          batchBytes = 0;
        }
      }
    }
    markCurrent(transaction);
    transaction.commit();
    LOG.info(
        "Migrated {} code entries in {} s", entries, (System.currentTimeMillis() - start) / 1000);
  }
}
