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

import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The value layout of the code column family: a format byte, then the code and whatever the format
 * stores next to it. Since the byte, entries of older formats can sit next to current ones and are
 * read as they are; a migration only brings them up to date once, so that no code is ever loaded
 * without the data its format carries.
 *
 * <p>The column family records the format all its entries have reached under a reserved key, so
 * that code written before the format byte existed is migrated exactly once, whatever else has been
 * cleared around it. Such a legacy value is the bare code and cannot be told apart from a new one
 * by inspection.
 */
public final class CodeStorageFormat {
  private static final Logger LOG = LoggerFactory.getLogger(CodeStorageFormat.class);

  /** Reserved key holding the format every other entry in the column family has reached. */
  public static final byte[] FORMAT_KEY = "codeStorageFormat".getBytes(StandardCharsets.UTF_8);

  /** Reserved key holding the last key migrated, only present while a migration is under way. */
  public static final byte[] MIGRATION_KEY =
      "codeStorageMigration".getBytes(StandardCharsets.UTF_8);

  /** The code alone. */
  static final byte CODE = 1;

  /**
   * The code length, the code and its jump destination bitmask, so that a load never has to walk
   * the code.
   */
  static final byte CODE_WITH_JUMPDEST_ANALYSIS = 2;

  static final byte CURRENT = CODE_WITH_JUMPDEST_ANALYSIS;

  private static final int ANALYSIS_HEADER_SIZE = Byte.BYTES + Integer.BYTES;

  private static final long MIGRATION_BATCH_BYTES = 64L << 20;

  private CodeStorageFormat() {}

  public static boolean isReservedKey(final byte[] key) {
    return Arrays.equals(key, FORMAT_KEY) || Arrays.equals(key, MIGRATION_KEY);
  }

  public static byte[] encode(final Bytes code) {
    final long[] jumpDestBitMask = Code.jumpDestBitMaskOf(code);
    final byte[] value =
        new byte[ANALYSIS_HEADER_SIZE + code.size() + jumpDestBitMask.length * Long.BYTES];
    final ByteBuffer buffer = ByteBuffer.wrap(value);
    buffer.put(CODE_WITH_JUMPDEST_ANALYSIS).putInt(code.size()).put(code.toArrayUnsafe());
    buffer.asLongBuffer().put(jumpDestBitMask);
    return value;
  }

  public static StoredCode decode(final byte[] value) {
    if (value.length == 0) {
      throw new IllegalStateException("Empty code storage value");
    }
    return switch (value[0]) {
      case CODE -> StoredCode.withoutAnalysis(Bytes.wrap(value, 1, value.length - 1));
      case CODE_WITH_JUMPDEST_ANALYSIS -> decodeWithAnalysis(value);
      default -> throw new IllegalStateException("Unknown code storage format " + value[0]);
    };
  }

  private static StoredCode decodeWithAnalysis(final byte[] value) {
    final ByteBuffer buffer = ByteBuffer.wrap(value, 1, value.length - 1);
    final int codeSize = buffer.getInt();
    final long[] jumpDestBitMask = new long[(codeSize >> 6) + 1];
    if (value.length != ANALYSIS_HEADER_SIZE + codeSize + jumpDestBitMask.length * Long.BYTES) {
      throw new IllegalStateException(
          "Stored code of " + codeSize + " bytes has a value of " + value.length + " bytes");
    }
    buffer.position(ANALYSIS_HEADER_SIZE + codeSize);
    buffer.asLongBuffer().get(jumpDestBitMask);
    return new StoredCode(Bytes.wrap(value, ANALYSIS_HEADER_SIZE, codeSize), jumpDestBitMask);
  }

  /** Marks an empty or freshly cleared column family as being in the current format. */
  public static void markCurrent(final SegmentedKeyValueStorageTransaction transaction) {
    transaction.put(CODE_STORAGE, FORMAT_KEY, new byte[] {CURRENT});
    transaction.remove(CODE_STORAGE, MIGRATION_KEY);
  }

  /**
   * Rewrites every entry of the column family that is behind the current format, unless that has
   * been done already. Runs before the storage is handed out, so nothing writes code concurrently.
   * The progress is committed with every batch, so an interrupted migration carries on where it
   * stopped instead of encoding entries twice.
   */
  public static void migrate(final SegmentedKeyValueStorage storage) {
    final Optional<byte[]> reached = storage.get(CODE_STORAGE, FORMAT_KEY);
    if (reached.isPresent() && reached.get()[0] == CURRENT) {
      return;
    }
    // Without a format byte to go by, every entry is bare code
    final boolean legacy = reached.isEmpty();
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
        final byte[] stored = entry.getValue();
        if (isReservedKey(key)
            || lastMigrated.map(k -> Arrays.equals(k, key)).orElse(false)
            || (!legacy && stored[0] == CURRENT)) {
          continue;
        }
        if (entries == 0) {
          // In-memory and fresh databases have nothing to migrate and stay quiet
          LOG.info(
              "Migrating the code storage format, {}",
              lastMigrated.isPresent() ? "resuming where it stopped" : "starting");
        }
        final Bytes code = legacy ? Bytes.wrap(stored) : decode(stored).code();
        final byte[] value = encode(code);
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
    if (entries > 0) {
      LOG.info(
          "Migrated {} code entries in {} s", entries, (System.currentTimeMillis() - start) / 1000);
    }
  }
}
