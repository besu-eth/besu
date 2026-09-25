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
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Moves the code column family between the bare code older Besu versions wrote and the {@link
 * CodeStorageFormat}, in either direction, by rewriting it as a whole. The column family records
 * the format all its entries have reached under a reserved key, written in the same step as the
 * rewritten entries, so that the migration runs exactly once and an interrupted one starts over
 * from the untouched entries.
 */
public final class CodeStorageMigration {
  private static final Logger LOG = LoggerFactory.getLogger(CodeStorageMigration.class);

  /** Reserved key holding the format every other entry in the column family has reached. */
  public static final byte[] FORMAT_KEY = "codeStorageFormat".getBytes(StandardCharsets.UTF_8);

  private static final List<Pair<byte[], byte[]>> CURRENT_FORMAT =
      List.of(Pair.of(FORMAT_KEY, new byte[] {CodeStorageFormat.CURRENT.version}));

  private CodeStorageMigration() {}

  public static boolean isReservedKey(final byte[] key) {
    return Arrays.equals(key, FORMAT_KEY);
  }

  /** Marks an empty or freshly cleared column family as being in the current format. */
  public static void markCurrent(final SegmentedKeyValueStorage storage) {
    final SegmentedKeyValueStorageTransaction transaction = storage.startTransaction();
    transaction.put(CODE_STORAGE, FORMAT_KEY, new byte[] {CodeStorageFormat.CURRENT.version});
    transaction.commit();
  }

  /**
   * Rewrites every bare entry of the column family into the current format, unless that has been
   * done already. Runs before the storage is handed out, so nothing writes code concurrently.
   *
   * @param storage the storage holding the code column family
   */
  public static void migrate(final SegmentedKeyValueStorage storage) {
    if (storage.get(CODE_STORAGE, FORMAT_KEY).isPresent()) {
      return;
    }
    LOG.info("Migrating the code storage to format {}", CodeStorageFormat.CURRENT);
    storage.rewrite(
        CODE_STORAGE,
        (key, value) ->
            isReservedKey(key) ? null : CodeStorageFormat.CURRENT.encode(Bytes.wrap(value)),
        CURRENT_FORMAT);
    LOG.info("Migrated the code storage to format {}", CodeStorageFormat.CURRENT);
  }

  /**
   * Rewrites every entry of the column family back into the bare code older Besu versions read,
   * unless it holds bare code already.
   *
   * @param storage the storage holding the code column family
   */
  public static void revert(final SegmentedKeyValueStorage storage) {
    if (storage.get(CODE_STORAGE, FORMAT_KEY).isEmpty()) {
      return;
    }
    LOG.info("Reverting the code storage to bare code");
    storage.rewrite(
        CODE_STORAGE,
        (key, value) ->
            isReservedKey(key)
                ? null
                : CodeStorageFormat.of(value).decode(value, null).getBytes().toArrayUnsafe(),
        List.of());
    LOG.info("Reverted the code storage to bare code");
  }
}
