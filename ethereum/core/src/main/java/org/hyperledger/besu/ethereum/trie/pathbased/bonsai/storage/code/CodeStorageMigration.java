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

import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Moves a code column family keyed by code hash between the bare code {@link
 * CodeHashCodeStorageStrategy} writes and the analysed code {@link JumpDestCodeStorageStrategy}
 * writes, in either direction, by rewriting it as a whole. The marker of the target strategy is
 * written in the same step as the rewritten entries, so that the migration runs exactly once and an
 * interrupted one starts over from the untouched entries.
 */
public final class CodeStorageMigration {
  private static final Logger LOG = LoggerFactory.getLogger(CodeStorageMigration.class);

  private CodeStorageMigration() {}

  /**
   * Rewrites every bare entry of the column family into analysed code, unless that has been done
   * already. An empty column family is only marked. Runs before the storage is handed out, so
   * nothing writes code concurrently.
   *
   * @param storage the storage holding the code column family, keyed by code hash
   */
  public static void migrate(final SegmentedKeyValueStorage storage) {
    if (JumpDestCodeStorageStrategy.isMarked(storage)) {
      return;
    }
    if (isEmpty(storage)) {
      new JumpDestCodeStorageStrategy().markEmpty(storage);
      return;
    }
    LOG.info("Migrating the code storage to code with its jump destination analysis");
    storage.rewrite(
        CODE_STORAGE,
        (key, value) ->
            JumpDestCodeStorageStrategy.isMarkerKey(key)
                ? null
                : JumpDestCodeStorageStrategy.encode(Bytes.wrap(value)),
        List.of(
            Pair.of(JumpDestCodeStorageStrategy.MARKER_KEY, JumpDestCodeStorageStrategy.MARKER)));
    LOG.info("Migrated the code storage to code with its jump destination analysis");
  }

  /**
   * Rewrites every entry of the column family back into the bare code older Besu versions read,
   * unless it holds bare code already.
   *
   * @param storage the storage holding the code column family
   */
  public static void revert(final SegmentedKeyValueStorage storage) {
    if (!JumpDestCodeStorageStrategy.isMarked(storage)) {
      return;
    }
    LOG.info("Reverting the code storage to bare code");
    storage.rewrite(
        CODE_STORAGE,
        (key, value) ->
            JumpDestCodeStorageStrategy.isMarkerKey(key)
                ? null
                : JumpDestCodeStorageStrategy.decodeCode(value).toArrayUnsafe(),
        List.of());
    LOG.info("Reverted the code storage to bare code");
  }

  private static boolean isEmpty(final SegmentedKeyValueStorage storage) {
    try (Stream<byte[]> keys = storage.streamKeys(CODE_STORAGE)) {
      return keys.findAny().isEmpty();
    }
  }
}
