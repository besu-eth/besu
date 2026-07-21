/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.STORAGE_TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiTrieStorageKeys.STORAGE_TRIE_CF_MIGRATED_KEY;

import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Moves legacy storage trie nodes from {@link
 * org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier#TRIE_BRANCH_STORAGE} into
 * {@link
 * org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier#STORAGE_TRIE_BRANCH_STORAGE}.
 */
public final class BonsaiStorageTrieSegmentMigrator {

  private static final Logger LOG = LoggerFactory.getLogger(BonsaiStorageTrieSegmentMigrator.class);
  private static final byte[] MIGRATION_COMPLETE = new byte[] {1};

  private BonsaiStorageTrieSegmentMigrator() {}

  public static void migrateIfNeeded(final SegmentedKeyValueStorage composedWorldStateStorage) {
    if (composedWorldStateStorage.get(TRIE_BRANCH_STORAGE, STORAGE_TRIE_CF_MIGRATED_KEY).isPresent()) {
      return;
    }

    final AtomicLong migratedEntries = new AtomicLong();
    final SegmentedKeyValueStorageTransaction transaction =
        composedWorldStateStorage.startTransaction();
    try {
      composedWorldStateStorage
          .stream(TRIE_BRANCH_STORAGE)
          .forEach(
              entry -> {
                final byte[] key = entry.getKey();
                if (!BonsaiTrieStorageKeys.isStorageTrieKey(key)) {
                  return;
                }
                transaction.put(STORAGE_TRIE_BRANCH_STORAGE, key, entry.getValue());
                transaction.remove(TRIE_BRANCH_STORAGE, key);
                migratedEntries.incrementAndGet();
              });
      transaction.put(TRIE_BRANCH_STORAGE, STORAGE_TRIE_CF_MIGRATED_KEY, MIGRATION_COMPLETE);
      transaction.commit();
      if (migratedEntries.get() > 0) {
        LOG.info(
            "Migrated {} storage trie nodes from TRIE_BRANCH_STORAGE to STORAGE_TRIE_BRANCH_STORAGE",
            migratedEntries.get());
      } else {
        LOG.debug("Marked storage trie column family migration as complete");
      }
    } catch (final RuntimeException e) {
      transaction.rollback();
      throw e;
    }
  }
}
