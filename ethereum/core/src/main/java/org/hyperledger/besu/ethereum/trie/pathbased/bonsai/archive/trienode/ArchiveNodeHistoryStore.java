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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;

import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * Stores historical trie node values in the archive store, keyed by a combination of the natural
 * key and the block number at which the value was valid. For a given natural key and target block,
 * returns the latest value at or before that block.
 */
public final class ArchiveNodeHistoryStore {

  private final SegmentedKeyValueStorage storage;

  public ArchiveNodeHistoryStore(final SegmentedKeyValueStorage storage) {
    this.storage = Objects.requireNonNull(storage, "storage must not be null");
  }

  public void put(
      final SegmentedKeyValueStorageTransaction tx,
      final Bytes naturalKey,
      final long block,
      final Bytes nodeRlp) {
    final Bytes key = ArchiveNodeKey.historyKey(naturalKey, block);
    tx.put(TRIE_BRANCH_STORAGE_ARCHIVE, key.toArrayUnsafe(), nodeRlp.toArrayUnsafe());
  }

  public Optional<Bytes> getLatestBefore(final Bytes naturalKey, final long block) {
    final Bytes seekKey = ArchiveNodeKey.historyKey(naturalKey, block);
    return storage
        .getNearestBefore(TRIE_BRANCH_STORAGE_ARCHIVE, seekKey)
        .filter(nearest -> naturalKeyMatches(naturalKey, nearest.key()))
        .flatMap(nearest -> nearest.value().map(Bytes::wrap));
  }

  /**
   * Returns true if the foundKey is a history key for the same natural key as the given naturalKey.
   * This is used to filter out history keys that are for different natural keys when searching for
   * the latest value before a given block.
   */
  private static boolean naturalKeyMatches(final Bytes naturalKey, final Bytes foundKey) {
    return foundKey.size() >= naturalKey.size() + 8
        && ArchiveNodeKey.naturalKeyFromHistoryKey(foundKey).equals(naturalKey);
  }
}
