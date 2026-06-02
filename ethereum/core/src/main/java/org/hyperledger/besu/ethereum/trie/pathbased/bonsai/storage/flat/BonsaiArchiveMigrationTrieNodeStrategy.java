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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_MIGRATION;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Trie node strategy used by the Bonsai archive flat-DB migrator while it recomputes the historical
 * trie at each checkpoint.
 *
 * <p>Reads come from the single-version, plain-key {@code TRIE_BRANCH_MIGRATION} column family via
 * bloom-accelerated point lookups, exactly like a normal Bonsai {@code TRIE_BRANCH_STORAGE} build.
 * This replaces the parent's I/O-bound reverse {@code seekForPrev} over the multi-version {@code
 * TRIE_BRANCH_STORAGE_ARCHIVE} CF for the migration compute path.
 *
 * <p>Writes are inherited from {@link BonsaiArchiveTrieNodeStrategy}: each new/changed node is
 * written both to the migration CF (plain, overwrite-in-place) via the {@link
 * BonsaiTrieNodeStrategy} base and to {@code TRIE_BRANCH_STORAGE_ARCHIVE} (suffixed by the
 * window-boundary block) as the durable proof output. The proof query path is unaffected: it
 * continues to read the archive CF through {@link BonsaiArchiveTrieNodeStrategy}.
 */
public class BonsaiArchiveMigrationTrieNodeStrategy extends BonsaiArchiveTrieNodeStrategy {

  // Fallback for nodes not yet in the migration CF (e.g. unchanged genesis nodes).
  // TRIE_BRANCH_STORAGE holds the genesis/live trie; TRIE_BRANCH_MIGRATION is only populated
  // as persist() writes checkpoints, so before the first checkpoint it is empty.
  private static final BonsaiTrieNodeStrategy STORAGE_FALLBACK = new BonsaiTrieNodeStrategy();

  public BonsaiArchiveMigrationTrieNodeStrategy(
      final Long trieNodeCheckpointInterval, final BonsaiCachedMerkleTrieLoader trieLoader) {
    super(
        trieNodeCheckpointInterval, trieLoader, new BonsaiTrieNodeStrategy(TRIE_BRANCH_MIGRATION));
  }

  @Override
  public Optional<Bytes> getFlatAccountTrieNode(
      final Bytes location, final Bytes32 nodeHash, final SegmentedKeyValueStorage storage) {
    // Migration CF first; fall back to TRIE_BRANCH_STORAGE for genesis/unchanged nodes.
    return baseStrategy
        .getFlatAccountTrieNode(location, nodeHash, storage)
        .or(() -> STORAGE_FALLBACK.getFlatAccountTrieNode(location, nodeHash, storage));
  }

  @Override
  public Optional<Bytes> getFlatStorageTrieNode(
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash,
      final SegmentedKeyValueStorage storage) {
    return baseStrategy
        .getFlatStorageTrieNode(accountHash, location, nodeHash, storage)
        .or(
            () ->
                STORAGE_FALLBACK.getFlatStorageTrieNode(accountHash, location, nodeHash, storage));
  }
}
