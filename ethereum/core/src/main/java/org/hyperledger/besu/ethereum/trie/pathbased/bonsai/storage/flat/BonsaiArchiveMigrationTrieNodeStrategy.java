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

import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.BonsaiCachedMerkleTrieLoader;

/**
 * Trie node strategy used by the Bonsai archive flat-DB migrator while it recomputes the historical
 * trie at each checkpoint.
 *
 * <p>Reads first check {@code TRIE_BRANCH_STORAGE_ARCHIVE} (via the parent's nearest-before lookup
 * for previously persisted checkpoint nodes) then fall back to the standard {@code
 * TRIE_BRANCH_STORAGE} for genesis/unchanged nodes that have not yet been written to the archive
 * CF. Writes go only to {@code TRIE_BRANCH_STORAGE_ARCHIVE}.
 */
public class BonsaiArchiveMigrationTrieNodeStrategy extends BonsaiArchiveTrieNodeStrategy {

  public BonsaiArchiveMigrationTrieNodeStrategy(
      final Long trieNodeCheckpointInterval, final BonsaiCachedMerkleTrieLoader trieLoader) {
    // Base strategy falls back to TRIE_BRANCH_STORAGE so genesis/unchanged nodes are accessible
    // before the first checkpoint persist writes them to the archive CF.
    super(trieNodeCheckpointInterval, trieLoader, new BonsaiTrieNodeStrategy());
  }
}
