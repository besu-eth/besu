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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.request.v2;

import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;

import java.util.NavigableMap;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Integrates downloaded snap/2 account ranges into the Forest world-state account trie.
 *
 * <p>Forest has no flat database: accounts are only reachable via the MPT, so the trie and its
 * account trie root must be kept consistent after every downloaded batch. Each call opens the trie
 * at the current tracked root, inserts the new accounts, commits only the dirty nodes, and returns
 * the new root for the caller to persist atomically. Successive calls chain so that the stored trie
 * always reflects exactly the accounts downloaded so far; the account trie root matches the pivot
 * block's state root only once all ranges are complete.
 */
final class ForestTrieStitcher {

  private final WorldStateStorageCoordinator coordinator;

  ForestTrieStitcher(final WorldStateStorageCoordinator coordinator) {
    this.coordinator = coordinator;
  }

  /**
   * Inserts {@code downloadedAccounts} into the trie at {@code currentTrackedRoot}, commits dirty
   * nodes via {@code updater}, and returns the new root. The caller must persist the returned root
   * via {@code putWorldStateRoot} in the same batch.
   */
  Bytes32 stitchAccounts(
      final Bytes32 currentTrackedRoot,
      final NavigableMap<Bytes32, Bytes> downloadedAccounts,
      final WorldStateKeyValueStorage.Updater updater) {

    if (downloadedAccounts.isEmpty()) {
      return currentTrackedRoot;
    }

    final MerkleTrie<Bytes, Bytes> trie =
        new StoredMerklePatriciaTrie<>(
            coordinator::getAccountStateTrieNode,
            currentTrackedRoot,
            Function.identity(),
            Function.identity());

    for (final var entry : downloadedAccounts.entrySet()) {
      trie.put(entry.getKey(), entry.getValue());
    }

    trie.commit(
        (location, hash, value) ->
            WorldStateStorageCoordinator.applyForStrategy(
                updater,
                onBonsai -> {},
                onForest -> onForest.putAccountStateTrieNode(hash, value)));

    return Bytes32.wrap(trie.getRootHash());
  }
}
