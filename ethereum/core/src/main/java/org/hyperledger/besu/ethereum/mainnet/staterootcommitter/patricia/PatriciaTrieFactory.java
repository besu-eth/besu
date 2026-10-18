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
package org.hyperledger.besu.ethereum.mainnet.staterootcommitter.patricia;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.mainnet.parallelization.BlockProcessingExecutors;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.NoOpMerkleTrie;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.TrieNodeKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.patricia.ParallelStoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;

import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** Builds the Patricia tries of a Bonsai world state. */
public final class PatriciaTrieFactory {

  private PatriciaTrieFactory() {}

  /** Account state trie rooted at the current world state root. */
  public static MerkleTrie<Bytes, Bytes> createAccountStateTrie(final BonsaiWorldState worldState) {
    return createTrie(
        worldState,
        (location, hash) ->
            worldState
                .getBonsaiCachedMerkleTrieLoader()
                .getAccountStateTrieNode(worldState.getWorldStateStorage(), location, hash),
        Bytes32.wrap(worldState.getWorldStateRootHash().getBytes()),
        BlockProcessingExecutors.accountTrieForkJoinPool());
  }

  /** Storage trie for the given account rooted at the provided storage root. */
  public static MerkleTrie<Bytes, Bytes> createStorageTrie(
      final BonsaiWorldState worldState, final Hash accountHash, final Hash storageRoot) {
    return createTrie(
        worldState,
        (location, key) ->
            worldState
                .getBonsaiCachedMerkleTrieLoader()
                .getAccountStorageTrieNode(
                    worldState.getWorldStateStorage(), accountHash, location, key),
        Bytes32.wrap(storageRoot.getBytes()),
        BlockProcessingExecutors.storageTrieForkJoinPool());
  }

  /** All the storage slots of an account, keyed by slot hash. */
  public static Map<Bytes32, Bytes> getAllAccountStorage(
      final BonsaiWorldState worldState, final Address address, final Hash rootHash) {
    final MerkleTrie<Bytes, Bytes> storageTrie =
        createTrie(
            worldState,
            (location, key) ->
                worldState
                    .getWorldStateStorage()
                    .getTrieNode(TrieNodeKey.of(address.addressHash(), location), key),
            Bytes32.wrap(rootHash.getBytes()),
            BlockProcessingExecutors.accountTrieForkJoinPool());
    return storageTrie.entriesFrom(Bytes32.ZERO, Integer.MAX_VALUE);
  }

  /** Sequential trie for the per-transaction frontier roots, too small for the parallel trie. */
  public static MerkleTrie<Bytes, Bytes> createFrontierTrie(
      final BonsaiWorldState worldState, final NodeLoader nodeLoader, final Bytes32 rootHash) {
    if (worldState.isTrieDisabled()) {
      return new NoOpMerkleTrie<>();
    }
    return new StoredMerklePatriciaTrie<>(
        nodeLoader, rootHash, Function.identity(), Function.identity());
  }

  private static MerkleTrie<Bytes, Bytes> createTrie(
      final BonsaiWorldState worldState,
      final NodeLoader nodeLoader,
      final Bytes32 rootHash,
      final ForkJoinPool forkJoinPool) {
    if (worldState.isTrieDisabled()) {
      return new NoOpMerkleTrie<>();
    }
    if (worldState.isParallelStateRootComputationEnabled()) {
      return new ParallelStoredMerklePatriciaTrie<>(
          nodeLoader, rootHash, Function.identity(), Function.identity(), forkJoinPool);
    }
    return new StoredMerklePatriciaTrie<>(
        nodeLoader, rootHash, Function.identity(), Function.identity());
  }
}
