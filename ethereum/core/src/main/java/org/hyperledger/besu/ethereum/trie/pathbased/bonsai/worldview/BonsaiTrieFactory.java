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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview;

import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.NoOpMerkleTrie;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.ethereum.trie.patricia.ParallelStoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;

import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Creates Merkle trie instances based on the execution context. This centralizes the decision of
 * which trie implementation to use, keeping it out of the callers.
 *
 * <p>The abstraction is intentionally minimal and package-private. It is introduced to eliminate
 * the overhead of {@link ParallelStoredMerklePatriciaTrie} in latency-sensitive paths (like
 * per-transaction {@code frontierRootHash()} calls) while preserving it for throughput-oriented
 * batch computation. The structure is designed so the policy can be elevated to a broader scope in
 * the future without changing callers.
 */
public class BonsaiTrieFactory {

  /**
   * Describes the execution profile under which a trie is being created. This is not a choice of
   * implementation — it is a declaration of latency/throughput intent that the factory maps to the
   * appropriate trie type.
   */
  enum TrieMode {
    /** May use parallel trie if the global config allows it. */
    PARALLELIZE_ALLOWED,

    /** Always uses sequential trie regardless of config. */
    ALWAYS_SEQUENTIAL
  }

  private final WorldStateConfig worldStateConfig;

  BonsaiTrieFactory(final WorldStateConfig worldStateConfig) {
    this.worldStateConfig = worldStateConfig;
  }

  /**
   * Creates a Merkle trie appropriate for the given trieMode context.
   *
   * @param nodeLoader loader for trie nodes from storage
   * @param rootHash root hash to start from
   * @param trieMode the execution context declaring latency/throughput intent
   * @return a trie instance; never null
   */
  MerkleTrie<Bytes, Bytes> create(
      final NodeLoader nodeLoader, final Bytes32 rootHash, final TrieMode trieMode) {
    if (worldStateConfig.isTrieDisabled()) {
      return new NoOpMerkleTrie<>();
    }
    if (trieMode == TrieMode.PARALLELIZE_ALLOWED
        && worldStateConfig.isParallelStateRootComputationEnabled()) {
      return new ParallelStoredMerklePatriciaTrie<>(
          nodeLoader, rootHash, Function.identity(), Function.identity());
    }
    return new StoredMerklePatriciaTrie<>(
        nodeLoader, rootHash, Function.identity(), Function.identity());
  }
}
