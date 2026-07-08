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

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.NoOpMerkleTrie;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.ethereum.trie.patricia.ParallelStoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class BonsaiTrieFactoryTest {

  private static final Bytes32 EMPTY_ROOT = Bytes32.ZERO;

  @Test
  void parallelizeAllowedWithParallelEnabledReturnsParallelTrie() {
    final BonsaiTrieFactory factory = createFactory(false, true);
    final MerkleTrie<Bytes, Bytes> trie =
        factory.create(
            emptyNodeLoader(), EMPTY_ROOT, BonsaiTrieFactory.TrieMode.PARALLELIZE_ALLOWED);
    assertThat(trie).isInstanceOf(ParallelStoredMerklePatriciaTrie.class);
  }

  @Test
  void alwaysSequentialWithParallelEnabledReturnsSequentialTrie() {
    final BonsaiTrieFactory factory = createFactory(false, true);
    final MerkleTrie<Bytes, Bytes> trie =
        factory.create(emptyNodeLoader(), EMPTY_ROOT, BonsaiTrieFactory.TrieMode.ALWAYS_SEQUENTIAL);
    assertThat(trie).isInstanceOf(StoredMerklePatriciaTrie.class);
    assertThat(trie).isNotInstanceOf(ParallelStoredMerklePatriciaTrie.class);
  }

  @Test
  void parallelizeAllowedWithParallelDisabledReturnsSequentialTrie() {
    final BonsaiTrieFactory factory = createFactory(false, false);
    final MerkleTrie<Bytes, Bytes> trie =
        factory.create(
            emptyNodeLoader(), EMPTY_ROOT, BonsaiTrieFactory.TrieMode.PARALLELIZE_ALLOWED);
    assertThat(trie).isInstanceOf(StoredMerklePatriciaTrie.class);
    assertThat(trie).isNotInstanceOf(ParallelStoredMerklePatriciaTrie.class);
  }

  @Test
  void trieDisabledReturnsNoOpForParallelizeAllowed() {
    final BonsaiTrieFactory factory = createFactory(true, true);
    final MerkleTrie<Bytes, Bytes> trie =
        factory.create(
            emptyNodeLoader(), EMPTY_ROOT, BonsaiTrieFactory.TrieMode.PARALLELIZE_ALLOWED);
    assertThat(trie).isInstanceOf(NoOpMerkleTrie.class);
  }

  @Test
  void trieDisabledReturnsNoOpForAlwaysSequential() {
    final BonsaiTrieFactory factory = createFactory(true, true);
    final MerkleTrie<Bytes, Bytes> trie =
        factory.create(emptyNodeLoader(), EMPTY_ROOT, BonsaiTrieFactory.TrieMode.ALWAYS_SEQUENTIAL);
    assertThat(trie).isInstanceOf(NoOpMerkleTrie.class);
  }

  private static BonsaiTrieFactory createFactory(
      final boolean trieDisabled, final boolean parallelEnabled) {
    final WorldStateConfig config =
        WorldStateConfig.newBuilder()
            .trieDisabled(trieDisabled)
            .parallelStateRootComputationEnabled(parallelEnabled)
            .build();
    return new BonsaiTrieFactory(config);
  }

  private static org.hyperledger.besu.ethereum.trie.NodeLoader emptyNodeLoader() {
    return (location, hash) -> Optional.empty();
  }
}
