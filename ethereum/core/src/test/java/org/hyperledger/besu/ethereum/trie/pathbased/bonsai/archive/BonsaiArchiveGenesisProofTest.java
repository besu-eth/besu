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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveHistoryReader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveIndexProgress;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeHistoryStore;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveProofNodeLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

/**
 * Regression test for the genesis-state ordering requirement of the bonsai archive proofs feature.
 *
 * <p>Genesis state is written during {@code BesuControllerBuilder.build()} via {@code
 * GenesisState.writeStateTo(...)}. If the {@link ArchiveTrieNodeStrategy} is installed
 * <em>after</em> that write, the genesis (block 0) trie nodes are persisted with the default
 * strategy and never captured into the archive, so {@code eth_getProof} fails for any account
 * untouched since genesis.
 *
 * <p>Unlike {@code BonsaiArchiveStateProofIntegrationTest}, which installs the strategy in {@code
 * setUp()} and then hand-drives {@code putFlatAccountTrieNode}, this test drives the real {@code
 * worldState.persist(...)} path (block 0 is derived from the absent {@code WORLD_BLOCK_NUMBER_KEY},
 * exactly as on a fresh database) so that <em>when</em> the strategy is installed relative to the
 * genesis write is what determines the outcome.
 */
class BonsaiArchiveGenesisProofTest {

  private static final Address ACCOUNT =
      Address.fromHexString("0xabababababababababababababababababababab");

  @Test
  void genesisTrieNodesAreArchivedWhenStrategyInstalledBeforeGenesisWrite() {
    final Fixture fixture = new Fixture();

    // Install the archive strategy BEFORE writing genesis (matches BesuControllerBuilder ordering).
    fixture.installArchiveStrategy();
    fixture.writeGenesisAccount();

    // Block 0 is covered and the account state-trie root node is served from the archive.
    assertThat(fixture.indexProgress.isBlockIndexed(0L)).isTrue();
    final NodeLoader loader = ArchiveProofNodeLoader.forAccount(fixture.historyReader, 0L);
    assertThat(loader.getNode(Bytes.EMPTY, Bytes32.wrap(fixture.worldState.rootHash().getBytes())))
        .isPresent();
  }

  @Test
  void genesisTrieNodesAreNotArchivedWhenStrategyInstalledAfterGenesisWrite() {
    final Fixture fixture = new Fixture();

    // Reproduces the bug: writing genesis first, then installing the strategy, loses block 0.
    fixture.writeGenesisAccount();
    fixture.installArchiveStrategy();

    assertThat(fixture.indexProgress.isBlockIndexed(0L)).isFalse();
    final NodeLoader loader = ArchiveProofNodeLoader.forAccount(fixture.historyReader, 0L);
    assertThat(loader.getNode(Bytes.EMPTY, Bytes32.wrap(fixture.worldState.rootHash().getBytes())))
        .isEmpty();
  }

  /** An in-memory bonsai world state wired up with archive trie-node read primitives. */
  private static final class Fixture {
    private final BonsaiWorldState worldState;
    private final BonsaiWorldStateKeyValueStorage storage;
    private final ArchiveNodeHistoryStore historyStore;
    private final ArchiveIndexProgress indexProgress;
    private final ArchiveHistoryReader historyReader;

    private Fixture() {
      final Blockchain blockchain = mock(Blockchain.class);
      final BonsaiWorldStateProvider provider =
          InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(blockchain);
      worldState = (BonsaiWorldState) provider.getWorldState();
      storage = worldState.getWorldStateStorage();
      final SegmentedKeyValueStorage composed = storage.getComposedWorldStateStorage();
      historyStore = new ArchiveNodeHistoryStore(composed);
      indexProgress = new ArchiveIndexProgress(composed);
      historyReader = new ArchiveHistoryReader(historyStore);
    }

    private void installArchiveStrategy() {
      storage.setTrieNodeStrategy(
          new ArchiveTrieNodeStrategy(
              new BonsaiTrieNodeStrategy(), historyStore, indexProgress, () -> true));
    }

    private void writeGenesisAccount() {
      final WorldUpdater updater = worldState.updater();
      updater.createAccount(ACCOUNT, 0, Wei.of(1_000_000));
      updater.commit();
      // Null header => genesis-style persist: WORLD_BLOCK_NUMBER_KEY is absent, so block == 0.
      worldState.persist(null);
    }
  }
}
