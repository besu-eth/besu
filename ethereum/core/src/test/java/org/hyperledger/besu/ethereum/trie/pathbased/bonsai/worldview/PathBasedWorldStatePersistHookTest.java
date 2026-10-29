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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview;

import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.WorldStateConfig.createStatefulConfigWithTrie;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.TrieNodeStrategy;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class PathBasedWorldStatePersistHookTest {

  private static final Address ACCOUNT =
      Address.fromHexString("0x1111111111111111111111111111111111111111");

  private TrieNodeStrategy trieNodeStrategy;
  private BonsaiWorldState worldState;

  @BeforeEach
  void setUp() {
    final InMemoryKeyValueStorageProvider provider = new InMemoryKeyValueStorageProvider();
    final BonsaiWorldStateKeyValueStorage worldStateStorage =
        new BonsaiWorldStateKeyValueStorage(
            provider, new NoOpMetricsSystem(), DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
    trieNodeStrategy = spy(new BonsaiTrieNodeStrategy());
    worldStateStorage.setTrieNodeStrategy(trieNodeStrategy);
    worldState =
        new BonsaiWorldState(
            InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(
                mock(Blockchain.class)),
            worldStateStorage,
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new BonsaiCodeCache());

    final WorldUpdater updater = worldState.updater();
    updater.createAccount(ACCOUNT, 1, Wei.of(1L));
    updater.commit();
  }

  @Test
  void persistCallsBlockPersistHookBeforeTrieWrites() {
    final BlockHeader header =
        new BlockHeaderTestFixture()
            .number(7L)
            .stateRoot(worldState.frontierRootHash())
            .buildHeader();
    worldState.persist(header);

    final InOrder inOrder = inOrder(trieNodeStrategy);
    inOrder.verify(trieNodeStrategy).onBlockPersist(any(), any(), eq(header));
    inOrder.verify(trieNodeStrategy, atLeastOnce()).putTrieNode(any(), any(), any(), any(), any());
  }

  @Test
  void persistWithTrieDisabledDoesNotCallHook() {
    worldState.disableTrie();
    worldState.persist(new BlockHeaderTestFixture().number(7L).buildHeader());

    verify(trieNodeStrategy, never()).onBlockPersist(any(), any(), any());
  }

  @Test
  void persistWithNullHeaderDoesNotCallHook() {
    worldState.persist(null);

    verify(trieNodeStrategy, never()).onBlockPersist(any(), any(), any());
  }
}
