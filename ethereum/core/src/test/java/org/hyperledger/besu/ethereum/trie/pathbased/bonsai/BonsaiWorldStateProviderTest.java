/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.core.WorldStateHealerHelper.throwingWorldStateHealerSupplier;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams.withStateRootAndBlockHashAndUpdateNodeHead;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class BonsaiWorldStateProviderTest {

  private static final DataStorageConfiguration DEFAULT_CONFIG =
      DataStorageConfiguration.DEFAULT_BONSAI_CONFIG;

  private final BlockHeaderTestFixture blockBuilder = new BlockHeaderTestFixture();

  @Mock private Blockchain blockchain;
  @Mock private StorageProvider storageProvider;
  @Mock private SegmentedKeyValueStorage segmentedKeyValueStorage;
  @Mock private KeyValueStorage trieLogStorage;
  @Mock private SegmentedKeyValueStorageTransaction segmentedKeyValueStorageTransaction;
  @Mock private BonsaiCachedWorldStorageManager cachedWorldStorageManager;
  @Mock private TrieLogManager trieLogManager;

  private BonsaiWorldStateProvider bonsaiWorldStateArchive;

  @BeforeEach
  void setUp() {
    when(storageProvider.getStorageBySegmentIdentifiers(anyList()))
        .thenReturn(segmentedKeyValueStorage);
    when(segmentedKeyValueStorage.startTransaction())
        .thenReturn(segmentedKeyValueStorageTransaction);
    when(storageProvider.getStorageBySegmentIdentifier(any())).thenReturn(trieLogStorage);
    when(trieLogStorage.startTransaction()).thenReturn(mock(KeyValueStorageTransaction.class));
  }

  @Test
  void shouldReturnPersistedStateWhenRequested() {
    bonsaiWorldStateArchive = createBonsaiWorldStateProvider();

    final BlockHeader genesis = blockBuilder.number(0).buildHeader();
    final BlockHeader blockHeader1 =
        blockBuilder.number(1).parentHash(genesis.getHash()).buildHeader();

    bonsaiWorldStateArchive.getWorldState().persist(genesis);
    bonsaiWorldStateArchive.getWorldState().persist(blockHeader1);

    when(blockchain.getBlockHeader(genesis.getHash())).thenReturn(Optional.of(genesis));
    when(blockchain.getBlockHeader(blockHeader1.getHash())).thenReturn(Optional.of(blockHeader1));

    final TrieLogLayer trieLogLayer1 = mockTrieLogLayer(blockHeader1.getHash());
    when(trieLogManager.getTrieLogLayer(blockHeader1.getHash()))
        .thenReturn(Optional.of(trieLogLayer1));

    final Optional<BonsaiWorldState> worldStateGenesis =
        bonsaiWorldStateArchive
            .getWorldState(withBlockHeaderAndUpdateNodeHead(genesis))
            .map(BonsaiWorldState.class::cast);
    assertThat(worldStateGenesis)
        .isPresent()
        .hasValueSatisfying(
            ws -> assertThat(ws.getWorldStateBlockHash()).isEqualTo(genesis.getBlockHash()));

    final Optional<BonsaiWorldState> worldState1 =
        bonsaiWorldStateArchive
            .getWorldState(withBlockHeaderAndUpdateNodeHead(blockHeader1))
            .map(BonsaiWorldState.class::cast);
    assertThat(worldState1)
        .isPresent()
        .hasValueSatisfying(
            ws -> assertThat(ws.getWorldStateBlockHash()).isEqualTo(blockHeader1.getBlockHash()));
  }

  @Test
  void shouldReturnEmptyWhenLoadingMoreThanMaxLayersBack() {
    bonsaiWorldStateArchive =
        new BonsaiWorldStateProvider(
            new BonsaiWorldStateKeyValueStorage(
                storageProvider, new NoOpMetricsSystem(), DEFAULT_CONFIG),
            blockchain,
            ImmutablePathBasedExtraStorageConfiguration.builder().maxLayersToLoad(512L).build(),
            new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem()),
            null,
            EvmConfiguration.DEFAULT,
            throwingWorldStateHealerSupplier(),
            new CodeCache());

    final BlockHeader genesis = blockBuilder.number(0).buildHeader();
    final BlockHeader blockHeader512 =
        blockBuilder.number(512).parentHash(genesis.getHash()).buildHeader();

    when(blockchain.getBlockHeader(genesis.getHash())).thenReturn(Optional.of(genesis));
    when(blockchain.getBlockHeader(blockHeader512.getHash()))
        .thenReturn(Optional.of(blockHeader512));
    when(blockchain.getChainHeadHeader()).thenReturn(blockHeader512);

    assertThat(bonsaiWorldStateArchive.getWorldState(withBlockHeaderAndNoUpdateNodeHead(genesis)))
        .isEmpty();
    verify(cachedWorldStorageManager, never()).getWorldState(any(Hash.class));
  }

  @Test
  void shouldReturnWorldStateWhenLoadingLessThanMaxLayersBack() {
    bonsaiWorldStateArchive = createBonsaiWorldStateProvider();

    final BlockHeader genesis = blockBuilder.number(0).buildHeader();
    final BlockHeader blockHeader511 =
        blockBuilder.number(511).parentHash(genesis.getHash()).buildHeader();

    when(blockchain.getBlockHeader(genesis.getHash())).thenReturn(Optional.of(genesis));
    when(blockchain.getBlockHeader(blockHeader511.getHash()))
        .thenReturn(Optional.of(blockHeader511));

    bonsaiWorldStateArchive.getWorldState().persist(blockHeader511);

    final BonsaiWorldState mockWorldState = createMockWorldState(genesis.getHash());
    when(cachedWorldStorageManager.getWorldState(genesis.getHash()))
        .thenReturn(Optional.of(mockWorldState));

    when(trieLogManager.getMaxLayersToLoad()).thenReturn(512L);
    when(blockchain.getChainHeadHeader()).thenReturn(blockHeader511);

    assertThat(bonsaiWorldStateArchive.getWorldState(withBlockHeaderAndNoUpdateNodeHead(genesis)))
        .isPresent()
        .containsInstanceOf(BonsaiWorldState.class);
  }

  @Test
  void shouldRollbackStateWhenStorageInconsistencyDetected() {
    final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage =
        new BonsaiWorldStateKeyValueStorage(
            storageProvider, new NoOpMetricsSystem(), DEFAULT_CONFIG);

    bonsaiWorldStateArchive = spy(createBonsaiWorldStateProvider(worldStateKeyValueStorage));

    final BlockHeader genesis = blockBuilder.number(0).buildHeader();
    final BlockHeader blockHeader1 =
        blockBuilder.number(1).parentHash(genesis.getHash()).buildHeader();

    when(blockchain.getBlockHeader(genesis.getHash())).thenReturn(Optional.of(genesis));
    when(blockchain.getBlockHeader(blockHeader1.getHash())).thenReturn(Optional.of(blockHeader1));

    bonsaiWorldStateArchive.getWorldState().persist(genesis);
    bonsaiWorldStateArchive.getWorldState().persist(blockHeader1);

    final TrieLogLayer trieLogLayer1 = mockTrieLogLayer(blockHeader1.getHash());
    when(trieLogManager.getTrieLogLayer(blockHeader1.getHash()))
        .thenReturn(Optional.of(trieLogLayer1));

    assertThat(
            bonsaiWorldStateArchive.getWorldState(
                withStateRootAndBlockHashAndUpdateNodeHead(null, genesis.getHash())))
        .isPresent()
        .containsInstanceOf(BonsaiWorldState.class);

    // Verify that trie log layer was retrieved to perform rollback
    verify(trieLogManager).getTrieLogLayer(blockHeader1.getBlockHash());
  }

  @Test
  void shouldNotRollbackStateWhenStorageIsConsistent() {
    final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage =
        new BonsaiWorldStateKeyValueStorage(
            storageProvider, new NoOpMetricsSystem(), DEFAULT_CONFIG);

    bonsaiWorldStateArchive = spy(createBonsaiWorldStateProvider(worldStateKeyValueStorage));

    final BlockHeader genesis = blockBuilder.number(0).buildHeader();
    final BlockHeader blockHeader1 =
        blockBuilder.number(1).parentHash(genesis.getHash()).buildHeader();

    when(blockchain.getBlockHeader(genesis.getHash())).thenReturn(Optional.of(genesis));
    when(blockchain.getBlockHeader(blockHeader1.getHash())).thenReturn(Optional.of(blockHeader1));

    bonsaiWorldStateArchive.getWorldState().persist(genesis);
    bonsaiWorldStateArchive.getWorldState().persist(blockHeader1);

    assertThat(
            bonsaiWorldStateArchive.getWorldState(
                withStateRootAndBlockHashAndUpdateNodeHead(null, blockHeader1.getHash())))
        .isPresent()
        .containsInstanceOf(BonsaiWorldState.class);

    // Verify that no rollback was attempted when block is present
    verify(trieLogManager, never()).getTrieLogLayer(any());
  }

  @Test
  void shouldRollbackAndRollForwardDuringReorg() {
    final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage =
        new BonsaiWorldStateKeyValueStorage(
            storageProvider, new NoOpMetricsSystem(), DEFAULT_CONFIG);

    bonsaiWorldStateArchive = spy(createBonsaiWorldStateProvider(worldStateKeyValueStorage));

    final BlockHeader genesis = blockBuilder.number(0).buildHeader();
    final BlockHeader blockHeader1 =
        blockBuilder.number(1).timestamp(1).parentHash(genesis.getHash()).buildHeader();
    final BlockHeader blockHeader1Reorg =
        blockBuilder.number(1).timestamp(2).parentHash(genesis.getHash()).buildHeader();

    when(blockchain.getBlockHeader(genesis.getHash())).thenReturn(Optional.of(genesis));
    when(blockchain.getBlockHeader(blockHeader1.getHash())).thenReturn(Optional.of(blockHeader1));
    when(blockchain.getBlockHeader(blockHeader1Reorg.getHash()))
        .thenReturn(Optional.of(blockHeader1Reorg));

    bonsaiWorldStateArchive.getWorldState().persist(genesis);
    bonsaiWorldStateArchive.getWorldState().persist(blockHeader1);

    final TrieLogLayer trieLogLayer1 = mockTrieLogLayer(blockHeader1.getHash());
    final TrieLogLayer trieLogLayer1Reorg = mockTrieLogLayer(blockHeader1Reorg.getHash());

    when(trieLogManager.getTrieLogLayer(blockHeader1.getHash()))
        .thenReturn(Optional.of(trieLogLayer1));
    when(trieLogManager.getTrieLogLayer(blockHeader1Reorg.getHash()))
        .thenReturn(Optional.of(trieLogLayer1Reorg));

    assertThat(
            bonsaiWorldStateArchive.getWorldState(
                withStateRootAndBlockHashAndUpdateNodeHead(null, blockHeader1Reorg.getHash())))
        .isPresent()
        .containsInstanceOf(BonsaiWorldState.class);

    // Verify that both rollback and roll forward were performed
    verify(trieLogManager).getTrieLogLayer(blockHeader1.getHash());
    verify(trieLogManager).getTrieLogLayer(blockHeader1Reorg.getHash());
  }

  // Helper methods

  private BonsaiWorldStateProvider createBonsaiWorldStateProvider() {
    return createBonsaiWorldStateProvider(
        new BonsaiWorldStateKeyValueStorage(
            storageProvider, new NoOpMetricsSystem(), DEFAULT_CONFIG));
  }

  private BonsaiWorldStateProvider createBonsaiWorldStateProvider(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage) {
    return new BonsaiWorldStateProvider(
        cachedWorldStorageManager,
        DEFAULT_CONFIG.getPathBasedExtraStorageConfiguration(),
        trieLogManager,
        worldStateKeyValueStorage,
        blockchain,
        new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem()),
        EvmConfiguration.DEFAULT,
        throwingWorldStateHealerSupplier(),
        new CodeCache());
  }

  private BonsaiWorldState createMockWorldState(final Hash blockHash) {
    final BonsaiWorldState mockWorldState = mock(BonsaiWorldState.class);
    when(mockWorldState.blockHash()).thenReturn(blockHash);
    when(mockWorldState.freezeStorage()).thenReturn(mockWorldState);
    return mockWorldState;
  }

  private TrieLogLayer mockTrieLogLayer(final Hash blockHash) {
    final TrieLogLayer trieLogLayer = mock(TrieLogLayer.class);
    when(trieLogLayer.getBlockHash()).thenReturn(blockHash);
    return trieLogLayer;
  }
}
