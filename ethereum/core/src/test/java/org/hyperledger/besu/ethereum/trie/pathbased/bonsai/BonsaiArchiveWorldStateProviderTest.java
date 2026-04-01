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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_HASH_KEY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveFlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFullFlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BonsaiArchiveWorldStateProviderTest {

  private static final long MAX_LAYERS = 512L;
  private static final long CHAIN_HEAD = 10000L;

  private Blockchain blockchain;
  private BlockHeader chainHeadHeader;
  private BonsaiArchiveWorldStateProvider provider;

  @BeforeEach
  void setup() {
    blockchain = mock(Blockchain.class);
    chainHeadHeader = new BlockHeaderTestFixture().number(CHAIN_HEAD).buildHeader();
    when(blockchain.getChainHeadHeader()).thenReturn(chainHeadHeader);
    when(blockchain.getChainHeadBlockNumber()).thenReturn(CHAIN_HEAD);
    when(blockchain.getBlockHeader(chainHeadHeader.getHash()))
        .thenReturn(Optional.of(chainHeadHeader));

    provider = createProvider(true);
  }

  @Test
  void historicalQuery_returnsBonsaiWorldStateBackedByArchiveReadStorage() {
    final BlockHeader historicalHeader =
        new BlockHeaderTestFixture().number(CHAIN_HEAD - MAX_LAYERS - 1).buildHeader();
    when(blockchain.getBlockHeader(historicalHeader.getHash()))
        .thenReturn(Optional.of(historicalHeader));

    final var result =
        provider.getWorldState(
            WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(historicalHeader));

    assertThat(result).isPresent();
    final BonsaiWorldState worldState = (BonsaiWorldState) result.get();
    assertThat(worldState.getWorldStateStorage().getFlatDbStrategy())
        .isInstanceOf(BonsaiArchiveFlatDbStrategy.class);
  }

  @Test
  void recentQuery_returnsBonsaiWorldStateBackedByMainStorage() {
    final var result =
        provider.getWorldState(
            WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(chainHeadHeader));

    assertThat(result).isPresent();
    final BonsaiWorldState worldState = (BonsaiWorldState) result.get();
    assertThat(worldState.getWorldStateStorage().getFlatDbStrategy())
        .isInstanceOf(BonsaiFullFlatDbStrategy.class);
  }

  @Test
  void headUpdateQuery_returnsBonsaiWorldStateBackedByMainStorage() {
    final var result =
        provider.getWorldState(
            WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(chainHeadHeader));

    assertThat(result).isPresent();
    final BonsaiWorldState worldState = (BonsaiWorldState) result.get();
    assertThat(worldState.getWorldStateStorage().getFlatDbStrategy())
        .isInstanceOf(BonsaiFullFlatDbStrategy.class);
  }

  @Test
  void historicalQuery_beforeMigrationComplete_returnsEmpty() {
    // Migration not yet complete: flatDbMode is FULL, not ARCHIVE.
    // The archive path is skipped; super.getWorldState() is called instead,
    // which cannot serve blocks beyond maxLayersToLoad via trie-log rollback.
    final BonsaiArchiveWorldStateProvider providerNotYetMigrated = createProvider(false);

    final BlockHeader historicalHeader =
        new BlockHeaderTestFixture().number(CHAIN_HEAD - MAX_LAYERS - 1).buildHeader();

    final var result =
        providerNotYetMigrated.getWorldState(
            WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(historicalHeader));

    assertThat(result).isEmpty();
  }

  @Test
  void recentQuery_beforeMigrationComplete_returnsBonsaiWorldStateBackedByMainStorage() {
    // Migration not yet complete: falls through to super.getWorldState() which serves
    // recent blocks normally via trie-log rollback from the cached head state.
    final BonsaiArchiveWorldStateProvider providerNotYetMigrated = createProvider(false);

    final var result =
        providerNotYetMigrated.getWorldState(
            WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(chainHeadHeader));

    assertThat(result).isPresent();
    final BonsaiWorldState worldState = (BonsaiWorldState) result.get();
    assertThat(worldState.getWorldStateStorage().getFlatDbStrategy())
        .isInstanceOf(BonsaiFullFlatDbStrategy.class);
  }

  private BonsaiArchiveWorldStateProvider createProvider(final boolean archiveModeReady) {
    final var config =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(DataStorageFormat.X_BONSAI_ARCHIVE)
            .pathBasedExtraStorageConfiguration(
                ImmutablePathBasedExtraStorageConfiguration.builder()
                    .maxLayersToLoad(MAX_LAYERS)
                    .build())
            .build();
    final BonsaiWorldStateKeyValueStorage worldStateStorage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(), new NoOpMetricsSystem(), config);
    if (archiveModeReady) {
      worldStateStorage.upgradeToArchiveFlatDbMode();
    }

    // Seed the chain head block hash so the head world state is cached under it,
    // allowing non-historical queries to find it via cachedWorldStorageManager.
    final var tx = worldStateStorage.getComposedWorldStateStorage().startTransaction();
    tx.put(
        TRIE_BRANCH_STORAGE,
        WORLD_BLOCK_HASH_KEY,
        chainHeadHeader.getHash().getBytes().toArrayUnsafe());
    tx.commit();

    return new BonsaiArchiveWorldStateProvider(
        worldStateStorage,
        blockchain,
        config,
        null,
        null,
        EvmConfiguration.DEFAULT,
        () -> null,
        new CodeCache(),
        new NoOpMetricsSystem());
  }
}
