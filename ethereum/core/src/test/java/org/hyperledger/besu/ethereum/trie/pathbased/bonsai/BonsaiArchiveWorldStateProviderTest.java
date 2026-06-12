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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_HASH_KEY;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
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
    provider.setArchiveMigrationProgressSupplier(() -> CHAIN_HEAD);

    final var result =
        provider.getWorldState(
            WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(historicalHeader));

    assertThat(result).isPresent();
    final BonsaiWorldState worldState = (BonsaiWorldState) result.get();
    assertThat(worldState.getWorldStateStorage().getFlatDbStrategy())
        .isInstanceOf(BonsaiArchiveFlatDbStrategy.class);
  }

  @Test
  void historicalQuery_migratorBehindQueryBlock_fallsThroughToSuper() {
    // Migrator has not yet processed the requested block.
    // Provider should fall through to super.getWorldState(), which cannot serve blocks
    // beyond maxLayersToLoad via trie-log rollback → empty result.
    final long queryBlockNumber = CHAIN_HEAD - MAX_LAYERS - 1;
    final BlockHeader historicalHeader =
        new BlockHeaderTestFixture().number(queryBlockNumber).buildHeader();
    when(blockchain.getBlockHeader(historicalHeader.getHash()))
        .thenReturn(Optional.of(historicalHeader));
    provider.setArchiveMigrationProgressSupplier(() -> queryBlockNumber - 1);

    final var result =
        provider.getWorldState(
            WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(historicalHeader));

    assertThat(result).isEmpty();
  }

  @Test
  void historicalQuery_atExactBoundary_migratorBehind_fallsThroughToSuper() {
    // Models the "new block just arrived, migrator hasn't caught up yet" race:
    // queryBlock sits exactly at head - maxLayersToLoad (the historical routing threshold),
    // migrator progress is one block short. Gate must refuse the archive route.
    final long queryBlockNumber = CHAIN_HEAD - MAX_LAYERS;
    final BlockHeader boundaryHeader =
        new BlockHeaderTestFixture().number(queryBlockNumber).buildHeader();
    when(blockchain.getBlockHeader(boundaryHeader.getHash()))
        .thenReturn(Optional.of(boundaryHeader));
    provider.setArchiveMigrationProgressSupplier(() -> queryBlockNumber - 1);

    final var result =
        provider.getWorldState(
            WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(boundaryHeader));

    assertThat(result).isEmpty();
  }

  @Test
  void historicalQuery_migratorAtQueryBlock_usesArchive() {
    final long queryBlockNumber = CHAIN_HEAD - MAX_LAYERS - 1;
    final BlockHeader historicalHeader =
        new BlockHeaderTestFixture().number(queryBlockNumber).buildHeader();
    when(blockchain.getBlockHeader(historicalHeader.getHash()))
        .thenReturn(Optional.of(historicalHeader));
    provider.setArchiveMigrationProgressSupplier(() -> queryBlockNumber);

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

  // --- Proofs path tests ---

  @Test
  void proofsEnabled_targetIsCheckpointBlock_returnsProofWorldState() {
    // With interval=100, targetNumber=99 → nearestCheckpoint = ((99+100)/100)*100 - 1 = 99
    // so target IS the checkpoint. No trie-log rollback needed; returns immediately.
    final long interval = 100L;
    final BonsaiArchiveWorldStateProvider proofsProvider =
        createProviderWithProofs(true, true, interval);
    proofsProvider.setArchiveMigrationProgressSupplier(() -> CHAIN_HEAD);

    final long targetNumber = interval - 1; // 99
    final BlockHeader targetHeader =
        new BlockHeaderTestFixture().number(targetNumber).buildHeader();
    when(blockchain.getBlockHeader(targetHeader.getHash())).thenReturn(Optional.of(targetHeader));
    when(blockchain.getBlockHeaderSafe(targetNumber)).thenReturn(Optional.of(targetHeader));

    final var result =
        proofsProvider.getWorldState(
            WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(targetHeader));

    assertThat(result).isPresent();
    final BonsaiWorldState worldState = (BonsaiWorldState) result.get();
    assertThat(worldState.getWorldStateStorage().getFlatDbStrategy())
        .isInstanceOf(BonsaiArchiveFlatDbStrategy.class);
  }

  @Test
  void proofsEnabled_headUpdateQuery_bypassesProofPath() {
    // shouldWorldStateUpdateHead=true means isHistoricalQuery returns false; falls through to
    // super.getWorldState() which returns the cached head state normally.
    final BonsaiArchiveWorldStateProvider proofsProvider =
        createProviderWithProofs(true, true, 100L);

    final var result =
        proofsProvider.getWorldState(
            WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(chainHeadHeader));

    assertThat(result).isPresent();
    final BonsaiWorldState worldState = (BonsaiWorldState) result.get();
    assertThat(worldState.getWorldStateStorage().getFlatDbStrategy())
        .isInstanceOf(BonsaiFullFlatDbStrategy.class);
  }

  @Test
  void proofsEnabled_blockHashNotInChain_fallsThroughToSuper() {
    // Block 50 is far behind the head but isHistoricalQuery requires archiveMigrationProgress
    // to cover the target block. With default progress=-1 the gate fails and super is called,
    // which cannot serve this block → empty result.
    final BonsaiArchiveWorldStateProvider proofsProvider =
        createProviderWithProofs(true, true, 100L);

    final BlockHeader unknownHeader = new BlockHeaderTestFixture().number(50L).buildHeader();

    final var result =
        proofsProvider.getWorldState(
            WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(unknownHeader));

    assertThat(result).isEmpty();
  }

  @Test
  void proofsDisabled_historicalBlock_usesArchiveFlatDbPath() {
    // When stateProofsEnabled=false the proofs branch is skipped; historical queries
    // use the archive flat-db path as before.
    final BonsaiArchiveWorldStateProvider archiveProvider =
        createProviderWithProofs(true, false, 100L);

    final BlockHeader historicalHeader =
        new BlockHeaderTestFixture().number(CHAIN_HEAD - MAX_LAYERS - 1).buildHeader();
    when(blockchain.getBlockHeader(historicalHeader.getHash()))
        .thenReturn(Optional.of(historicalHeader));
    archiveProvider.setArchiveMigrationProgressSupplier(() -> CHAIN_HEAD);

    final var result =
        archiveProvider.getWorldState(
            WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(historicalHeader));

    assertThat(result).isPresent();
    final BonsaiWorldState worldState = (BonsaiWorldState) result.get();
    assertThat(worldState.getWorldStateStorage().getFlatDbStrategy())
        .isInstanceOf(BonsaiArchiveFlatDbStrategy.class);
  }

  // --- Checkpoint direction selection tests ---

  @Test
  void proofsEnabled_floorCheckpointCloser_requestsFloorBlockNumber() {
    // interval=100, target=110: floor=99 (dist=11), ceiling=199 (dist=89) → floor selected
    final long interval = 100L;
    final BonsaiArchiveWorldStateProvider proofsProvider =
        createProviderWithProofs(true, true, interval);
    proofsProvider.setArchiveMigrationProgressSupplier(() -> CHAIN_HEAD);

    final long targetNumber = 110L;
    final BlockHeader targetHeader =
        new BlockHeaderTestFixture().number(targetNumber).buildHeader();
    final BlockHeader floorHeader = new BlockHeaderTestFixture().number(99L).buildHeader();

    when(blockchain.getBlockHeader(targetHeader.getHash())).thenReturn(Optional.of(targetHeader));
    when(blockchain.getBlockHeaderSafe(99L)).thenReturn(Optional.of(floorHeader));
    // ceiling=199 not mocked — returns Optional.empty() by Mockito default

    // getWorldState throws MerkleTrieException in the unit test (no real trie state).
    // The checkpoint-selection verify below is the assertion under test.
    assertThatThrownBy(
            () ->
                proofsProvider.getWorldState(
                    WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(targetHeader)))
        .isInstanceOf(MerkleTrieException.class);

    verify(blockchain, atLeastOnce()).getBlockHeaderSafe(99L); // floor was chosen
    verify(blockchain, never()).getBlockHeaderSafe(199L); // ceiling was NOT requested
  }

  @Test
  void proofsEnabled_ceilingCheckpointCloser_requestsCeilingBlockNumber() {
    // interval=100, target=180: floor=99 (dist=81), ceiling=199 (dist=19) → ceiling selected
    final long interval = 100L;
    final BonsaiArchiveWorldStateProvider proofsProvider =
        createProviderWithProofs(true, true, interval);
    proofsProvider.setArchiveMigrationProgressSupplier(() -> CHAIN_HEAD);

    final long targetNumber = 180L;
    final BlockHeader targetHeader =
        new BlockHeaderTestFixture().number(targetNumber).buildHeader();
    final BlockHeader ceilingHeader = new BlockHeaderTestFixture().number(199L).buildHeader();

    when(blockchain.getBlockHeader(targetHeader.getHash())).thenReturn(Optional.of(targetHeader));
    when(blockchain.getBlockHeaderSafe(199L)).thenReturn(Optional.of(ceilingHeader));
    // floor=99 not mocked — returns Optional.empty() by Mockito default

    assertThatThrownBy(
            () ->
                proofsProvider.getWorldState(
                    WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(targetHeader)))
        .isInstanceOf(MerkleTrieException.class);

    verify(blockchain, never()).getBlockHeaderSafe(99L); // floor was NOT requested
    verify(blockchain, atLeastOnce()).getBlockHeaderSafe(199L); // ceiling was chosen
  }

  @Test
  void proofsEnabled_targetInFirstWindow_alwaysRequestsCeiling() {
    // interval=100, target=50: floor=(-1) does not exist → ceiling=99 always selected
    final long interval = 100L;
    final BonsaiArchiveWorldStateProvider proofsProvider =
        createProviderWithProofs(true, true, interval);
    proofsProvider.setArchiveMigrationProgressSupplier(() -> CHAIN_HEAD);

    final long targetNumber = 50L;
    final BlockHeader targetHeader =
        new BlockHeaderTestFixture().number(targetNumber).buildHeader();
    final BlockHeader ceilingHeader = new BlockHeaderTestFixture().number(99L).buildHeader();

    when(blockchain.getBlockHeader(targetHeader.getHash())).thenReturn(Optional.of(targetHeader));
    when(blockchain.getBlockHeaderSafe(99L)).thenReturn(Optional.of(ceilingHeader));

    assertThatThrownBy(
            () ->
                proofsProvider.getWorldState(
                    WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(targetHeader)))
        .isInstanceOf(MerkleTrieException.class);

    verify(blockchain, atLeastOnce()).getBlockHeaderSafe(99L); // ceiling is the only checkpoint
  }

  private BonsaiArchiveWorldStateProvider createProvider(final boolean archiveModeReady) {
    return createProviderWithProofs(archiveModeReady, false, 100L);
  }

  private BonsaiArchiveWorldStateProvider createProviderWithProofs(
      final boolean archiveModeReady,
      final boolean stateProofsEnabled,
      final long checkpointInterval) {
    final var config =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(DataStorageFormat.X_BONSAI_ARCHIVE)
            .pathBasedExtraStorageConfiguration(
                ImmutablePathBasedExtraStorageConfiguration.builder()
                    .maxLayersToLoad(MAX_LAYERS)
                    .unstable(
                        ImmutablePathBasedExtraStorageConfiguration.PathBasedUnstable.builder()
                            .stateProofsEnabled(stateProofsEnabled)
                            .archiveTrieNodeCheckpointInterval(checkpointInterval)
                            .build())
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
