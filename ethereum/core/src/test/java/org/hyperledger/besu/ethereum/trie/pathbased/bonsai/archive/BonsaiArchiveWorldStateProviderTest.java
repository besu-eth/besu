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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.BonsaiArchiveWorldStateProvider.CONFIRMATION_DEPTH;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_BLOCK_HASH_KEY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeHistoryStore;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeHistoryStore.ArchivedBlock;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeCodec;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFullFlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.patricia.SimpleMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableExtraStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.function.LongSupplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BonsaiArchiveWorldStateProviderTest {

  private static final long MAX_LAYERS = 512L;
  private static final long CHAIN_HEAD = 10000L;

  private Blockchain blockchain;
  private BlockHeader chainHeadHeader;
  private BonsaiArchiveWorldStateProvider provider;
  private BonsaiWorldStateKeyValueStorage worldStateStorage;
  private Long seededProgress;
  private boolean seedWorldStateBlockHash = true;
  private boolean instantFinality = false;
  private LongSupplier networkHead = () -> CHAIN_HEAD;

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
  void archiveProof_failurePropagates() {
    final long queryBlockNumber = CHAIN_HEAD - MAX_LAYERS - 1;
    final BlockHeader historicalHeader =
        new BlockHeaderTestFixture().number(queryBlockNumber).buildHeader();
    final var liveStorage = worldStateStorage.getComposedWorldStateStorage();
    final var tx = liveStorage.startTransaction();
    new ArchiveNodeHistoryStore(liveStorage)
        .saveArchiveProgress(
            tx, new ArchivedBlock(queryBlockNumber, historicalHeader.getBlockHash()));
    tx.commit();
    when(blockchain.getBlockHashByNumber(queryBlockNumber))
        .thenReturn(Optional.of(historicalHeader.getBlockHash()));

    final RuntimeException failure = new RuntimeException("proof failure");
    assertThatThrownBy(
            () ->
                provider.getAccountProof(
                    historicalHeader,
                    Address.ZERO,
                    List.of(),
                    proof -> {
                      throw failure;
                    }))
        .isSameAs(failure);
  }

  @Test
  void archiveProof_blockBelowCoverageIsServedFromArchive() {
    final long queryBlockNumber = CHAIN_HEAD - MAX_LAYERS - 1;
    final BlockHeader historicalHeader =
        new BlockHeaderTestFixture().number(queryBlockNumber).buildHeader();
    archiveThrough(new BlockHeaderTestFixture().number(queryBlockNumber + 1).buildHeader());
    canonical(historicalHeader);

    final RuntimeException served = new RuntimeException("served from archive");
    assertThatThrownBy(
            () ->
                provider.getAccountProof(
                    historicalHeader,
                    Address.ZERO,
                    List.of(),
                    proof -> {
                      throw served;
                    }))
        .isSameAs(served);
  }

  @Test
  void archiveProof_blockAboveCoverageFallsBackToRollback() {
    final long queryBlockNumber = CHAIN_HEAD - MAX_LAYERS - 1;
    final BlockHeader historicalHeader =
        new BlockHeaderTestFixture().number(queryBlockNumber).buildHeader();
    archiveThrough(new BlockHeaderTestFixture().number(queryBlockNumber - 1).buildHeader());

    // The archived path would run the mapper; the rollback fallback can't reach this block.
    final Optional<Object> result =
        provider.getAccountProof(
            historicalHeader,
            Address.ZERO,
            List.of(),
            proof -> {
              throw new AssertionError("archived proof served past coverage");
            });

    assertThat(result).isEmpty();
  }

  @Test
  void archiveProof_provesAccountFromArchivedRoot() {
    final BlockHeader header = archiveSingleAccountRoot(Wei.of(7));

    assertThat(provider.getAccountProof(header, Address.ZERO, List.of(), proof -> proof))
        .hasValueSatisfying(
            proof ->
                assertThat(proof.getStateTrieAccountValue())
                    .hasValueSatisfying(
                        account -> assertThat(account.getBalance()).isEqualTo(Wei.of(7))));
  }

  @Test
  void archiveProof_rootFromAnotherForkIsNotServed() {
    final BlockHeader archived = archiveSingleAccountRoot(Wei.of(7));
    // the canonical block at the archived height after a reorg the archive missed
    final BlockHeader reorged =
        new BlockHeaderTestFixture()
            .number(archived.getNumber())
            .stateRoot(Hash.hash(Bytes.of(1)))
            .buildHeader();
    canonical(reorged);

    assertThatThrownBy(
            () -> provider.getAccountProof(reorged, Address.ZERO, List.of(), proof -> proof))
        .isInstanceOf(MerkleTrieException.class);
  }

  // Archives a one-account world state as the canonical block below the head's trie-log window.
  private BlockHeader archiveSingleAccountRoot(final Wei balance) {
    final SimpleMerklePatriciaTrie<Bytes32, Bytes> trie = new SimpleMerklePatriciaTrie<>(b -> b);
    final Bytes account =
        RLP.encode(
            new PatriciaTrieAccountValue(0, balance, Hash.EMPTY_TRIE_HASH, Hash.EMPTY)::writeTo);
    final Bytes32 key = Bytes32.wrap(Address.ZERO.addressHash().getBytes());
    trie.put(key, account);
    // a single-leaf trie's root is the only node in the proof
    final Bytes rootNode = trie.getValueWithProof(key).getProofRelatedNodes().getFirst();
    final BlockHeader header =
        new BlockHeaderTestFixture()
            .number(CHAIN_HEAD - MAX_LAYERS - 1)
            .stateRoot(Hash.wrap(trie.getRootHash()))
            .buildHeader();
    final var liveStorage = worldStateStorage.getComposedWorldStateStorage();
    final var tx = liveStorage.startTransaction();
    final ArchiveNodeHistoryStore historyStore = new ArchiveNodeHistoryStore(liveStorage);
    historyStore.putEncoded(
        tx,
        ArchiveNodeKey.historyKey(ArchiveNodeKey.account(Bytes.EMPTY), header.getNumber()),
        ArchiveNodeHistoryStore.encodeStoredValue(0, ArchiveTrieNodeCodec.encodeFull(rootNode)));
    historyStore.saveArchiveProgress(tx, new ArchivedBlock(header.getNumber(), header.getHash()));
    tx.commit();
    canonical(header);
    return header;
  }

  private void archiveThrough(final BlockHeader last) {
    final var liveStorage = worldStateStorage.getComposedWorldStateStorage();
    final var tx = liveStorage.startTransaction();
    new ArchiveNodeHistoryStore(liveStorage)
        .saveArchiveProgress(tx, new ArchivedBlock(last.getNumber(), last.getHash()));
    tx.commit();
    canonical(last);
  }

  private void canonical(final BlockHeader header) {
    when(blockchain.getBlockHashByNumber(header.getNumber()))
        .thenReturn(Optional.of(header.getHash()));
  }

  @Test
  void archiveProof_forkBlockAtArchivedHeightIsNotServedFromArchive() {
    final long queryBlockNumber = CHAIN_HEAD - MAX_LAYERS - 1;
    final BlockHeader fork =
        new BlockHeaderTestFixture().number(queryBlockNumber).extraData(Bytes.of(9)).buildHeader();
    archiveThrough(new BlockHeaderTestFixture().number(queryBlockNumber + 1).buildHeader());
    canonical(new BlockHeaderTestFixture().number(queryBlockNumber).buildHeader());

    // The archived path would run the mapper; the rollback fallback can't reach this block.
    final Optional<Object> result =
        provider.getAccountProof(
            fork,
            Address.ZERO,
            List.of(),
            proof -> {
              throw new AssertionError("canonical archived proof served for a fork block");
            });

    assertThat(result).isEmpty();
  }

  @Test
  void archiveProof_servedWhileLastArchivedBlockIsNotAppendedYet() {
    final long queryBlockNumber = CHAIN_HEAD - MAX_LAYERS - 1;
    final BlockHeader historicalHeader =
        new BlockHeaderTestFixture().number(queryBlockNumber).buildHeader();
    // a block is archived with its world state, before it's appended to the chain
    final BlockHeader archivedNotAppended =
        new BlockHeaderTestFixture().number(CHAIN_HEAD + 1).buildHeader();
    final var liveStorage = worldStateStorage.getComposedWorldStateStorage();
    final var tx = liveStorage.startTransaction();
    new ArchiveNodeHistoryStore(liveStorage)
        .saveArchiveProgress(
            tx, new ArchivedBlock(archivedNotAppended.getNumber(), archivedNotAppended.getHash()));
    tx.commit();
    when(blockchain.getBlockHashByNumber(archivedNotAppended.getNumber()))
        .thenReturn(Optional.empty());
    canonical(historicalHeader);

    final RuntimeException served = new RuntimeException("served from archive");
    assertThatThrownBy(
            () ->
                provider.getAccountProof(
                    historicalHeader,
                    Address.ZERO,
                    List.of(),
                    proof -> {
                      throw served;
                    }))
        .isSameAs(served);
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

  @Test
  void installsInlineStrategyWhenProofsEnabled() {
    seededProgress = 4L;
    final BonsaiArchiveWorldStateProvider proofsProvider = createProvider(true, true);

    assertThat(proofsProvider.getWorldStateKeyValueStorage().getTrieNodeStrategy())
        .isInstanceOf(ArchiveTrieNodeStrategy.class);
    proofsProvider.close();
  }

  @Test
  void leavesDefaultStrategyWhenProofsDisabled() {
    assertThat(provider.getWorldStateKeyValueStorage().getTrieNodeStrategy())
        .isInstanceOf(BonsaiTrieNodeStrategy.class);
  }

  @Test
  void everyBlockIsFinalWithInstantFinality() {
    instantFinality = true;
    final BonsaiArchiveWorldStateProvider bftProvider = createProvider(true);

    assertThat(bftProvider.isFinal(CHAIN_HEAD)).isTrue();
  }

  @Test
  void withoutFinalizedBlockOnlyBlocksBuriedUnderTheDepthAreFinal() {
    when(blockchain.getFinalized()).thenReturn(Optional.empty());
    networkHead = () -> 1000L;
    final BonsaiArchiveWorldStateProvider depthProvider = createProvider(true);

    assertThat(depthProvider.isFinal(1000L - CONFIRMATION_DEPTH)).isTrue();
    assertThat(depthProvider.isFinal(1000L - CONFIRMATION_DEPTH + 1)).isFalse();
  }

  @Test
  void genesisIsAlwaysFinal() {
    when(blockchain.getFinalized()).thenReturn(Optional.empty());
    networkHead = () -> 1L;
    final BonsaiArchiveWorldStateProvider depthProvider = createProvider(true);

    assertThat(depthProvider.isFinal(0L)).isTrue();
    assertThat(depthProvider.isFinal(1L)).isFalse();
  }

  @Test
  void isFinalComparesAgainstImportedFinalized() {
    final BlockHeader finalized = new BlockHeaderTestFixture().number(3).buildHeader();
    when(blockchain.getFinalized()).thenReturn(Optional.of(finalized.getHash()));
    when(blockchain.getBlockHeader(finalized.getHash())).thenReturn(Optional.of(finalized));

    assertThat(provider.isFinal(3)).isTrue();
    assertThat(provider.isFinal(4)).isFalse();
  }

  @Test
  void finalizedWithoutHeaderFallsBackToTheDepth() {
    final Hash finalizedHash = Hash.hash(Bytes.of(3));
    when(blockchain.getFinalized()).thenReturn(Optional.of(finalizedHash));
    when(blockchain.getBlockHeader(finalizedHash)).thenReturn(Optional.empty());
    networkHead = () -> 1000L;
    final BonsaiArchiveWorldStateProvider depthProvider = createProvider(true);

    assertThat(depthProvider.isFinal(1000L - CONFIRMATION_DEPTH)).isTrue();
    assertThat(depthProvider.isFinal(1000L - CONFIRMATION_DEPTH + 1)).isFalse();
  }

  @Test
  void startupFailsWhenProofsEnabledPastGenesisWithoutCoverage() {
    when(blockchain.getChainHeadBlockNumber()).thenReturn(5L);

    assertThatThrownBy(() -> createProvider(true, true))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("no proof history");
  }

  @Test
  void startupFailsWhenGenesisWasPersistedWithoutProofs() {
    when(blockchain.getChainHeadBlockNumber()).thenReturn(0L);

    assertThatThrownBy(() -> createProvider(true, true))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("no proof history");
  }

  @Test
  void startupSucceedsOnFreshChain() {
    when(blockchain.getChainHeadBlockNumber()).thenReturn(0L);
    seedWorldStateBlockHash = false;

    createProvider(true, true).close();
  }

  @Test
  void startupSucceedsWithCoverageFromGenesis() {
    when(blockchain.getChainHeadBlockNumber()).thenReturn(5L);
    seededProgress = 4L;

    createProvider(true, true).close();
  }

  @Test
  void startupIgnoresCoverageWhenProofsDisabled() {
    when(blockchain.getChainHeadBlockNumber()).thenReturn(5L);

    createProvider(true, false);
  }

  private BonsaiArchiveWorldStateProvider createProvider(final boolean archiveModeReady) {
    return createProvider(archiveModeReady, false);
  }

  private BonsaiArchiveWorldStateProvider createProvider(
      final boolean archiveModeReady, final boolean stateProofsEnabled) {
    final var config =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(DataStorageFormat.X_BONSAI_ARCHIVE)
            .extraStorageConfiguration(
                ImmutableExtraStorageConfiguration.builder()
                    .maxLayersToLoad(MAX_LAYERS)
                    .unstable(
                        ImmutableExtraStorageConfiguration.Unstable.builder()
                            .bonsaiArchiveStateProofsEnabled(stateProofsEnabled)
                            .build())
                    .build())
            .build();
    worldStateStorage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(), new NoOpMetricsSystem(), config);
    if (archiveModeReady) {
      worldStateStorage.upgradeToArchiveFlatDbMode();
    }

    // Seed the chain head block hash so the head world state is cached under it,
    // allowing non-historical queries to find it via worldStateCacheManager.
    final var tx = worldStateStorage.getComposedWorldStateStorage().startTransaction();
    if (seedWorldStateBlockHash) {
      tx.put(
          TRIE_BRANCH_STORAGE,
          WORLD_BLOCK_HASH_KEY,
          chainHeadHeader.getHash().getBytes().toArrayUnsafe());
    }
    if (seededProgress != null) {
      new ArchiveNodeHistoryStore(worldStateStorage.getComposedWorldStateStorage())
          .saveArchiveProgress(tx, new ArchivedBlock(seededProgress, Hash.ZERO));
    }
    tx.commit();

    return new BonsaiArchiveWorldStateProvider(
        worldStateStorage,
        blockchain,
        config,
        null,
        null,
        EvmConfiguration.DEFAULT,
        new BonsaiCodeCache(),
        new NoOpMetricsSystem(),
        Optional.empty(),
        () -> Executors.newFixedThreadPool(2),
        instantFinality,
        networkHead);
  }
}
