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
import static org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig.createStatefulConfigWithTrie;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.DefaultBlockchain;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.proof.WorldStateProof;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStoragePrefixedKeyBlockchainStorage;
import org.hyperledger.besu.ethereum.storage.keyvalue.VariablesKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveFlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsaiarchive.BonsaiFlatDbToArchiveMigrator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeHashCodeStorageStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end test for archive state proofs that require rolling a historical world state back
 * across multiple blocks, including storage-trie reconstruction from archived checkpoint nodes.
 *
 * <p>Reproduces the live-node scenario: a chain is produced with an account whose storage changes
 * every block, the Bonsai-archive migration persists a trie checkpoint, then a proof is requested
 * for a block before the checkpoint which forces {@code rollArchiveProofWorldStateToBlockHash} to
 * roll the checkpoint world state back and rebuild the account's storage trie from archived nodes.
 */
class BonsaiArchiveStorageProofIntegrationTest {

  private static final Address ACCOUNT =
      Address.fromHexString("0x1111111111111111111111111111111111111111");
  // Created at block 1, untouched until block 6, then modified every block from 6 onwards. Its
  // account-trie leaf is therefore absent from a 4..5 roll-forward diff but rewritten (at the
  // next window's suffix) by the block-7 checkpoint, and different again at HEAD.
  private static final Address STABLE_THEN_CHURNING_ACCOUNT =
      Address.fromHexString("0x2222222222222222222222222222222222222222");
  private static final Wei STABLE_BALANCE = Wei.of(500L);
  private static final long INTERVAL = 4L; // trie checkpoint at blocks 3, 7, ...
  private static final long ARCHIVE_BOUNDARY = 4L; // small so early blocks are "historical"
  private static final int NUM_BLOCKS = 10;
  private static final long TARGET_BLOCK = 2L; // before first checkpoint (block 3)

  private BonsaiFlatDbToArchiveMigrator migrator;
  private MutableBlockchain blockchain;

  @AfterEach
  void tearDown() {
    if (migrator != null) {
      migrator.close();
    }
  }

  @Test
  void historicalStorageProof_rollsBackAcrossCheckpoint_withoutMissingTrieNode() throws Exception {
    final BonsaiArchiveWorldStateProvider archiveProvider = buildMigratedArchive();
    final BlockHeader targetHeader = blockchain.getBlockHeader(TARGET_BLOCK).orElseThrow();

    final Optional<WorldStateProof> proof =
        archiveProvider.getAccountProof(
            targetHeader, ACCOUNT, List.of(UInt256.ONE), Function.identity());

    assertThat(proof)
        .withFailMessage("archive proof should be available for historical block %d", TARGET_BLOCK)
        .isPresent();
    // Slot 1 was set to the block number, so at block 2 it must prove value 2.
    assertThat(proof.get().getStorageProof(UInt256.ONE)).isNotEmpty();
    assertThat(proof.get().getStorageValue(UInt256.ONE)).isEqualTo(UInt256.valueOf(TARGET_BLOCK));
  }

  @Test
  void historicalStorageProof_rollsForwardFromFloorCheckpoint_acrossWindowBoundary()
      throws Exception {
    final BonsaiArchiveWorldStateProvider archiveProvider = buildMigratedArchive();
    // Block 5 sits in the window after the floor checkpoint (block 3, archived at suffix 0) and
    // equidistant from the ceiling checkpoint (block 7, archived at suffix 4), so the proof world
    // state rolls FORWARD from checkpoint 3. Trie-node reads must stay pinned to checkpoint 3's
    // window: a read context of the target block number (5) would resolve to checkpoint 7's
    // archived nodes (suffix 4), shadowing checkpoint 3's trie and failing the proof traversal
    // with "Unable to load trie node value".
    final long target = 5L;
    final BlockHeader targetHeader = blockchain.getBlockHeader(target).orElseThrow();

    final Optional<WorldStateProof> proof =
        archiveProvider.getAccountProof(
            targetHeader, ACCOUNT, List.of(UInt256.ONE), Function.identity());

    assertThat(proof)
        .withFailMessage("archive proof should be available for historical block %d", target)
        .isPresent();
    // Slot 1 was set to the block number, so at block 5 it must prove value 5.
    assertThat(proof.get().getStorageProof(UInt256.ONE)).isNotEmpty();
    assertThat(proof.get().getStorageValue(UInt256.ONE)).isEqualTo(UInt256.valueOf(target));

    // The stable account's trie leaf is not part of the 4..5 roll-forward diff, so its proof must
    // be served from checkpoint 3's archived nodes (suffix 0). Checkpoint 7 rewrote the same
    // location at suffix 4 with a different balance, and HEAD differs again — both would fail the
    // node-hash check if the read context were not pinned to the rolled-from checkpoint's window.
    final Optional<WorldStateProof> stableProof =
        archiveProvider.getAccountProof(
            targetHeader, STABLE_THEN_CHURNING_ACCOUNT, List.of(), Function.identity());

    assertThat(stableProof)
        .withFailMessage(
            "archive proof for an account outside the roll-forward diff should be available")
        .isPresent();
    assertThat(stableProof.get().getStateTrieAccountValue())
        .isPresent()
        .hasValueSatisfying(v -> assertThat(v.getBalance()).isEqualByComparingTo(STABLE_BALANCE));
  }

  @Test
  void historicalStorageProof_absentSlot_returnsValidExclusionProof() throws Exception {
    final BonsaiArchiveWorldStateProvider archiveProvider = buildMigratedArchive();
    final BlockHeader targetHeader = blockchain.getBlockHeader(TARGET_BLOCK).orElseThrow();

    // Slot 0 is never written by applyBlockChanges (only slots 1 and 100+i), so at the historical
    // block it is absent. The archive proof must still be present and prove value zero with a
    // non-empty exclusion proof witness reconstructed from the rolled-back storage trie.
    final UInt256 absentSlot = UInt256.ZERO;
    final Optional<WorldStateProof> proof =
        archiveProvider.getAccountProof(
            targetHeader, ACCOUNT, List.of(absentSlot), Function.identity());

    assertThat(proof)
        .withFailMessage("archive exclusion proof should be available for historical block")
        .isPresent();
    assertThat(proof.get().getStorageValue(absentSlot)).isEqualTo(UInt256.ZERO);
    // A valid exclusion proof is a non-empty Merkle witness (the divergent leaf / branch path).
    assertThat(proof.get().getStorageProof(absentSlot)).isNotEmpty();
  }

  private BonsaiArchiveWorldStateProvider buildMigratedArchive() throws Exception {
    final BlockDataGenerator blockGen = new BlockDataGenerator();
    final Block genesis = blockGen.genesisBlock();
    blockchain =
        DefaultBlockchain.createMutable(
            genesis,
            new KeyValueStoragePrefixedKeyBlockchainStorage(
                new org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage(),
                new VariablesKeyValueStorage(
                    new org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage()),
                new MainnetBlockHeaderFunctions(),
                false),
            new NoOpMetricsSystem(),
            0);

    // The archive node's shared storage: holds HEAD trie/flat data, trie logs, and (after
    // migration) the archive column families. The head provider is built directly over this
    // provider so its trie-log manager reads/writes the same storage the migrator/archive
    // provider use (saveTrieLog persists to the world state's storage).
    final InMemoryKeyValueStorageProvider sharedProvider = new InMemoryKeyValueStorageProvider();
    final BonsaiWorldStateKeyValueStorage headStorage =
        (BonsaiWorldStateKeyValueStorage)
            sharedProvider.createWorldStateStorage(DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
    final BonsaiWorldStateProvider headArchive =
        new BonsaiWorldStateProvider(
            headStorage,
            blockchain,
            DataStorageConfiguration.DEFAULT_BONSAI_CONFIG.getPathBasedExtraStorageConfiguration(),
            new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem()),
            null,
            EvmConfiguration.DEFAULT,
            () -> null,
            new CodeCache());

    // Parallel throwaway state purely to compute the correct state root of each block.
    final InMemoryKeyValueStorageProvider rootProvider = new InMemoryKeyValueStorageProvider();
    final BonsaiWorldStateKeyValueStorage rootStorage =
        (BonsaiWorldStateKeyValueStorage)
            rootProvider.createWorldStateStorage(DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
    final BonsaiWorldStateProvider rootArchive =
        new BonsaiWorldStateProvider(
            rootStorage,
            blockchain,
            DataStorageConfiguration.DEFAULT_BONSAI_CONFIG.getPathBasedExtraStorageConfiguration(),
            new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem()),
            null,
            EvmConfiguration.DEFAULT,
            () -> null,
            new CodeCache());

    final BonsaiWorldState headState =
        new BonsaiWorldState(
            headArchive,
            headStorage,
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new CodeCache());
    final BonsaiWorldState rootState =
        new BonsaiWorldState(
            rootArchive,
            rootStorage,
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new CodeCache());

    BlockHeader parent = genesis.getHeader();
    for (int i = 1; i <= NUM_BLOCKS; i++) {
      // Apply identical changes to both states.
      applyBlockChanges(rootState, i);
      // Compute the root by persisting the throwaway state with no header (no verification).
      rootState.persist(null);
      final Hash rootHash = rootState.rootHash();

      final BlockHeader header =
          new BlockHeaderTestFixture()
              .number(i)
              .parentHash(parent.getHash())
              .stateRoot(rootHash)
              .buildHeader();

      applyBlockChanges(headState, i);
      headState.persist(header); // verifies root matches + saves a real trie log
      blockchain.appendBlock(new Block(header, BlockBody.empty()), List.of());
      parent = header;
    }

    // ---- Run the real migrator into the shared storage's archive column families.
    final ImmutableDataStorageConfiguration archiveConfig =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(DataStorageFormat.X_BONSAI_ARCHIVE)
            .pathBasedExtraStorageConfiguration(
                ImmutablePathBasedExtraStorageConfiguration.builder()
                    .maxLayersToLoad(ARCHIVE_BOUNDARY)
                    .unstable(
                        ImmutablePathBasedExtraStorageConfiguration.PathBasedUnstable.builder()
                            .stateProofsEnabled(true)
                            .archiveTrieNodeCheckpointInterval(INTERVAL)
                            .build())
                    .build())
            .build();

    final BonsaiWorldStateKeyValueStorage archiveStorage =
        (BonsaiWorldStateKeyValueStorage) sharedProvider.createWorldStateStorage(archiveConfig);

    // Migrator trie-log manager: boundary disabled (archive every block) + real logs from HEAD.
    final TrieLogManager headTrieLogManager = headArchive.getTrieLogManager();
    final TrieLogManager migratorTrieLogManager = mock(TrieLogManager.class);
    when(migratorTrieLogManager.getMaxLayersToLoad()).thenReturn(0L);
    when(migratorTrieLogManager.getTrieLogLayer(any()))
        .thenAnswer(inv -> headTrieLogManager.getTrieLogLayer(inv.getArgument(0)));

    final BonsaiArchiveFlatDbStrategy archiveFlatStrategy =
        new BonsaiArchiveFlatDbStrategy(
            new NoOpMetricsSystem(), new CodeHashCodeStorageStrategy(), INTERVAL);

    migrator =
        new BonsaiFlatDbToArchiveMigrator(
            archiveStorage,
            migratorTrieLogManager,
            blockchain,
            Executors.newScheduledThreadPool(1),
            new NoOpMetricsSystem(),
            archiveFlatStrategy);
    migrator.migrate().get(30, TimeUnit.SECONDS);

    assertThat(migrator.getMigratedBlockNumber()).isEqualTo(NUM_BLOCKS);

    // ---- Request a proof for an early block before the first checkpoint.
    final BonsaiArchiveWorldStateProvider archiveProvider =
        new BonsaiArchiveWorldStateProvider(
            archiveStorage,
            blockchain,
            archiveConfig,
            new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem()),
            null,
            EvmConfiguration.DEFAULT,
            () -> null,
            new CodeCache(),
            new NoOpMetricsSystem());
    archiveProvider.setArchiveMigrationProgressSupplier(() -> (long) NUM_BLOCKS);
    return archiveProvider;
  }

  private void applyBlockChanges(final BonsaiWorldState state, final int blockNumber) {
    final WorldUpdater updater = state.updater();
    if (blockNumber == 1) {
      updater.createAccount(ACCOUNT, 0, Wei.of(1_000_000L));
      updater.createAccount(STABLE_THEN_CHURNING_ACCOUNT, 0, STABLE_BALANCE);
    }
    final MutableAccount account = updater.getAccount(ACCOUNT);
    account.setStorageValue(UInt256.ONE, UInt256.valueOf(blockNumber));
    // Touch a rotating slot so the storage trie changes shape across the checkpoint window.
    account.setStorageValue(UInt256.valueOf(100 + blockNumber), UInt256.valueOf(blockNumber * 7L));
    if (blockNumber >= 6) {
      updater
          .getAccount(STABLE_THEN_CHURNING_ACCOUNT)
          .setBalance(STABLE_BALANCE.add(Wei.of(blockNumber)));
    }
    updater.commit();
  }
}
