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
package org.hyperledger.besu.ethereum.mainnet.staterootcommitter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.core.WorldStateHealerHelper.throwingWorldStateHealerSupplier;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig.createStatefulConfigWithTrie;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.GenesisState;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.mainnet.ImmutableBalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleBuilder;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpecAdapters;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BalanceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.CodeChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.NonceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.StorageChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListAddressView;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.storage.keyvalue.WorldStatePreimageKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.forest.worldview.ForestMutableWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogFactoryImpl;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.math.BigInteger;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@link StateRootCommitter} across Forest, BAL, and Default (sync) modes.
 *
 * <p>Verifies that identical block transitions produce the same state root regardless of committer,
 * and that Bonsai persistence semantics (flat DB, trie log, frozen vs writable) hold.
 */
class StateRootCommitterIntegrationTest {

  private static final Duration BAL_TIMEOUT = Duration.ofSeconds(5);

  private static ImmutableBalConfiguration balConfiguration() {
    return ImmutableBalConfiguration.builder()
        .balStateRootTimeout(BAL_TIMEOUT)
        .isBalStateRootEnabled(true)
        .build();
  }

  private static final Address CONTRACT =
      Address.fromHexString("0x00000000000000000000000000000000c0de");
  private static final Address EOA =
      Address.fromHexString("0x00000000000000000000000000000000000000e0");
  private static final Bytes CONTRACT_CODE = Bytes.fromHexString("0x608060405234801561001057600080fd5b50");
  private static final StorageSlotKey SLOT = new StorageSlotKey(UInt256.valueOf(7));
  private static final UInt256 SLOT_VALUE = UInt256.valueOf(0xABCDL);

  private ExecutionContextTestFixture bonsaiFixture;
  private ExecutionContextTestFixture forestFixture;

  @BeforeEach
  void setUp() {
    bonsaiFixture =
        ExecutionContextTestFixture.builder(GenesisConfig.mainnet())
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .build();
    forestFixture = ExecutionContextTestFixture.builder(GenesisConfig.mainnet()).build();
  }

  @AfterEach
  void tearDown() throws Exception {
    bonsaiFixture.getStateArchive().close();
    forestFixture.getStateArchive().close();
  }

  @Nested
  class CrossModeRootEquivalence {

    @Test
    void defaultBalAndForestProduceSameRoot_complexBlock() {
      final BlockChange blockChange = BlockChange.complex();

      final Hash defaultRoot =
          EmptyCrossModeHarness.create().persistBonsaiWithDefault(blockChange);
      final Hash balRoot = EmptyCrossModeHarness.create().persistBonsaiWithBal(blockChange);
      final Hash forestRoot = EmptyCrossModeHarness.create().persistForest(blockChange);

      assertThat(balRoot).isEqualTo(defaultRoot);
      assertThat(forestRoot).isEqualTo(defaultRoot);
    }

    @Test
    void defaultBalAndForestProduceSameRoot_balanceAndNonce() {
      final BlockChange blockChange = BlockChange.balanceAndNonce(EOA, Wei.of(10_000), 5L);

      final Hash defaultRoot =
          EmptyCrossModeHarness.create().persistBonsaiWithDefault(blockChange);
      final Hash balRoot = EmptyCrossModeHarness.create().persistBonsaiWithBal(blockChange);
      final Hash forestRoot = EmptyCrossModeHarness.create().persistForest(blockChange);

      assertThat(balRoot).isEqualTo(defaultRoot);
      assertThat(forestRoot).isEqualTo(defaultRoot);
    }

    @Test
    void defaultAndBalProduceSameRoot_balanceAndNonceOnly() {
      final BlockChange blockChange = BlockChange.balanceAndNonce(EOA, Wei.of(42_000), 3L);

      final Hash defaultRoot =
          EmptyCrossModeHarness.create().persistBonsaiWithDefault(blockChange);
      final Hash balRoot = EmptyCrossModeHarness.create().persistBonsaiWithBal(blockChange);

      assertThat(balRoot).isEqualTo(defaultRoot);
    }

    @Test
    void defaultAndBalProduceSameRoot_codeAndStorage_forestMatches() {
      final BlockChange blockChange =
          BlockChange.codeAndStorage(CONTRACT, CONTRACT_CODE, SLOT, SLOT_VALUE);

      final Hash defaultRoot =
          EmptyCrossModeHarness.create().persistBonsaiWithDefault(blockChange);
      final Hash balRoot = EmptyCrossModeHarness.create().persistBonsaiWithBal(blockChange);
      final Hash forestRoot = EmptyCrossModeHarness.create().persistForest(blockChange);

      assertThat(balRoot).isEqualTo(defaultRoot);
      assertThat(forestRoot).isEqualTo(defaultRoot);
    }

    @Test
    void defaultAndBalProduceSameRoot_accountDeletion() {
      final BlockChange create = BlockChange.balanceAndNonce(CONTRACT, Wei.of(1), 1L);
      final BlockChange delete = BlockChange.deleteAccount(CONTRACT);

      final EmptyCrossModeHarness defaultHarness = EmptyCrossModeHarness.create();
      defaultHarness.persistBonsaiWithDefault(create);
      final Hash defaultRoot = defaultHarness.persistBonsaiWithDefault(delete);

      final EmptyCrossModeHarness balHarness = EmptyCrossModeHarness.create();
      balHarness.persistBonsaiWithBal(create);
      final Hash balRoot = balHarness.persistBonsaiWithBal(delete);

      final EmptyCrossModeHarness forestHarness = EmptyCrossModeHarness.create();
      forestHarness.persistForest(create);
      final Hash forestRoot = forestHarness.persistForest(delete);

      assertThat(balRoot).isEqualTo(defaultRoot);
      assertThat(forestRoot).isEqualTo(defaultRoot);
    }

    @Test
    void defaultAndBalProduceSameRoot_storageZeroClearsSlot() {
      final BlockChange create =
          BlockChange.codeAndStorage(CONTRACT, CONTRACT_CODE, SLOT, SLOT_VALUE);
      final BlockChange clearSlot =
          BlockChange.codeAndStorage(CONTRACT, CONTRACT_CODE, SLOT, UInt256.ZERO);

      final EmptyCrossModeHarness defaultHarness = EmptyCrossModeHarness.create();
      defaultHarness.persistBonsaiWithDefault(create);
      final Hash defaultRoot = defaultHarness.persistBonsaiWithDefault(clearSlot);

      final EmptyCrossModeHarness balHarness = EmptyCrossModeHarness.create();
      balHarness.persistBonsaiWithBal(create);
      final Hash balRoot = balHarness.persistBonsaiWithBal(clearSlot);

      final EmptyCrossModeHarness forestHarness = EmptyCrossModeHarness.create();
      forestHarness.persistForest(create);
      final Hash forestRoot = forestHarness.persistForest(clearSlot);

      assertThat(balRoot).isEqualTo(defaultRoot);
      assertThat(forestRoot).isEqualTo(defaultRoot);
    }

    @Test
    void dryRunComputeMatchesPersistedRoot() {
      final EmptyCrossModeHarness harness = EmptyCrossModeHarness.create();
      final BlockChange blockChange = BlockChange.complex();
      final Hash expectedRoot = harness.computeRootWithoutPersist(blockChange);
      final Hash persistedRoot = harness.persistBonsaiWithPrecomputedRoot(expectedRoot);
      assertThat(persistedRoot).isEqualTo(expectedRoot);
    }

    @Test
    void accountDeletion_restoresGenesisRoot_afterCreateAndDelete() {
      final BlockChange create = BlockChange.balanceAndNonce(CONTRACT, Wei.of(1), 1L);
      final BlockChange delete = BlockChange.deleteAccount(CONTRACT);
      final Hash genesisRoot = EmptyCrossModeHarness.create().forestGenesisRoot();

      final EmptyCrossModeHarness forestHarness = EmptyCrossModeHarness.create();
      final Hash afterCreate = forestHarness.persistForest(create);
      assertThat(afterCreate).isNotEqualTo(genesisRoot);

      final Hash afterDelete = forestHarness.persistForest(delete);
      assertThat(afterDelete).isEqualTo(genesisRoot);
    }

    @Test
    void multiBlock_defaultBalAndForestStayAligned() {
      final BlockChange block1 = BlockChange.balanceAndNonce(EOA, Wei.of(1_000), 1L);
      final BlockChange block2 =
          BlockChange.codeAndStorage(CONTRACT, CONTRACT_CODE, SLOT, SLOT_VALUE);
      final BlockChange block3 = BlockChange.deleteAccount(EOA);

      final EmptyCrossModeHarness defaultHarness = EmptyCrossModeHarness.create();
      defaultHarness.persistBonsaiWithDefault(block1);
      defaultHarness.persistBonsaiWithDefault(block2);
      final Hash defaultRoot = defaultHarness.persistBonsaiWithDefault(block3);

      final EmptyCrossModeHarness balHarness = EmptyCrossModeHarness.create();
      balHarness.persistBonsaiWithBal(block1);
      balHarness.persistBonsaiWithBal(block2);
      final Hash balRoot = balHarness.persistBonsaiWithBal(block3);

      final EmptyCrossModeHarness forestHarness = EmptyCrossModeHarness.create();
      forestHarness.persistForest(block1);
      forestHarness.persistForest(block2);
      final Hash forestRoot = forestHarness.persistForest(block3);

      assertThat(balRoot).isEqualTo(defaultRoot);
      assertThat(forestRoot).isEqualTo(defaultRoot);
    }

    @Test
    void frozenAndNonFrozen_defaultCommitter_sameRoot() {
      final BlockChange blockChange = BlockChange.complex();

      final BonsaiKvHarness nonFrozenHarness = BonsaiKvHarness.create();
      final BlockHeader parent = nonFrozenHarness.persistParent();
      final BlockHeader blockHeader = nonFrozenHarness.childHeader(parent, blockChange);

      final Hash nonFrozenRoot;
      try (BonsaiWorldState worldState = nonFrozenHarness.newWritableWorldState()) {
        blockChange.apply(worldState.updater());
        worldState.updater().commit();
        worldState.persist(blockHeader, new DefaultStateRootCommitter());
        nonFrozenRoot = worldState.rootHash();
      }

      final BonsaiKvHarness frozenHarness = BonsaiKvHarness.create();
      final BlockHeader frozenParent = frozenHarness.persistParent();
      final BlockHeader frozenHeader = frozenHarness.childHeader(frozenParent, blockChange);

      final Hash frozenRoot;
      try (BonsaiWorldState worldState = frozenHarness.newWritableWorldState()) {
        blockChange.apply(worldState.updater());
        worldState.updater().commit();
        worldState.freezeStorage();
        worldState.persist(frozenHeader, new DefaultStateRootCommitter());
        frozenRoot = worldState.rootHash();
      }

      assertThat(frozenRoot).isEqualTo(nonFrozenRoot);
    }

    @Test
    void stateRootCommitterFactory_selectsExpectedCommitter() {
      final BlockChange blockChange = BlockChange.complex();
      final BonsaiKvHarness harness = BonsaiKvHarness.create();
      final BlockHeader parent = harness.persistParent();
      final BlockHeader blockHeader = harness.childHeader(parent, blockChange);
      final StateRootCommitterFactory factory = new StateRootCommitterFactory(balConfiguration());

      assertThat(
              factory.forBlock(
                  harness.protocolContext(), blockHeader, Optional.empty(), false))
          .isInstanceOf(DefaultStateRootCommitter.class);
      assertThat(
              factory.forBlock(
                  harness.protocolContext(),
                  blockHeader,
                  Optional.of(blockChange.toBal()),
                  false))
          .isInstanceOf(BalStateRootCommitter.class);
    }
  }

  @Nested
  class BonsaiStorageSemantics {

    private BonsaiKvHarness harness;

    @BeforeEach
    void setUpHarness() {
      harness = BonsaiKvHarness.create();
    }

    @Test
    void nonFrozenPersist_writesFlatDbAndTrieLog() {
      final BlockChange blockChange = BlockChange.complex();
      final BlockHeader parent = harness.persistParent();
      final BlockHeader blockHeader = harness.childHeader(parent, blockChange);

      try (BonsaiWorldState worldState = harness.newWritableWorldState()) {
        blockChange.apply(worldState.updater());
        worldState.updater().commit();
        worldState.persist(blockHeader, new DefaultStateRootCommitter());
      }

      assertThat(harness.trieLogExists(blockHeader)).isTrue();

      try (BonsaiWorldState worldState = harness.newWritableWorldState()) {
        assertThat(worldState.get(CONTRACT)).isNotNull();
        assertThat(worldState.get(EOA)).isNotNull();
        assertThat(worldState.get(CONTRACT).getCode()).isEqualTo(CONTRACT_CODE);
        assertThat(worldState.get(CONTRACT).getStorageValue(SLOT.getSlotKey().orElseThrow()))
            .isEqualTo(SLOT_VALUE);
      }
    }

    @Test
    void frozenPersist_writesTrieLogWithoutFlatAccountData() {
      final BlockChange blockChange = BlockChange.complex();
      final BlockHeader parent = harness.persistParent();
      final BlockHeader blockHeader = harness.childHeader(parent, blockChange);

      final int accountKeysBefore = harness.accountFlatDbKeyCount();
      final int codeKeysBefore = harness.codeFlatDbKeyCount();
      final int storageKeysBefore = harness.storageFlatDbKeyCount();

      try (BonsaiWorldState worldState = harness.newWritableWorldState()) {
        blockChange.apply(worldState.updater());
        worldState.updater().commit();
        worldState.freezeStorage();
        worldState.persist(blockHeader, new DefaultStateRootCommitter());
      }

      assertThat(harness.trieLogExists(blockHeader)).isTrue();
      assertThat(harness.accountFlatDbKeyCount()).isEqualTo(accountKeysBefore);
      assertThat(harness.codeFlatDbKeyCount()).isEqualTo(codeKeysBefore);
      assertThat(harness.storageFlatDbKeyCount()).isEqualTo(storageKeysBefore);
      assertThat(harness.accountExistsInFlatDb(CONTRACT)).isFalse();
    }

    @Test
    void frozenTrieLogRollForward_matchesNonFrozenFlatDatabase() {
      final BlockChange blockChange = BlockChange.complex();
      final BlockHeader parent = harness.persistParent();
      final BlockHeader blockHeader = harness.childHeader(parent, blockChange);

      final KvSnapshot referenceSnapshot;
      try (BonsaiWorldState worldState = harness.newWritableWorldState()) {
        blockChange.apply(worldState.updater());
        worldState.updater().commit();
        worldState.persist(blockHeader, new DefaultStateRootCommitter());
        referenceSnapshot = harness.captureFlatDbSnapshot();
      }

      final BonsaiKvHarness frozenHarness = BonsaiKvHarness.create();
      final BlockHeader frozenParent = frozenHarness.persistParent();
      final BlockHeader frozenBlockHeader = frozenHarness.childHeader(frozenParent, blockChange);

      try (BonsaiWorldState frozenWorldState = frozenHarness.newWritableWorldState()) {
        blockChange.apply(frozenWorldState.updater());
        frozenWorldState.updater().commit();
        frozenWorldState.freezeStorage();
        frozenWorldState.persist(frozenBlockHeader, new DefaultStateRootCommitter());
      }

      assertThat(frozenHarness.trieLogExists(frozenBlockHeader)).isTrue();
      assertThat(frozenHarness.accountExistsInFlatDb(CONTRACT)).isFalse();

      final BonsaiKvHarness replayHarness = BonsaiKvHarness.create();
      replayHarness.persistParent();

      try (BonsaiWorldState replayWorldState = replayHarness.newWritableWorldState()) {
        final TrieLogLayer trieLog = frozenHarness.readTrieLog(frozenBlockHeader);
        final BonsaiWorldStateUpdateAccumulator accumulator =
            (BonsaiWorldStateUpdateAccumulator) replayWorldState.updater();
        accumulator.rollForward(trieLog);
        accumulator.commit();
        replayWorldState.persist(null);
      }

      replayHarness.assertFlatDbMatches(referenceSnapshot);
    }

    @Test
    void balFrozenPersist_computesRootWithoutFlatDbWrites() {
      final BlockChange blockChange = BlockChange.complex();
      final BlockHeader parent = harness.persistParent();
      final BlockHeader blockHeader = harness.childHeader(parent, blockChange);
      final BlockAccessList bal = blockChange.toBal();

      final int accountKeysBefore = harness.accountFlatDbKeyCount();

      try (BonsaiWorldState worldState = harness.newWritableWorldState()) {
        blockChange.apply(worldState.updater());
        worldState.updater().commit();
        worldState.freezeStorage();
        final BalStateRootCommitter committer =
            new BalStateRootCommitter(
                harness.protocolContext(),
                blockHeader,
                BlockAccessListAddressView.of(bal),
                true);
        worldState.persist(blockHeader, committer);
      }

      assertThat(harness.trieLogExists(blockHeader)).isTrue();
      assertThat(harness.accountFlatDbKeyCount()).isEqualTo(accountKeysBefore);
    }

    @Test
    void balNonFrozenPersist_matchesDefaultFlatDatabase() {
      final BlockChange blockChange = BlockChange.complex();
      final BlockHeader parent = harness.persistParent();
      final BlockHeader blockHeader = harness.childHeader(parent, blockChange);
      final BlockAccessList bal = blockChange.toBal();

      final KvSnapshot defaultSnapshot;
      try (BonsaiWorldState worldState = harness.newWritableWorldState()) {
        blockChange.apply(worldState.updater());
        worldState.updater().commit();
        worldState.persist(blockHeader, new DefaultStateRootCommitter());
        defaultSnapshot = harness.captureFlatDbSnapshot();
      }

      final BonsaiKvHarness balHarness = BonsaiKvHarness.create();
      final BlockHeader balParent = balHarness.persistParent();
      final BlockHeader balBlockHeader = balHarness.childHeader(balParent, blockChange);
      final StateRootCommitterFactory factory = new StateRootCommitterFactory(balConfiguration());

      try (BonsaiWorldState worldState = balHarness.newWritableWorldState()) {
        blockChange.apply(worldState.updater());
        worldState.updater().commit();
        final StateRootCommitter committer =
            factory.forBlock(
                balHarness.protocolContext(),
                balBlockHeader,
                Optional.of(bal),
                worldState.isStorageFrozen());
        worldState.persist(balBlockHeader, committer);
      }

      balHarness.assertFlatDbMatches(defaultSnapshot);
    }
  }

  @Nested
  class ForestStorageSemantics {

    @Test
    void forestPersist_writesWorldStateTrieNodes() {
      final InMemoryKeyValueStorageProvider provider = new InMemoryKeyValueStorageProvider();
      final ForestMutableWorldState worldState =
          new ForestMutableWorldState(
              provider.createWorldStateStorage(DataStorageConfiguration.DEFAULT_FOREST_CONFIG),
              new WorldStatePreimageKeyValueStorage(new InMemoryKeyValueStorage()),
              EvmConfiguration.DEFAULT);

      final int keysBefore =
          provider
              .getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.WORLD_STATE)
              .getAllKeysThat(k -> true)
              .size();

      final WorldUpdater updater = worldState.updater();
      final MutableAccount account = updater.createAccount(CONTRACT);
      account.setBalance(Wei.of(1_000));
      account.setCode(CONTRACT_CODE);
      account.setStorageValue(SLOT.getSlotKey().orElseThrow(), SLOT_VALUE);
      updater.commit();

      final Hash rootBeforePersist = worldState.rootHash();
      worldState.persist(null, ForestStateRootCommitter.INSTANCE);

      final int keysAfter =
          provider
              .getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.WORLD_STATE)
              .getAllKeysThat(k -> true)
              .size();

      assertThat(keysAfter).isGreaterThan(keysBefore);
      assertThat(worldState.get(CONTRACT)).isNotNull();
      assertThat(worldState.get(CONTRACT).getCode()).isEqualTo(CONTRACT_CODE);
      assertThat(worldState.get(CONTRACT).getStorageValue(SLOT.getSlotKey().orElseThrow()))
          .isEqualTo(SLOT_VALUE);
      assertThat(worldState.rootHash()).isEqualTo(rootBeforePersist);
    }
  }

  // ---------------------------------------------------------------------------
  // Empty-state cross-mode harness (isolated Bonsai + Forest + BAL)
  // ---------------------------------------------------------------------------

  private static final class EmptyCrossModeHarness {

    private final BonsaiWorldState bonsaiWorldState;
    private final ForestMutableWorldState forestWorldState;
    private final ProtocolContext protocolContext;
    private BlockHeader chainHead;

    private EmptyCrossModeHarness(
        final BonsaiWorldState bonsaiWorldState,
        final ForestMutableWorldState forestWorldState,
        final ProtocolContext protocolContext,
        final BlockHeader chainHead) {
      this.bonsaiWorldState = bonsaiWorldState;
      this.forestWorldState = forestWorldState;
      this.protocolContext = protocolContext;
      this.chainHead = chainHead;
    }

    static EmptyCrossModeHarness create() {
      final GenesisState genesisState =
          GenesisState.fromConfig(
              GenesisConfig.mainnet(),
              new ProtocolScheduleBuilder(
                      GenesisConfig.mainnet().getConfigOptions(),
                      Optional.of(BigInteger.valueOf(42)),
                      ProtocolSpecAdapters.create(0, Function.identity()),
                      false,
                      EvmConfiguration.DEFAULT,
                      MiningConfiguration.MINING_DISABLED,
                      new BadBlockManager(),
                      false,
                      ImmutableBalConfiguration.builder().build(),
                      new NoOpMetricsSystem())
                  .createProtocolSchedule(),
              new CodeCache());
      final MutableBlockchain blockchain =
          InMemoryKeyValueStorageProvider.createInMemoryBlockchain(genesisState.getBlock());

      final InMemoryKeyValueStorageProvider bonsaiProvider = new InMemoryKeyValueStorageProvider();
      final BonsaiWorldStateKeyValueStorage bonsaiKv =
          new BonsaiWorldStateKeyValueStorage(
              bonsaiProvider, new NoOpMetricsSystem(), DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
      final BonsaiWorldStateProvider bonsaiArchive =
          new BonsaiWorldStateProvider(
              bonsaiKv,
              blockchain,
              DataStorageConfiguration.DEFAULT_BONSAI_CONFIG.getPathBasedExtraStorageConfiguration(),
              new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem()),
              null,
              EvmConfiguration.DEFAULT,
              throwingWorldStateHealerSupplier(),
              new CodeCache());
      genesisState.writeStateTo(bonsaiArchive.getWorldState());

      final BonsaiWorldState bonsaiWorldState =
          new BonsaiWorldState(
              bonsaiArchive,
              bonsaiKv,
              EvmConfiguration.DEFAULT,
              createStatefulConfigWithTrie(),
              new CodeCache());

      final InMemoryKeyValueStorageProvider forestProvider = new InMemoryKeyValueStorageProvider();
      final ForestMutableWorldState forestWorldState =
          new ForestMutableWorldState(
              forestProvider.createWorldStateStorage(DataStorageConfiguration.DEFAULT_FOREST_CONFIG),
              forestProvider.createWorldStatePreimageStorage(),
              EvmConfiguration.DEFAULT);
      genesisState.writeStateTo(forestWorldState);

      final ProtocolContext protocolContext =
          new ProtocolContext.Builder()
              .withBlockchain(blockchain)
              .withWorldStateArchive(bonsaiArchive)
              .build();

      return new EmptyCrossModeHarness(
          bonsaiWorldState, forestWorldState, protocolContext, genesisState.getBlock().getHeader());
    }

    Hash persistBonsaiWithDefault(final BlockChange blockChange) {
      blockChange.apply(bonsaiWorldState.updater());
      bonsaiWorldState.updater().commit();
      final Hash root =
          new DefaultStateRootCommitter()
              .compute(bonsaiWorldState, null, bonsaiWorldState.updater())
              .root();
      final BlockHeader blockHeader = childHeader(root);
      bonsaiWorldState.persist(blockHeader, new DefaultStateRootCommitter());
      appendBlock(blockHeader);
      return bonsaiWorldState.rootHash();
    }

    Hash persistBonsaiWithBal(final BlockChange blockChange) {
      blockChange.apply(bonsaiWorldState.updater());
      bonsaiWorldState.updater().commit();
      final Hash root =
          new DefaultStateRootCommitter()
              .compute(bonsaiWorldState, null, bonsaiWorldState.updater())
              .root();
      final BlockHeader blockHeader = childHeader(root);
      final StateRootCommitterFactory factory = new StateRootCommitterFactory(balConfiguration());
      final StateRootCommitter committer =
          factory.forBlock(
              protocolContext,
              blockHeader,
              Optional.of(blockChange.toBal()),
              bonsaiWorldState.isStorageFrozen());
      bonsaiWorldState.persist(blockHeader, committer);
      appendBlock(blockHeader);
      return bonsaiWorldState.rootHash();
    }

    Hash persistForest(final BlockChange blockChange) {
      final WorldUpdater updater = forestWorldState.updater();
      blockChange.apply(updater);
      updater.commit();
      forestWorldState.persist(null, ForestStateRootCommitter.INSTANCE);
      return forestWorldState.rootHash();
    }

    Hash forestGenesisRoot() {
      return forestWorldState.rootHash();
    }

    Hash computeRootWithoutPersist(final BlockChange blockChange) {
      blockChange.apply(bonsaiWorldState.updater());
      bonsaiWorldState.updater().commit();
      return new DefaultStateRootCommitter()
          .compute(bonsaiWorldState, null, bonsaiWorldState.updater())
          .root();
    }

    Hash persistBonsaiWithPrecomputedRoot(final Hash expectedRoot) {
      final BlockHeader blockHeader = childHeader(expectedRoot);
      bonsaiWorldState.persist(blockHeader, new DefaultStateRootCommitter());
      appendBlock(blockHeader);
      return bonsaiWorldState.rootHash();
    }

    private BlockHeader childHeader(final Hash stateRoot) {
      return new BlockHeaderTestFixture()
          .parentHash(chainHead.getHash())
          .number(chainHead.getNumber() + 1L)
          .stateRoot(stateRoot)
          .buildHeader();
    }

    private void appendBlock(final BlockHeader blockHeader) {
      protocolContext
          .getBlockchain()
          .appendBlock(new Block(blockHeader, BlockBody.empty()), List.of());
      chainHead = blockHeader;
    }
  }

  // ---------------------------------------------------------------------------
  // Legacy fixture helpers (kept for nested Bonsai storage tests)
  // ---------------------------------------------------------------------------
  // ---------------------------------------------------------------------------
  // Block change model
  // ---------------------------------------------------------------------------

  private record BlockChange(
      List<Consumer<WorldUpdater>> mutations, List<AccountChanges> balAccounts) {

    void apply(final WorldUpdater updater) {
      mutations.forEach(m -> m.accept(updater));
    }

    BlockAccessList toBal() {
      return new BlockAccessList(balAccounts);
    }

    static BlockChange complex() {
      return new BlockChange(
          List.of(
              u -> {
                final MutableAccount contract = u.getOrCreate(CONTRACT);
                contract.setBalance(Wei.of(5_000_000));
                contract.setCode(CONTRACT_CODE);
                contract.setStorageValue(SLOT.getSlotKey().orElseThrow(), SLOT_VALUE);
              },
              u -> {
                final MutableAccount eoa = u.getOrCreate(EOA);
                eoa.setBalance(Wei.of(10_000_000));
                eoa.setNonce(12L);
              }),
          List.of(
              new AccountChanges(
                  CONTRACT,
                  List.of(new SlotChanges(SLOT, List.of(new StorageChange(0, SLOT_VALUE)))),
                  List.of(),
                  List.of(new BalanceChange(0, Wei.of(5_000_000))),
                  List.of(),
                  List.of(new CodeChange(0, CONTRACT_CODE))),
              new AccountChanges(
                  EOA,
                  List.of(),
                  List.of(),
                  List.of(new BalanceChange(0, Wei.of(10_000_000))),
                  List.of(new NonceChange(0, 12L)),
                  List.of())));
    }

    static BlockChange balanceAndNonce(final Address address, final Wei balance, final long nonce) {
      return new BlockChange(
          List.of(
              u -> {
                final MutableAccount account = u.getOrCreate(address);
                account.setBalance(balance);
                account.setNonce(nonce);
              }),
          List.of(
              new AccountChanges(
                  address,
                  List.of(),
                  List.of(),
                  List.of(new BalanceChange(0, balance)),
                  List.of(new NonceChange(0, nonce)),
                  List.of())));
    }

    static BlockChange codeAndStorage(
        final Address address,
        final Bytes code,
        final StorageSlotKey slot,
        final UInt256 value) {
      return new BlockChange(
          List.of(
              u -> {
                final MutableAccount account = u.getOrCreate(address);
                account.setCode(code);
                account.setStorageValue(slot.getSlotKey().orElseThrow(), value);
              }),
          List.of(
              new AccountChanges(
                  address,
                  List.of(new SlotChanges(slot, List.of(new StorageChange(0, value)))),
                  List.of(),
                  List.of(),
                  List.of(),
                  List.of(new CodeChange(0, code)))));
    }

    static BlockChange deleteAccount(final Address address) {
      return new BlockChange(
          List.of(u -> u.deleteAccount(address)),
          List.of(
              new AccountChanges(
                  address,
                  List.of(),
                  List.of(),
                  List.of(new BalanceChange(0, Wei.ZERO)),
                  List.of(new NonceChange(0, 0L)),
                  List.of(new CodeChange(0, Bytes.EMPTY)))));
    }
  }

  // ---------------------------------------------------------------------------
  // Bonsai KV harness (direct segment access)
  // ---------------------------------------------------------------------------

  private static final class BonsaiKvHarness {

    private final MutableBlockchain blockchain;
    private final BonsaiWorldStateProvider archive;
    private final ProtocolContext protocolContext;
    private final KeyValueStorage accountStorage;
    private final KeyValueStorage codeStorage;
    private final KeyValueStorage storageStorage;
    private final KeyValueStorage trieLogStorage;

    private BonsaiKvHarness(
        final MutableBlockchain blockchain,
        final BonsaiWorldStateProvider archive,
        final ProtocolContext protocolContext,
        final KeyValueStorage accountStorage,
        final KeyValueStorage codeStorage,
        final KeyValueStorage storageStorage,
        final KeyValueStorage trieLogStorage) {
      this.blockchain = blockchain;
      this.archive = archive;
      this.protocolContext = protocolContext;
      this.accountStorage = accountStorage;
      this.codeStorage = codeStorage;
      this.storageStorage = storageStorage;
      this.trieLogStorage = trieLogStorage;
    }

    static BonsaiKvHarness create() {
      final InMemoryKeyValueStorageProvider provider = new InMemoryKeyValueStorageProvider();
      final var protocolSchedule =
          new ProtocolScheduleBuilder(
                  GenesisConfig.mainnet().getConfigOptions(),
                  Optional.of(BigInteger.valueOf(42)),
                  ProtocolSpecAdapters.create(0, Function.identity()),
                  false,
                  EvmConfiguration.DEFAULT,
                  MiningConfiguration.MINING_DISABLED,
                  new BadBlockManager(),
                  false,
                  ImmutableBalConfiguration.builder().build(),
                  new NoOpMetricsSystem())
              .createProtocolSchedule();
      final GenesisState genesisState =
          GenesisState.fromConfig(GenesisConfig.mainnet(), protocolSchedule, new CodeCache());
      final MutableBlockchain blockchain =
          InMemoryKeyValueStorageProvider.createInMemoryBlockchain(genesisState.getBlock());
      final BonsaiWorldStateKeyValueStorage kvStorage =
          new BonsaiWorldStateKeyValueStorage(
              provider, new NoOpMetricsSystem(), DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
      final BonsaiWorldStateProvider archive =
          new BonsaiWorldStateProvider(
              kvStorage,
              blockchain,
              DataStorageConfiguration.DEFAULT_BONSAI_CONFIG.getPathBasedExtraStorageConfiguration(),
              new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem()),
              null,
              EvmConfiguration.DEFAULT,
              throwingWorldStateHealerSupplier(),
              new CodeCache());
      genesisState.writeStateTo(archive.getWorldState());
      final ProtocolContext protocolContext =
          new ProtocolContext.Builder()
              .withBlockchain(blockchain)
              .withWorldStateArchive(archive)
              .build();
      return new BonsaiKvHarness(
          blockchain,
          archive,
          protocolContext,
          provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE),
          provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.CODE_STORAGE),
          provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE),
          provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.TRIE_LOG_STORAGE));
    }

    ProtocolContext protocolContext() {
      return protocolContext;
    }

    BonsaiWorldState newWritableWorldState() {
      return (BonsaiWorldState) archive.getWorldState();
    }

    BlockHeader persistParent() {
      final BlockHeader parent = blockchain.getChainHeadHeader();
      archive.getWorldState().persist(parent);
      return parent;
    }

    BlockHeader childHeader(final BlockHeader parent, final BlockChange blockChange) {
      try (BonsaiWorldState worldState = newWritableWorldState()) {
        blockChange.apply(worldState.updater());
        worldState.updater().commit();
        final Hash root =
            new DefaultStateRootCommitter()
                .compute(worldState, null, worldState.updater())
                .root();
        return new BlockHeaderTestFixture()
            .parentHash(parent.getHash())
            .number(parent.getNumber() + 1L)
            .stateRoot(root)
            .buildHeader();
      }
    }

    boolean trieLogExists(final BlockHeader blockHeader) {
      return trieLogStorage
          .get(blockHeader.getHash().getBytes().toArrayUnsafe())
          .isPresent();
    }

    boolean accountExistsInFlatDb(final Address address) {
      return accountStorage.get(address.addressHash().getBytes().toArrayUnsafe()).isPresent();
    }

    int accountFlatDbKeyCount() {
      return accountStorage.getAllKeysThat(k -> true).size();
    }

    int codeFlatDbKeyCount() {
      return codeStorage.getAllKeysThat(k -> true).size();
    }

    int storageFlatDbKeyCount() {
      return storageStorage.getAllKeysThat(k -> true).size();
    }

    TrieLogLayer readTrieLog(final BlockHeader blockHeader) {
      final byte[] serialized =
          trieLogStorage.get(blockHeader.getHash().getBytes().toArrayUnsafe()).orElseThrow();
      return TrieLogFactoryImpl.readFrom(new BytesValueRLPInput(Bytes.wrap(serialized), false));
    }

    KvSnapshot captureFlatDbSnapshot() {
      return new KvSnapshot(
          snapshotStorage(accountStorage),
          snapshotStorage(codeStorage),
          snapshotStorage(storageStorage));
    }

    void assertFlatDbMatches(final KvSnapshot expected) {
      assertStorageEqual(accountStorage, expected.account());
      assertStorageEqual(codeStorage, expected.code());
      assertStorageEqual(storageStorage, expected.storage());
    }

    private static Map<Bytes, Bytes> snapshotStorage(final KeyValueStorage storage) {
      return storage.getAllKeysThat(k -> true).stream()
          .map(Bytes::wrap)
          .collect(
              Collectors.toMap(
                  key -> key, key -> Bytes.wrap(storage.get(key.toArrayUnsafe()).orElseThrow())));
    }

    private static void assertStorageEqual(
        final KeyValueStorage actual, final Map<Bytes, Bytes> expected) {
      final Set<Bytes> actualKeys = snapshotStorage(actual).keySet();
      assertThat(actualKeys).isEqualTo(expected.keySet());
      for (final Map.Entry<Bytes, Bytes> entry : expected.entrySet()) {
        assertThat(Bytes.wrap(actual.get(entry.getKey().toArrayUnsafe()).orElseThrow()))
            .isEqualTo(entry.getValue());
      }
    }
  }

  private record KvSnapshot(
      Map<Bytes, Bytes> account, Map<Bytes, Bytes> code, Map<Bytes, Bytes> storage) {}
}
