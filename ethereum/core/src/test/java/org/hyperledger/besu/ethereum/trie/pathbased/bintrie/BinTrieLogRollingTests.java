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
package org.hyperledger.besu.ethereum.trie.pathbased.bintrie;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig.createStatefulConfigWithTrie;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.storage.BinTrieWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.trielog.BinTrieTrieLogFactoryImpl;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.worldview.BinTrieWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.worldview.BinTrieWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BinTrieLogRollingTests {

  private BinTrieWorldStateProvider archive;

  private InMemoryKeyValueStorageProvider provider;
  private KeyValueStorage accountStorage;
  private KeyValueStorage codeStorage;
  private KeyValueStorage storageStorage;
  private KeyValueStorage trieBranchStorage;
  private KeyValueStorage trieLogStorage;

  private InMemoryKeyValueStorageProvider secondProvider;
  private BinTrieWorldStateProvider secondArchive;
  private KeyValueStorage secondAccountStorage;
  private KeyValueStorage secondCodeStorage;
  private KeyValueStorage secondStorageStorage;
  private KeyValueStorage secondTrieBranchStorage;
  private KeyValueStorage secondTrieLogStorage;

  private final Blockchain blockchain = mock(Blockchain.class);

  private static final Address addressOne =
      Address.fromHexString("0x1111111111111111111111111111111111111111");

  private static final BlockHeader headerOne =
      new BlockHeader(
          Hash.ZERO,
          Hash.EMPTY_LIST_HASH,
          Address.ZERO,
          Hash.fromHexString("0xbd8d4d9416ab946e2e519e4091faf513834d9bf313781003f11da1f356f3f8e4"),
          Hash.EMPTY_TRIE_HASH,
          Hash.EMPTY_LIST_HASH,
          org.hyperledger.besu.datatypes.LogsBloomFilter.builder().build(),
          Difficulty.ONE,
          1,
          0,
          0,
          0,
          Bytes.EMPTY,
          Wei.ZERO,
          Bytes32.wrap(Hash.ZERO.getBytes()),
          0,
          Hash.EMPTY_LIST_HASH,
          null,
          null,
          null,
          null,
          null,
          null,
          new MainnetBlockHeaderFunctions());

  private static final BlockHeader headerTwo =
      new BlockHeader(
          headerOne.getHash(),
          Hash.EMPTY_LIST_HASH,
          Address.ZERO,
          Hash.fromHexString("0x651d5be0881e46d24f6ec37cf74abcd3df12a1076442cfd65ff9c4c9f98f60be"),
          Hash.EMPTY_TRIE_HASH,
          Hash.EMPTY_LIST_HASH,
          org.hyperledger.besu.datatypes.LogsBloomFilter.builder().build(),
          Difficulty.ONE,
          2,
          0,
          0,
          0,
          Bytes.EMPTY,
          Wei.ZERO,
          Bytes32.wrap(Hash.ZERO.getBytes()),
          0,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          new MainnetBlockHeaderFunctions());

  private static final BlockHeader headerThree =
      new BlockHeader(
          headerTwo.getHash(),
          Hash.EMPTY_LIST_HASH,
          Address.ZERO,
          Hash.fromHexString("0x751d5be0881e46d24f6ec37cf74abcd3df12a1076442cfd65ff9c4c9f98f60bf"),
          Hash.EMPTY_TRIE_HASH,
          Hash.EMPTY_LIST_HASH,
          org.hyperledger.besu.datatypes.LogsBloomFilter.builder().build(),
          Difficulty.ONE,
          3,
          0,
          0,
          0,
          Bytes.EMPTY,
          Wei.ZERO,
          Bytes32.wrap(Hash.ZERO.getBytes()),
          0,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          new MainnetBlockHeaderFunctions());

  /** Provides the two FlatDb configurations: FULL and STEM. */
  static Stream<Arguments> flatDbConfigurations() {
    // FULL flat db configuration
    DataStorageConfiguration fullConfig =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(DataStorageFormat.BINTRIE)
            .pathBasedExtraStorageConfiguration(
                ImmutablePathBasedExtraStorageConfiguration.builder()
                    .unstable(
                        ImmutablePathBasedExtraStorageConfiguration.PathBasedUnstable.builder()
                            .fullFlatDbEnabled(true)
                            .codeStoredByCodeHashEnabled(true)
                            .build())
                    .build())
            .build();

    // STEM flat db configuration
    DataStorageConfiguration stemConfig =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(DataStorageFormat.BINTRIE)
            .pathBasedExtraStorageConfiguration(
                ImmutablePathBasedExtraStorageConfiguration.builder()
                    .unstable(
                        ImmutablePathBasedExtraStorageConfiguration.PathBasedUnstable.builder()
                            .fullFlatDbEnabled(false)
                            .codeStoredByCodeHashEnabled(true)
                            .build())
                    .build())
            .build();

    return Stream.of(Arguments.of("FULL", fullConfig), Arguments.of("STEM", stemConfig));
  }

  @BeforeEach
  void createStorage() {
    provider = new InMemoryKeyValueStorageProvider();
    archive = InMemoryKeyValueStorageProvider.createBinTrieInMemoryWorldStateArchive(blockchain);

    accountStorage =
        provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE);
    codeStorage = provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.CODE_STORAGE);
    storageStorage =
        provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE);
    trieBranchStorage =
        provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.BINTRIE_BRANCH_STORAGE);
    trieLogStorage =
        provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.TRIE_LOG_STORAGE);

    secondProvider = new InMemoryKeyValueStorageProvider();
    secondArchive =
        InMemoryKeyValueStorageProvider.createBinTrieInMemoryWorldStateArchive(blockchain);

    secondAccountStorage =
        secondProvider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE);
    secondCodeStorage =
        secondProvider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.CODE_STORAGE);
    secondStorageStorage =
        secondProvider.getStorageBySegmentIdentifier(
            KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE);
    secondTrieBranchStorage =
        secondProvider.getStorageBySegmentIdentifier(
            KeyValueSegmentIdentifier.BINTRIE_BRANCH_STORAGE);
    secondTrieLogStorage =
        secondProvider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.TRIE_LOG_STORAGE);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("flatDbConfigurations")
  void simpleRollForwardTest(final String name, final DataStorageConfiguration config) {
    final BinTrieWorldState worldState =
        new BinTrieWorldState(
            archive,
            new BinTrieWorldStateKeyValueStorage(provider, new NoOpMetricsSystem(), config),
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new CodeCache());
    final WorldUpdater updater = worldState.updater();

    final MutableAccount mutableAccount = updater.createAccount(addressOne, 1, Wei.of(1L));
    mutableAccount.setCode(Bytes.of(0, 1, 2));
    mutableAccount.setStorageValue(UInt256.ONE, UInt256.ONE);
    updater.commit();
    worldState.persist(headerOne);

    final BinTrieWorldState secondWorldState =
        new BinTrieWorldState(
            secondArchive,
            new BinTrieWorldStateKeyValueStorage(secondProvider, new NoOpMetricsSystem(), config),
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new CodeCache());
    final BinTrieWorldStateUpdateAccumulator secondUpdater =
        (BinTrieWorldStateUpdateAccumulator) secondWorldState.updater();

    final TrieLogLayer layer = getTrieLogLayer(trieLogStorage, headerOne.getHash());

    secondUpdater.rollForward(layer);
    secondUpdater.commit();
    secondWorldState.persist(headerOne);

    assertKeyValueStorageEqual(accountStorage, secondAccountStorage);
    assertKeyValueStorageEqual(codeStorage, secondCodeStorage);
    assertKeyValueStorageEqual(storageStorage, secondStorageStorage);
    final KeyValueStorageTransaction tx = trieBranchStorage.startTransaction();
    tx.remove(BinTrieWorldStateKeyValueStorage.WORLD_BLOCK_HASH_KEY);
    tx.commit();
    assertKeyValueStorageEqual(trieBranchStorage, secondTrieBranchStorage);
    assertKeyValueSubset(trieLogStorage, secondTrieLogStorage);
    assertThat(secondWorldState.rootHash()).isEqualByComparingTo(worldState.rootHash());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("flatDbConfigurations")
  void rollForwardTwice(final String name, final DataStorageConfiguration config) {
    final BinTrieWorldState worldState =
        new BinTrieWorldState(
            archive,
            new BinTrieWorldStateKeyValueStorage(provider, new NoOpMetricsSystem(), config),
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new CodeCache());

    final WorldUpdater updater = worldState.updater();
    final MutableAccount mutableAccount = updater.createAccount(addressOne, 1, Wei.of(1L));
    mutableAccount.setCode(Bytes.of(0, 1, 2));
    mutableAccount.setStorageValue(UInt256.ONE, UInt256.ONE);
    updater.commit();

    worldState.persist(headerOne);

    final WorldUpdater updater2 = worldState.updater();
    final MutableAccount mutableAccount2 = updater2.getAccount(addressOne);
    mutableAccount2.setStorageValue(UInt256.ONE, UInt256.valueOf(2));
    updater2.commit();

    worldState.persist(headerTwo);

    final BinTrieWorldState secondWorldState =
        new BinTrieWorldState(
            secondArchive,
            new BinTrieWorldStateKeyValueStorage(secondProvider, new NoOpMetricsSystem(), config),
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new CodeCache());
    final BinTrieWorldStateUpdateAccumulator secondUpdater =
        (BinTrieWorldStateUpdateAccumulator) secondWorldState.updater();

    final TrieLogLayer layerOne = getTrieLogLayer(trieLogStorage, headerOne.getHash());
    secondUpdater.rollForward(layerOne);
    secondUpdater.commit();
    secondWorldState.persist(headerOne);

    final TrieLogLayer layerTwo = getTrieLogLayer(trieLogStorage, headerTwo.getHash());
    secondUpdater.rollForward(layerTwo);
    secondUpdater.commit();
    secondWorldState.persist(headerTwo);

    assertKeyValueStorageEqual(accountStorage, secondAccountStorage);
    assertKeyValueStorageEqual(codeStorage, secondCodeStorage);
    assertKeyValueStorageEqual(storageStorage, secondStorageStorage);
    final KeyValueStorageTransaction tx = trieBranchStorage.startTransaction();
    tx.remove(BinTrieWorldStateKeyValueStorage.WORLD_BLOCK_HASH_KEY);
    tx.commit();
    assertKeyValueStorageEqual(trieBranchStorage, secondTrieBranchStorage);
    assertKeyValueSubset(trieLogStorage, secondTrieLogStorage);
    assertThat(secondWorldState.rootHash()).isEqualByComparingTo(worldState.rootHash());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("flatDbConfigurations")
  void rollBackOnce(final String name, final DataStorageConfiguration config) {
    final BinTrieWorldState worldState =
        new BinTrieWorldState(
            archive,
            new BinTrieWorldStateKeyValueStorage(provider, new NoOpMetricsSystem(), config),
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new CodeCache());

    final WorldUpdater updater = worldState.updater();
    final MutableAccount mutableAccount = updater.createAccount(addressOne, 1, Wei.of(1L));
    mutableAccount.setCode(Bytes.of(0, 1, 2));
    mutableAccount.setStorageValue(UInt256.ONE, UInt256.ONE);
    updater.commit();

    worldState.persist(headerOne);

    final WorldUpdater updater2 = worldState.updater();
    final MutableAccount mutableAccount2 = updater2.getAccount(addressOne);
    mutableAccount2.setStorageValue(UInt256.ONE, UInt256.valueOf(2));
    updater2.commit();

    worldState.persist(headerTwo);

    final BinTrieWorldStateUpdateAccumulator firstRollbackUpdater =
        (BinTrieWorldStateUpdateAccumulator) worldState.updater();

    final TrieLogLayer layerTwo = getTrieLogLayer(trieLogStorage, headerTwo.getHash());
    firstRollbackUpdater.rollBack(layerTwo);

    worldState.persist(headerOne);

    final BinTrieWorldState secondWorldState =
        new BinTrieWorldState(
            secondArchive,
            new BinTrieWorldStateKeyValueStorage(secondProvider, new NoOpMetricsSystem(), config),
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new CodeCache());

    final WorldUpdater secondUpdater = secondWorldState.updater();
    final MutableAccount secondMutableAccount =
        secondUpdater.createAccount(addressOne, 1, Wei.of(1L));
    secondMutableAccount.setCode(Bytes.of(0, 1, 2));
    secondMutableAccount.setStorageValue(UInt256.ONE, UInt256.ONE);
    secondUpdater.commit();

    secondWorldState.persist(headerOne);

    assertKeyValueStorageEqual(accountStorage, secondAccountStorage);
    assertKeyValueStorageEqual(codeStorage, secondCodeStorage);
    assertKeyValueStorageEqual(storageStorage, secondStorageStorage);
    final KeyValueStorageTransaction tx = trieBranchStorage.startTransaction();
    tx.remove(BinTrieWorldStateKeyValueStorage.WORLD_BLOCK_HASH_KEY);
    tx.commit();
    assertKeyValueStorageEqual(trieBranchStorage, secondTrieBranchStorage);
    // Skip TrieLog comparison for rollback - prior values are different by design
    assertThat(secondWorldState.rootHash()).isEqualByComparingTo(worldState.rootHash());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("flatDbConfigurations")
  void rollBackTwice(final String name, final DataStorageConfiguration config) {
    final BinTrieWorldState worldState =
        new BinTrieWorldState(
            archive,
            new BinTrieWorldStateKeyValueStorage(provider, new NoOpMetricsSystem(), config),
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new CodeCache());

    // Block 1: Create account with code and storage
    final WorldUpdater updater = worldState.updater();
    final MutableAccount mutableAccount = updater.createAccount(addressOne, 1, Wei.of(1L));
    mutableAccount.setCode(Bytes.of(0, 1, 2));
    mutableAccount.setStorageValue(UInt256.ONE, UInt256.ONE);
    updater.commit();
    worldState.persist(headerOne);

    // Block 2: Modify storage to value 2
    final WorldUpdater updater2 = worldState.updater();
    final MutableAccount mutableAccount2 = updater2.getAccount(addressOne);
    mutableAccount2.setStorageValue(UInt256.ONE, UInt256.valueOf(2));
    updater2.commit();
    worldState.persist(headerTwo);

    // Block 3: Modify storage to value 3
    final WorldUpdater updater3 = worldState.updater();
    final MutableAccount mutableAccount3 = updater3.getAccount(addressOne);
    mutableAccount3.setStorageValue(UInt256.ONE, UInt256.valueOf(3));
    updater3.commit();
    worldState.persist(headerThree);

    // Rollback from block 3 to block 2
    final BinTrieWorldStateUpdateAccumulator rollbackUpdater1 =
        (BinTrieWorldStateUpdateAccumulator) worldState.updater();
    final TrieLogLayer layerThree = getTrieLogLayer(trieLogStorage, headerThree.getHash());
    rollbackUpdater1.rollBack(layerThree);
    worldState.persist(headerTwo);

    // Rollback from block 2 to block 1
    final BinTrieWorldStateUpdateAccumulator rollbackUpdater2 =
        (BinTrieWorldStateUpdateAccumulator) worldState.updater();
    final TrieLogLayer layerTwo = getTrieLogLayer(trieLogStorage, headerTwo.getHash());
    rollbackUpdater2.rollBack(layerTwo);
    worldState.persist(headerOne);

    // Create second world state from scratch with block 1 state
    final BinTrieWorldState secondWorldState =
        new BinTrieWorldState(
            secondArchive,
            new BinTrieWorldStateKeyValueStorage(secondProvider, new NoOpMetricsSystem(), config),
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new CodeCache());

    final WorldUpdater secondUpdater = secondWorldState.updater();
    final MutableAccount secondMutableAccount =
        secondUpdater.createAccount(addressOne, 1, Wei.of(1L));
    secondMutableAccount.setCode(Bytes.of(0, 1, 2));
    secondMutableAccount.setStorageValue(UInt256.ONE, UInt256.ONE);
    secondUpdater.commit();
    secondWorldState.persist(headerOne);

    // Compare the two world states
    assertKeyValueStorageEqual(accountStorage, secondAccountStorage);
    assertKeyValueStorageEqual(codeStorage, secondCodeStorage);
    assertKeyValueStorageEqual(storageStorage, secondStorageStorage);
    final KeyValueStorageTransaction tx = trieBranchStorage.startTransaction();
    tx.remove(BinTrieWorldStateKeyValueStorage.WORLD_BLOCK_HASH_KEY);
    tx.commit();
    assertKeyValueStorageEqual(trieBranchStorage, secondTrieBranchStorage);
    // Skip TrieLog comparison for rollback - prior values are different by design
    assertThat(secondWorldState.rootHash()).isEqualByComparingTo(worldState.rootHash());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("flatDbConfigurations")
  void simpleAccountCreation(final String name, final DataStorageConfiguration config) {
    final BinTrieWorldState worldState =
        new BinTrieWorldState(
            archive,
            new BinTrieWorldStateKeyValueStorage(provider, new NoOpMetricsSystem(), config),
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new CodeCache());
    final WorldUpdater updater = worldState.updater();

    final MutableAccount mutableAccount = updater.createAccount(addressOne, 1, Wei.of(1L));
    mutableAccount.setCode(Bytes.of(0, 1, 2));
    mutableAccount.setStorageValue(UInt256.ONE, UInt256.ONE);
    updater.commit();
    worldState.persist(headerOne);

    assertThat(worldState.get(addressOne)).isNotNull();
    assertThat(worldState.get(addressOne).getBalance()).isEqualTo(Wei.of(1L));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("flatDbConfigurations")
  void accountUpdatePersistence(final String name, final DataStorageConfiguration config) {
    final BinTrieWorldState worldState =
        new BinTrieWorldState(
            archive,
            new BinTrieWorldStateKeyValueStorage(provider, new NoOpMetricsSystem(), config),
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new CodeCache());
    final WorldUpdater updater = worldState.updater();

    final MutableAccount mutableAccount = updater.createAccount(addressOne, 1, Wei.of(1L));
    mutableAccount.setCode(Bytes.of(0, 1, 2));
    mutableAccount.setStorageValue(UInt256.ONE, UInt256.ONE);
    updater.commit();
    worldState.persist(headerOne);

    // Update account
    final WorldUpdater updater2 = worldState.updater();
    final MutableAccount mutableAccount2 = updater2.getAccount(addressOne);
    mutableAccount2.setBalance(Wei.of(100L));
    updater2.commit();
    worldState.persist(headerTwo);

    assertThat(worldState.get(addressOne)).isNotNull();
    assertThat(worldState.get(addressOne).getBalance()).isEqualTo(Wei.of(100L));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("flatDbConfigurations")
  void storageUpdatePersistence(final String name, final DataStorageConfiguration config) {
    final BinTrieWorldState worldState =
        new BinTrieWorldState(
            archive,
            new BinTrieWorldStateKeyValueStorage(provider, new NoOpMetricsSystem(), config),
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new CodeCache());
    final WorldUpdater updater = worldState.updater();

    final MutableAccount mutableAccount = updater.createAccount(addressOne, 1, Wei.of(1L));
    mutableAccount.setCode(Bytes.of(0, 1, 2));
    mutableAccount.setStorageValue(UInt256.ONE, UInt256.ONE);
    updater.commit();
    worldState.persist(headerOne);

    // Update storage only
    final WorldUpdater updater2 = worldState.updater();
    final MutableAccount mutableAccount2 = updater2.getAccount(addressOne);
    mutableAccount2.setStorageValue(UInt256.ONE, UInt256.valueOf(42));
    updater2.commit();
    worldState.persist(headerTwo);

    assertThat(worldState.getStorageValue(addressOne, UInt256.ONE)).isEqualTo(UInt256.valueOf(42));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("flatDbConfigurations")
  void rollBackAccountDeletion(final String name, final DataStorageConfiguration config) {
    final BinTrieWorldStateKeyValueStorage storage =
        new BinTrieWorldStateKeyValueStorage(provider, new NoOpMetricsSystem(), config);
    final BinTrieWorldState worldState =
        new BinTrieWorldState(
            archive,
            storage,
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new CodeCache());

    // Verify account does not exist initially
    assertThat(worldState.get(addressOne)).isNull();
    assertThat(storage.getAccount(addressOne, worldState)).isEmpty();

    // Create account with nonce = 2
    final WorldUpdater updater = worldState.updater();
    final MutableAccount mutableAccount = updater.createAccount(addressOne, 2, Wei.of(100L));
    mutableAccount.setCode(Bytes.of(0, 1, 2));
    mutableAccount.setStorageValue(UInt256.ONE, UInt256.valueOf(42));
    updater.commit();
    worldState.persist(headerOne);

    // Verify account exists after creation
    assertThat(worldState.get(addressOne)).isNotNull();
    assertThat(worldState.get(addressOne).getNonce()).isEqualTo(2);
    assertThat(worldState.get(addressOne).getBalance()).isEqualTo(Wei.of(100L));
    assertThat(storage.getAccount(addressOne, worldState)).isPresent();

    // Rollback the account creation
    final BinTrieWorldStateUpdateAccumulator rollbackUpdater =
        (BinTrieWorldStateUpdateAccumulator) worldState.updater();
    final TrieLogLayer layer = getTrieLogLayer(trieLogStorage, headerOne.getHash());
    rollbackUpdater.rollBack(layer);
    worldState.persist(null);

    // Verify account no longer exists in world state
    assertThat(worldState.get(addressOne)).isNull();

    // Verify account no longer exists in flat db storage
    assertThat(storage.getAccount(addressOne, worldState)).isEmpty();
  }

  private TrieLogLayer getTrieLogLayer(final KeyValueStorage storage, final Hash blockHash) {
    return storage
        .get(blockHash.getBytes().toArrayUnsafe())
        .map(
            bytes ->
                BinTrieTrieLogFactoryImpl.readFrom(
                    new BytesValueRLPInput(Bytes.wrap(bytes), false)))
        .orElseThrow();
  }

  private static void assertKeyValueStorageEqual(
      final KeyValueStorage first, final KeyValueStorage second) {
    final var firstKeys =
        first.getAllKeysThat(k -> true).stream().map(Bytes::wrap).collect(Collectors.toSet());
    final var secondKeys =
        second.getAllKeysThat(k -> true).stream().map(Bytes::wrap).collect(Collectors.toSet());

    assertThat(firstKeys).isEqualTo(secondKeys);
    for (final Bytes key : firstKeys) {
      assertThat(Bytes.wrap(first.get(key.toArrayUnsafe()).get()))
          .isEqualByComparingTo(Bytes.wrap(second.get(key.toArrayUnsafe()).get()));
    }
  }

  private static void assertKeyValueSubset(
      final KeyValueStorage largerSet, final KeyValueStorage smallerSet) {
    final var largerKeys =
        largerSet.getAllKeysThat(k -> true).stream().map(Bytes::wrap).collect(Collectors.toSet());
    final var smallerKeys =
        smallerSet.getAllKeysThat(k -> true).stream().map(Bytes::wrap).collect(Collectors.toSet());

    assertThat(largerKeys).containsAll(smallerKeys);
    for (final Bytes key : largerKeys) {
      if (smallerKeys.contains(key)) {
        assertThat(Bytes.wrap(largerSet.get(key.toArrayUnsafe()).get()))
            .isEqualByComparingTo(Bytes.wrap(smallerSet.get(key.toArrayUnsafe()).get()));
      }
    }
  }
}
