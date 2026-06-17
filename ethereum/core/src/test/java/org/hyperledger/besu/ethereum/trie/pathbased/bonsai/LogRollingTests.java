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
import static org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig.createStatefulConfigWithTrie;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.LogsBloomFilter;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.BonsaiTrieLogFactory;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.code.PathBasedCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;

import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class LogRollingTests {

  private BonsaiWorldStateProvider archive;

  private InMemoryKeyValueStorageProvider provider;
  private KeyValueStorage accountStorage;
  private KeyValueStorage codeStorage;
  private KeyValueStorage storageStorage;
  private KeyValueStorage trieBranchStorage;
  private KeyValueStorage trieLogStorage;

  private InMemoryKeyValueStorageProvider secondProvider;
  private BonsaiWorldStateProvider secondArchive;
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
          Hash.fromHexString("0x0ecfa454ddfe6b740f4af7b7f4c61b5c6bac2854efd2b07b27b1f53dba9bb46c"),
          Hash.EMPTY_TRIE_HASH,
          Hash.EMPTY_LIST_HASH,
          LogsBloomFilter.builder().build(),
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
          null, // blobGasUsed
          null,
          null,
          null,
          null,
          null, // slotNumber
          new MainnetBlockHeaderFunctions());
  private static final BlockHeader headerTwo =
      new BlockHeader(
          headerOne.getHash(),
          Hash.EMPTY_LIST_HASH,
          Address.ZERO,
          Hash.fromHexString("0x5b675f79cd11ba67266161d79a8d5be3ac330dfbb76300a4f15d76b610b18193"),
          Hash.EMPTY_TRIE_HASH,
          Hash.EMPTY_LIST_HASH,
          LogsBloomFilter.builder().build(),
          Difficulty.ONE,
          1,
          0,
          0,
          0,
          Bytes.EMPTY,
          Wei.ZERO,
          Bytes32.wrap(Hash.ZERO.getBytes()),
          0,
          null,
          null, // blobGasUsed
          null,
          null,
          null,
          null,
          null, // slotNumber
          new MainnetBlockHeaderFunctions());

  @BeforeEach
  void createStorage() {
    provider = new InMemoryKeyValueStorageProvider();
    archive = InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(blockchain);
    accountStorage =
        provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE);
    codeStorage = provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.CODE_STORAGE);
    storageStorage =
        provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE);
    trieBranchStorage =
        provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE);
    trieLogStorage =
        provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.TRIE_LOG_STORAGE);

    secondProvider = new InMemoryKeyValueStorageProvider();
    secondArchive =
        InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(blockchain);
    secondAccountStorage =
        secondProvider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE);
    secondCodeStorage =
        secondProvider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.CODE_STORAGE);
    secondStorageStorage =
        secondProvider.getStorageBySegmentIdentifier(
            KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE);
    secondTrieBranchStorage =
        secondProvider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE);
    secondTrieLogStorage =
        secondProvider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.TRIE_LOG_STORAGE);
  }

  @Test
  void simpleRollForwardTest() {

    final BonsaiWorldState worldState =
        new BonsaiWorldState(
            archive,
            new BonsaiWorldStateKeyValueStorage(
                provider, new NoOpMetricsSystem(), DataStorageConfiguration.DEFAULT_BONSAI_CONFIG),
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new PathBasedCodeCache());
    final WorldUpdater updater = worldState.updater();

    final MutableAccount mutableAccount = updater.createAccount(addressOne, 1, Wei.of(1L));
    mutableAccount.setCode(Bytes.of(0, 1, 2));
    mutableAccount.setStorageValue(UInt256.ONE, UInt256.ONE);
    updater.commit();
    worldState.persist(headerOne);

    final BonsaiWorldState secondWorldState =
        new BonsaiWorldState(
            secondArchive,
            new BonsaiWorldStateKeyValueStorage(
                secondProvider,
                new NoOpMetricsSystem(),
                DataStorageConfiguration.DEFAULT_BONSAI_CONFIG),
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new PathBasedCodeCache());
    final BonsaiWorldStateUpdateAccumulator secondUpdater =
        (BonsaiWorldStateUpdateAccumulator) secondWorldState.updater();

    final Optional<byte[]> value =
        trieLogStorage.get(headerOne.getHash().getBytes().toArrayUnsafe());

    final TrieLogLayer layer =
        BonsaiTrieLogFactory.readFrom(new BytesValueRLPInput(Bytes.wrap(value.get()), false));

    secondUpdater.rollForward(layer);
    secondUpdater.commit();
    secondWorldState.persist(null);

    assertKeyValueStorageEqual(accountStorage, secondAccountStorage);
    assertKeyValueStorageEqual(codeStorage, secondCodeStorage);
    assertKeyValueStorageEqual(storageStorage, secondStorageStorage);
    final KeyValueStorageTransaction tx = trieBranchStorage.startTransaction();
    tx.remove(BonsaiWorldStateKeyValueStorage.WORLD_BLOCK_HASH_KEY);
    tx.commit();
    assertKeyValueStorageEqual(trieBranchStorage, secondTrieBranchStorage);
    // trie logs won't be the same, we shouldn't generate logs on rolls.
    assertKeyValueSubset(trieLogStorage, secondTrieLogStorage);
    assertThat(secondWorldState.rootHash()).isEqualByComparingTo(worldState.rootHash());
  }

  @Test
  void rollForwardTwice() {
    final BonsaiWorldState worldState =
        new BonsaiWorldState(
            archive,
            new BonsaiWorldStateKeyValueStorage(
                provider, new NoOpMetricsSystem(), DataStorageConfiguration.DEFAULT_BONSAI_CONFIG),
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new PathBasedCodeCache());

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

    final BonsaiWorldState secondWorldState =
        new BonsaiWorldState(
            secondArchive,
            new BonsaiWorldStateKeyValueStorage(
                secondProvider,
                new NoOpMetricsSystem(),
                DataStorageConfiguration.DEFAULT_BONSAI_CONFIG),
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new PathBasedCodeCache());
    final BonsaiWorldStateUpdateAccumulator secondUpdater =
        (BonsaiWorldStateUpdateAccumulator) secondWorldState.updater();

    final TrieLogLayer layerOne = getTrieLogLayer(trieLogStorage, headerOne.getHash());
    secondUpdater.rollForward(layerOne);
    secondUpdater.commit();
    secondWorldState.persist(null);

    final TrieLogLayer layerTwo = getTrieLogLayer(trieLogStorage, headerTwo.getHash());
    secondUpdater.rollForward(layerTwo);
    secondUpdater.commit();
    secondWorldState.persist(null);

    assertKeyValueStorageEqual(accountStorage, secondAccountStorage);
    assertKeyValueStorageEqual(codeStorage, secondCodeStorage);
    assertKeyValueStorageEqual(storageStorage, secondStorageStorage);
    final KeyValueStorageTransaction tx = trieBranchStorage.startTransaction();
    tx.remove(BonsaiWorldStateKeyValueStorage.WORLD_BLOCK_HASH_KEY);
    tx.commit();
    assertKeyValueStorageEqual(trieBranchStorage, secondTrieBranchStorage);
    // trie logs won't be the same, we shouldn't generate logs on rolls.
    assertKeyValueSubset(trieLogStorage, secondTrieLogStorage);
    assertThat(secondWorldState.rootHash()).isEqualByComparingTo(worldState.rootHash());
  }

  @Test
  void rollBackOnce() {
    final BonsaiWorldState worldState =
        new BonsaiWorldState(
            archive,
            new BonsaiWorldStateKeyValueStorage(
                provider, new NoOpMetricsSystem(), DataStorageConfiguration.DEFAULT_BONSAI_CONFIG),
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new PathBasedCodeCache());

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
    final BonsaiWorldStateUpdateAccumulator firstRollbackUpdater =
        (BonsaiWorldStateUpdateAccumulator) worldState.updater();

    final TrieLogLayer layerTwo = getTrieLogLayer(trieLogStorage, headerTwo.getHash());
    firstRollbackUpdater.rollBack(layerTwo);

    worldState.persist(headerOne);

    final BonsaiWorldState secondWorldState =
        new BonsaiWorldState(
            secondArchive,
            new BonsaiWorldStateKeyValueStorage(
                secondProvider,
                new NoOpMetricsSystem(),
                DataStorageConfiguration.DEFAULT_BONSAI_CONFIG),
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new PathBasedCodeCache());

    final WorldUpdater secondUpdater = secondWorldState.updater();
    final MutableAccount secondMutableAccount =
        secondUpdater.createAccount(addressOne, 1, Wei.of(1L));
    secondMutableAccount.setCode(Bytes.of(0, 1, 2));
    secondMutableAccount.setStorageValue(UInt256.ONE, UInt256.ONE);
    secondUpdater.commit();

    secondWorldState.persist(null);

    assertKeyValueStorageEqual(accountStorage, secondAccountStorage);
    assertKeyValueStorageEqual(codeStorage, secondCodeStorage);
    assertKeyValueStorageEqual(storageStorage, secondStorageStorage);
    final KeyValueStorageTransaction tx = trieBranchStorage.startTransaction();
    tx.remove(BonsaiWorldStateKeyValueStorage.WORLD_BLOCK_HASH_KEY);
    tx.commit();
    assertKeyValueStorageEqual(trieBranchStorage, secondTrieBranchStorage);
    // trie logs won't be the same, we don't delete the roll back log
    assertKeyValueSubset(trieLogStorage, secondTrieLogStorage);
    assertThat(secondWorldState.rootHash()).isEqualByComparingTo(worldState.rootHash());
  }

  private TrieLogLayer getTrieLogLayer(final KeyValueStorage storage, final Hash key) {
    return storage
        .get(key.getBytes().toArrayUnsafe())
        .map(
            bytes ->
                BonsaiTrieLogFactory.readFrom(new BytesValueRLPInput(Bytes.wrap(bytes), false)))
        .get();
  }

  private static void assertKeyValueStorageEqual(
      final KeyValueStorage first, final KeyValueStorage second) {
    final var firstKeys =
        first.getAllKeysThat(k -> true).stream().map(Bytes::wrap).collect(Collectors.toSet());
    final var secondKeys =
        second.getAllKeysThat(k -> true).stream().map(Bytes::wrap).collect(Collectors.toSet());

    assertThat(secondKeys).isEqualTo(firstKeys);
    for (final Bytes key : firstKeys) {
      assertThat(Bytes.wrap(second.get(key.toArrayUnsafe()).get()))
          .isEqualByComparingTo(Bytes.wrap(first.get(key.toArrayUnsafe()).get()));
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
