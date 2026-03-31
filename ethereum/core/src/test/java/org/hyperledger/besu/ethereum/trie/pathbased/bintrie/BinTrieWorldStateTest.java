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
package org.hyperledger.besu.ethereum.trie.pathbased.bintrie;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig.createStatefulConfigWithTrie;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.storage.BinTrieWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.worldview.BinTrieWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.worldview.BinTrieWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.common.cache.PathBasedCachedWorldStorageManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.NoOpTrieLogManager;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BinTrieWorldStateTest {

  private final BlockHeaderTestFixture blockBuilder = new BlockHeaderTestFixture();
  private PathBasedCachedWorldStorageManager cachedWorldStorageManager;
  private BinTrieWorldStateKeyValueStorage worldStateKeyValueStorage;

  private static final Address ADDRESS_ONE =
      Address.fromHexString("0x1111111111111111111111111111111111111111");
  private static final Address ADDRESS_TWO =
      Address.fromHexString("0x2222222222222222222222222222222222222222");
  private static final Address ADDRESS_THREE =
      Address.fromHexString("0x3333333333333333333333333333333333333333");

  @BeforeEach
  void setUp() {
    cachedWorldStorageManager = mock(PathBasedCachedWorldStorageManager.class);
    worldStateKeyValueStorage =
        new BinTrieWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            DataStorageConfiguration.DEFAULT_BINTRIE_CONFIG);
  }

  @Test
  void shouldCreateEmptyWorldState() {
    final BinTrieWorldState worldState = createBinTrieWorldState();
    assertThat(worldState).isNotNull();
    assertThat(worldState.rootHash()).isNotNull();
  }

  @Test
  void shouldCreateAndPersistAccount() {
    final BinTrieWorldState worldState = createBinTrieWorldState();

    final WorldUpdater updater = worldState.updater();
    final MutableAccount account = updater.createAccount(ADDRESS_ONE, 0, Wei.of(1000));
    assertThat(account).isNotNull();
    assertThat(account.getAddress()).isEqualTo(ADDRESS_ONE);
    assertThat(account.getBalance()).isEqualTo(Wei.of(1000));
    updater.commit();

    final BlockHeader blockHeader = blockBuilder.number(0).buildHeader();
    worldState.persist(blockHeader);

    assertThat(worldState.rootHash()).isNotNull();
    assertThat(worldState.get(ADDRESS_ONE)).isNotNull();
    assertThat(worldState.get(ADDRESS_ONE).getBalance()).isEqualTo(Wei.of(1000));
  }

  @Test
  void shouldUpdateAccountBalance() {
    final BinTrieWorldState worldState = createBinTrieWorldState();

    WorldUpdater updater = worldState.updater();
    updater.createAccount(ADDRESS_ONE, 0, Wei.of(1000));
    updater.commit();

    BlockHeader blockHeader1 = blockBuilder.number(0).buildHeader();
    worldState.persist(blockHeader1);

    assertThat(worldState.get(ADDRESS_ONE)).isNotNull();
    assertThat(worldState.get(ADDRESS_ONE).getBalance()).isEqualTo(Wei.of(1000));

    updater = worldState.updater();
    MutableAccount account = updater.getAccount(ADDRESS_ONE);
    assertThat(account).isNotNull();
    account.setBalance(Wei.of(2000));
    updater.commit();

    BlockHeader blockHeader2 =
        blockBuilder.number(1).parentHash(blockHeader1.getHash()).buildHeader();
    worldState.persist(blockHeader2);

    assertThat(worldState.get(ADDRESS_ONE).getBalance()).isEqualTo(Wei.of(2000));
  }

  @Test
  void shouldHandleMultipleAccounts() {
    final BinTrieWorldState worldState = createBinTrieWorldState();

    final WorldUpdater updater = worldState.updater();
    updater.createAccount(ADDRESS_ONE, 0, Wei.of(1000));
    updater.createAccount(ADDRESS_TWO, 0, Wei.of(2000));
    updater.commit();

    final BlockHeader blockHeader = blockBuilder.number(0).buildHeader();
    worldState.persist(blockHeader);

    assertThat(worldState.get(ADDRESS_ONE)).isNotNull();
    assertThat(worldState.get(ADDRESS_ONE).getBalance()).isEqualTo(Wei.of(1000));
    assertThat(worldState.get(ADDRESS_TWO)).isNotNull();
    assertThat(worldState.get(ADDRESS_TWO).getBalance()).isEqualTo(Wei.of(2000));
  }

  @Test
  void shouldUpdateAccountNonce() {
    final BinTrieWorldState worldState = createBinTrieWorldState();

    WorldUpdater updater = worldState.updater();
    updater.createAccount(ADDRESS_ONE, 0, Wei.of(1000));
    updater.commit();

    BlockHeader blockHeader1 = blockBuilder.number(0).buildHeader();
    worldState.persist(blockHeader1);

    assertThat(worldState.get(ADDRESS_ONE).getNonce()).isEqualTo(0);

    updater = worldState.updater();
    MutableAccount account = updater.getAccount(ADDRESS_ONE);
    account.setNonce(5);
    updater.commit();

    BlockHeader blockHeader2 =
        blockBuilder.number(1).parentHash(blockHeader1.getHash()).buildHeader();
    worldState.persist(blockHeader2);

    assertThat(worldState.get(ADDRESS_ONE).getNonce()).isEqualTo(5);
  }

  @Test
  void shouldSetAndGetCode() {
    final BinTrieWorldState worldState = createBinTrieWorldState();
    final Bytes code = Bytes.of(0x60, 0x00, 0x60, 0x00, 0xF3);

    WorldUpdater updater = worldState.updater();
    MutableAccount account = updater.createAccount(ADDRESS_ONE, 0, Wei.of(1000));
    account.setCode(code);
    updater.commit();

    BlockHeader blockHeader = blockBuilder.number(0).buildHeader();
    worldState.persist(blockHeader);

    Account retrievedAccount = worldState.get(ADDRESS_ONE);
    assertThat(retrievedAccount).isNotNull();
    assertThat(retrievedAccount.getCodeHash()).isEqualTo(Hash.hash(code));
  }

  @Test
  void shouldSetAndGetStorage() {
    final BinTrieWorldState worldState = createBinTrieWorldState();

    WorldUpdater updater = worldState.updater();
    MutableAccount account = updater.createAccount(ADDRESS_ONE, 0, Wei.of(1000));
    account.setStorageValue(UInt256.ONE, UInt256.valueOf(100));
    account.setStorageValue(UInt256.valueOf(2), UInt256.valueOf(200));
    updater.commit();

    BlockHeader blockHeader = blockBuilder.number(0).buildHeader();
    worldState.persist(blockHeader);

    assertThat(worldState.get(ADDRESS_ONE)).isNotNull();
  }

  @Test
  void shouldDeleteAccount() {
    final BinTrieWorldState worldState = createBinTrieWorldState();

    WorldUpdater updater = worldState.updater();
    updater.createAccount(ADDRESS_ONE, 0, Wei.of(1000));
    updater.commit();

    BlockHeader blockHeader1 = blockBuilder.number(0).buildHeader();
    worldState.persist(blockHeader1);

    assertThat(worldState.get(ADDRESS_ONE)).isNotNull();

    updater = worldState.updater();
    updater.deleteAccount(ADDRESS_ONE);
    updater.commit();

    BlockHeader blockHeader2 =
        blockBuilder.number(1).parentHash(blockHeader1.getHash()).buildHeader();
    worldState.persist(blockHeader2);

    assertThat(worldState.get(ADDRESS_ONE)).isNull();
  }

  @Test
  void shouldHandleMultipleUpdatesToSameAccount() {
    final BinTrieWorldState worldState = createBinTrieWorldState();

    WorldUpdater updater = worldState.updater();
    MutableAccount account = updater.createAccount(ADDRESS_ONE, 0, Wei.of(1000));
    account.setNonce(1);
    account.setBalance(Wei.of(2000));
    account.setStorageValue(UInt256.ONE, UInt256.valueOf(100));
    updater.commit();

    BlockHeader blockHeader = blockBuilder.number(0).buildHeader();
    worldState.persist(blockHeader);

    Account retrieved = worldState.get(ADDRESS_ONE);
    assertThat(retrieved.getNonce()).isEqualTo(1);
    assertThat(retrieved.getBalance()).isEqualTo(Wei.of(2000));
  }

  @Test
  void shouldHandleMultipleBlockPersists() {
    final BinTrieWorldState worldState = createBinTrieWorldState();

    WorldUpdater updater = worldState.updater();
    updater.createAccount(ADDRESS_ONE, 0, Wei.of(1000));
    updater.commit();
    BlockHeader blockHeader1 = blockBuilder.number(0).buildHeader();
    worldState.persist(blockHeader1);

    updater = worldState.updater();
    updater.createAccount(ADDRESS_TWO, 0, Wei.of(2000));
    updater.commit();
    BlockHeader blockHeader2 =
        blockBuilder.number(1).parentHash(blockHeader1.getHash()).buildHeader();
    worldState.persist(blockHeader2);

    updater = worldState.updater();
    updater.createAccount(ADDRESS_THREE, 0, Wei.of(3000));
    updater.commit();
    BlockHeader blockHeader3 =
        blockBuilder.number(2).parentHash(blockHeader2.getHash()).buildHeader();
    worldState.persist(blockHeader3);

    assertThat(worldState.get(ADDRESS_ONE).getBalance()).isEqualTo(Wei.of(1000));
    assertThat(worldState.get(ADDRESS_TWO).getBalance()).isEqualTo(Wei.of(2000));
    assertThat(worldState.get(ADDRESS_THREE).getBalance()).isEqualTo(Wei.of(3000));
  }

  @Test
  void shouldReturnNullForNonExistentAccount() {
    final BinTrieWorldState worldState = createBinTrieWorldState();
    assertThat(worldState.get(ADDRESS_ONE)).isNull();
  }

  @Test
  void shouldHandleEmptyCode() {
    final BinTrieWorldState worldState = createBinTrieWorldState();

    WorldUpdater updater = worldState.updater();
    MutableAccount account = updater.createAccount(ADDRESS_ONE, 0, Wei.of(1000));
    account.setCode(Bytes.EMPTY);
    updater.commit();

    BlockHeader blockHeader = blockBuilder.number(0).buildHeader();
    worldState.persist(blockHeader);

    Account retrieved = worldState.get(ADDRESS_ONE);
    assertThat(retrieved).isNotNull();
    assertThat(retrieved.getCodeHash()).isEqualTo(Hash.EMPTY);
  }

  @Test
  void shouldHandleZeroBalance() {
    final BinTrieWorldState worldState = createBinTrieWorldState();

    WorldUpdater updater = worldState.updater();
    updater.createAccount(ADDRESS_ONE, 0, Wei.ZERO);
    updater.commit();

    BlockHeader blockHeader = blockBuilder.number(0).buildHeader();
    worldState.persist(blockHeader);

    Account retrieved = worldState.get(ADDRESS_ONE);
    assertThat(retrieved).isNotNull();
    assertThat(retrieved.getBalance()).isEqualTo(Wei.ZERO);
  }

  @Test
  void shouldGetAccumulatorCopy() {
    final BinTrieWorldState worldState = createBinTrieWorldState();

    WorldUpdater updater = worldState.updater();
    updater.createAccount(ADDRESS_ONE, 0, Wei.of(1000));
    updater.commit();

    BinTrieWorldStateUpdateAccumulator accumulator =
        (BinTrieWorldStateUpdateAccumulator) worldState.updater();
    BinTrieWorldStateUpdateAccumulator copy =
        (BinTrieWorldStateUpdateAccumulator) accumulator.copy();

    assertThat(copy).isNotNull();
    assertThat(copy).isNotSameAs(accumulator);
  }

  @Test
  void shouldFreezeStorage() {
    final BinTrieWorldState worldState = createBinTrieWorldState();

    WorldUpdater updater = worldState.updater();
    updater.createAccount(ADDRESS_ONE, 0, Wei.of(1000));
    updater.commit();

    BlockHeader blockHeader = blockBuilder.number(0).buildHeader();
    worldState.persist(blockHeader);

    worldState.freezeStorage();

    assertThat(worldState.get(ADDRESS_ONE)).isNotNull();
  }

  @Test
  void shouldGetFrontierRootHash() {
    final BinTrieWorldState worldState = createBinTrieWorldState();

    WorldUpdater updater = worldState.updater();
    updater.createAccount(ADDRESS_ONE, 0, Wei.of(1000));
    updater.commit();

    Hash frontierHash = worldState.frontierRootHash();
    assertThat(frontierHash).isNotNull();
  }

  @Test
  void shouldGetStorageValue() {
    final BinTrieWorldState worldState = createBinTrieWorldState();

    WorldUpdater updater = worldState.updater();
    MutableAccount account = updater.createAccount(ADDRESS_ONE, 0, Wei.of(1000));
    account.setStorageValue(UInt256.ONE, UInt256.valueOf(42));
    updater.commit();

    BlockHeader blockHeader = blockBuilder.number(0).buildHeader();
    worldState.persist(blockHeader);

    UInt256 value = worldState.getStorageValue(ADDRESS_ONE, UInt256.ONE);
    assertThat(value).isEqualTo(UInt256.valueOf(42));
  }

  @Test
  void shouldReturnZeroForNonExistentStorageSlot() {
    final BinTrieWorldState worldState = createBinTrieWorldState();

    WorldUpdater updater = worldState.updater();
    updater.createAccount(ADDRESS_ONE, 0, Wei.of(1000));
    updater.commit();

    BlockHeader blockHeader = blockBuilder.number(0).buildHeader();
    worldState.persist(blockHeader);

    UInt256 value = worldState.getStorageValue(ADDRESS_ONE, UInt256.valueOf(999));
    assertThat(value).isEqualTo(UInt256.ZERO);
  }

  private BinTrieWorldState createBinTrieWorldState() {
    return new BinTrieWorldState(
        worldStateKeyValueStorage,
        cachedWorldStorageManager,
        new NoOpTrieLogManager(),
        EvmConfiguration.DEFAULT,
        createStatefulConfigWithTrie(),
        new CodeCache());
  }
}
