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
package org.hyperledger.besu.ethereum.trie.forest.worldview;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryWorldState;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.storage.keyvalue.WorldStatePreimageKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.evm.account.AccountStorageEntry;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldState;
import org.hyperledger.besu.evm.worldstate.WorldState.StreamableAccount;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

// TODO: make that an abstract mutable world state test, and create sub-class for all world state
// implementations.
class ForestMutableWorldStateTest {
  // The following test cases are loosely derived from the testTransactionToItself
  // GeneralStateReferenceTest.

  private static final Address ADDRESS =
      Address.fromHexString("0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b");

  private static MutableWorldState createEmpty(final ForestWorldStateKeyValueStorage storage) {
    final WorldStatePreimageKeyValueStorage preimageStorage =
        new WorldStatePreimageKeyValueStorage(new InMemoryKeyValueStorage());
    return new ForestMutableWorldState(storage, preimageStorage, EvmConfiguration.DEFAULT);
  }

  private static MutableWorldState createEmpty() {
    return createInMemoryWorldState();
  }

  @Test
  void rootHash_Empty() {
    final MutableWorldState worldState = createEmpty();
    assertThat(worldState.rootHash().getBytes()).isEqualTo(MerkleTrie.EMPTY_TRIE_NODE_HASH);

    worldState.persist(null);
    assertThat(worldState.rootHash().getBytes()).isEqualTo(MerkleTrie.EMPTY_TRIE_NODE_HASH);
  }

  @Test
  void containsAccount_AccountDoesNotExist() {
    final WorldState worldState = createEmpty();
    assertThat(worldState.get(ADDRESS)).isNull();
  }

  @Test
  void containsAccount_AccountExists() {
    final MutableWorldState worldState = createEmpty();
    final WorldUpdater updater = worldState.updater();
    updater.createAccount(ADDRESS).setBalance(Wei.of(100000));
    updater.commit();
    assertThat(worldState.get(ADDRESS)).isNotNull();
    assertThat(worldState.rootHash())
        .isEqualTo(
            Hash.fromHexString(
                "0xa3e1c133a5a51b03399ed9ad0380f3182e9e18322f232b816dd4b9094f871e1b"));
  }

  @Test
  void removeAccount_AccountDoesNotExist() {
    final MutableWorldState worldState = createEmpty();
    final WorldUpdater updater = worldState.updater();
    updater.deleteAccount(ADDRESS);
    updater.commit();
    assertThat(worldState.rootHash().getBytes()).isEqualTo(MerkleTrie.EMPTY_TRIE_NODE_HASH);

    worldState.persist(null);
    assertThat(worldState.rootHash().getBytes()).isEqualTo(MerkleTrie.EMPTY_TRIE_NODE_HASH);
  }

  @Test
  void removeAccount_UpdatedAccount() {
    final MutableWorldState worldState = createEmpty();
    final WorldUpdater updater = worldState.updater();
    updater.createAccount(ADDRESS).setBalance(Wei.of(100000));
    updater.deleteAccount(ADDRESS);
    updater.commit();
    assertThat(worldState.rootHash().getBytes()).isEqualTo(MerkleTrie.EMPTY_TRIE_NODE_HASH);

    worldState.persist(null);
    assertThat(worldState.rootHash().getBytes()).isEqualTo(MerkleTrie.EMPTY_TRIE_NODE_HASH);
  }

  @Test
  void removeAccount_AccountExists() {
    // Create a world state with one account
    final MutableWorldState worldState = createEmpty();
    WorldUpdater updater = worldState.updater();
    updater.createAccount(ADDRESS).setBalance(Wei.of(100000));
    updater.commit();
    assertThat(worldState.get(ADDRESS)).isNotNull();
    assertThat(worldState.rootHash()).isNotEqualTo(MerkleTrie.EMPTY_TRIE_NODE_HASH);

    // Delete account
    updater = worldState.updater();
    updater.deleteAccount(ADDRESS);
    assertThat(updater.get(ADDRESS)).isNull();
    assertThat(updater.getAccount(ADDRESS)).isNull();
    updater.commit();
    assertThat(updater.get(ADDRESS)).isNull();

    assertThat(worldState.rootHash().getBytes()).isEqualTo(MerkleTrie.EMPTY_TRIE_NODE_HASH);
  }

  @Test
  void removeAccount_AccountExistsAndIsPersisted() {
    // Create a world state with one account
    final MutableWorldState worldState = createEmpty();
    WorldUpdater updater = worldState.updater();
    updater.createAccount(ADDRESS).setBalance(Wei.of(100000));
    updater.commit();
    worldState.persist(null);
    assertThat(worldState.get(ADDRESS)).isNotNull();
    assertThat(worldState.rootHash()).isNotEqualTo(MerkleTrie.EMPTY_TRIE_NODE_HASH);

    // Delete account
    updater = worldState.updater();
    updater.deleteAccount(ADDRESS);
    assertThat(updater.get(ADDRESS)).isNull();
    assertThat(updater.getAccount(ADDRESS)).isNull();
    // Check account is gone after committing
    updater.commit();
    assertThat(updater.get(ADDRESS)).isNull();
    // And after persisting
    worldState.persist(null);
    assertThat(updater.get(ADDRESS)).isNull();

    assertThat(worldState.rootHash().getBytes()).isEqualTo(MerkleTrie.EMPTY_TRIE_NODE_HASH);
  }

  @Test
  void streamAccounts_empty() {
    final MutableWorldState worldState = createEmpty();
    final Stream<StreamableAccount> accounts = worldState.streamAccounts(Bytes32.ZERO, 10);
    assertThat(accounts.count()).isZero();
  }

  @Test
  void streamAccounts_singleAccount() {
    final MutableWorldState worldState = createEmpty();
    final WorldUpdater updater = worldState.updater();
    updater.createAccount(ADDRESS).setBalance(Wei.of(100000));
    updater.commit();

    List<StreamableAccount> accounts =
        worldState.streamAccounts(Bytes32.ZERO, 10).collect(Collectors.toList());
    assertThat(accounts).hasSize(1);
    assertThat(accounts.getFirst().getAddress()).hasValue(ADDRESS);
    assertThat(accounts.getFirst().getBalance()).isEqualTo(Wei.of(100000));

    // Check again after persisting
    worldState.persist(null);
    accounts = worldState.streamAccounts(Bytes32.ZERO, 10).collect(Collectors.toList());
    assertThat(accounts).hasSize(1);
    assertThat(accounts.getFirst().getAddress()).hasValue(ADDRESS);
    assertThat(accounts.getFirst().getBalance()).isEqualTo(Wei.of(100000));
  }

  @Test
  void streamAccounts_multipleAccounts() {
    final Address addr1 = Address.fromHexString("0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b");
    final Address addr2 = Address.fromHexString("0xa94f5374fce5edbc8e2a8697c15331677e6ebf0c");

    final MutableWorldState worldState = createEmpty();
    final WorldUpdater updater = worldState.updater();

    // Create an account
    final MutableAccount accountA = updater.createAccount(addr1);
    accountA.setBalance(Wei.of(100000));
    // Create another
    final MutableAccount accountB = updater.createAccount(addr2);
    accountB.setNonce(1);
    // Commit changes
    updater.commit();

    final boolean accountAIsFirst =
        accountA
                .getAddressHash()
                .getBytes()
                .toUnsignedBigInteger()
                .compareTo(accountB.getAddressHash().getBytes().toUnsignedBigInteger())
            < 0;
    final Hash startHash = accountAIsFirst ? accountA.getAddressHash() : accountB.getAddressHash();

    // Get first account
    final List<StreamableAccount> firstAccount =
        worldState.streamAccounts(Bytes32.wrap(startHash.getBytes()), 1).toList();
    assertThat(firstAccount).hasSize(1);
    assertThat(firstAccount.getFirst().getAddress())
        .hasValue(accountAIsFirst ? accountA.getAddress() : accountB.getAddress());

    // Get both accounts
    final List<StreamableAccount> allAccounts = worldState.streamAccounts(Bytes32.ZERO, 2).toList();
    assertThat(allAccounts).hasSize(2);
    assertThat(allAccounts.get(0).getAddress())
        .hasValue(accountAIsFirst ? accountA.getAddress() : accountB.getAddress());
    assertThat(allAccounts.get(1).getAddress())
        .hasValue(accountAIsFirst ? accountB.getAddress() : accountA.getAddress());

    // Get second account
    final Bytes32 startHashForSecondAccount = UInt256.fromBytes(startHash.getBytes()).add(1L);
    final List<StreamableAccount> secondAccount =
        worldState.streamAccounts(startHashForSecondAccount, 100).toList();
    assertThat(secondAccount).hasSize(1);
    assertThat(secondAccount.getFirst().getAddress())
        .hasValue(accountAIsFirst ? accountB.getAddress() : accountA.getAddress());
  }

  @Test
  void commitAndPersist() {
    final KeyValueStorage storage = new InMemoryKeyValueStorage();
    final ForestWorldStateKeyValueStorage kvWorldStateStorage =
        new ForestWorldStateKeyValueStorage(storage);
    final MutableWorldState worldState = createEmpty(kvWorldStateStorage);
    final WorldUpdater updater = worldState.updater();
    final Wei newBalance = Wei.of(100000);
    final Hash expectedRootHash =
        Hash.fromHexString("0xa3e1c133a5a51b03399ed9ad0380f3182e9e18322f232b816dd4b9094f871e1b");

    // Update account and assert we get the expected response from updater
    updater.createAccount(ADDRESS).setBalance(newBalance);
    assertThat(updater.get(ADDRESS)).isNotNull();
    assertThat(updater.get(ADDRESS).getBalance()).isEqualTo(newBalance);

    // Commit and check assertions
    updater.commit();
    assertThat(worldState.rootHash()).isEqualTo(expectedRootHash);
    assertThat(worldState.get(ADDRESS)).isNotNull();
    assertThat(worldState.get(ADDRESS).getBalance()).isEqualTo(newBalance);

    // Check that storage is empty before persisting
    assertThat(
            kvWorldStateStorage.isWorldStateAvailable(
                Bytes32.wrap(worldState.rootHash().getBytes())))
        .isFalse();

    // Persist and re-run assertions
    worldState.persist(null);

    assertThat(
            kvWorldStateStorage.isWorldStateAvailable(
                Bytes32.wrap(worldState.rootHash().getBytes())))
        .isTrue();
    assertThat(worldState.rootHash()).isEqualTo(expectedRootHash);
    assertThat(worldState.get(ADDRESS)).isNotNull();
    assertThat(worldState.get(ADDRESS).getBalance()).isEqualTo(newBalance);

    // Create new world state and check that it can access modified address
    final MutableWorldState newWorldState =
        new ForestMutableWorldState(
            Bytes32.wrap(expectedRootHash.getBytes()),
            new ForestWorldStateKeyValueStorage(storage),
            new WorldStatePreimageKeyValueStorage(new InMemoryKeyValueStorage()),
            EvmConfiguration.DEFAULT);
    assertThat(newWorldState.rootHash()).isEqualTo(expectedRootHash);
    assertThat(newWorldState.get(ADDRESS)).isNotNull();
    assertThat(newWorldState.get(ADDRESS).getBalance()).isEqualTo(newBalance);
  }

  @Test
  void getAccountNonce_AccountExists() {
    final MutableWorldState worldState = createEmpty();
    final WorldUpdater updater = worldState.updater();
    updater.createAccount(ADDRESS).setNonce(1L);
    updater.commit();
    assertThat(worldState.get(ADDRESS).getNonce()).isEqualTo(1L);
    assertThat(worldState.rootHash())
        .isEqualTo(
            Hash.fromHexString(
                "0x9648b05cc2eef5513ae2edfe16bfcedb3d1c60ffb5dff3fc501bd3e4ae39f536"));
  }

  @Test
  void replaceAccountNonce() {
    final MutableWorldState worldState = createEmpty();
    final WorldUpdater updater = worldState.updater();
    final MutableAccount account = updater.createAccount(ADDRESS);
    account.setNonce(1L);
    account.setNonce(2L);
    updater.commit();
    assertThat(worldState.get(ADDRESS).getNonce()).isEqualTo(2L);
    assertThat(worldState.rootHash())
        .isEqualTo(
            Hash.fromHexString(
                "0x7f64d13e61301a5154a5f06483a38572629e977b316cbe5a28b5f0522010a4bf"));
  }

  @Test
  void getAccountBalance_AccountExists() {
    final MutableWorldState worldState = createEmpty();
    final WorldUpdater updater = worldState.updater();
    updater.createAccount(ADDRESS).setBalance(Wei.of(100000));
    updater.commit();
    assertThat(worldState.get(ADDRESS).getBalance()).isEqualTo(Wei.of(100000));
  }

  @Test
  void replaceAccountBalance() {
    final MutableWorldState worldState = createEmpty();
    final WorldUpdater updater = worldState.updater();
    final MutableAccount account = updater.createAccount(ADDRESS);
    account.setBalance(Wei.of(100000));
    account.setBalance(Wei.of(200000));
    updater.commit();
    assertThat(worldState.get(ADDRESS).getBalance()).isEqualTo(Wei.of(200000));
    assertThat(worldState.rootHash())
        .isEqualTo(
            Hash.fromHexString(
                "0xbfa4e0598cc2b810a8ccc4a2d9a4c575574d05c9c4a7f915e6b8545953a5051e"));
  }

  @Test
  void setStorageValue_ZeroValue() {
    final MutableWorldState worldState = createEmpty();
    final WorldUpdater updater = worldState.updater();
    final MutableAccount account = updater.createAccount(ADDRESS);
    account.setBalance(Wei.of(100000));
    account.setStorageValue(UInt256.ZERO, UInt256.ZERO);
    updater.commit();
    assertThat(worldState.get(ADDRESS).getStorageValue(UInt256.ZERO)).isEqualTo(UInt256.ZERO);
    assertThat(worldState.rootHash())
        .isEqualTo(
            Hash.fromHexString(
                "0xa3e1c133a5a51b03399ed9ad0380f3182e9e18322f232b816dd4b9094f871e1b"));
  }

  @Test
  void setStorageValue_NonzeroValue() {
    final MutableWorldState worldState = createEmpty();
    final WorldUpdater updater = worldState.updater();
    final MutableAccount account = updater.createAccount(ADDRESS);
    account.setBalance(Wei.of(100000));
    account.setStorageValue(UInt256.ONE, UInt256.valueOf(2));
    updater.commit();
    assertThat(worldState.get(ADDRESS).getStorageValue(UInt256.ONE)).isEqualTo(UInt256.valueOf(2));
    assertThat(worldState.rootHash())
        .isEqualTo(
            Hash.fromHexString(
                "0xd31ce0bf3bf8790083a8ebde418244fda3b1cca952d7119ed244f86d03044656"));
  }

  @Test
  void replaceStorageValue_NonzeroValue() {
    final MutableWorldState worldState = createEmpty();
    final WorldUpdater updater = worldState.updater();
    final MutableAccount account = updater.createAccount(ADDRESS);
    account.setBalance(Wei.of(100000));
    account.setStorageValue(UInt256.ONE, UInt256.valueOf(2));
    account.setStorageValue(UInt256.ONE, UInt256.valueOf(3));
    updater.commit();
    assertThat(worldState.get(ADDRESS).getStorageValue(UInt256.ONE)).isEqualTo(UInt256.valueOf(3));
    assertThat(worldState.rootHash())
        .isEqualTo(
            Hash.fromHexString(
                "0x1d0ddb5079fe5b8689124b68c9e5bb3f4d8e13c2f7489d24f088c78fd45e058d"));
  }

  @Test
  void replaceStorageValue_ZeroValue() {
    final MutableWorldState worldState = createEmpty();
    final WorldUpdater updater = worldState.updater();
    final MutableAccount account = updater.createAccount(ADDRESS);
    account.setBalance(Wei.of(100000));
    account.setStorageValue(UInt256.ONE, UInt256.valueOf(2));
    account.setStorageValue(UInt256.ONE, UInt256.ZERO);
    updater.commit();
    assertThat(worldState.rootHash())
        .isEqualTo(
            Hash.fromHexString(
                "0xa3e1c133a5a51b03399ed9ad0380f3182e9e18322f232b816dd4b9094f871e1b"));
  }

  @Test
  void getOriginalStorageValue() {
    final MutableWorldState worldState = createEmpty();
    final WorldUpdater setupUpdater = worldState.updater();
    final MutableAccount setupAccount = setupUpdater.createAccount(ADDRESS);
    setupAccount.setStorageValue(UInt256.ONE, UInt256.valueOf(2));
    setupUpdater.commit();

    final WorldUpdater updater = worldState.updater();
    final MutableAccount account = updater.getOrCreate(ADDRESS);
    assertThat(account.getOriginalStorageValue(UInt256.ONE)).isEqualTo(UInt256.valueOf(2));

    account.setStorageValue(UInt256.ONE, UInt256.valueOf(3));
    assertThat(account.getOriginalStorageValue(UInt256.ONE)).isEqualTo(UInt256.valueOf(2));
  }

  @Test
  void originalStorageValueIsAlwaysZeroIfStorageWasCleared() {
    final MutableWorldState worldState = createEmpty();
    final WorldUpdater setupUpdater = worldState.updater();
    final MutableAccount setupAccount = setupUpdater.createAccount(ADDRESS);
    setupAccount.setStorageValue(UInt256.ONE, UInt256.valueOf(2));
    setupUpdater.commit();

    final WorldUpdater updater = worldState.updater();
    final MutableAccount account = updater.getOrCreate(ADDRESS);

    account.clearStorage();
    assertThat(account.getOriginalStorageValue(UInt256.ONE)).isEqualTo(UInt256.ZERO);
  }

  @Test
  void clearStorage() {
    final UInt256 storageKey = UInt256.ONE;
    final UInt256 storageValue = UInt256.valueOf(2L);

    // Create a world state with one account
    final MutableWorldState worldState = createEmpty();
    final WorldUpdater updater = worldState.updater();
    MutableAccount account = updater.createAccount(ADDRESS);
    account.setBalance(Wei.of(100000));
    account.setStorageValue(storageKey, storageValue);
    assertThat(account.getStorageValue(storageKey)).isEqualTo(storageValue);

    // Clear storage
    account = updater.getAccount(ADDRESS);
    assertThat(account).isNotNull();
    assertThat(account.getStorageValue(storageKey)).isEqualTo(storageValue);
    account.clearStorage();
    assertThat(account.getStorageValue(storageKey)).isEqualTo(UInt256.ZERO);
    assertThat(updater.get(ADDRESS).getStorageValue(storageKey)).isEqualTo(UInt256.ZERO);

    // Check storage is cleared after committing
    updater.commit();
    assertThat(updater.getAccount(ADDRESS).getStorageValue(storageKey)).isEqualTo(UInt256.ZERO);
    assertThat(updater.get(ADDRESS).getStorageValue(storageKey)).isEqualTo(UInt256.ZERO);
    assertThat(worldState.get(ADDRESS).getStorageValue(storageKey)).isEqualTo(UInt256.ZERO);

    // And after persisting
    assertThat(updater.getAccount(ADDRESS).getStorageValue(storageKey)).isEqualTo(UInt256.ZERO);
    assertThat(updater.get(ADDRESS).getStorageValue(storageKey)).isEqualTo(UInt256.ZERO);
    assertThat(worldState.get(ADDRESS).getStorageValue(storageKey)).isEqualTo(UInt256.ZERO);
  }

  @Test
  void clearStorage_AfterPersisting() {
    final UInt256 storageKey = UInt256.ONE;
    final UInt256 storageValue = UInt256.valueOf(2L);

    // Create a world state with one account
    final MutableWorldState worldState = createEmpty();
    final WorldUpdater updater = worldState.updater();
    MutableAccount account = updater.createAccount(ADDRESS);
    account.setBalance(Wei.of(100000));
    account.setStorageValue(storageKey, storageValue);
    updater.commit();
    worldState.persist(null);
    assertThat(worldState.get(ADDRESS)).isNotNull();
    assertThat(worldState.rootHash()).isNotEqualTo(MerkleTrie.EMPTY_TRIE_NODE_HASH);

    // Clear storage
    account = updater.getAccount(ADDRESS);
    assertThat(account).isNotNull();
    assertThat(account.getStorageValue(storageKey)).isEqualTo(storageValue);
    account.clearStorage();
    assertThat(account.getStorageValue(storageKey)).isEqualTo(UInt256.ZERO);
    assertThat(updater.get(ADDRESS).getStorageValue(storageKey)).isEqualTo(UInt256.ZERO);
    assertThat(worldState.get(ADDRESS).getStorageValue(storageKey)).isEqualTo(storageValue);

    // Check storage is cleared after committing
    updater.commit();
    assertThat(updater.getAccount(ADDRESS).getStorageValue(storageKey)).isEqualTo(UInt256.ZERO);
    assertThat(updater.get(ADDRESS).getStorageValue(storageKey)).isEqualTo(UInt256.ZERO);
    assertThat(worldState.get(ADDRESS).getStorageValue(storageKey)).isEqualTo(UInt256.ZERO);

    // And after persisting
    assertThat(updater.getAccount(ADDRESS).getStorageValue(storageKey)).isEqualTo(UInt256.ZERO);
    assertThat(updater.get(ADDRESS).getStorageValue(storageKey)).isEqualTo(UInt256.ZERO);
    assertThat(worldState.get(ADDRESS).getStorageValue(storageKey)).isEqualTo(UInt256.ZERO);
  }

  @Test
  void clearStorageThenEdit() {
    final UInt256 storageKey = UInt256.ONE;
    final UInt256 originalStorageValue = UInt256.valueOf(2L);
    final UInt256 newStorageValue = UInt256.valueOf(3L);

    // Create a world state with one account
    final MutableWorldState worldState = createEmpty();
    final WorldUpdater updater = worldState.updater();
    MutableAccount account = updater.createAccount(ADDRESS);
    account.setBalance(Wei.of(100000));
    account.setStorageValue(storageKey, originalStorageValue);
    assertThat(account.getStorageValue(storageKey)).isEqualTo(originalStorageValue);

    // Clear storage then edit
    account = updater.getAccount(ADDRESS);
    assertThat(account).isNotNull();
    assertThat(account.getStorageValue(storageKey)).isEqualTo(originalStorageValue);
    assertThat(updater.get(ADDRESS).getStorageValue(storageKey)).isEqualTo(originalStorageValue);
    account.clearStorage();
    account.setStorageValue(storageKey, newStorageValue);
    assertThat(account.getStorageValue(storageKey)).isEqualTo(newStorageValue);

    // Check storage is cleared after committing
    updater.commit();
    assertThat(updater.getAccount(ADDRESS).getStorageValue(storageKey)).isEqualTo(newStorageValue);
    assertThat(updater.get(ADDRESS).getStorageValue(storageKey)).isEqualTo(newStorageValue);
    assertThat(worldState.get(ADDRESS).getStorageValue(storageKey)).isEqualTo(newStorageValue);

    // And after persisting
    assertThat(updater.getAccount(ADDRESS).getStorageValue(storageKey)).isEqualTo(newStorageValue);
    assertThat(updater.get(ADDRESS).getStorageValue(storageKey)).isEqualTo(newStorageValue);
    assertThat(worldState.get(ADDRESS).getStorageValue(storageKey)).isEqualTo(newStorageValue);
  }

  @Test
  void clearStorageThenEditAfterPersisting() {
    final UInt256 storageKey = UInt256.ONE;
    final UInt256 originalStorageValue = UInt256.valueOf(2L);
    final UInt256 newStorageValue = UInt256.valueOf(3L);

    // Create a world state with one account
    final MutableWorldState worldState = createEmpty();
    final WorldUpdater updater = worldState.updater();
    MutableAccount account = updater.createAccount(ADDRESS);
    account.setBalance(Wei.of(100000));
    account.setStorageValue(storageKey, originalStorageValue);
    assertThat(account.getStorageValue(storageKey)).isEqualTo(originalStorageValue);
    updater.commit();
    worldState.persist(null);

    // Clear storage then edit
    account = updater.getAccount(ADDRESS);
    assertThat(account).isNotNull();
    assertThat(account.getStorageValue(storageKey)).isEqualTo(originalStorageValue);
    assertThat(updater.get(ADDRESS).getStorageValue(storageKey)).isEqualTo(originalStorageValue);
    account.clearStorage();
    account.setStorageValue(storageKey, newStorageValue);
    assertThat(account.getStorageValue(storageKey)).isEqualTo(newStorageValue);
    assertThat(worldState.get(ADDRESS).getStorageValue(storageKey)).isEqualTo(originalStorageValue);

    // Check storage is cleared after committing
    updater.commit();
    assertThat(updater.getAccount(ADDRESS).getStorageValue(storageKey)).isEqualTo(newStorageValue);
    assertThat(updater.get(ADDRESS).getStorageValue(storageKey)).isEqualTo(newStorageValue);
    assertThat(worldState.get(ADDRESS).getStorageValue(storageKey)).isEqualTo(newStorageValue);

    // And after persisting
    assertThat(updater.getAccount(ADDRESS).getStorageValue(storageKey)).isEqualTo(newStorageValue);
    assertThat(updater.get(ADDRESS).getStorageValue(storageKey)).isEqualTo(newStorageValue);
    assertThat(worldState.get(ADDRESS).getStorageValue(storageKey)).isEqualTo(newStorageValue);
  }

  @Test
  void replaceAccountCode() {
    final MutableWorldState worldState = createEmpty();
    final WorldUpdater updater = worldState.updater();
    final MutableAccount account = updater.createAccount(ADDRESS);
    account.setBalance(Wei.of(100000));
    account.setCode(Bytes.of(1, 2, 3));
    account.setCode(Bytes.of(3, 2, 1));
    updater.commit();
    assertThat(worldState.get(ADDRESS).getCode()).isEqualTo(Bytes.of(3, 2, 1));
    assertThat(worldState.rootHash())
        .isEqualTo(
            Hash.fromHexString(
                "0xc14f5e30581de9155ea092affa665fad83bcd9f98e45c4a42885b9b36d939702"));
  }

  @Test
  void revert() {
    final MutableWorldState worldState = createEmpty();
    final WorldUpdater updater1 = worldState.updater();
    final MutableAccount account1 = updater1.createAccount(ADDRESS);
    account1.setBalance(Wei.of(200000));
    updater1.commit();

    final WorldUpdater updater2 = worldState.updater();
    final MutableAccount account2 = updater2.getAccount(ADDRESS);
    account2.setBalance(Wei.of(300000));
    assertThat(updater2.get(ADDRESS).getBalance()).isEqualTo(Wei.of(300000));

    updater2.revert();
    assertThat(updater2.get(ADDRESS).getBalance()).isEqualTo(Wei.of(200000));

    updater2.commit();
    assertThat(worldState.get(ADDRESS).getBalance()).isEqualTo(Wei.of(200000));

    assertThat(worldState.rootHash())
        .isEqualTo(
            Hash.fromHexString(
                "0xbfa4e0598cc2b810a8ccc4a2d9a4c575574d05c9c4a7f915e6b8545953a5051e"));
  }

  @Test
  void shouldReturnNullForGetMutableWhenAccountDeletedInAncestor() {
    final MutableWorldState worldState = createEmpty();
    final WorldUpdater updater1 = worldState.updater();
    final MutableAccount account1 = updater1.createAccount(ADDRESS);
    updater1.commit();
    assertThat(updater1.get(ADDRESS))
        .isEqualToComparingOnlyGivenFields(account1, "address", "nonce", "balance", "codeHash");
    updater1.deleteAccount(ADDRESS);

    final WorldUpdater updater2 = updater1.updater();
    assertThat(updater2.get(ADDRESS)).isNull();

    final WorldUpdater updater3 = updater2.updater();
    assertThat(updater3.getAccount(ADDRESS)).isNull();
  }

  @Test
  void shouldCombineUnchangedAndChangedValuesWhenRetrievingStorageEntries() {
    final MutableWorldState worldState = createEmpty();
    WorldUpdater updater = worldState.updater();
    MutableAccount account = updater.createAccount(ADDRESS);
    account.setBalance(Wei.of(100000));
    account.setStorageValue(UInt256.ONE, UInt256.valueOf(2));
    account.setStorageValue(UInt256.valueOf(2), UInt256.valueOf(5));
    updater.commit();

    final List<AccountStorageEntry> initialSetOfEntries = new ArrayList<>();
    initialSetOfEntries.add(AccountStorageEntry.forKeyAndValue(UInt256.ONE, UInt256.valueOf(2)));
    initialSetOfEntries.add(
        AccountStorageEntry.forKeyAndValue(UInt256.valueOf(2), UInt256.valueOf(5)));
    final Map<Bytes32, AccountStorageEntry> initialEntries = new TreeMap<>();
    initialSetOfEntries.forEach(entry -> initialEntries.put(entry.getKeyHash(), entry));

    updater = worldState.updater();
    account = updater.getAccount(ADDRESS);
    account.setStorageValue(UInt256.ONE, UInt256.valueOf(3));
    account.setStorageValue(UInt256.valueOf(3), UInt256.valueOf(6));

    final List<AccountStorageEntry> finalSetOfEntries = new ArrayList<>();
    finalSetOfEntries.add(AccountStorageEntry.forKeyAndValue(UInt256.ONE, UInt256.valueOf(3)));
    finalSetOfEntries.add(
        AccountStorageEntry.forKeyAndValue(UInt256.valueOf(2), UInt256.valueOf(5)));
    finalSetOfEntries.add(
        AccountStorageEntry.forKeyAndValue(UInt256.valueOf(3), UInt256.valueOf(6)));
    final Map<Bytes32, AccountStorageEntry> finalEntries = new TreeMap<>();
    finalSetOfEntries.forEach(entry -> finalEntries.put(entry.getKeyHash(), entry));

    assertThat(account.storageEntriesFrom(Bytes32.wrap(Hash.ZERO.getBytes()), 10))
        .isEqualTo(finalEntries);
    assertThat(updater.get(ADDRESS).storageEntriesFrom(Bytes32.wrap(Hash.ZERO.getBytes()), 10))
        .isEqualTo(finalEntries);
    assertThat(worldState.get(ADDRESS).storageEntriesFrom(Bytes32.wrap(Hash.ZERO.getBytes()), 10))
        .isEqualTo(initialEntries);

    worldState.persist(null);
    assertThat(updater.get(ADDRESS).storageEntriesFrom(Bytes32.wrap(Hash.ZERO.getBytes()), 10))
        .isEqualTo(finalEntries);
    assertThat(worldState.get(ADDRESS).storageEntriesFrom(Bytes32.wrap(Hash.ZERO.getBytes()), 10))
        .isEqualTo(initialEntries);

    updater.commit();
    assertThat(worldState.get(ADDRESS).storageEntriesFrom(Bytes32.wrap(Hash.ZERO.getBytes()), 10))
        .isEqualTo(finalEntries);

    worldState.persist(null);
    assertThat(worldState.get(ADDRESS).storageEntriesFrom(Bytes32.wrap(Hash.ZERO.getBytes()), 10))
        .isEqualTo(finalEntries);
  }
}
