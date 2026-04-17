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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.bouncycastle.util.Arrays.concatenate;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_ARCHIVE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_ROOT_HASH_KEY;
import static org.hyperledger.besu.ethereum.worldstate.PathBasedExtraStorageConfiguration.DEFAULT_MAX_LAYERS_TO_LOAD;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.TrieGenerator;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.StorageEntriesCollector;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.common.StorageSubscriber;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

public class BonsaiWorldStateKeyValueStorageTest {

  public static Collection<Object[]> flatDbMode() {
    return Arrays.asList(
        new Object[][] {{FlatDbMode.FULL}, {FlatDbMode.PARTIAL}, {FlatDbMode.ARCHIVE}});
  }

  public static Stream<Arguments> flatDbModeKeyMapperAndSegment() {
    Function<byte[], byte[]> flatDBKey = (key) -> key; // No-op

    // For archive we want <32-byte-hex>000000000000000n where n is the current archive block number
    Function<byte[], byte[]> flatDBArchiveKey =
        (key) -> concatenate(key, Bytes.ofUnsignedLong(2).toArrayUnsafe());

    return Stream.of(
        Arguments.of(FlatDbMode.FULL, flatDBKey, ACCOUNT_INFO_STATE),
        Arguments.of(FlatDbMode.PARTIAL, flatDBKey, ACCOUNT_INFO_STATE),
        Arguments.of(FlatDbMode.ARCHIVE, flatDBArchiveKey, ACCOUNT_INFO_STATE_ARCHIVE));
  }

  public static Collection<Object[]> flatDbModeAndCodeStorageMode() {
    return Arrays.asList(
        new Object[][] {
          {FlatDbMode.FULL, false},
          {FlatDbMode.PARTIAL, false},
          {FlatDbMode.ARCHIVE, false},
          {FlatDbMode.FULL, true},
          {FlatDbMode.PARTIAL, true},
          {FlatDbMode.ARCHIVE, true}
        });
  }

  BonsaiWorldStateKeyValueStorage storage;

  public BonsaiWorldStateKeyValueStorage setUp(final FlatDbMode flatDbMode) {
    return setUp(flatDbMode, false);
  }

  public BonsaiWorldStateKeyValueStorage setUp(
      final FlatDbMode flatDbMode, final boolean useCodeHashStorage) {
    if (flatDbMode.equals(FlatDbMode.ARCHIVE)) {
      storage = emptyArchiveStorage(useCodeHashStorage);
      storage.upgradeToArchiveFlatDbMode();
    } else if (flatDbMode.equals(FlatDbMode.FULL)) {
      storage = emptyStorage(useCodeHashStorage);
      storage.upgradeToFullFlatDbMode();
    } else if (flatDbMode.equals(FlatDbMode.PARTIAL)) {
      storage = emptyStorage(useCodeHashStorage);
      storage.downgradeToPartialFlatDbMode();
    }
    return storage;
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  void getCode_returnsEmpty(final FlatDbMode flatDbMode) {
    setUp(flatDbMode);
    assertThat(storage.getCode(Hash.EMPTY, Hash.EMPTY)).contains(Bytes.EMPTY);
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  void getAccountStateTrieNode_returnsEmptyNode(final FlatDbMode flatDbMode) {
    setUp(flatDbMode);
    assertThat(storage.getAccountStateTrieNode(Bytes.EMPTY, MerkleTrie.EMPTY_TRIE_NODE_HASH))
        .contains(MerkleTrie.EMPTY_TRIE_NODE);
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  void getAccountStorageTrieNode_returnsEmptyNode(final FlatDbMode flatDbMode) {
    setUp(flatDbMode);
    assertThat(
            storage.getAccountStorageTrieNode(
                Hash.EMPTY, Bytes.EMPTY, MerkleTrie.EMPTY_TRIE_NODE_HASH))
        .contains(MerkleTrie.EMPTY_TRIE_NODE);
  }

  @ParameterizedTest
  @MethodSource("flatDbModeAndCodeStorageMode")
  void getCode_saveAndGetSpecialValues(
      final FlatDbMode flatDbMode, final boolean accountHashCodeStorage) {
    setUp(flatDbMode, accountHashCodeStorage);
    storage
        .updater()
        .putCode(Hash.EMPTY, MerkleTrie.EMPTY_TRIE_NODE)
        .putCode(Hash.EMPTY, Bytes.EMPTY)
        .commit();

    assertThat(storage.getCode(Hash.hash(MerkleTrie.EMPTY_TRIE_NODE), Hash.EMPTY))
        .contains(MerkleTrie.EMPTY_TRIE_NODE);
  }

  @ParameterizedTest
  @MethodSource("flatDbModeAndCodeStorageMode")
  void getCode_saveAndGetRegularValue(
      final FlatDbMode flatDbMode, final boolean accountHashCodeStorage) {
    setUp(flatDbMode, accountHashCodeStorage);
    final Bytes bytes = Bytes.fromHexString("0x123456");
    storage.updater().putCode(Hash.EMPTY, bytes).commit();

    assertThat(storage.getCode(Hash.hash(bytes), Hash.EMPTY)).contains(bytes);
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  void getAccountStateTrieNode_saveAndGetSpecialValues(final FlatDbMode flatDbMode) {
    setUp(flatDbMode);
    storage
        .updater()
        .putAccountStateTrieNode(
            Bytes.EMPTY,
            Bytes32.wrap(Hash.hash(MerkleTrie.EMPTY_TRIE_NODE).getBytes()),
            MerkleTrie.EMPTY_TRIE_NODE)
        .putAccountStateTrieNode(
            Bytes.EMPTY, Bytes32.wrap(Hash.hash(Bytes.EMPTY).getBytes()), Bytes.EMPTY)
        .commit();

    assertThat(storage.getAccountStateTrieNode(Bytes.EMPTY, MerkleTrie.EMPTY_TRIE_NODE_HASH))
        .contains(MerkleTrie.EMPTY_TRIE_NODE);
    assertThat(storage.getAccountStateTrieNode(Bytes.EMPTY, Bytes32.wrap(Hash.EMPTY.getBytes())))
        .contains(Bytes.EMPTY);
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  void getAccountStateTrieNode_saveAndGetRegularValue(final FlatDbMode flatDbMode) {
    setUp(flatDbMode);
    final Bytes location = Bytes.fromHexString("0x01");
    final Bytes bytes = Bytes.fromHexString("0x123456");

    storage
        .updater()
        .putAccountStateTrieNode(location, Bytes32.wrap(Hash.hash(bytes).getBytes()), bytes)
        .commit();

    assertThat(storage.getAccountStateTrieNode(location, Bytes32.wrap(Hash.hash(bytes).getBytes())))
        .contains(bytes);
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  void getAccountStorageTrieNode_saveAndGetSpecialValues(final FlatDbMode flatDbMode) {
    setUp(flatDbMode);

    storage
        .updater()
        .putAccountStorageTrieNode(
            Hash.EMPTY,
            Bytes.EMPTY,
            Bytes32.wrap(Hash.hash(MerkleTrie.EMPTY_TRIE_NODE).getBytes()),
            MerkleTrie.EMPTY_TRIE_NODE)
        .putAccountStorageTrieNode(
            Hash.EMPTY, Bytes.EMPTY, Bytes32.wrap(Hash.hash(Bytes.EMPTY).getBytes()), Bytes.EMPTY)
        .commit();

    assertThat(
            storage.getAccountStorageTrieNode(
                Hash.EMPTY,
                Bytes.EMPTY,
                Bytes32.wrap(Hash.hash(MerkleTrie.EMPTY_TRIE_NODE).getBytes())))
        .contains(MerkleTrie.EMPTY_TRIE_NODE);
    assertThat(
            storage.getAccountStorageTrieNode(
                Hash.EMPTY, Bytes.EMPTY, Bytes32.wrap(Hash.hash(Bytes.EMPTY).getBytes())))
        .contains(Bytes.EMPTY);
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  void getAccountStorageTrieNode_saveAndGetRegularValue(final FlatDbMode flatDbMode) {
    setUp(flatDbMode);
    final Hash accountHash = Address.fromHexString("0x1").addressHash();
    final Bytes location = Bytes.fromHexString("0x01");
    final Bytes bytes = Bytes.fromHexString("0x123456");

    storage
        .updater()
        .putAccountStorageTrieNode(
            accountHash, location, Bytes32.wrap(Hash.hash(bytes).getBytes()), bytes)
        .commit();

    assertThat(
            storage.getAccountStorageTrieNode(
                accountHash, location, Bytes32.wrap(Hash.hash(bytes).getBytes())))
        .contains(bytes);
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  void getAccount_notLoadFromTrieWhenEmptyAndFlatDbFullMode(final FlatDbMode flatDbMode) {
    Assumptions.assumeTrue(flatDbMode == FlatDbMode.FULL);
    final BonsaiWorldStateKeyValueStorage storage = spy(setUp(flatDbMode));
    final WorldStateStorageCoordinator coordinator = new WorldStateStorageCoordinator(storage);
    final MerkleTrie<Bytes, Bytes> trie = TrieGenerator.generateTrie(coordinator, 1);

    final TreeMap<Bytes32, Bytes> accounts =
        (TreeMap<Bytes32, Bytes>)
            trie.entriesFrom(
                root ->
                    StorageEntriesCollector.collectEntries(
                        root, Bytes32.wrap(Hash.ZERO.getBytes()), 1));

    // save world state root hash
    final BonsaiWorldStateKeyValueStorage.Updater updater = storage.updater();
    updater
        .getWorldStateTransaction()
        .put(TRIE_BRANCH_STORAGE, WORLD_ROOT_HASH_KEY, trie.getRootHash().toArrayUnsafe());
    updater.commit();

    // remove flat database
    storage.downgradeToPartialFlatDbMode();
    storage.clearFlatDatabase();
    storage.upgradeToFullFlatDbMode();

    Mockito.reset(storage);

    assertThat(storage.getAccount(Hash.wrap(accounts.firstKey()))).isEmpty();

    verify(storage, times(0)).getAccountStateTrieNode(any(), eq(trie.getRootHash()));
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  void getAccount_loadFromTrieWhenEmptyAndFlatDbPartialMode(final FlatDbMode flatDbMode) {
    Assumptions.assumeTrue(flatDbMode == FlatDbMode.PARTIAL);
    final BonsaiWorldStateKeyValueStorage storage = spy(setUp(flatDbMode));
    final WorldStateStorageCoordinator coordinator = new WorldStateStorageCoordinator(storage);
    final MerkleTrie<Bytes, Bytes> trie = TrieGenerator.generateTrie(coordinator, 1);
    final TreeMap<Bytes32, Bytes> accounts =
        (TreeMap<Bytes32, Bytes>)
            trie.entriesFrom(
                root ->
                    StorageEntriesCollector.collectEntries(
                        root, Bytes32.wrap(Hash.ZERO.getBytes()), 1));

    // save world state root hash
    final BonsaiWorldStateKeyValueStorage.Updater updater = storage.updater();
    updater
        .getWorldStateTransaction()
        .put(TRIE_BRANCH_STORAGE, WORLD_ROOT_HASH_KEY, trie.getRootHash().toArrayUnsafe());
    updater.commit();

    // remove flat database
    storage.clearFlatDatabase();

    Mockito.reset(storage);

    assertThat(storage.getAccount(Hash.wrap(accounts.firstKey())))
        .contains(accounts.firstEntry().getValue());

    verify(storage, times(1)).getAccountStateTrieNode(any(), eq(trie.getRootHash()));
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  void shouldUsePartialDBStrategyAfterDowngradingMode(final FlatDbMode flatDbMode) {
    Assumptions.assumeTrue(flatDbMode == FlatDbMode.PARTIAL);
    final BonsaiWorldStateKeyValueStorage storage = spy(setUp(flatDbMode));
    final WorldStateStorageCoordinator coordinator = new WorldStateStorageCoordinator(storage);
    final MerkleTrie<Bytes, Bytes> trie = TrieGenerator.generateTrie(coordinator, 1);
    final TreeMap<Bytes32, Bytes> accounts =
        (TreeMap<Bytes32, Bytes>)
            trie.entriesFrom(
                root ->
                    StorageEntriesCollector.collectEntries(
                        root, Bytes32.wrap(Hash.ZERO.getBytes()), 1));

    // save world state root hash
    final BonsaiWorldStateKeyValueStorage.Updater updater = storage.updater();
    updater
        .getWorldStateTransaction()
        .put(TRIE_BRANCH_STORAGE, WORLD_ROOT_HASH_KEY, trie.getRootHash().toArrayUnsafe());
    updater.commit();

    Mockito.reset(storage);

    // remove flat database
    storage.clearFlatDatabase();

    storage.upgradeToFullFlatDbMode();
    assertThat(storage.getAccount(Hash.wrap(accounts.firstKey()))).isEmpty();

    storage.downgradeToPartialFlatDbMode();
    assertThat(storage.getAccount(Hash.wrap(accounts.firstKey())))
        .contains(accounts.firstEntry().getValue());
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  void getStorage_loadFromTrieWhenEmptyWithPartialMode(final FlatDbMode flatDbMode) {
    final BonsaiWorldStateKeyValueStorage storage = spy(setUp(flatDbMode));
    Assumptions.assumeTrue(flatDbMode == FlatDbMode.PARTIAL);

    final WorldStateStorageCoordinator coordinator = new WorldStateStorageCoordinator(storage);
    final MerkleTrie<Bytes, Bytes> trie = TrieGenerator.generateTrie(coordinator, 1);
    final TreeMap<Bytes32, Bytes> accounts =
        (TreeMap<Bytes32, Bytes>)
            trie.entriesFrom(
                root ->
                    StorageEntriesCollector.collectEntries(
                        root, Bytes32.wrap(Hash.ZERO.getBytes()), 1));

    final PmtStateTrieAccountValue stateTrieAccountValue =
        PmtStateTrieAccountValue.readFrom(RLP.input(accounts.firstEntry().getValue()));

    final StoredMerklePatriciaTrie<Bytes, Bytes> storageTrie =
        new StoredMerklePatriciaTrie<>(
            (Bytes location, Bytes32 hash) ->
                storage.getAccountStorageTrieNode(Hash.wrap(accounts.firstKey()), location, hash),
            Bytes32.wrap(stateTrieAccountValue.getStorageRoot().getBytes()),
            b -> b,
            b -> b);

    final TreeMap<Bytes32, Bytes> slots =
        (TreeMap<Bytes32, Bytes>)
            storageTrie.entriesFrom(
                root ->
                    StorageEntriesCollector.collectEntries(
                        root, Bytes32.wrap(Hash.ZERO.getBytes()), 1));

    // save world state root hash
    final BonsaiWorldStateKeyValueStorage.Updater updater = storage.updater();
    updater
        .getWorldStateTransaction()
        .put(TRIE_BRANCH_STORAGE, WORLD_ROOT_HASH_KEY, trie.getRootHash().toArrayUnsafe());
    updater.commit();

    // remove flat database
    storage.clearFlatDatabase();

    assertThat(
            storage.getStorageValueByStorageSlotKey(
                Hash.wrap(accounts.firstKey()),
                new StorageSlotKey(Hash.wrap(slots.firstKey()), Optional.empty())))
        .map(Bytes::toShortHexString)
        .contains(slots.firstEntry().getValue().toShortHexString());

    verify(storage, times(2))
        .getAccountStorageTrieNode(
            eq(Hash.wrap(accounts.firstKey())), any(), eq(storageTrie.getRootHash()));
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  void getStorage_loadFromTrieWhenEmptyWithFullMode(final FlatDbMode flatDbMode) {
    Assumptions.assumeTrue(flatDbMode == FlatDbMode.FULL);
    final BonsaiWorldStateKeyValueStorage storage = spy(setUp(flatDbMode));
    storage.upgradeToFullFlatDbMode();
    final WorldStateStorageCoordinator coordinator = new WorldStateStorageCoordinator(storage);
    final MerkleTrie<Bytes, Bytes> trie = TrieGenerator.generateTrie(coordinator, 1);

    // save world state root hash
    final BonsaiWorldStateKeyValueStorage.Updater updater = storage.updater();
    updater
        .getWorldStateTransaction()
        .put(TRIE_BRANCH_STORAGE, WORLD_ROOT_HASH_KEY, trie.getRootHash().toArrayUnsafe());
    updater.commit();

    // remove flat database
    storage.clearFlatDatabase();
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  void clear_reloadFlatDbStrategy(final FlatDbMode flatDbMode) {
    final BonsaiWorldStateKeyValueStorage storage = spy(setUp(flatDbMode));

    // save world state root hash
    final BonsaiWorldStateKeyValueStorage.Updater updater = storage.updater();
    updater.putAccountInfoState(Hash.ZERO, Bytes32.random()).commit();

    storage
        .getWorldStateBlockNumber()
        .ifPresent(
            (currentBlock) ->
                updateStorageArchiveBlock(
                    storage.getComposedWorldStateStorage(), currentBlock + 1));
    assertThat(storage.getAccount(Hash.ZERO)).isNotEmpty();

    // clear
    storage.clear();

    assertThat(storage.getFlatDbStrategy()).isNotNull();

    assertThat(storage.getAccount(Hash.ZERO)).isEmpty();
  }

  @ParameterizedTest
  @MethodSource("flatDbModeKeyMapperAndSegment")
  void clear_putGetAccountFlatDbStrategy(
      final FlatDbMode flatDbMode,
      final Function<byte[], byte[]> keyMapper,
      final KeyValueSegmentIdentifier segment) {
    final BonsaiWorldStateKeyValueStorage storage = spy(setUp(flatDbMode));

    // save world state root hash
    final BonsaiWorldStateKeyValueStorage.Updater updater = storage.updater();

    Address account = Address.fromHexString("0x1cda99fb95e5418ae3bdc3bab5c4efa4a5a58a7c");

    // RLP encoded account: address = 0x1cda99fb95e5418ae3bdc3bab5c4efa4a5a58a7c, balance =
    // 0x0000000000000000000000000000000000000000000000007b5e41a364ea8bfc, nonce = 15768
    updater
        .putAccountInfoState(
            account.addressHash(),
            Bytes.fromHexString(
                "0xF84E823D98887B5E41A364EA8BFCA056E81F171BCC55A6FF8345E692C0F86E5B48E01B996CADC001622FB5E363B421A0C5D2460186F7233C927E7DB2DCC703C0E500B653CA82273B7BFAD8045D85A470"))
        .commit();

    storage
        .getWorldStateBlockNumber()
        .ifPresent(
            (currentBlock) ->
                updateStorageArchiveBlock(
                    storage.getComposedWorldStateStorage(), currentBlock + 1));

    assertThat(storage.getAccount(account.addressHash())).isNotEmpty();

    // Get the raw key/value out of storage and check that as well. The key differs between flat DB
    // and flat archive DB
    // and we want to ensure keys put to the archive DB include the archive block context/suffix
    byte[] lookupKey = keyMapper.apply(account.addressHash().getBytes().toArrayUnsafe());
    assertThat(Bytes.wrap(storage.getComposedWorldStateStorage().get(segment, lookupKey).get()))
        .isEqualTo(
            Bytes.fromHexString(
                "0xF84E823D98887B5E41A364EA8BFCA056E81F171BCC55A6FF8345E692C0F86E5B48E01B996CADC001622FB5E363B421A0C5D2460186F7233C927E7DB2DCC703C0E500B653CA82273B7BFAD8045D85A470"));

    BonsaiAccount retrievedAccount =
        BonsaiAccount.fromRLP(
            null, account, storage.getAccount(account.addressHash()).get(), false, new CodeCache());
    assertThat(retrievedAccount.getBalance())
        .isEqualTo(
            Wei.fromHexString(
                "0x0000000000000000000000000000000000000000000000007b5e41a364ea8bfc"));
    assertThat(retrievedAccount.getNonce()).isEqualTo(15768);

    // clear
    storage.clear();

    assertThat(storage.getFlatDbStrategy()).isNotNull();

    assertThat(storage.getAccount(account.addressHash())).isEmpty();
  }

  @ParameterizedTest
  @MethodSource({"flatDbModeKeyMapperAndSegment"})
  void clear_streamFlatAccounts(
      final FlatDbMode flatDbMode,
      final Function<byte[], byte[]> keyMapper,
      final KeyValueSegmentIdentifier segment) {
    final BonsaiWorldStateKeyValueStorage storage = spy(setUp(flatDbMode));

    // save world state root hash
    BonsaiWorldStateKeyValueStorage.Updater updater = storage.updater();

    // Put 3 accounts
    Address account1 =
        Address.fromHexString(
            "0x1111111111111111111111111111111111111111"); // 3rd entry in DB after hashing
    Bytes32 account1Value = Bytes32.random();
    updater.putAccountInfoState(account1.addressHash(), account1Value).commit();
    updater = storage.updater();
    Address account2 =
        Address.fromHexString(
            "0x2222222222222222222222222222222222222222"); // 1st entry in the DB after hashing
    Bytes32 account2Value = Bytes32.random();
    updater.putAccountInfoState(account2.addressHash(), account2Value).commit();
    updater = storage.updater();
    Address account3 =
        Address.fromHexString(
            "0x3333333333333333333333333333333333333333"); // 2nd entry in the DB after hashing
    Bytes32 account3Value = Bytes32.random();
    updater.putAccountInfoState(account3.addressHash(), account3Value).commit();

    // Check that the K/V store entries are correct
    // Convert the key to lookup the entry we expect to find in K/V storage. No-op for everything
    // except ARCHIVE, which needs to append the 000000000000000x suffix to the key
    byte[] lookupKey = keyMapper.apply(account1.addressHash().getBytes().toArrayUnsafe());
    assertThat(Bytes32.wrap(storage.getComposedWorldStateStorage().get(segment, lookupKey).get()))
        .isEqualTo(account1Value);
    lookupKey = keyMapper.apply(account2.addressHash().getBytes().toArrayUnsafe());
    assertThat(Bytes32.wrap(storage.getComposedWorldStateStorage().get(segment, lookupKey).get()))
        .isEqualTo(account2Value);
    lookupKey = keyMapper.apply(account3.addressHash().getBytes().toArrayUnsafe());
    assertThat(Bytes32.wrap(storage.getComposedWorldStateStorage().get(segment, lookupKey).get()))
        .isEqualTo(account3Value);

    // Streaming the entire range to ensure we get all 3 accounts back
    assertThat(
            storage
                .streamFlatAccounts(
                    Bytes32.wrap(
                        Hash.fromHexString(
                                "0x0000000000000000000000000000000000000000000000000000000000000000")
                            .getBytes()),
                    Bytes32.wrap(
                        Hash.fromHexString(
                                "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")
                            .getBytes()),
                    1000)
                .size())
        .isEqualTo(3);
    assertThat(
            storage
                .streamFlatAccounts(
                    Bytes32.wrap(
                        Hash.fromHexString(
                                "0x0000000000000000000000000000000000000000000000000000000000000000")
                            .getBytes()),
                    Bytes32.wrap(
                        Hash.fromHexString(
                                "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")
                            .getBytes()),
                    1000)
                .firstEntry()
                .getKey())
        .isEqualTo(account2.addressHash().getBytes()); // NB: Account 2 hash is first in the DB
    assertThat(
            storage
                .streamFlatAccounts(
                    Bytes32.wrap(
                        Hash.fromHexString(
                                "0x0000000000000000000000000000000000000000000000000000000000000000")
                            .getBytes()),
                    Bytes32.wrap(
                        Hash.fromHexString(
                                "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")
                            .getBytes()),
                    1000)
                .lastEntry()
                .getKey())
        .isEqualTo(account1.addressHash().getBytes()); // NB: Account 1 hash is 3rd/last in the DB

    // clear
    storage.clear();

    assertThat(storage.getFlatDbStrategy()).isNotNull();

    assertThat(storage.getAccount(account1.addressHash())).isEmpty();
    assertThat(storage.getAccount(account2.addressHash())).isEmpty();
    assertThat(storage.getAccount(account3.addressHash())).isEmpty();
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  void clear_streamFlatAccountsMultipleStateChanges(final FlatDbMode flatDbMode) {
    final BonsaiWorldStateKeyValueStorage storage = spy(setUp(flatDbMode));

    // save world state root hash
    BonsaiWorldStateKeyValueStorage.Updater updater = storage.updater();

    // Put 3 accounts
    Address account1 =
        Address.fromHexString(
            "0x1111111111111111111111111111111111111111"); // 3rd entry in DB after hashing
    updater.putAccountInfoState(account1.addressHash(), Bytes32.random()).commit();
    updater = storage.updater();
    Address account2 =
        Address.fromHexString(
            "0x2222222222222222222222222222222222222222"); // 1st entry in the DB after hashing
    updater.putAccountInfoState(account2.addressHash(), Bytes32.random()).commit();
    updater = storage.updater();
    Address account3 =
        Address.fromHexString(
            "0x3333333333333333333333333333333333333333"); // 2nd entry in the DB after hashing
    updater.putAccountInfoState(account3.addressHash(), Bytes32.random()).commit();

    // Update the middle account several times. For an archive mode DB this will result in N
    // additional entries in the DB, but streaming the accounts should only return the most recent
    // entry

    // Update the account at block 2
    updateStorageArchiveBlock(storage.getComposedWorldStateStorage(), 2);
    updater = storage.updater();
    updater.putAccountInfoState(account3.addressHash(), Bytes32.random()).commit();

    // Update the account at block 3
    updateStorageArchiveBlock(storage.getComposedWorldStateStorage(), 3);
    updater = storage.updater();
    updater.putAccountInfoState(account3.addressHash(), Bytes32.random()).commit();

    // Update the account at block 4
    updateStorageArchiveBlock(storage.getComposedWorldStateStorage(), 4);
    Bytes32 finalStateUpdate = Bytes32.random();
    updater = storage.updater();
    updater.putAccountInfoState(account3.addressHash(), finalStateUpdate).commit();

    // Streaming the entire range to ensure we only get 3 accounts back
    assertThat(
            storage
                .streamFlatAccounts(
                    Bytes32.wrap(
                        Hash.fromHexString(
                                "0x0000000000000000000000000000000000000000000000000000000000000000")
                            .getBytes()),
                    Bytes32.wrap(
                        Hash.fromHexString(
                                "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")
                            .getBytes()),
                    1000)
                .size())
        .isEqualTo(3);

    // Check that account 2 is the first entry (as per its hash)
    assertThat(
            storage
                .streamFlatAccounts(
                    Bytes32.wrap(
                        Hash.fromHexString(
                                "0x0000000000000000000000000000000000000000000000000000000000000000")
                            .getBytes()),
                    Bytes32.wrap(
                        Hash.fromHexString(
                                "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")
                            .getBytes()),
                    1000)
                .firstEntry()
                .getKey())
        .isEqualTo(account2.addressHash().getBytes()); // NB: Account 2 hash is first in the DB

    // Check that account 1 is the last entry (as per its hash)
    assertThat(
            storage
                .streamFlatAccounts(
                    Bytes32.wrap(
                        Hash.fromHexString(
                                "0x0000000000000000000000000000000000000000000000000000000000000000")
                            .getBytes()),
                    Bytes32.wrap(
                        Hash.fromHexString(
                                "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")
                            .getBytes()),
                    1000)
                .lastEntry()
                .getKey())
        .isEqualTo(account1.addressHash().getBytes()); // NB: Account 1 hash is 3rd/last in the DB

    // Check the state for account 3 is the final state update at block 4, not an earlier state
    assertThat(
            storage
                .streamFlatAccounts(
                    Bytes32.wrap(
                        Hash.fromHexString(
                                "0x0000000000000000000000000000000000000000000000000000000000000000")
                            .getBytes()),
                    Bytes32.wrap(
                        Hash.fromHexString(
                                "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")
                            .getBytes()),
                    1000)
                .get(Bytes32.wrap(account3.addressHash().getBytes())))
        .isEqualTo(finalStateUpdate);

    // clear
    storage.clear();

    assertThat(storage.getFlatDbStrategy()).isNotNull();

    assertThat(storage.getAccount(account1.addressHash())).isEmpty();
    assertThat(storage.getAccount(account2.addressHash())).isEmpty();
    assertThat(storage.getAccount(account3.addressHash())).isEmpty();
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  void clear_streamFlatStorageMultipleStateChanges(final FlatDbMode flatDbMode) {
    final BonsaiWorldStateKeyValueStorage storage = spy(setUp(flatDbMode));

    // save world state root hash
    BonsaiWorldStateKeyValueStorage.Updater updater = storage.updater();

    // Put 3 accounts
    Address account1 =
        Address.fromHexString(
            "0x1111111111111111111111111111111111111111"); // 3rd entry in DB after hashing
    updater
        .putStorageValueBySlotHash(
            account1.addressHash(),
            new StorageSlotKey(UInt256.ONE).getSlotHash(),
            UInt256.fromHexString("0x11"))
        .commit();

    updater = storage.updater();
    Address account2 =
        Address.fromHexString(
            "0x2222222222222222222222222222222222222222"); // 1st entry in the DB after hashing
    updater
        .putStorageValueBySlotHash(
            account2.addressHash(),
            new StorageSlotKey(UInt256.ONE).getSlotHash(),
            UInt256.fromHexString("0x22"))
        .commit();

    updater = storage.updater();
    Address account3 =
        Address.fromHexString(
            "0x3333333333333333333333333333333333333333"); // 2nd entry in the DB after hashing
    final StorageSlotKey slot1 = new StorageSlotKey(UInt256.ONE);
    updater
        .putStorageValueBySlotHash(
            account3.addressHash(),
            new StorageSlotKey(UInt256.ONE).getSlotHash(),
            UInt256.fromHexString("0x33"))
        .commit();

    // Update the middle account several times. For an archive mode DB this will result in N
    // additional entries in the DB, but streaming the accounts should only return the most recent
    // entry

    // Update the storage at block 2
    updateStorageArchiveBlock(storage.getComposedWorldStateStorage(), 2);
    updater = storage.updater();
    updater
        .putStorageValueBySlotHash(
            account3.addressHash(), slot1.getSlotHash(), UInt256.fromHexString("0x12"))
        .commit();

    // Update the account at block 3
    updateStorageArchiveBlock(storage.getComposedWorldStateStorage(), 3);
    updater = storage.updater();
    updater
        .putStorageValueBySlotHash(
            account3.addressHash(), slot1.getSlotHash(), UInt256.fromHexString("0x13"))
        .commit();

    // Update the account at block 4
    updateStorageArchiveBlock(storage.getComposedWorldStateStorage(), 4);
    updater = storage.updater();
    updater
        .putStorageValueBySlotHash(
            account3.addressHash(), slot1.getSlotHash(), UInt256.fromHexString("0x14"))
        .commit();

    // Check that every account only has 1 entry for slot 1 (even account 3 which updated the same
    // slot 4 times)
    assertThat(
            storage
                .streamFlatStorages(
                    account1.addressHash(),
                    Bytes32.wrap(
                        Hash.fromHexString(
                                "0x0000000000000000000000000000000000000000000000000000000000000000")
                            .getBytes()),
                    Bytes32.wrap(
                        Hash.fromHexString(
                                "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")
                            .getBytes()),
                    1000)
                .size())
        .isEqualTo(1);

    assertThat(
            storage
                .streamFlatStorages(
                    account2.addressHash(),
                    Bytes32.wrap(
                        Hash.fromHexString(
                                "0x0000000000000000000000000000000000000000000000000000000000000000")
                            .getBytes()),
                    Bytes32.wrap(
                        Hash.fromHexString(
                                "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")
                            .getBytes()),
                    1000)
                .size())
        .isEqualTo(1);

    assertThat(
            storage
                .streamFlatStorages(
                    account3.addressHash(),
                    Bytes32.wrap(
                        Hash.fromHexString(
                                "0x0000000000000000000000000000000000000000000000000000000000000000")
                            .getBytes()),
                    Bytes32.wrap(
                        Hash.fromHexString(
                                "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")
                            .getBytes()),
                    1000)
                .size())
        .isEqualTo(1);

    // Check that the storage state for account 3's storage slot 1 is the latest value that was
    // stored
    assertThat(
            storage
                .streamFlatStorages(
                    account3.addressHash(),
                    Bytes32.wrap(
                        Hash.fromHexString(
                                "0x0000000000000000000000000000000000000000000000000000000000000000")
                            .getBytes()),
                    Bytes32.wrap(
                        Hash.fromHexString(
                                "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")
                            .getBytes()),
                    1000)
                .get(Bytes32.wrap(slot1.getSlotHash().getBytes())))
        .isEqualTo(Bytes.fromHexString("0x14"));

    // clear
    storage.clear();

    assertThat(storage.getFlatDbStrategy()).isNotNull();
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  void reconcilesNonConflictingUpdaters(final FlatDbMode flatDbMode) {
    setUp(flatDbMode);
    final Hash accountHashA = Address.fromHexString("0x1").addressHash();
    final Hash accountHashB = Address.fromHexString("0x2").addressHash();
    final Hash accountHashD = Address.fromHexString("0x4").addressHash();
    final Bytes bytesA = Bytes.fromHexString("0x12");
    final Bytes bytesB = Bytes.fromHexString("0x1234");
    final Bytes bytesC = Bytes.fromHexString("0x123456");

    final BonsaiWorldStateKeyValueStorage.Updater updaterA = storage.updater();
    final BonsaiWorldStateKeyValueStorage.Updater updaterB = storage.updater();

    updaterA.putCode(accountHashA, bytesA);
    updaterB.putCode(accountHashB, bytesA);
    updaterB.putCode(accountHashB, bytesB);
    updaterA.putCode(accountHashD, bytesC);

    updaterA.commit();
    updaterB.commit();

    assertThat(storage.getCode(Hash.hash(bytesB), accountHashB)).contains(bytesB);
    assertThat(storage.getCode(Hash.hash(bytesC), accountHashD)).contains(bytesC);
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  void isWorldStateAvailable_defaultIsFalse(final FlatDbMode flatDbMode) {
    setUp(flatDbMode);
    if (flatDbMode.equals(FlatDbMode.ARCHIVE)) {
      assertThat(emptyArchiveStorage().isWorldStateAvailable(UInt256.valueOf(1), Hash.EMPTY))
          .isFalse();
    } else {
      assertThat(emptyStorage().isWorldStateAvailable(UInt256.valueOf(1), Hash.EMPTY)).isFalse();
    }
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  void isWorldStateAvailable_StateAvailableByRootHash(final FlatDbMode flatDbMode) {
    setUp(flatDbMode);

    final BonsaiWorldStateKeyValueStorage.Updater updater = storage.updater();
    final Bytes32 rootHashKey = Bytes32.fromHexString("0x01");
    updater
        .getWorldStateTransaction()
        .put(TRIE_BRANCH_STORAGE, WORLD_ROOT_HASH_KEY, rootHashKey.toArrayUnsafe());
    updater.commit();
    assertThat(storage.isWorldStateAvailable(rootHashKey, Hash.EMPTY)).isTrue();
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  void isWorldStateAvailable_afterCallingSaveWorldstate(final FlatDbMode flatDbMode) {
    setUp(flatDbMode);

    final BonsaiWorldStateKeyValueStorage.Updater updater = storage.updater();

    final Bytes blockHash = Bytes32.fromHexString("0x01");
    final Bytes32 nodeHashKey = Bytes32.fromHexString("0x02");
    final Bytes nodeValue = Bytes32.fromHexString("0x03");

    assertThat(storage.isWorldStateAvailable(Bytes32.wrap(nodeHashKey), Hash.EMPTY)).isFalse();

    updater.saveWorldState(blockHash, nodeHashKey, nodeValue);
    updater.commit();

    assertThat(storage.isWorldStateAvailable(Bytes32.wrap(nodeHashKey), Hash.EMPTY)).isTrue();
  }

  private BonsaiWorldStateKeyValueStorage emptyStorage() {
    return new BonsaiWorldStateKeyValueStorage(
        new InMemoryKeyValueStorageProvider(),
        new NoOpMetricsSystem(),
        DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
  }

  private BonsaiWorldStateKeyValueStorage emptyArchiveStorage() {
    final BonsaiWorldStateKeyValueStorage archiveStorage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            DataStorageConfiguration.DEFAULT_BONSAI_ARCHIVE_CONFIG);
    updateStorageArchiveBlock(archiveStorage.getComposedWorldStateStorage(), 1);
    return archiveStorage;
  }

  private BonsaiWorldStateKeyValueStorage emptyStorage(final boolean useCodeHashStorage) {
    return new BonsaiWorldStateKeyValueStorage(
        new InMemoryKeyValueStorageProvider(),
        new NoOpMetricsSystem(),
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .pathBasedExtraStorageConfiguration(
                ImmutablePathBasedExtraStorageConfiguration.builder()
                    .maxLayersToLoad(DEFAULT_MAX_LAYERS_TO_LOAD)
                    .unstable(
                        ImmutablePathBasedExtraStorageConfiguration.PathBasedUnstable.builder()
                            .codeStoredByCodeHashEnabled(useCodeHashStorage)
                            .build())
                    .build())
            .build());
  }

  private BonsaiWorldStateKeyValueStorage emptyArchiveStorage(final boolean useCodeHashStorage) {
    final BonsaiWorldStateKeyValueStorage archiveStorage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            ImmutableDataStorageConfiguration.builder()
                .dataStorageFormat(DataStorageFormat.X_BONSAI_ARCHIVE)
                .pathBasedExtraStorageConfiguration(
                    ImmutablePathBasedExtraStorageConfiguration.builder()
                        .maxLayersToLoad(3L)
                        .limitTrieLogsEnabled(true)
                        .unstable(
                            ImmutablePathBasedExtraStorageConfiguration.PathBasedUnstable.builder()
                                .codeStoredByCodeHashEnabled(useCodeHashStorage)
                                .build())
                        .build())
                .build());
    updateStorageArchiveBlock(archiveStorage.getComposedWorldStateStorage(), 1);
    return archiveStorage;
  }

  @Test
  void successfulPruneReturnsTrue() {
    final KeyValueStorage mockTrieLogStorage = mock(KeyValueStorage.class);
    when(mockTrieLogStorage.tryDelete(any())).thenReturn(true);
    final BonsaiWorldStateKeyValueStorage storage = setupSpyStorage(mockTrieLogStorage);
    assertThat(storage.pruneTrieLog(Hash.ZERO)).isTrue();
  }

  @Test
  void failedPruneReturnsFalse() {
    final KeyValueStorage mockTrieLogStorage = mock(KeyValueStorage.class);
    when(mockTrieLogStorage.tryDelete(any())).thenReturn(false);
    final BonsaiWorldStateKeyValueStorage storage = setupSpyStorage(mockTrieLogStorage);
    assertThat(storage.pruneTrieLog(Hash.ZERO)).isFalse();
  }

  @Test
  void exceptionalPruneReturnsFalse() {
    final KeyValueStorage mockTrieLogStorage = mock(KeyValueStorage.class);
    when(mockTrieLogStorage.tryDelete(any())).thenThrow(new RuntimeException("test exception"));
    final BonsaiWorldStateKeyValueStorage storage = setupSpyStorage(mockTrieLogStorage);
    assertThat(storage.pruneTrieLog(Hash.ZERO)).isFalse();
  }

  @Test
  void upgradeToArchiveFlatDbModeNotifiesSubscribersToClearCache() {
    final BonsaiWorldStateKeyValueStorage archiveStorage = emptyArchiveStorage();
    final StorageSubscriber subscriber = mock(StorageSubscriber.class);
    archiveStorage.subscribe(subscriber);

    archiveStorage.upgradeToArchiveFlatDbMode();

    verify(subscriber).onClearFlatDatabaseStorage();
  }

  private BonsaiWorldStateKeyValueStorage setupSpyStorage(
      final KeyValueStorage mockTrieLogStorage) {
    final StorageProvider mockStorageProvider = spy(new InMemoryKeyValueStorageProvider());
    when(mockStorageProvider.getStorageBySegmentIdentifier(
            KeyValueSegmentIdentifier.TRIE_LOG_STORAGE))
        .thenReturn(mockTrieLogStorage);

    return new BonsaiWorldStateKeyValueStorage(
        mockStorageProvider,
        new NoOpMetricsSystem(),
        DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
  }

  private static void updateStorageArchiveBlock(
      final SegmentedKeyValueStorage storage, final long blockNumber) {
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(
        TRIE_BRANCH_STORAGE,
        WORLD_BLOCK_NUMBER_KEY,
        Bytes.ofUnsignedLong(blockNumber).toArrayUnsafe());
    tx.commit();
  }
}
