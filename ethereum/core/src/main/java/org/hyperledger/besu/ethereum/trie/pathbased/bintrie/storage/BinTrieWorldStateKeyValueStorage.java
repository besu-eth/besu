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
package org.hyperledger.besu.ethereum.trie.pathbased.bintrie.storage;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.BINTRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.BinTrieAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.storage.flat.BinTrieFlatDbReaderStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.storage.flat.BinTrieFlatDbStrategyProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.FlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldView;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.ethereum.worldstate.WorldStateKeyValueStorage;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Key-Value storage for Binary Trie world state.
 *
 * <p>Uses a single unified trie branch storage segment (BINTRIE_BRANCH_STORAGE) instead of separate
 * storage/account tries like Bonsai.
 */
public class BinTrieWorldStateKeyValueStorage extends PathBasedWorldStateKeyValueStorage
    implements WorldStateKeyValueStorage {

  protected final BinTrieFlatDbStrategyProvider flatDbStrategyProvider;

  public BinTrieWorldStateKeyValueStorage(
      final StorageProvider provider,
      final MetricsSystem metricsSystem,
      final DataStorageConfiguration dataStorageConfiguration) {
    super(
        provider.getStorageBySegmentIdentifiers(
            List.of(
                ACCOUNT_INFO_STATE,
                CODE_STORAGE,
                ACCOUNT_STORAGE_STORAGE,
                TRIE_BRANCH_STORAGE,
                BINTRIE_BRANCH_STORAGE)),
        provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.TRIE_LOG_STORAGE));
    this.flatDbStrategyProvider =
        new BinTrieFlatDbStrategyProvider(metricsSystem, dataStorageConfiguration);
    this.flatDbStrategyProvider.loadFlatDbStrategy(composedWorldStateStorage);
  }

  public BinTrieWorldStateKeyValueStorage(
      final BinTrieFlatDbStrategyProvider flatDbStrategyProvider,
      final SegmentedKeyValueStorage composedWorldStateStorage,
      final KeyValueStorage trieLogStorage) {
    super(composedWorldStateStorage, trieLogStorage);
    this.flatDbStrategyProvider = flatDbStrategyProvider;
  }

  @Override
  public DataStorageFormat getDataStorageFormat() {
    return DataStorageFormat.BINTRIE;
  }

  @Override
  public FlatDbMode getFlatDbMode() {
    return flatDbStrategyProvider.getFlatDbMode();
  }

  @Override
  public FlatDbStrategy getFlatDbStrategy() {
    return flatDbStrategyProvider.getFlatDbStrategy(composedWorldStateStorage);
  }

  public BinTrieFlatDbStrategyProvider getFlatDbStrategyProvider() {
    return flatDbStrategyProvider;
  }

  public Optional<BinTrieAccount> getAccount(
      final Address address, final PathBasedWorldView context) {
    return getBinTrieReaderStrategy().getFlatAccount(address, context, composedWorldStateStorage);
  }

  public Optional<Bytes> getStorageValueByStorageSlotKey(
      final Address address, final StorageSlotKey storageSlotKey) {
    return getBinTrieReaderStrategy()
        .getFlatStorageValueByStorageSlotKey(address, storageSlotKey, composedWorldStateStorage);
  }

  private BinTrieFlatDbReaderStrategy getBinTrieReaderStrategy() {
    return (BinTrieFlatDbReaderStrategy) getFlatDbStrategy();
  }

  public Optional<Bytes> getCode(final Hash codeHash, final Hash accountHash) {
    if (codeHash.equals(Hash.EMPTY)) {
      return Optional.of(Bytes.EMPTY);
    }
    return composedWorldStateStorage
        .get(CODE_STORAGE, accountHash.getBytes().toArrayUnsafe())
        .map(Bytes::wrap);
  }

  @Override
  public Optional<Bytes> getStateTrieNode(final Bytes location) {
    return composedWorldStateStorage
        .get(BINTRIE_BRANCH_STORAGE, location.toArrayUnsafe())
        .map(Bytes::wrap);
  }

  public Optional<Bytes32> getStateTrieNodeByHash(final Bytes32 nodeHash) {
    return composedWorldStateStorage
        .get(BINTRIE_BRANCH_STORAGE, nodeHash.toArrayUnsafe())
        .map(Bytes32::wrap);
  }

  public void putTrieNode(final Bytes location, final Bytes value) {
    final SegmentedKeyValueStorageTransaction tx = composedWorldStateStorage.startTransaction();
    tx.put(BINTRIE_BRANCH_STORAGE, location.toArrayUnsafe(), value.toArrayUnsafe());
    tx.commit();
  }

  @Override
  public PathBasedWorldStateKeyValueStorage.Updater updater() {
    return new BinTrieUpdater(
        composedWorldStateStorage.startTransaction(),
        trieLogStorage.startTransaction(),
        composedWorldStateStorage);
  }

  /** Updater for BinTrie world state storage. */
  public static class BinTrieUpdater implements PathBasedWorldStateKeyValueStorage.Updater {

    private final SegmentedKeyValueStorageTransaction composedWorldStateTransaction;
    private final KeyValueStorageTransaction trieLogStorageTransaction;

    public BinTrieUpdater(
        final SegmentedKeyValueStorageTransaction composedWorldStateTransaction,
        final KeyValueStorageTransaction trieLogStorageTransaction,
        @SuppressWarnings("unused") final SegmentedKeyValueStorage worldStorage) {
      this.composedWorldStateTransaction = composedWorldStateTransaction;
      this.trieLogStorageTransaction = trieLogStorageTransaction;
    }

    public BinTrieUpdater putTrieNode(final Bytes location, final Bytes value) {
      composedWorldStateTransaction.put(
          BINTRIE_BRANCH_STORAGE, location.toArrayUnsafe(), value.toArrayUnsafe());
      return this;
    }

    public BinTrieUpdater removeTrieNode(final Bytes location) {
      composedWorldStateTransaction.remove(BINTRIE_BRANCH_STORAGE, location.toArrayUnsafe());
      return this;
    }

    public BinTrieUpdater putCode(final Hash accountHash, final Hash codeHash, final Bytes code) {
      if (code.isEmpty()) {
        return this;
      }
      composedWorldStateTransaction.put(
          CODE_STORAGE, accountHash.getBytes().toArrayUnsafe(), code.toArrayUnsafe());
      return this;
    }

    public BinTrieUpdater removeCode(final Hash accountHash) {
      composedWorldStateTransaction.remove(CODE_STORAGE, accountHash.getBytes().toArrayUnsafe());
      return this;
    }

    public BinTrieUpdater removeCode(final Hash accountHash, final Hash codeHash) {
      return removeCode(accountHash);
    }

    public BinTrieUpdater putAccountInfoState(final Hash accountHash, final Bytes accountData) {
      composedWorldStateTransaction.put(
          ACCOUNT_INFO_STATE, accountHash.getBytes().toArrayUnsafe(), accountData.toArrayUnsafe());
      return this;
    }

    public BinTrieUpdater removeAccountInfoState(final Hash accountHash) {
      composedWorldStateTransaction.remove(
          ACCOUNT_INFO_STATE, accountHash.getBytes().toArrayUnsafe());
      return this;
    }

    public BinTrieUpdater putStorageValueBySlotHash(
        final Hash accountHash, final Hash slotHash, final Bytes32 value) {
      composedWorldStateTransaction.put(
          ACCOUNT_STORAGE_STORAGE,
          Bytes.concatenate(accountHash.getBytes(), slotHash.getBytes()).toArrayUnsafe(),
          value.toArrayUnsafe());
      return this;
    }

    public BinTrieUpdater removeStorageValueBySlotHash(
        final Hash accountHash, final Hash slotHash) {
      composedWorldStateTransaction.remove(
          ACCOUNT_STORAGE_STORAGE,
          Bytes.concatenate(accountHash.getBytes(), slotHash.getBytes()).toArrayUnsafe());
      return this;
    }

    public BinTrieUpdater putAccountData(final Bytes key, final Bytes value) {
      composedWorldStateTransaction.put(
          ACCOUNT_INFO_STATE, key.toArrayUnsafe(), value.toArrayUnsafe());
      return this;
    }

    public BinTrieUpdater putStorageData(final Bytes key, final Bytes value) {
      composedWorldStateTransaction.put(
          ACCOUNT_STORAGE_STORAGE, key.toArrayUnsafe(), value.toArrayUnsafe());
      return this;
    }

    @Override
    public PathBasedWorldStateKeyValueStorage.Updater saveWorldState(
        final Bytes blockHash, final Bytes32 nodeHash, final Bytes node) {
      composedWorldStateTransaction.put(
          TRIE_BRANCH_STORAGE, WORLD_ROOT_HASH_KEY, nodeHash.toArrayUnsafe());
      composedWorldStateTransaction.put(
          TRIE_BRANCH_STORAGE, WORLD_BLOCK_HASH_KEY, blockHash.toArrayUnsafe());
      return this;
    }

    @Override
    public SegmentedKeyValueStorageTransaction getWorldStateTransaction() {
      return composedWorldStateTransaction;
    }

    @Override
    public KeyValueStorageTransaction getTrieLogStorageTransaction() {
      return trieLogStorageTransaction;
    }

    @Override
    public void commit() {
      composedWorldStateTransaction.commit();
      trieLogStorageTransaction.commit();
    }

    @Override
    public void commitTrieLogOnly() {
      trieLogStorageTransaction.commit();
    }

    @Override
    public void commitComposedOnly() {
      composedWorldStateTransaction.commit();
    }

    @Override
    public void rollback() {
      composedWorldStateTransaction.rollback();
      trieLogStorageTransaction.rollback();
    }
  }
}
