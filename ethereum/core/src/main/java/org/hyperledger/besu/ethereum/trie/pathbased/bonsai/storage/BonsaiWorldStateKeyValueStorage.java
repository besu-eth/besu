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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFlatDbStrategyProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.FlatDbStrategy;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.evm.account.AccountStorageEntry;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;

import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

public class BonsaiWorldStateKeyValueStorage extends PathBasedWorldStateKeyValueStorage
    implements WorldStateKeyValueStorage {
  protected final BonsaiFlatDbStrategyProvider flatDbStrategyProvider;

  public BonsaiWorldStateKeyValueStorage(
      final StorageProvider provider,
      final MetricsSystem metricsSystem,
      final DataStorageConfiguration dataStorageConfiguration) {
    super(
        provider.getStorageBySegmentIdentifiers(
            List.of(
                ACCOUNT_INFO_STATE, CODE_STORAGE, ACCOUNT_STORAGE_STORAGE, TRIE_BRANCH_STORAGE)),
        provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.TRIE_LOG_STORAGE));
    this.flatDbStrategyProvider =
        new BonsaiFlatDbStrategyProvider(metricsSystem, dataStorageConfiguration);
    flatDbStrategyProvider.loadFlatDbStrategy(composedWorldStateStorage);
  }

  public BonsaiWorldStateKeyValueStorage(
      final BonsaiFlatDbStrategyProvider flatDbStrategyProvider,
      final SegmentedKeyValueStorage composedWorldStateStorage,
      final KeyValueStorage trieLogStorage) {
    super(composedWorldStateStorage, trieLogStorage);
    this.flatDbStrategyProvider = flatDbStrategyProvider;
  }

  @Override
  public DataStorageFormat getDataStorageFormat() {
    return DataStorageFormat.BONSAI;
  }

  @Override
  public FlatDbMode getFlatDbMode() {
    return flatDbStrategyProvider.getFlatDbMode();
  }

  public Optional<Bytes> getCode(final Hash codeHash, final Hash accountHash) {
    if (codeHash.equals(Hash.EMPTY)) {
      return Optional.of(Bytes.EMPTY);
    } else {
      return getFlatDbStrategy().getFlatCode(codeHash, accountHash, composedWorldStateStorage);
    }
  }

  public Optional<Bytes> getAccount(final Hash accountHash) {
    return getFlatDbStrategy()
        .getFlatAccount(
            this::getWorldStateRootHash,
            this::getAccountStateTrieNode,
            accountHash,
            composedWorldStateStorage);
  }

  public Optional<Bytes> getAccountStateTrieNode(final Bytes location, final Bytes32 nodeHash) {
    if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      return Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
    } else {
      return composedWorldStateStorage
          .get(TRIE_BRANCH_STORAGE, location.toArrayUnsafe())
          .map(Bytes::wrap)
          .filter(b -> Hash.hash(b).getBytes().equals(nodeHash));
    }
  }

  public Optional<Bytes> getAccountStorageTrieNode(
      final Hash accountHash, final Bytes location, final Bytes32 nodeHash) {
    if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      return Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
    } else {
      return composedWorldStateStorage
          .get(
              TRIE_BRANCH_STORAGE,
              Bytes.concatenate(accountHash.getBytes(), location).toArrayUnsafe())
          .map(Bytes::wrap)
          .filter(b -> Hash.hash(b).getBytes().equals(nodeHash));
    }
  }

  public Optional<Bytes> getTrieNodeUnsafe(final Bytes key) {
    return composedWorldStateStorage.get(TRIE_BRANCH_STORAGE, key.toArrayUnsafe()).map(Bytes::wrap);
  }

  public Optional<Bytes> getStorageValueByStorageSlotKey(
      final Hash accountHash, final StorageSlotKey storageSlotKey) {
    return getStorageValueByStorageSlotKey(
        () ->
            getAccount(accountHash)
                .map(
                    b ->
                        PmtStateTrieAccountValue.readFrom(
                                org.hyperledger.besu.ethereum.rlp.RLP.input(b))
                            .getStorageRoot()),
        accountHash,
        storageSlotKey);
  }

  public Optional<Bytes> getStorageValueByStorageSlotKey(
      final Supplier<Optional<Hash>> storageRootSupplier,
      final Hash accountHash,
      final StorageSlotKey storageSlotKey) {
    return getFlatDbStrategy()
        .getFlatStorageValueByStorageSlotKey(
            this::getWorldStateRootHash,
            storageRootSupplier,
            (location, hash) -> getAccountStorageTrieNode(accountHash, location, hash),
            accountHash,
            storageSlotKey,
            composedWorldStateStorage);
  }

  public NavigableMap<Bytes32, AccountStorageEntry> storageEntriesFrom(
      final Hash addressHash, final Bytes32 startKeyHash, final int limit) {
    throw new RuntimeException("Bonsai Tries does not currently support enumerating storage");
  }

  public void upgradeToFullFlatDbMode() {
    flatDbStrategyProvider.upgradeToFullFlatDbMode(composedWorldStateStorage);
  }

  public void downgradeToPartialFlatDbMode() {
    flatDbStrategyProvider.downgradeToPartialFlatDbMode(composedWorldStateStorage);
  }

  @Override
  public void clear() {
    super.clear();
    flatDbStrategyProvider.loadFlatDbStrategy(
        composedWorldStateStorage); // force reload of flat db reader strategy
  }

  @Override
  public BonsaiFlatDbStrategy getFlatDbStrategy() {
    return (BonsaiFlatDbStrategy)
        flatDbStrategyProvider.getFlatDbStrategy(composedWorldStateStorage);
  }

  @Override
  public Updater updater() {
    return new Updater(
        composedWorldStateStorage.startTransaction(),
        trieLogStorage.startTransaction(),
        getFlatDbStrategy(),
        composedWorldStateStorage);
  }

  public static class Updater implements PathBasedWorldStateKeyValueStorage.Updater {

    private final SegmentedKeyValueStorageTransaction composedWorldStateTransaction;
    private final KeyValueStorageTransaction trieLogStorageTransaction;
    private final FlatDbStrategy flatDbStrategy;
    private final SegmentedKeyValueStorage worldStorage;

    public Updater(
        final SegmentedKeyValueStorageTransaction composedWorldStateTransaction,
        final KeyValueStorageTransaction trieLogStorageTransaction,
        final FlatDbStrategy flatDbStrategy,
        final SegmentedKeyValueStorage worldStorage) {

      this.composedWorldStateTransaction = composedWorldStateTransaction;
      this.trieLogStorageTransaction = trieLogStorageTransaction;
      this.flatDbStrategy = flatDbStrategy;
      this.worldStorage =
          worldStorage; // An update could need to read from world storage to decide how to PUT to
      // it (i.e. Bonsai archive)
    }

    public Updater removeCode(final Hash accountHash, final Hash codeHash) {
      flatDbStrategy.removeFlatCode(
          worldStorage, composedWorldStateTransaction, accountHash, codeHash);
      return this;
    }

    public Updater putCode(final Hash accountHash, final Bytes code) {
      // Skip the hash calculation for empty code
      final Hash codeHash = code.size() == 0 ? Hash.EMPTY : Hash.hash(code);
      return putCode(accountHash, codeHash, code);
    }

    public Updater putCode(final Hash accountHash, final Hash codeHash, final Bytes code) {
      if (code.isEmpty()) {
        // Don't save empty values
        return this;
      }
      flatDbStrategy.putFlatCode(
          worldStorage, composedWorldStateTransaction, accountHash, codeHash, code);
      return this;
    }

    public Updater removeAccountInfoState(final Hash accountHash) {
      flatDbStrategy.removeFlatAccount(worldStorage, composedWorldStateTransaction, accountHash);
      return this;
    }

    public Updater putAccountInfoState(final Hash accountHash, final Bytes accountValue) {
      if (accountValue.isEmpty()) {
        // Don't save empty values
        return this;
      }
      flatDbStrategy.putFlatAccount(
          worldStorage, composedWorldStateTransaction, accountHash, accountValue);
      return this;
    }

    @Override
    public Updater saveWorldState(final Bytes blockHash, final Bytes32 nodeHash, final Bytes node) {
      composedWorldStateTransaction.put(
          TRIE_BRANCH_STORAGE, Bytes.EMPTY.toArrayUnsafe(), node.toArrayUnsafe());
      composedWorldStateTransaction.put(
          TRIE_BRANCH_STORAGE, WORLD_ROOT_HASH_KEY, nodeHash.toArrayUnsafe());
      composedWorldStateTransaction.put(
          TRIE_BRANCH_STORAGE, WORLD_BLOCK_HASH_KEY, blockHash.toArrayUnsafe());
      return this;
    }

    public Updater putAccountStateTrieNode(
        final Bytes location, final Bytes32 nodeHash, final Bytes node) {
      if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
        // Don't save empty nodes
        return this;
      }
      composedWorldStateTransaction.put(
          TRIE_BRANCH_STORAGE, location.toArrayUnsafe(), node.toArrayUnsafe());
      return this;
    }

    public Updater removeAccountStateTrieNode(final Bytes location) {
      composedWorldStateTransaction.remove(TRIE_BRANCH_STORAGE, location.toArrayUnsafe());
      return this;
    }

    public synchronized Updater putAccountStorageTrieNode(
        final Hash accountHash, final Bytes location, final Bytes32 nodeHash, final Bytes node) {
      if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
        // Don't save empty nodes
        return this;
      }
      composedWorldStateTransaction.put(
          TRIE_BRANCH_STORAGE,
          Bytes.concatenate(accountHash.getBytes(), location).toArrayUnsafe(),
          node.toArrayUnsafe());
      return this;
    }

    public synchronized Updater putStorageValueBySlotHash(
        final Hash accountHash, final Hash slotHash, final Bytes storageValue) {
      flatDbStrategy.putFlatAccountStorageValueByStorageSlotHash(
          worldStorage, composedWorldStateTransaction, accountHash, slotHash, storageValue);
      return this;
    }

    public synchronized void removeStorageValueBySlotHash(
        final Hash accountHash, final Hash slotHash) {
      flatDbStrategy.removeFlatAccountStorageValueByStorageSlotHash(
          worldStorage, composedWorldStateTransaction, accountHash, slotHash);
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
      // write the log ahead, then the worldstate
      trieLogStorageTransaction.commit();
      composedWorldStateTransaction.commit();
    }

    @Override
    public void commitTrieLogOnly() {
      trieLogStorageTransaction.commit();
      composedWorldStateTransaction.close();
    }

    @Override
    public void commitComposedOnly() {
      composedWorldStateTransaction.commit();
      trieLogStorageTransaction.close();
    }

    @Override
    public void rollback() {
      composedWorldStateTransaction.rollback();
      trieLogStorageTransaction.rollback();
    }
  }
}
