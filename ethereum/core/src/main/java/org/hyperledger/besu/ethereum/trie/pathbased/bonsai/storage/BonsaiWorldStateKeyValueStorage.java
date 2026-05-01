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
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache.CacheManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache.CacheManager.ByteArrayWrapper;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache.VersionedCacheManager;
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
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BonsaiWorldStateKeyValueStorage extends PathBasedWorldStateKeyValueStorage
    implements WorldStateKeyValueStorage {
  private static final Logger LOG = LoggerFactory.getLogger(BonsaiWorldStateKeyValueStorage.class);

  protected final BonsaiFlatDbStrategyProvider flatDbStrategyProvider;
  protected final CacheManager cacheManager;
  private long cacheVersion;

  public BonsaiWorldStateKeyValueStorage(
      final StorageProvider provider,
      final MetricsSystem metricsSystem,
      final DataStorageConfiguration dataStorageConfiguration) {
    this(
        provider,
        metricsSystem,
        dataStorageConfiguration,
        createCacheManager(dataStorageConfiguration, metricsSystem));
  }

  public BonsaiWorldStateKeyValueStorage(
      final StorageProvider provider,
      final MetricsSystem metricsSystem,
      final DataStorageConfiguration dataStorageConfiguration,
      final CacheManager cacheManager) {
    super(
        provider.getStorageBySegmentIdentifiers(
            List.of(
                ACCOUNT_INFO_STATE, CODE_STORAGE, ACCOUNT_STORAGE_STORAGE, TRIE_BRANCH_STORAGE)),
        provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.TRIE_LOG_STORAGE));
    this.flatDbStrategyProvider =
        new BonsaiFlatDbStrategyProvider(metricsSystem, dataStorageConfiguration);
    flatDbStrategyProvider.loadFlatDbStrategy(composedWorldStateStorage);

    this.cacheManager = cacheManager;
    this.cacheVersion = cacheManager.getCurrentVersion();
  }

  public BonsaiWorldStateKeyValueStorage(
      final BonsaiFlatDbStrategyProvider flatDbStrategyProvider,
      final SegmentedKeyValueStorage composedWorldStateStorage,
      final KeyValueStorage trieLogStorage,
      final CacheManager cacheManager,
      final long cacheVersion) {
    super(composedWorldStateStorage, trieLogStorage);
    this.flatDbStrategyProvider = flatDbStrategyProvider;
    this.cacheManager = cacheManager;
    this.cacheVersion = cacheVersion;
  }

  private static CacheManager createCacheManager(
      final DataStorageConfiguration dataStorageConfiguration, final MetricsSystem metricsSystem) {
    if (dataStorageConfiguration.getBonsaiCacheEnabled()) {
      return new VersionedCacheManager(
          100_000, // accountCacheSize
          500_000, // storageCacheSize
          metricsSystem);
    } else {
      return CacheManager.EMPTY_CACHE;
    }
  }

  @Override
  public DataStorageFormat getDataStorageFormat() {
    return DataStorageFormat.BONSAI;
  }

  @Override
  public FlatDbMode getFlatDbMode() {
    return flatDbStrategyProvider.getFlatDbMode();
  }

  public long getCurrentVersion() {
    return cacheVersion;
  }

  public Optional<Bytes> getAccount(final Hash accountHash) {
    final byte[] key = accountHash.getBytes().toArrayUnsafe();
    return cacheManager.getFromCacheOrStorage(
        ACCOUNT_INFO_STATE,
        key,
        getCurrentVersion(),
        () ->
            getFlatDbStrategy()
                .getFlatAccount(
                    this::getWorldStateRootHash,
                    this::getAccountStateTrieNode,
                    accountHash,
                    composedWorldStateStorage));
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
    final byte[] key =
        Bytes.concatenate(accountHash.getBytes(), storageSlotKey.getSlotHash().getBytes())
            .toArrayUnsafe();
    return cacheManager.getFromCacheOrStorage(
        ACCOUNT_STORAGE_STORAGE,
        key,
        getCurrentVersion(),
        () ->
            getFlatDbStrategy()
                .getFlatStorageValueByStorageSlotKey(
                    this::getWorldStateRootHash,
                    storageRootSupplier,
                    (location, hash) -> getAccountStorageTrieNode(accountHash, location, hash),
                    accountHash,
                    storageSlotKey,
                    composedWorldStateStorage));
  }

  public Optional<Bytes> getCode(final Hash codeHash, final Hash accountHash) {
    if (codeHash.equals(Hash.EMPTY)) {
      return Optional.of(Bytes.EMPTY);
    }
    return getFlatDbStrategy().getFlatCode(codeHash, accountHash, composedWorldStateStorage);
  }

  public Optional<Bytes> getAccountStateTrieNode(final Bytes location, final Bytes32 nodeHash) {
    if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      return Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
    }
    return composedWorldStateStorage
        .get(TRIE_BRANCH_STORAGE, location.toArrayUnsafe())
        .map(Bytes::wrap)
        .filter(b -> Hash.hash(b).getBytes().equals(nodeHash));
  }

  public Optional<Bytes> getAccountStorageTrieNode(
      final Hash accountHash, final Bytes location, final Bytes32 nodeHash) {
    if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      return Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
    }
    return composedWorldStateStorage
        .get(
            TRIE_BRANCH_STORAGE,
            Bytes.concatenate(accountHash.getBytes(), location).toArrayUnsafe())
        .map(Bytes::wrap)
        .filter(b -> Hash.hash(b).getBytes().equals(nodeHash));
  }

  public Optional<Bytes> getTrieNodeUnsafe(final Bytes key) {
    return composedWorldStateStorage.get(TRIE_BRANCH_STORAGE, key.toArrayUnsafe()).map(Bytes::wrap);
  }

  public NavigableMap<Bytes32, AccountStorageEntry> storageEntriesFrom(
      final Hash addressHash, final Bytes32 startKeyHash, final int limit) {
    throw new RuntimeException("Bonsai Tries does not currently support enumerating storage");
  }

  public void upgradeToFullFlatDbMode() {
    flatDbStrategyProvider.upgradeToFullFlatDbMode(composedWorldStateStorage);
    cacheManager.clear(ACCOUNT_INFO_STATE);
    cacheManager.clear(ACCOUNT_STORAGE_STORAGE);
  }

  public void upgradeToArchiveFlatDbMode() {
    flatDbStrategyProvider.upgradeToArchiveFlatDbMode(composedWorldStateStorage);
    // Invalidate cached world state snapshots that were created under the previous strategy.
    // Snapshots share the flatDbStrategyProvider, so after the switch they would use the new
    // ARCHIVE read path against stale snapshot data that lacks complete archive-keyed entries.
    subscribers.forEach(
        subscriber -> {
          try {
            subscriber.onClearFlatDatabaseStorage();
          } catch (final Exception e) {
            LOG.error("Error notifying subscriber of flat database storage upgrade", e);
          }
        });
  }

  public void downgradeToPartialFlatDbMode() {
    flatDbStrategyProvider.downgradeToPartialFlatDbMode(composedWorldStateStorage);
    cacheManager.clear(ACCOUNT_INFO_STATE);
    cacheManager.clear(ACCOUNT_STORAGE_STORAGE);
  }

  @Override
  public void clear() {
    super.clear();
    cacheManager.clear(ACCOUNT_INFO_STATE);
    cacheManager.clear(ACCOUNT_STORAGE_STORAGE);
    flatDbStrategyProvider.loadFlatDbStrategy(composedWorldStateStorage);
  }

  @Override
  public void clearFlatDatabase() {
    super.clearFlatDatabase();
    cacheManager.clear(ACCOUNT_INFO_STATE);
    cacheManager.clear(ACCOUNT_STORAGE_STORAGE);
  }

  @Override
  public BonsaiFlatDbStrategy getFlatDbStrategy() {
    return (BonsaiFlatDbStrategy)
        flatDbStrategyProvider.getFlatDbStrategy(composedWorldStateStorage);
  }

  @Override
  public Updater updater() {
    return new CachedUpdater(
        composedWorldStateStorage.startTransaction(),
        trieLogStorage.startTransaction(),
        getFlatDbStrategy(),
        composedWorldStateStorage);
  }

  public long getCacheSize(final SegmentIdentifier segment) {
    return cacheManager.getCacheSize(segment);
  }

  public boolean isCached(final SegmentIdentifier segment, final byte[] key) {
    return cacheManager.isCached(segment, key);
  }

  public Optional<CacheManager.VersionedValue> getCachedValue(
      final SegmentIdentifier segment, final byte[] key) {
    return cacheManager.getCachedValue(segment, key);
  }

  public CacheManager getCacheManager() {
    return cacheManager;
  }

  public long getCacheVersion() {
    return cacheVersion;
  }

  /** Base updater that writes directly to storage without cache management. */
  public static class Updater implements PathBasedWorldStateKeyValueStorage.Updater {

    protected final SegmentedKeyValueStorageTransaction composedWorldStateTransaction;
    protected final KeyValueStorageTransaction trieLogStorageTransaction;
    protected final FlatDbStrategy flatDbStrategy;
    protected final SegmentedKeyValueStorage worldStorage;

    public Updater(
        final SegmentedKeyValueStorageTransaction composedWorldStateTransaction,
        final KeyValueStorageTransaction trieLogStorageTransaction,
        final FlatDbStrategy flatDbStrategy,
        final SegmentedKeyValueStorage worldStorage) {

      this.composedWorldStateTransaction = composedWorldStateTransaction;
      this.trieLogStorageTransaction = trieLogStorageTransaction;
      this.flatDbStrategy = flatDbStrategy;
      this.worldStorage = worldStorage;
    }

    public Updater removeCode(final Hash accountHash, final Hash codeHash) {
      flatDbStrategy.removeFlatCode(
          worldStorage, composedWorldStateTransaction, accountHash, codeHash);
      return this;
    }

    public Updater putCode(final Hash accountHash, final Bytes code) {
      final Hash codeHash = code.size() == 0 ? Hash.EMPTY : Hash.hash(code);
      return putCode(accountHash, codeHash, code);
    }

    public Updater putCode(final Hash accountHash, final Hash codeHash, final Bytes code) {
      if (code.isEmpty()) {
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

  /**
   * Cached updater that stages changes and updates cache on commit. Used only by base storage (not
   * snapshots or layers).
   */
  public class CachedUpdater extends Updater {

    private final Map<SegmentIdentifier, Map<ByteArrayWrapper, byte[]>> pending = new HashMap<>();
    private final Map<SegmentIdentifier, Map<ByteArrayWrapper, Boolean>> pendingRemovals =
        new HashMap<>();

    public CachedUpdater(
        final SegmentedKeyValueStorageTransaction composedWorldStateTransaction,
        final KeyValueStorageTransaction trieLogStorageTransaction,
        final FlatDbStrategy flatDbStrategy,
        final SegmentedKeyValueStorage worldStorage) {
      super(composedWorldStateTransaction, trieLogStorageTransaction, flatDbStrategy, worldStorage);
    }

    @Override
    public Updater putAccountInfoState(final Hash accountHash, final Bytes accountValue) {
      if (!accountValue.isEmpty()) {
        stagePut(
            ACCOUNT_INFO_STATE,
            accountHash.getBytes().toArrayUnsafe(),
            accountValue.toArrayUnsafe());
      }
      return super.putAccountInfoState(accountHash, accountValue);
    }

    @Override
    public Updater removeAccountInfoState(final Hash accountHash) {
      stageRemoval(ACCOUNT_INFO_STATE, accountHash.getBytes().toArrayUnsafe());
      return super.removeAccountInfoState(accountHash);
    }

    @Override
    public synchronized Updater putStorageValueBySlotHash(
        final Hash accountHash, final Hash slotHash, final Bytes storageValue) {
      final byte[] key =
          Bytes.concatenate(accountHash.getBytes(), slotHash.getBytes()).toArrayUnsafe();
      stagePut(ACCOUNT_STORAGE_STORAGE, key, storageValue.toArrayUnsafe());
      return super.putStorageValueBySlotHash(accountHash, slotHash, storageValue);
    }

    @Override
    public synchronized void removeStorageValueBySlotHash(
        final Hash accountHash, final Hash slotHash) {
      final byte[] key =
          Bytes.concatenate(accountHash.getBytes(), slotHash.getBytes()).toArrayUnsafe();
      stageRemoval(ACCOUNT_STORAGE_STORAGE, key);
      super.removeStorageValueBySlotHash(accountHash, slotHash);
    }

    private void stagePut(final SegmentIdentifier segment, final byte[] key, final byte[] value) {
      pending.computeIfAbsent(segment, k -> new HashMap<>()).put(new ByteArrayWrapper(key), value);
    }

    private void stageRemoval(final SegmentIdentifier segment, final byte[] key) {
      pendingRemovals
          .computeIfAbsent(segment, k -> new HashMap<>())
          .put(new ByteArrayWrapper(key), true);
    }

    private void clearStaged() {
      pending.clear();
      pendingRemovals.clear();
    }

    protected void updateCache() {
      cacheVersion = cacheManager.incrementAndGetVersion();
      // Apply all pending updates to cache
      pending.forEach(
          (segment, updates) ->
              updates.forEach(
                  (wrapper, value) ->
                      cacheManager.putInCache(segment, wrapper.getData(), value, cacheVersion)));

      // Apply all pending removals to cache
      pendingRemovals.forEach(
          (segment, removals) ->
              removals
                  .keySet()
                  .forEach(
                      wrapper ->
                          cacheManager.removeFromCache(segment, wrapper.getData(), cacheVersion)));

      clearStaged();
      cacheManager.performMaintenance();
    }

    @Override
    public void commit() {
      updateCache();
      super.commit();
    }

    @Override
    public void commitTrieLogOnly() {
      clearStaged();
      super.commitTrieLogOnly();
    }

    @Override
    public void commitComposedOnly() {
      updateCache();
      super.commitComposedOnly();
    }

    @Override
    public void rollback() {
      clearStaged();
      super.rollback();
    }
  }
}
