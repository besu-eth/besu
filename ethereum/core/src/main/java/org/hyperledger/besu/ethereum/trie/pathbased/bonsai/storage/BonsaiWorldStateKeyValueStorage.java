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
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache.FlatDbCacheManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache.VersionedFlatDbCacheManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFlatDbStrategyProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.FlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.TrieNodeStrategy;
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
import org.hyperledger.besu.util.Subscribers;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import kotlin.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BonsaiWorldStateKeyValueStorage implements WorldStateKeyValueStorage, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(BonsaiWorldStateKeyValueStorage.class);

  // 0x776f726c64426c6f636b48617368
  public static final byte[] WORLD_BLOCK_HASH_KEY =
      "worldBlockHash".getBytes(StandardCharsets.UTF_8);
  // 0x776f726c64426c6f636b4e756d626572
  public static final byte[] WORLD_BLOCK_NUMBER_KEY =
      "worldBlockNumber".getBytes(StandardCharsets.UTF_8);
  // 0x776f726c64526f6f74
  public static final byte[] WORLD_ROOT_HASH_KEY = "worldRoot".getBytes(StandardCharsets.UTF_8);

  private final AtomicBoolean shouldClose = new AtomicBoolean(false);

  protected final AtomicBoolean isClosed = new AtomicBoolean(false);

  protected final Subscribers<StorageSubscriber> subscribers = Subscribers.create();
  protected final SegmentedKeyValueStorage composedWorldStateStorage;
  protected final KeyValueStorage trieLogStorage;

  protected final BonsaiFlatDbStrategyProvider flatDbStrategyProvider;
  protected final FlatDbCacheManager cacheManager;
  private volatile long cacheVersion;
  protected volatile TrieNodeStrategy trieNodeStrategy;

  // --- Construction ---

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
      final FlatDbCacheManager cacheManager) {
    this.composedWorldStateStorage =
        provider.getStorageBySegmentIdentifiers(
            List.of(
                ACCOUNT_INFO_STATE, CODE_STORAGE, ACCOUNT_STORAGE_STORAGE, TRIE_BRANCH_STORAGE));
    this.trieLogStorage =
        provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.TRIE_LOG_STORAGE);
    this.flatDbStrategyProvider =
        new BonsaiFlatDbStrategyProvider(metricsSystem, dataStorageConfiguration);
    flatDbStrategyProvider.loadFlatDbStrategy(composedWorldStateStorage);

    this.cacheManager = cacheManager;
    this.cacheVersion = cacheManager.getCurrentVersion();
    this.trieNodeStrategy = new BonsaiTrieNodeStrategy();
  }

  public BonsaiWorldStateKeyValueStorage(
      final BonsaiFlatDbStrategyProvider flatDbStrategyProvider,
      final SegmentedKeyValueStorage composedWorldStateStorage,
      final KeyValueStorage trieLogStorage,
      final FlatDbCacheManager cacheManager,
      final long cacheVersion) {
    this(
        flatDbStrategyProvider,
        composedWorldStateStorage,
        trieLogStorage,
        cacheManager,
        cacheVersion,
        new BonsaiTrieNodeStrategy());
  }

  public BonsaiWorldStateKeyValueStorage(
      final BonsaiFlatDbStrategyProvider flatDbStrategyProvider,
      final SegmentedKeyValueStorage composedWorldStateStorage,
      final KeyValueStorage trieLogStorage,
      final FlatDbCacheManager cacheManager,
      final long cacheVersion,
      final TrieNodeStrategy trieNodeStrategy) {
    this.composedWorldStateStorage = composedWorldStateStorage;
    this.trieLogStorage = trieLogStorage;
    this.flatDbStrategyProvider = flatDbStrategyProvider;
    this.cacheManager = cacheManager;
    this.cacheVersion = cacheVersion;
    this.trieNodeStrategy = trieNodeStrategy;
  }

  private static FlatDbCacheManager createCacheManager(
      final DataStorageConfiguration dataStorageConfiguration, final MetricsSystem metricsSystem) {
    if (dataStorageConfiguration
        .getPathBasedExtraStorageConfiguration()
        .getUnstable()
        .getBonsaiCrossBlockCacheEnabled()) {
      return new VersionedFlatDbCacheManager(
          dataStorageConfiguration
              .getPathBasedExtraStorageConfiguration()
              .getUnstable()
              .getBonsaiCrossBlockCacheAccountSize(),
          dataStorageConfiguration
              .getPathBasedExtraStorageConfiguration()
              .getUnstable()
              .getBonsaiCrossBlockCacheStorageSize(),
          metricsSystem);
    } else {
      return FlatDbCacheManager.NO_OP_CACHE;
    }
  }

  // --- Lifecycle ---

  @Override
  public synchronized void close() throws Exception {
    shouldClose.set(true);
    tryClose();
  }

  public synchronized long subscribe(final StorageSubscriber sub) {
    if (isClosed.get()) {
      throw new RuntimeException("Storage is marked to close or has already closed");
    }
    return subscribers.subscribe(sub);
  }

  public synchronized void unSubscribe(final long id) {
    subscribers.unsubscribe(id);
    try {
      tryClose();
    } catch (Exception e) {
      LOG.atWarn()
          .setMessage("exception while trying to close : {}")
          .addArgument(e::getMessage)
          .log();
    }
  }

  protected synchronized void tryClose() throws Exception {
    if (shouldClose.get() && subscribers.getSubscriberCount() < 1) {
      doClose();
    }
  }

  protected synchronized void doClose() throws Exception {
    if (!isClosed.get()) {
      subscribers.forEach(StorageSubscriber::onCloseStorage);
      composedWorldStateStorage.close();
      trieLogStorage.close();
      isClosed.set(true);
    }
  }

  // --- Basic accessors ---

  public SegmentedKeyValueStorage getComposedWorldStateStorage() {
    return composedWorldStateStorage;
  }

  public KeyValueStorage getTrieLogStorage() {
    return trieLogStorage;
  }

  @Override
  public DataStorageFormat getDataStorageFormat() {
    return DataStorageFormat.BONSAI;
  }

  public FlatDbMode getFlatDbMode() {
    return flatDbStrategyProvider.getFlatDbMode();
  }

  public BonsaiFlatDbStrategy getFlatDbStrategy() {
    return (BonsaiFlatDbStrategy)
        flatDbStrategyProvider.getFlatDbStrategy(composedWorldStateStorage);
  }

  public BonsaiFlatDbStrategyProvider getFlatDbStrategyProvider() {
    return flatDbStrategyProvider;
  }

  public TrieNodeStrategy getTrieNodeStrategy() {
    return trieNodeStrategy;
  }

  public void setTrieNodeStrategy(final TrieNodeStrategy strategy) {
    this.trieNodeStrategy = strategy;
  }

  public FlatDbCacheManager getCacheManager() {
    return cacheManager;
  }

  public long getCurrentVersion() {
    return cacheVersion;
  }

  public long getCacheSize(final SegmentIdentifier segment) {
    return cacheManager.getCacheSize(segment);
  }

  public boolean isCached(final SegmentIdentifier segment, final Bytes key) {
    return cacheManager.isCached(segment, key);
  }

  public Optional<FlatDbCacheManager.VersionedValue> getCachedValue(
      final SegmentIdentifier segment, final Bytes key) {
    return cacheManager.getCachedValue(segment, key);
  }

  // --- World state metadata ---

  public Optional<Hash> getWorldStateBlockHash() {
    return composedWorldStateStorage
        .get(TRIE_BRANCH_STORAGE, WORLD_BLOCK_HASH_KEY)
        .map(Bytes32::wrap)
        .map(Hash::wrap);
  }

  public Optional<Long> getWorldStateBlockNumber() {
    return composedWorldStateStorage
        .get(TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY)
        .map(bytes -> Bytes.wrap(bytes).toLong());
  }

  public Optional<Bytes> getWorldStateRootHash() {
    return composedWorldStateStorage.get(TRIE_BRANCH_STORAGE, WORLD_ROOT_HASH_KEY).map(Bytes::wrap);
  }

  public Optional<Bytes> getStateTrieNode(final Bytes location) {
    return composedWorldStateStorage
        .get(TRIE_BRANCH_STORAGE, location.toArrayUnsafe())
        .map(Bytes::wrap);
  }

  public boolean isWorldStateAvailable(final Bytes32 rootHash, final Hash blockHash) {
    return composedWorldStateStorage
        .get(TRIE_BRANCH_STORAGE, WORLD_ROOT_HASH_KEY)
        .map(Bytes32::wrap)
        .map(
            hash ->
                hash.equals(rootHash)
                    || trieLogStorage.containsKey(blockHash.getBytes().toArrayUnsafe()))
        .orElse(false);
  }

  // --- Trie log ---

  public Optional<byte[]> getTrieLog(final Hash blockHash) {
    return trieLogStorage.get(blockHash.getBytes().toArrayUnsafe());
  }

  public Stream<byte[]> streamTrieLogKeys(final long limit) {
    return trieLogStorage.streamKeys().limit(limit);
  }

  public void clearTrieLog() {
    subscribers.forEach(StorageSubscriber::onClearTrieLog);
    trieLogStorage.clear();
  }

  public boolean pruneTrieLog(final Hash blockHash) {
    try {
      return trieLogStorage.tryDelete(blockHash.getBytes().toArrayUnsafe());
    } catch (Exception e) {
      LOG.error("Error pruning trie log for block hash {}", blockHash, e);
      return false;
    }
  }

  // --- Reads: accounts, storage, code, trie nodes ---

  public Optional<Bytes> getAccount(final Hash accountHash) {
    return cacheManager.getFromCacheOrStorage(
        ACCOUNT_INFO_STATE,
        accountHash.getBytes(),
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
    final Bytes key =
        Bytes.concatenate(accountHash.getBytes(), storageSlotKey.getSlotHash().getBytes());
    return cacheManager.getFromCacheOrStorage(
        ACCOUNT_STORAGE_STORAGE,
        key,
        getCurrentVersion(),
        () ->
            getFlatDbStrategy()
                .getFlatStorageValueByStorageSlotKey(
                    this::getWorldStateRootHash,
                    () -> getAccount(accountHash),
                    (location, hash) -> getAccountStorageTrieNode(accountHash, location, hash),
                    accountHash,
                    storageSlotKey,
                    composedWorldStateStorage));
  }

  public Optional<Bytes> getStorageValueByStorageSlotKey(
      final Supplier<Optional<Bytes>> accountSupplier,
      final Hash accountHash,
      final StorageSlotKey storageSlotKey) {
    final Bytes key =
        Bytes.concatenate(accountHash.getBytes(), storageSlotKey.getSlotHash().getBytes());
    return cacheManager.getFromCacheOrStorage(
        ACCOUNT_STORAGE_STORAGE,
        key,
        getCurrentVersion(),
        () ->
            getFlatDbStrategy()
                .getFlatStorageValueByStorageSlotKey(
                    this::getWorldStateRootHash,
                    accountSupplier,
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
    return trieNodeStrategy
        .getFlatAccountTrieNode(location, nodeHash, composedWorldStateStorage)
        .filter(b -> Hash.hash(b).getBytes().equals(nodeHash));
  }

  public Optional<Bytes> getAccountStorageTrieNode(
      final Hash accountHash, final Bytes location, final Bytes32 nodeHash) {
    if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      return Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
    }
    return trieNodeStrategy
        .getFlatStorageTrieNode(accountHash, location, nodeHash, composedWorldStateStorage)
        .filter(b -> Hash.hash(b).getBytes().equals(nodeHash));
  }

  /** Unsafe raw read by fully-qualified key, with no node-hash verification. */
  public Optional<Bytes> getTrieNodeUnsafe(final Bytes key) {
    return composedWorldStateStorage.get(TRIE_BRANCH_STORAGE, key.toArrayUnsafe()).map(Bytes::wrap);
  }

  public NavigableMap<Bytes32, Bytes> streamFlatAccounts(
      final Bytes startKeyHash, final Bytes32 endKeyHash, final long max) {
    return getFlatDbStrategy()
        .streamAccountFlatDatabase(composedWorldStateStorage, startKeyHash, endKeyHash, max);
  }

  public NavigableMap<Bytes32, Bytes> streamFlatAccounts(
      final Bytes startKeyHash, final Predicate<Pair<Bytes32, Bytes>> takeWhile) {
    return getFlatDbStrategy()
        .streamAccountFlatDatabase(composedWorldStateStorage, startKeyHash, takeWhile);
  }

  public NavigableMap<Bytes32, Bytes> streamFlatStorages(
      final Hash accountHash, final Bytes startKeyHash, final Bytes32 endKeyHash, final long max) {
    return getFlatDbStrategy()
        .streamStorageFlatDatabase(
            composedWorldStateStorage, accountHash, startKeyHash, endKeyHash, max);
  }

  public NavigableMap<Bytes32, Bytes> streamFlatStorages(
      final Hash accountHash,
      final Bytes startKeyHash,
      final Predicate<Pair<Bytes32, Bytes>> takeWhile) {
    return getFlatDbStrategy()
        .streamStorageFlatDatabase(composedWorldStateStorage, accountHash, startKeyHash, takeWhile);
  }

  public NavigableMap<Bytes32, AccountStorageEntry> storageEntriesFrom(
      final Hash addressHash, final Bytes32 startKeyHash, final int limit) {
    throw new RuntimeException("Bonsai Tries does not currently support enumerating storage");
  }

  // --- Mode switching and clearing ---

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

  @Override
  public void clear() {
    subscribers.forEach(StorageSubscriber::onClearStorage);
    getFlatDbStrategy().clearAll(composedWorldStateStorage);
    composedWorldStateStorage.clear(TRIE_BRANCH_STORAGE);
    trieLogStorage.clear();
    cacheManager.clear(ACCOUNT_INFO_STATE);
    cacheManager.clear(ACCOUNT_STORAGE_STORAGE);
    flatDbStrategyProvider.loadFlatDbStrategy(composedWorldStateStorage);
  }

  public void clearFlatDatabase() {
    subscribers.forEach(StorageSubscriber::onClearFlatDatabaseStorage);
    getFlatDbStrategy().resetOnResync(composedWorldStateStorage);
    cacheManager.clear(ACCOUNT_INFO_STATE);
    cacheManager.clear(ACCOUNT_STORAGE_STORAGE);
  }

  public void clearTrie() {
    subscribers.forEach(StorageSubscriber::onClearTrie);
    composedWorldStateStorage.clear(TRIE_BRANCH_STORAGE);
  }

  // --- Updater factory ---

  @Override
  public Updater updater() {
    return new CachedUpdater(
        composedWorldStateStorage.startTransaction(),
        trieLogStorage.startTransaction(),
        getFlatDbStrategy(),
        composedWorldStateStorage,
        trieNodeStrategy);
  }

  /** Base updater that writes directly to storage without cache management. */
  public static class Updater implements WorldStateKeyValueStorage.Updater {

    protected final SegmentedKeyValueStorageTransaction composedWorldStateTransaction;
    protected final KeyValueStorageTransaction trieLogStorageTransaction;
    protected final FlatDbStrategy flatDbStrategy;
    protected final SegmentedKeyValueStorage worldStorage;
    protected final TrieNodeStrategy trieNodeStrategy;

    public Updater(
        final SegmentedKeyValueStorageTransaction composedWorldStateTransaction,
        final KeyValueStorageTransaction trieLogStorageTransaction,
        final FlatDbStrategy flatDbStrategy,
        final SegmentedKeyValueStorage worldStorage,
        final TrieNodeStrategy trieNodeStrategy) {

      this.composedWorldStateTransaction = composedWorldStateTransaction;
      this.trieLogStorageTransaction = trieLogStorageTransaction;
      this.flatDbStrategy = flatDbStrategy;
      this.worldStorage = worldStorage;
      this.trieNodeStrategy = trieNodeStrategy;
    }

    // --- World state metadata ---

    public Updater saveWorldState(final Bytes blockHash, final Bytes32 nodeHash, final Bytes node) {
      composedWorldStateTransaction.put(
          TRIE_BRANCH_STORAGE, Bytes.EMPTY.toArrayUnsafe(), node.toArrayUnsafe());
      composedWorldStateTransaction.put(
          TRIE_BRANCH_STORAGE, WORLD_BLOCK_HASH_KEY, blockHash.toArrayUnsafe());
      composedWorldStateTransaction.put(
          TRIE_BRANCH_STORAGE, WORLD_ROOT_HASH_KEY, nodeHash.toArrayUnsafe());
      return this;
    }

    // --- Accounts ---

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

    // --- Code ---

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

    // --- Storage slots ---

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

    // --- Trie nodes ---

    public Updater putAccountStateTrieNode(
        final Bytes location, final Bytes32 nodeHash, final Bytes node) {
      if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
        return this;
      }
      trieNodeStrategy.putFlatAccountTrieNode(
          worldStorage, composedWorldStateTransaction, location, nodeHash, node);
      return this;
    }

    public Updater removeAccountStateTrieNode(final Bytes location) {
      trieNodeStrategy.removeFlatAccountStateTrieNode(
          worldStorage, composedWorldStateTransaction, location);
      return this;
    }

    public synchronized Updater putAccountStorageTrieNode(
        final Hash accountHash, final Bytes location, final Bytes32 nodeHash, final Bytes node) {
      if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
        return this;
      }
      trieNodeStrategy.putFlatStorageTrieNode(
          worldStorage, composedWorldStateTransaction, accountHash, location, nodeHash, node);
      return this;
    }

    // --- Transaction access ---

    public SegmentedKeyValueStorageTransaction getWorldStateTransaction() {
      return composedWorldStateTransaction;
    }

    public KeyValueStorageTransaction getTrieLogStorageTransaction() {
      return trieLogStorageTransaction;
    }

    // --- Commit / rollback ---

    @Override
    public void commit() {
      trieLogStorageTransaction.commit();
      composedWorldStateTransaction.commit();
    }

    public void commitTrieLogOnly() {
      trieLogStorageTransaction.commit();
      composedWorldStateTransaction.close();
    }

    public void commitComposedOnly() {
      composedWorldStateTransaction.commit();
      trieLogStorageTransaction.close();
    }

    public void rollback() {
      composedWorldStateTransaction.rollback();
      trieLogStorageTransaction.rollback();
    }
  }

  /**
   * Cached updater that stages changes and refreshes the cache only after a successful storage
   * commit ({@code updateCache()} is not run if {@code super.commit()} fails). Used only by base
   * storage (not snapshots or layers).
   */
  public class CachedUpdater extends Updater {

    private static final int INITIAL_CACHE_MAP_CAPACITY = 1024;

    /** Single map per segment. Value {@code null} encodes a staged removal (last-write-wins). */
    private final Map<SegmentIdentifier, Map<Bytes, Bytes>> pending = new HashMap<>();

    public CachedUpdater(
        final SegmentedKeyValueStorageTransaction composedWorldStateTransaction,
        final KeyValueStorageTransaction trieLogStorageTransaction,
        final FlatDbStrategy flatDbStrategy,
        final SegmentedKeyValueStorage worldStorage,
        final TrieNodeStrategy trieNodeStrategy) {
      super(
          composedWorldStateTransaction,
          trieLogStorageTransaction,
          flatDbStrategy,
          worldStorage,
          trieNodeStrategy);
    }

    // --- Accounts ---

    @Override
    public Updater putAccountInfoState(final Hash accountHash, final Bytes accountValue) {
      if (!accountValue.isEmpty()) {
        stagePut(ACCOUNT_INFO_STATE, accountHash.getBytes(), accountValue);
      }
      return super.putAccountInfoState(accountHash, accountValue);
    }

    @Override
    public Updater removeAccountInfoState(final Hash accountHash) {
      stageRemoval(ACCOUNT_INFO_STATE, accountHash.getBytes());
      return super.removeAccountInfoState(accountHash);
    }

    // --- Storage slots ---

    @Override
    public synchronized Updater putStorageValueBySlotHash(
        final Hash accountHash, final Hash slotHash, final Bytes storageValue) {
      stagePut(
          ACCOUNT_STORAGE_STORAGE,
          Bytes.concatenate(accountHash.getBytes(), slotHash.getBytes()),
          storageValue);
      return super.putStorageValueBySlotHash(accountHash, slotHash, storageValue);
    }

    @Override
    public synchronized void removeStorageValueBySlotHash(
        final Hash accountHash, final Hash slotHash) {
      stageRemoval(
          ACCOUNT_STORAGE_STORAGE, Bytes.concatenate(accountHash.getBytes(), slotHash.getBytes()));
      super.removeStorageValueBySlotHash(accountHash, slotHash);
    }

    // --- Cache staging helpers ---

    private void stagePut(final SegmentIdentifier segment, final Bytes key, final Bytes value) {
      pending
          .computeIfAbsent(segment, s -> new HashMap<>(INITIAL_CACHE_MAP_CAPACITY))
          .put(key, value);
    }

    private void stageRemoval(final SegmentIdentifier segment, final Bytes key) {
      pending
          .computeIfAbsent(segment, s -> new HashMap<>(INITIAL_CACHE_MAP_CAPACITY))
          .put(key, null);
    }

    private void clearStaged() {
      pending.clear();
    }

    protected void incrementCacheVersion() {
      cacheVersion = cacheManager.incrementAndGetVersion();
    }

    protected void updateCache() {
      pending.forEach(
          (segment, updates) ->
              updates.forEach(
                  (key, value) -> {
                    if (value == null) {
                      cacheManager.removeFromCache(segment, key, cacheVersion);
                    } else {
                      cacheManager.putInCache(segment, key, value, cacheVersion);
                    }
                  }));
      clearStaged();
      cacheManager.scheduleAsyncMaintenance();
    }

    // --- Commit / rollback ---

    @Override
    public void commit() {
      incrementCacheVersion();
      super.commit();
      updateCache();
    }

    @Override
    public void commitTrieLogOnly() {
      clearStaged();
      super.commitTrieLogOnly();
    }

    @Override
    public void commitComposedOnly() {
      incrementCacheVersion();
      super.commitComposedOnly();
      updateCache();
    }

    @Override
    public void rollback() {
      clearStaged();
      super.rollback();
    }
  }
}
