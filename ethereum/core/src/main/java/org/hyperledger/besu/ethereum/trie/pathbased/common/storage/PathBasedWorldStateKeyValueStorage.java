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
package org.hyperledger.besu.ethereum.trie.pathbased.common.storage;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_ARCHIVE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_FREEZER;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_ARCHIVE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_FREEZER;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveFlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
import org.hyperledger.besu.ethereum.trie.pathbased.common.StorageSubscriber;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.FlatDbStrategy;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;
import org.hyperledger.besu.util.Subscribers;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Stream;

import kotlin.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PathBasedWorldStateKeyValueStorage
    implements WorldStateKeyValueStorage, AutoCloseable {
  private static final Logger LOG =
      LoggerFactory.getLogger(PathBasedWorldStateKeyValueStorage.class);

  // 0x776f726c64526f6f74
  public static final byte[] WORLD_ROOT_HASH_KEY = "worldRoot".getBytes(StandardCharsets.UTF_8);
  // 0x776f726c64426c6f636b48617368
  public static final byte[] WORLD_BLOCK_HASH_KEY =
      "worldBlockHash".getBytes(StandardCharsets.UTF_8);
  // 0x776f726c64426c6f636b4e756d626572
  public static final byte[] WORLD_BLOCK_NUMBER_KEY =
      "worldBlockNumber".getBytes(StandardCharsets.UTF_8);

  // 0x6172636869766564426C6F636B73
  public static final byte[] ARCHIVED_BLOCKS = "archivedBlocks".getBytes(StandardCharsets.UTF_8);

  private final AtomicBoolean shouldClose = new AtomicBoolean(false);

  protected final AtomicBoolean isClosed = new AtomicBoolean(false);

  protected final Subscribers<StorageSubscriber> subscribers = Subscribers.create();
  protected final SegmentedKeyValueStorage composedWorldStateStorage;
  protected final KeyValueStorage trieLogStorage;

  public PathBasedWorldStateKeyValueStorage(final StorageProvider provider) {
    this.composedWorldStateStorage =
        provider.getStorageBySegmentIdentifiers(
            List.of(
                ACCOUNT_INFO_STATE, CODE_STORAGE, ACCOUNT_STORAGE_STORAGE, TRIE_BRANCH_STORAGE));
    this.trieLogStorage =
        provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.TRIE_LOG_STORAGE);
  }

  public PathBasedWorldStateKeyValueStorage(
      final SegmentedKeyValueStorage composedWorldStateStorage,
      final KeyValueStorage trieLogStorage) {
    this.composedWorldStateStorage = composedWorldStateStorage;
    this.trieLogStorage = trieLogStorage;
  }

  public abstract FlatDbMode getFlatDbMode();

  public abstract FlatDbStrategy getFlatDbStrategy();

  @Override
  public abstract DataStorageFormat getDataStorageFormat();

  public SegmentedKeyValueStorage getComposedWorldStateStorage() {
    return composedWorldStateStorage;
  }

  public KeyValueStorage getTrieLogStorage() {
    return trieLogStorage;
  }

  public Optional<byte[]> getTrieLog(final Hash blockHash) {
    return trieLogStorage.get(blockHash.getBytes().toArrayUnsafe());
  }

  public Stream<byte[]> streamTrieLogKeys(final long limit) {
    return trieLogStorage.streamKeys().limit(limit);
  }

  public Optional<Bytes> getStateTrieNode(final Bytes location) {
    return composedWorldStateStorage
        .get(TRIE_BRANCH_STORAGE, location.toArrayUnsafe())
        .map(Bytes::wrap);
  }

  public Optional<Bytes> getWorldStateRootHash() {
    return composedWorldStateStorage.get(TRIE_BRANCH_STORAGE, WORLD_ROOT_HASH_KEY).map(Bytes::wrap);
  }

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

  @Override
  public void clear() {
    subscribers.forEach(StorageSubscriber::onClearStorage);
    getFlatDbStrategy().clearAll(composedWorldStateStorage);
    composedWorldStateStorage.clear(TRIE_BRANCH_STORAGE);
    trieLogStorage.clear();
  }

  public void clearTrieLog() {
    subscribers.forEach(StorageSubscriber::onClearTrieLog);
    trieLogStorage.clear();
  }

  public void clearTrie() {
    subscribers.forEach(StorageSubscriber::onClearTrie);
    composedWorldStateStorage.clear(TRIE_BRANCH_STORAGE);
  }

  public void clearFlatDatabase() {
    subscribers.forEach(StorageSubscriber::onClearFlatDatabaseStorage);
    getFlatDbStrategy().resetOnResync(composedWorldStateStorage);
  }

  @Override
  public abstract Updater updater();

  public boolean pruneTrieLog(final Hash blockHash) {
    try {
      return trieLogStorage.tryDelete(blockHash.getBytes().toArrayUnsafe());
    } catch (Exception e) {
      LOG.error("Error pruning trie log for block hash {}", blockHash, e);
      return false;
    }
  }

  /**
   * Move old account state from the primary DB archive segment to the archive freezer segment that
   * will only be used for historic state queries. This prevents performance degradation over time
   * for writes to the primary archive DB segments.
   *
   * @param previousBlockHeader the block header for the previous block, used to get the "nearest
   *     before" state
   * @param accountHash the account to archive old state for
   * @return the number of account states that were moved to the archive
   */
  public int archivePreviousAccountState(
      final Optional<BlockHeader> previousBlockHeader, final Hash accountHash) {
    AtomicInteger archivedStateCount = new AtomicInteger();
    if (previousBlockHeader.isPresent()) {
      try {
        // Get the key for the previous block
        final BonsaiContext previousContext =
            new BonsaiContext(previousBlockHeader.get().getNumber());
        final Bytes previousKey =
            Bytes.of(
                BonsaiArchiveFlatDbStrategy.calculateArchiveKeyWithMinSuffix(
                    previousContext, accountHash.getBytes().toArrayUnsafe()));

        Optional<SegmentedKeyValueStorage.NearestKeyValue> nextMatch;

        // Move all entries that match this address hash to the archive DB segment
        while ((nextMatch =
                composedWorldStateStorage
                    .getNearestBefore(ACCOUNT_INFO_STATE_ARCHIVE, previousKey)
                    .filter(
                        found ->
                            found.value().isPresent()
                                && accountHash.getBytes().commonPrefixLength(found.key())
                                    >= accountHash.getBytes().size()))
            .isPresent()) {
          nextMatch.stream()
              .forEach(
                  (nearestKey) -> {
                    moveDBEntry(
                        ACCOUNT_INFO_STATE_ARCHIVE,
                        ACCOUNT_INFO_STATE_FREEZER,
                        nearestKey.key().toArrayUnsafe(),
                        nearestKey.value().get());
                    archivedStateCount.getAndIncrement();
                  });
        }

        if (archivedStateCount.get() == 0) {
          // A lot of entries will have no previous history, so use trace to log when no previous
          // storage was found
          LOG.atTrace()
              .setMessage("no previous state found for block {}, address hash {}")
              .addArgument(previousBlockHeader.get().getNumber())
              .addArgument(accountHash)
              .log();
        } else {
          LOG.atDebug()
              .setMessage("{} storage entries archived for block {}, address hash {}")
              .addArgument(archivedStateCount.get())
              .addArgument(previousBlockHeader.get().getNumber())
              .addArgument(accountHash)
              .log();
        }
      } catch (Exception e) {
        LOG.error("Error moving account state for account {} to archived storage", accountHash, e);
      }
    }

    return archivedStateCount.get();
  }

  /**
   * Move old storage state from the primary archive DB segment to the archive freezer segment that
   * will only be used for historic state queries. This prevents performance degradation over time
   * for writes to the primary DB segments.
   *
   * @param previousBlockHeader the block header for the previous block, used to get the "nearest
   *     before" state
   * @param storageSlotKey the storage slot to archive old state for
   * @return the number of storage states that were moved to archive storage
   */
  public int archivePreviousStorageState(
      final Optional<BlockHeader> previousBlockHeader, final Bytes storageSlotKey) {
    AtomicInteger archivedStorageCount = new AtomicInteger();
    if (previousBlockHeader.isPresent()) {
      try {
        // Get the key for the previous block
        final BonsaiContext previousContext =
            new BonsaiContext(previousBlockHeader.get().getNumber());
        final Bytes previousKey =
            Bytes.of(
                BonsaiArchiveFlatDbStrategy.calculateArchiveKeyWithMinSuffix(
                    previousContext, storageSlotKey.toArrayUnsafe()));

        Optional<SegmentedKeyValueStorage.NearestKeyValue> nextMatch;

        // Move all entries that match the storage hash for this address & slot
        // to the archive DB segment
        while ((nextMatch =
                composedWorldStateStorage
                    .getNearestBefore(ACCOUNT_STORAGE_ARCHIVE, previousKey)
                    .filter(
                        found ->
                            found.value().isPresent()
                                && storageSlotKey.commonPrefixLength(found.key())
                                    >= storageSlotKey.size()))
            .isPresent()) {
          nextMatch.stream()
              .forEach(
                  (nearestKey) -> {
                    if (archivedStorageCount.get() > 0 && archivedStorageCount.get() % 100 == 0) {
                      // Log progress in case catching up causes there to be a large number of keys
                      // to move
                      LOG.atDebug()
                          .setMessage(
                              "{} storage entries archived for block {}, slot hash {}, latest key {}")
                          .addArgument(archivedStorageCount.get())
                          .addArgument(previousBlockHeader.get().getNumber())
                          .addArgument(storageSlotKey)
                          .addArgument(nearestKey.key())
                          .log();
                    }
                    moveDBEntry(
                        ACCOUNT_STORAGE_ARCHIVE,
                        ACCOUNT_STORAGE_FREEZER,
                        nearestKey.key().toArrayUnsafe(),
                        nearestKey.value().get());
                    archivedStorageCount.getAndIncrement();
                  });
        }

        if (archivedStorageCount.get() == 0) {
          // A lot of entries will have no previous history, so use trace to log when no previous
          // storage was found
          LOG.atTrace()
              .setMessage("no previous storage found for block {}, slot hash {}")
              .addArgument(previousBlockHeader.get().getNumber())
              .addArgument(storageSlotKey)
              .log();
        } else {
          LOG.atDebug()
              .setMessage("{} storage entries archived for block {}, slot hash {}")
              .addArgument(archivedStorageCount.get())
              .addArgument(previousBlockHeader.get().getNumber())
              .addArgument(storageSlotKey)
              .log();
        }
      } catch (Exception e) {
        LOG.error("Error moving storage state for slot {} to archived storage", storageSlotKey, e);
      }
    }

    return archivedStorageCount.get();
  }

  private void moveDBEntry(
      final SegmentIdentifier fromSegment,
      final SegmentIdentifier toSegment,
      final byte[] key,
      final byte[] value) {
    boolean retried = false;
    while (true) { // Allow for a single DB retry
      try {
        SegmentedKeyValueStorageTransaction tx = composedWorldStateStorage.startTransaction();
        tx.remove(fromSegment, key);
        tx.put(toSegment, key, value);
        tx.commit();
        break;
      } catch (StorageException se) {
        if (!retried && se.getMessage().contains("RocksDBException: Busy")) {
          retried = true;
        } else {
          break;
        }
      }
    }
  }

  public Optional<Long> getLatestArchivedBlock() {
    return composedWorldStateStorage
        .get(ACCOUNT_INFO_STATE_FREEZER, ARCHIVED_BLOCKS)
        .map(Bytes::wrap)
        .map(Bytes::toLong);
  }

  public void setLatestArchivedBlock(final Long blockNumber) {
    SegmentedKeyValueStorageTransaction tx = composedWorldStateStorage.startTransaction();
    tx.put(
        ACCOUNT_INFO_STATE_FREEZER,
        ARCHIVED_BLOCKS,
        Bytes.ofUnsignedLong(blockNumber).toArrayUnsafe());
    tx.commit();
  }

  @Override
  public synchronized void close() throws Exception {
    // when the storage clears, close
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
      // alert any subscribers we are closing:
      subscribers.forEach(StorageSubscriber::onCloseStorage);

      // close all of the KeyValueStorages:
      composedWorldStateStorage.close();
      trieLogStorage.close();

      // set storage closed
      isClosed.set(true);
    }
  }

  public interface Updater extends WorldStateKeyValueStorage.Updater {

    PathBasedWorldStateKeyValueStorage.Updater saveWorldState(
        final Bytes blockHash, final Bytes32 nodeHash, final Bytes node);

    SegmentedKeyValueStorageTransaction getWorldStateTransaction();

    KeyValueStorageTransaction getTrieLogStorageTransaction();

    @Override
    void commit();

    void commitTrieLogOnly();

    void commitComposedOnly();

    void rollback();
  }
}
