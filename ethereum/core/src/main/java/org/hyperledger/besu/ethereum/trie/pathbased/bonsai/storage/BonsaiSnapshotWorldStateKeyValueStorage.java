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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.trie.pathbased.common.StorageSubscriber;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedSnapshotWorldStateKeyValueStorage;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SnappableKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SnappedKeyValueStorage;

import java.util.Optional;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BonsaiSnapshotWorldStateKeyValueStorage extends BonsaiWorldStateKeyValueStorage
    implements PathBasedSnapshotWorldStateKeyValueStorage, StorageSubscriber {

  protected final BonsaiWorldStateKeyValueStorage parentWorldStateStorage;
  private static final Logger LOG =
      LoggerFactory.getLogger(BonsaiSnapshotWorldStateKeyValueStorage.class);
  private final long subscribeParentId;

  public BonsaiSnapshotWorldStateKeyValueStorage(
      final BonsaiWorldStateKeyValueStorage parentWorldStateStorage,
      final SnappedKeyValueStorage segmentedWorldStateStorage,
      final KeyValueStorage trieLogStorage) {
    super(
        parentWorldStateStorage.flatDbStrategyProvider,
        segmentedWorldStateStorage,
        trieLogStorage,
        parentWorldStateStorage.getPreimageStorage());
    this.parentWorldStateStorage = parentWorldStateStorage;
    this.subscribeParentId = parentWorldStateStorage.subscribe(this);
  }

  public BonsaiSnapshotWorldStateKeyValueStorage(
      final BonsaiWorldStateKeyValueStorage worldStateStorageKeyValueStorage) {
    this(
        worldStateStorageKeyValueStorage,
        ((SnappableKeyValueStorage) worldStateStorageKeyValueStorage.getComposedWorldStateStorage())
            .takeSnapshot(),
        worldStateStorageKeyValueStorage.getTrieLogStorage());
  }

  private boolean isClosedGet() {
    if (isClosed.get()) {
      Throwable t = new Throwable("Attempting to access closed worldstate");
      LOG.warn(t.getMessage(), t);
    }
    return isClosed.get();
  }

  @Override
  public Updater updater() {
    return new Updater(
        ((SnappedKeyValueStorage) composedWorldStateStorage).getSnapshotTransaction(),
        trieLogStorage.startTransaction(),
        getFlatDbStrategy(),
        composedWorldStateStorage);
  }

  @Override
  public Optional<Bytes> getAccount(final Hash accountHash) {
    return isClosedGet() ? Optional.empty() : super.getAccount(accountHash);
  }

  @Override
  public Optional<Bytes> getCode(final Hash codeHash, final Hash accountHash) {
    return isClosedGet() ? Optional.empty() : super.getCode(codeHash, accountHash);
  }

  @Override
  public Optional<Bytes> getAccountStateTrieNode(final Bytes location, final Bytes32 nodeHash) {
    return isClosedGet() ? Optional.empty() : super.getAccountStateTrieNode(location, nodeHash);
  }

  @Override
  public Optional<Bytes> getTrieNodeUnsafe(final Bytes key) {
    return isClosedGet() ? Optional.empty() : super.getTrieNodeUnsafe(key);
  }

  @Override
  public Optional<Bytes> getAccountStorageTrieNode(
      final Hash accountHash, final Bytes location, final Bytes32 nodeHash) {
    return isClosedGet()
        ? Optional.empty()
        : super.getAccountStorageTrieNode(accountHash, location, nodeHash);
  }

  @Override
  public Optional<byte[]> getTrieLog(final Hash blockHash) {
    return isClosedGet() ? Optional.empty() : super.getTrieLog(blockHash);
  }

  @Override
  public Optional<Bytes> getStateTrieNode(final Bytes location) {
    return isClosedGet() ? Optional.empty() : super.getStateTrieNode(location);
  }

  @Override
  public Optional<Bytes> getWorldStateRootHash() {
    return isClosedGet() ? Optional.empty() : super.getWorldStateRootHash();
  }

  @Override
  public Optional<Hash> getWorldStateBlockHash() {
    return isClosedGet() ? Optional.empty() : super.getWorldStateBlockHash();
  }

  @Override
  public Optional<Long> getWorldStateBlockNumber() {
    return isClosedGet() ? Optional.empty() : super.getWorldStateBlockNumber();
  }

  @Override
  public Optional<Bytes> getStorageValueByStorageSlotKey(
      final Hash accountHash, final StorageSlotKey storageSlotKey) {
    return isClosedGet()
        ? Optional.empty()
        : super.getStorageValueByStorageSlotKey(accountHash, storageSlotKey);
  }

  @Override
  public Optional<Bytes> getStorageValueByStorageSlotKey(
      final Supplier<Optional<Hash>> storageRootSupplier,
      final Hash accountHash,
      final StorageSlotKey storageSlotKey) {
    return isClosedGet()
        ? Optional.empty()
        : super.getStorageValueByStorageSlotKey(storageRootSupplier, accountHash, storageSlotKey);
  }

  @Override
  public boolean isWorldStateAvailable(final Bytes32 rootHash, final Hash blockHash) {
    return !isClosedGet() && super.isWorldStateAvailable(rootHash, blockHash);
  }

  @Override
  public void clear() {
    // snapshot storage does not implement clear
    throw new StorageException("Snapshot storage does not implement clear");
  }

  @Override
  public void clearFlatDatabase() {
    // snapshot storage does not implement clear
    throw new StorageException("Snapshot storage does not implement clear");
  }

  @Override
  public void clearTrieLog() {
    // snapshot storage does not implement clear
    throw new StorageException("Snapshot storage does not implement clear");
  }

  @Override
  public void onCloseStorage() {
    try {
      // when the parent storage clears, close regardless of subscribers
      doClose();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onClearStorage() {
    try {
      // when the parent storage clears, close regardless of subscribers
      doClose();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onClearFlatDatabaseStorage() {
    // when the parent storage clears, close regardless of subscribers
    try {
      doClose();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onClearTrieLog() {
    // when the parent storage clears, close regardless of subscribers
    try {
      doClose();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onClearTrie() {
    try {
      doClose();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected synchronized void doClose() throws Exception {
    if (!isClosedGet()) {
      // alert any subscribers we are closing:
      subscribers.forEach(StorageSubscriber::onCloseStorage);

      // close all of the SnappedKeyValueStorages:
      composedWorldStateStorage.close();

      // unsubscribe the parent worldstate
      parentWorldStateStorage.unSubscribe(subscribeParentId);

      // set storage closed
      isClosed.set(true);
    }
  }

  @Override
  public BonsaiWorldStateKeyValueStorage getParentWorldStateStorage() {
    return parentWorldStateStorage;
  }
}
