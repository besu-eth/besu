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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.common.StorageSubscriber;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedSnapshotWorldStateKeyValueStorage;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SnappableKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SnappedKeyValueStorage;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Snapshot-based storage for BinTrie world state. This class provides a read-only snapshot of the
 * world state at a specific point in time.
 */
public class BinTrieSnapshotWorldStateKeyValueStorage extends BinTrieWorldStateKeyValueStorage
    implements PathBasedSnapshotWorldStateKeyValueStorage, StorageSubscriber {

  private static final Logger LOG =
      LoggerFactory.getLogger(BinTrieSnapshotWorldStateKeyValueStorage.class);

  protected final BinTrieWorldStateKeyValueStorage parentWorldStateStorage;
  private final long subscribeParentId;

  public BinTrieSnapshotWorldStateKeyValueStorage(
      final BinTrieWorldStateKeyValueStorage parentWorldStateStorage,
      final SnappedKeyValueStorage segmentedWorldStateStorage,
      final KeyValueStorage trieLogStorage) {
    super(
        parentWorldStateStorage.getFlatDbStrategyProvider(),
        segmentedWorldStateStorage,
        trieLogStorage);
    this.parentWorldStateStorage = parentWorldStateStorage;
    this.subscribeParentId = parentWorldStateStorage.subscribe(this);
  }

  public BinTrieSnapshotWorldStateKeyValueStorage(
      final BinTrieWorldStateKeyValueStorage worldStateKeyValueStorage) {
    this(
        worldStateKeyValueStorage,
        ((SnappableKeyValueStorage) worldStateKeyValueStorage.getComposedWorldStateStorage())
            .takeSnapshot(),
        worldStateKeyValueStorage.getTrieLogStorage());
  }

  private boolean isClosedGet() {
    if (isClosed.get()) {
      LOG.warn("Attempting to access closed worldstate", new Throwable());
    }
    return isClosed.get();
  }

  @Override
  public BinTrieUpdater updater() {
    return new BinTrieUpdater(
        ((SnappedKeyValueStorage) composedWorldStateStorage).getSnapshotTransaction(),
        trieLogStorage.startTransaction(),
        composedWorldStateStorage);
  }

  @Override
  public Optional<Bytes> getCode(final Hash codeHash, final Hash accountHash) {
    return isClosedGet() ? Optional.empty() : super.getCode(codeHash, accountHash);
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
  public boolean isWorldStateAvailable(final Bytes32 rootHash, final Hash blockHash) {
    return !isClosedGet() && super.isWorldStateAvailable(rootHash, blockHash);
  }

  @Override
  public void clear() {
    throw new StorageException("Snapshot storage does not implement clear");
  }

  @Override
  public void clearFlatDatabase() {
    throw new StorageException("Snapshot storage does not implement clear");
  }

  @Override
  public void clearTrieLog() {
    throw new StorageException("Snapshot storage does not implement clear");
  }

  @Override
  public void onCloseStorage() {
    try {
      doClose();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onClearStorage() {
    try {
      doClose();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onClearFlatDatabaseStorage() {
    try {
      doClose();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onClearTrieLog() {
    try {
      doClose();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected synchronized void doClose() throws Exception {
    if (!isClosedGet()) {
      subscribers.forEach(StorageSubscriber::onCloseStorage);
      composedWorldStateStorage.close();
      parentWorldStateStorage.unSubscribe(subscribeParentId);
      isClosed.set(true);
    }
  }

  @Override
  public BinTrieWorldStateKeyValueStorage getParentWorldStateStorage() {
    return parentWorldStateStorage;
  }
}
