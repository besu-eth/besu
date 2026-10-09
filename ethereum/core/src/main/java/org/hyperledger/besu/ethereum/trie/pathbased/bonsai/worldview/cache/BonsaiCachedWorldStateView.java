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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.StorageSubscriber;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.PathBasedWorldState.StoredRootAndBlockHash;
import org.hyperledger.besu.plugin.data.BlockHeader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BonsaiCachedWorldStateView implements StorageSubscriber {

  /** The storage of the block and its root and block hash, read once and replaced together. */
  public record CachedStorage(
      BonsaiWorldStateKeyValueStorage worldStateStorage, StoredRootAndBlockHash rootAndBlockHash) {

    static CachedStorage of(final BonsaiWorldStateKeyValueStorage worldStateStorage) {
      return new CachedStorage(
          worldStateStorage, StoredRootAndBlockHash.readFrom(worldStateStorage));
    }
  }

  private volatile CachedStorage cachedStorage;
  private final BlockHeader blockHeader;
  private long worldViewSubscriberId;
  private static final Logger LOG = LoggerFactory.getLogger(BonsaiCachedWorldStateView.class);

  public BonsaiCachedWorldStateView(
      final BlockHeader blockHeader, final BonsaiWorldStateKeyValueStorage worldView) {
    this.blockHeader = blockHeader;
    this.cachedStorage = CachedStorage.of(worldView);
    this.worldViewSubscriberId = worldView.subscribe(this);
  }

  public BonsaiWorldStateKeyValueStorage getWorldStateStorage() {
    return cachedStorage.worldStateStorage();
  }

  /** The storage of the block with its root and block hash, from the same replacement. */
  public CachedStorage getCachedStorage() {
    return cachedStorage;
  }

  public long getBlockNumber() {
    return blockHeader.getNumber();
  }

  public Hash getBlockHash() {
    return blockHeader.getBlockHash();
  }

  public synchronized void close() {
    final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage =
        cachedStorage.worldStateStorage();
    worldStateKeyValueStorage.unSubscribe(this.worldViewSubscriberId);
    try {
      worldStateKeyValueStorage.close();
    } catch (final Exception e) {
      LOG.warn("Failed to close worldstate storage for block " + blockHeader.toLogString(), e);
    }
  }

  public synchronized void updateWorldStateStorage(
      final BonsaiWorldStateKeyValueStorage newWorldStateStorage) {
    long newSubscriberId = newWorldStateStorage.subscribe(this);
    final BonsaiWorldStateKeyValueStorage oldWorldStateStorage = cachedStorage.worldStateStorage();
    oldWorldStateStorage.unSubscribe(this.worldViewSubscriberId);
    this.cachedStorage = CachedStorage.of(newWorldStateStorage);
    this.worldViewSubscriberId = newSubscriberId;
    try {
      oldWorldStateStorage.close();
    } catch (final Exception e) {
      LOG.warn(
          "During update, failed to close prior worldstate storage for block "
              + blockHeader.toLogString(),
          e);
    }
  }
}
