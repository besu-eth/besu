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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.SnappedKeyValueStorage;
import org.hyperledger.besu.services.kvstore.LayeredKeyValueStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;

@SuppressWarnings("DoNotReturnNullOptionals")
public class BonsaiWorldStateLayerStorage extends BonsaiSnapshotWorldStateKeyValueStorage
    implements StorageSubscriber {

  public BonsaiWorldStateLayerStorage(final BonsaiWorldStateKeyValueStorage parent) {
    this(
        new LayeredKeyValueStorage(parent.getComposedWorldStateStorage()),
        parent.getTrieLogStorage(),
        parent);
  }

  protected BonsaiWorldStateLayerStorage(
      final SnappedKeyValueStorage composedWorldStateStorage,
      final KeyValueStorage trieLogStorage,
      final BonsaiWorldStateKeyValueStorage parent) {
    super(parent, composedWorldStateStorage, trieLogStorage);
  }

  /**
   * Get value from layer with cache support.
   *
   * @param segment the segment identifier
   * @param key the key
   * @param cacheFunction function to retrieve from cache given persistent storage
   * @return optional value as Bytes
   */
  private Optional<Bytes> getWithCache(
      final SegmentIdentifier segment,
      final Bytes key,
      final Function<SegmentedKeyValueStorage, Optional<Bytes>> cacheFunction) {
    return getComposedWorldStateStorage().get(segment, key, cacheFunction);
  }

  @Override
  public Optional<Bytes> getAccount(final Hash accountHash) {
    if (isClosedGet()) {
      return Optional.empty();
    }

    return getWithCache(
        ACCOUNT_INFO_STATE,
        accountHash.getBytes(),
        persistentStorage ->
            cacheManager.getFromCacheOrStorage(
                ACCOUNT_INFO_STATE,
                accountHash.getBytes(),
                getCurrentVersion(),
                () ->
                    getFlatDbStrategy()
                        .getFlatAccount(
                            this::getWorldStateRootHash,
                            this::getAccountStateTrieNode,
                            accountHash,
                            persistentStorage)));
  }

  @Override
  public Optional<Bytes> getStorageValueByStorageSlotKey(
      final Supplier<Optional<Hash>> storageRootSupplier,
      final Hash accountHash,
      final StorageSlotKey storageSlotKey) {

    if (isClosedGet()) {
      return Optional.empty();
    }

    final Bytes key =
        Bytes.concatenate(accountHash.getBytes(), storageSlotKey.getSlotHash().getBytes());

    return getWithCache(
        ACCOUNT_STORAGE_STORAGE,
        key,
        persistentStorage ->
            cacheManager.getFromCacheOrStorage(
                ACCOUNT_STORAGE_STORAGE,
                key,
                getCurrentVersion(),
                () ->
                    getFlatDbStrategy()
                        .getFlatStorageValueByStorageSlotKey(
                            this::getWorldStateRootHash,
                            storageRootSupplier,
                            (location, hash) ->
                                getAccountStorageTrieNode(accountHash, location, hash),
                            accountHash,
                            storageSlotKey,
                            persistentStorage)));
  }

  /**
   * Overlay-safe multi-get: layer-local values (including tombstones) are returned without writing
   * them into the shared head cache. Misses are delegated to the parent storage so only
   * persistent/parent results can warm {@link
   * org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache.FlatDbCacheManager}.
   */
  @Override
  public List<Optional<Bytes>> getMultipleFlat(
      final SegmentIdentifier segmentIdentifier, final List<byte[]> keys) {
    if (isClosedGet()) {
      // Empty list = no-op / closed; alignment handled by callers / cache manager
      return List.of();
    }
    if (keys.isEmpty()) {
      return List.of();
    }

    final List<Optional<Bytes>> results = new ArrayList<>(keys.size());
    final List<byte[]> missKeys = new ArrayList<>();
    final List<Integer> missIndices = new ArrayList<>();

    for (int i = 0; i < keys.size(); i++) {
      final byte[] rawKey = keys.get(i);
      final Optional<Optional<byte[]>> layerValue =
          getComposedWorldStateStorage().peekThisLayer(segmentIdentifier, rawKey);
      if (layerValue.isPresent()) {
        results.add(layerValue.get().map(Bytes::wrap));
      } else {
        results.add(null);
        missKeys.add(rawKey);
        missIndices.add(i);
      }
    }

    if (!missKeys.isEmpty()) {
      final List<Optional<Bytes>> parentValues =
          parentWorldStateStorage.getMultipleFlat(segmentIdentifier, missKeys);
      if (parentValues.size() == missKeys.size()) {
        for (int j = 0; j < missIndices.size(); j++) {
          results.set(missIndices.get(j), parentValues.get(j));
        }
      }
    }

    return results;
  }

  @Override
  public FlatDbMode getFlatDbMode() {
    return parentWorldStateStorage.getFlatDbMode();
  }

  @Override
  public BonsaiWorldStateLayerStorage clone() {
    return new BonsaiWorldStateLayerStorage(
        ((LayeredKeyValueStorage) composedWorldStateStorage).clone(),
        trieLogStorage,
        parentWorldStateStorage);
  }

  /** Merge this layer to a storage transaction. */
  public void mergeTo(final SegmentedKeyValueStorageTransaction transaction) {
    getComposedWorldStateStorage().mergeTo(transaction);
  }

  @Override
  public LayeredKeyValueStorage getComposedWorldStateStorage() {
    return (LayeredKeyValueStorage) super.getComposedWorldStateStorage();
  }
}
