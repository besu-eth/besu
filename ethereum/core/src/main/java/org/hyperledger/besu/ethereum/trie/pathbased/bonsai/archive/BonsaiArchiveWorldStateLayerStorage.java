/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateLayerStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SnappedKeyValueStorage;
import org.hyperledger.besu.services.kvstore.LayeredKeyValueStorage;

import java.util.Optional;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;

/**
 * Layered world-state storage used when serving Bonsai-archive historical state.
 *
 * <p>It differs from {@link BonsaiWorldStateLayerStorage} only in how flat account and storage
 * reads are dispatched: instead of routing through {@code getWithCache} (which evaluates the flat
 * lookup against the <em>raw parent</em> storage), it passes the {@link LayeredKeyValueStorage}
 * itself to the flat-DB strategy. The archive flat-DB strategy resolves the historical block
 * context from {@code WORLD_BLOCK_NUMBER_KEY}, and that key is written into this layer's in-memory
 * state by {@code persist(blockHeader)} inside {@code rollMutableArchiveStateToBlockHash}. Reading
 * it from the raw parent instead would yield the live HEAD block number, making archive reads
 * return HEAD-era values rather than the correct historical value.
 */
@SuppressWarnings("DoNotReturnNullOptionals")
public class BonsaiArchiveWorldStateLayerStorage extends BonsaiWorldStateLayerStorage {

  public BonsaiArchiveWorldStateLayerStorage(final BonsaiWorldStateKeyValueStorage parent) {
    super(parent);
  }

  protected BonsaiArchiveWorldStateLayerStorage(
      final SnappedKeyValueStorage composedWorldStateStorage,
      final KeyValueStorage trieLogStorage,
      final BonsaiWorldStateKeyValueStorage parent) {
    super(composedWorldStateStorage, trieLogStorage, parent);
  }

  @Override
  public Optional<Bytes> getAccount(final Hash accountHash) {
    if (isClosedGet()) {
      return Optional.empty();
    }
    // Pass the layer (which holds the historical WORLD_BLOCK_NUMBER_KEY set by persist()) to the
    // flat-DB strategy so getStateArchiveContextForRead returns the queried block, not HEAD.
    return getFlatDbStrategy()
        .getFlatAccount(
            this::getWorldStateRootHash,
            this::getAccountStateTrieNode,
            accountHash,
            getComposedWorldStateStorage());
  }

  @Override
  public Optional<Bytes> getStorageValueByStorageSlotKey(
      final Supplier<Optional<Hash>> storageRootSupplier,
      final Hash accountHash,
      final StorageSlotKey storageSlotKey) {
    if (isClosedGet()) {
      return Optional.empty();
    }
    // Pass the layer (which holds the historical WORLD_BLOCK_NUMBER_KEY set by persist()) to the
    // flat-DB strategy so getStateArchiveContextForRead returns the queried block, not HEAD.
    return getFlatDbStrategy()
        .getFlatStorageValueByStorageSlotKey(
            this::getWorldStateRootHash,
            storageRootSupplier,
            (location, hash) -> getAccountStorageTrieNode(accountHash, location, hash),
            accountHash,
            storageSlotKey,
            getComposedWorldStateStorage());
  }

  @Override
  public BonsaiArchiveWorldStateLayerStorage clone() {
    return new BonsaiArchiveWorldStateLayerStorage(
        ((LayeredKeyValueStorage) composedWorldStateStorage).clone(),
        trieLogStorage,
        parentWorldStateStorage);
  }
}
