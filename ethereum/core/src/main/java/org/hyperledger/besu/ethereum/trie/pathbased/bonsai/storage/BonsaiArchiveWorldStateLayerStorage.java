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
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.TrieNodeStrategy;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SnappedKeyValueStorage;
import org.hyperledger.besu.services.kvstore.LayeredKeyValueStorage;

import java.util.Optional;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Layered world-state storage used when serving Bonsai-archive state proofs.
 *
 * <p>It differs from {@link BonsaiWorldStateLayerStorage} only in how flat account and storage
 * reads are dispatched: instead of routing through {@code getWithCache} (which evaluates the flat
 * lookup against the <em>raw parent</em> storage), it passes the {@link LayeredKeyValueStorage}
 * itself to the flat-DB strategy. The archive flat-DB strategy resolves the historical block
 * context from {@code WORLD_BLOCK_NUMBER_KEY}, and that key is written into this layer's in-memory
 * state by {@code resetWorldStateToCheckpoint}. Reading it from the raw parent instead would yield
 * the live HEAD block number, making archive reads return HEAD-era values during rollback (causing
 * "nonces differ" / "Old value of slot does not match expected value").
 */
@SuppressWarnings("DoNotReturnNullOptionals")
public class BonsaiArchiveWorldStateLayerStorage extends BonsaiWorldStateLayerStorage {

  // The snapshot/layer constructor chain resets the trie-node strategy to the default
  // BonsaiTrieNodeStrategy (which reads HEAD's TRIE_BRANCH_STORAGE). For archive proofs we must
  // read historical trie nodes from TRIE_BRANCH_STORAGE_ARCHIVE using the layer's checkpoint
  // block context, so we override the trie-node getters to use the archive strategy. Interval is
  // null because reads never need it (only writes derive the window suffix from the interval).
  private final TrieNodeStrategy archiveTrieNodeStrategy = new BonsaiArchiveTrieNodeStrategy(null);

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
    // Pass the layer (which holds the checkpoint WORLD_BLOCK_NUMBER_KEY) to the flat-DB strategy.
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
  public Optional<Bytes> getAccountStateTrieNode(final Bytes location, final Bytes32 nodeHash) {
    if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      return Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
    }
    // First the in-memory layer's plain TRIE_BRANCH_STORAGE (rolled-back nodes that the proof
    // persist just wrote at the target block), then the archive CF (historical checkpoint /
    // unchanged nodes). The Hash.hash filter in each lookup discards stale HEAD nodes that the
    // layer's parent storage may still hold at the same location.
    return super.getAccountStateTrieNode(location, nodeHash)
        .or(
            () ->
                archiveTrieNodeStrategy
                    .getFlatAccountTrieNode(location, nodeHash, getComposedWorldStateStorage())
                    .filter(b -> Hash.hash(b).getBytes().equals(nodeHash)));
  }

  @Override
  public Optional<Bytes> getAccountStorageTrieNode(
      final Hash accountHash, final Bytes location, final Bytes32 nodeHash) {
    if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      return Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
    }
    return super.getAccountStorageTrieNode(accountHash, location, nodeHash)
        .or(
            () ->
                archiveTrieNodeStrategy
                    .getFlatStorageTrieNode(
                        accountHash, location, nodeHash, getComposedWorldStateStorage())
                    .filter(b -> Hash.hash(b).getBytes().equals(nodeHash)));
  }

  @Override
  public BonsaiArchiveWorldStateLayerStorage clone() {
    return new BonsaiArchiveWorldStateLayerStorage(
        ((LayeredKeyValueStorage) composedWorldStateStorage).clone(),
        trieLogStorage,
        parentWorldStateStorage);
  }
}
