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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload;

import static org.hyperledger.besu.metrics.BesuMetricCategory.BLOCKCHAIN;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.StorageSubscriber;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.TrieNodeKey;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.metrics.ObservableMetricsSystem;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.jspecify.annotations.Nullable;

public class BonsaiCachedMerkleTrieLoader implements StorageSubscriber {

  private static final ExecutorService VIRTUAL_POOL = Executors.newVirtualThreadPerTaskExecutor();

  private static final int ACCOUNT_CACHE_SIZE = 100_000;
  private static final int STORAGE_CACHE_SIZE = 200_000;

  // Guava locks a segment per write, and the preload walks write from many threads at once
  private static final int CACHE_CONCURRENCY =
      Math.max(4, Runtime.getRuntime().availableProcessors());

  private final Cache<Bytes, Bytes> accountNodes =
      CacheBuilder.newBuilder()
          .concurrencyLevel(CACHE_CONCURRENCY)
          .recordStats()
          .maximumSize(ACCOUNT_CACHE_SIZE)
          .build();
  private final Cache<Bytes, Bytes> storageNodes =
      CacheBuilder.newBuilder()
          .concurrencyLevel(CACHE_CONCURRENCY)
          .recordStats()
          .maximumSize(STORAGE_CACHE_SIZE)
          .build();

  public BonsaiCachedMerkleTrieLoader(final ObservableMetricsSystem metricsSystem) {
    metricsSystem.createGuavaCacheCollector(BLOCKCHAIN, "accountsNodes", accountNodes);
    metricsSystem.createGuavaCacheCollector(BLOCKCHAIN, "storageNodes", storageNodes);
  }

  public void preLoadAccount(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Hash worldStateRootHash,
      final Address account) {
    CompletableFuture.runAsync(
        () -> cacheAccountNodes(worldStateKeyValueStorage, worldStateRootHash, account),
        VIRTUAL_POOL);
  }

  @VisibleForTesting
  public void cacheAccountNodes(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Hash worldStateRootHash,
      final Address account) {
    final long storageSubscriberId = worldStateKeyValueStorage.subscribe(this);
    try {
      final StoredMerklePatriciaTrie<Bytes, Bytes> accountTrie =
          new StoredMerklePatriciaTrie<>(
              (location, hash) ->
                  loadAndCache(
                      accountNodes,
                      hash,
                      () ->
                          worldStateKeyValueStorage.getTrieNodeFromCacheOrStorage(location, hash)),
              Bytes32.wrap(worldStateRootHash.getBytes()),
              Function.identity(),
              Function.identity());
      accountTrie.get(account.addressHash().getBytes());
    } catch (MerkleTrieException e) {
      // ignore exception for the cache
    } finally {
      worldStateKeyValueStorage.unSubscribe(storageSubscriberId);
    }
  }

  /**
   * Preloads the path of a storage slot. A known storage root saves reading the root node from
   * storage to find it, and an empty storage trie has no path to preload.
   *
   * @param worldStateKeyValueStorage the storage to read the trie nodes from
   * @param account the account of the slot
   * @param storageRoot the account's storage root, or null to read the root node from storage
   * @param slotKey the slot
   */
  public void preLoadStorageSlot(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Address account,
      final @Nullable Hash storageRoot,
      final StorageSlotKey slotKey) {
    if (Hash.EMPTY_TRIE_HASH.equals(storageRoot)) {
      return;
    }
    CompletableFuture.runAsync(
        () -> cacheStorageNodes(worldStateKeyValueStorage, account, storageRoot, slotKey),
        VIRTUAL_POOL);
  }

  @VisibleForTesting
  public void cacheStorageNodes(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Address account,
      final @Nullable Hash storageRoot,
      final StorageSlotKey slotKey) {
    final Hash accountHash = account.addressHash();
    final long storageSubscriberId = worldStateKeyValueStorage.subscribe(this);
    try {
      if (storageRoot != null) {
        walkStorageTrie(
            worldStateKeyValueStorage, accountHash, Bytes32.wrap(storageRoot.getBytes()), slotKey);
      } else {
        worldStateKeyValueStorage
            .getTrieNodeUnsafe(TrieNodeKey.of(accountHash, Bytes.EMPTY))
            .ifPresent(
                rootNode ->
                    walkStorageTrie(
                        worldStateKeyValueStorage,
                        accountHash,
                        Bytes32.wrap(Hash.hash(rootNode).getBytes()),
                        slotKey));
      }
    } finally {
      worldStateKeyValueStorage.unSubscribe(storageSubscriberId);
    }
  }

  private void walkStorageTrie(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Hash accountHash,
      final Bytes32 storageRoot,
      final StorageSlotKey slotKey) {
    try {
      final StoredMerklePatriciaTrie<Bytes, Bytes> storageTrie =
          new StoredMerklePatriciaTrie<Bytes, Bytes>(
              (location, hash) ->
                  loadAndCache(
                      storageNodes,
                      hash,
                      () ->
                          worldStateKeyValueStorage.getTrieNodeFromCacheOrStorage(
                              TrieNodeKey.of(accountHash, location), hash)),
              storageRoot,
              Function.identity(),
              Function.identity());
      storageTrie.get(slotKey.getSlotHash().getBytes());
    } catch (MerkleTrieException e) {
      // ignore exception for the cache
    }
  }

  /** Only a node that was not cached yet is put, so a hit costs no write. */
  private static Optional<Bytes> loadAndCache(
      final Cache<Bytes, Bytes> cache,
      final Bytes32 nodeHash,
      final Supplier<Optional<Bytes>> loader) {
    if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      return Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
    }
    final Bytes cached = cache.getIfPresent(nodeHash);
    if (cached != null) {
      return Optional.of(cached);
    }
    final Optional<Bytes> node = loader.get();
    // nodeHash is a slice of the parent node's RLP; copy it so the key doesn't pin that
    node.ifPresent(bytes -> cache.put(nodeHash.copy(), bytes));
    return node;
  }

  public Optional<Bytes> getAccountStateTrieNode(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Bytes location,
      final Bytes32 nodeHash) {
    if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      return Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
    } else {
      return Optional.ofNullable(accountNodes.getIfPresent(nodeHash))
          .or(() -> worldStateKeyValueStorage.getTrieNodeFromCacheOrStorage(location, nodeHash));
    }
  }

  public Optional<Bytes> getAccountStorageTrieNode(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash) {
    if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      return Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
    } else {
      return Optional.ofNullable(storageNodes.getIfPresent(nodeHash))
          .or(
              () ->
                  worldStateKeyValueStorage.getTrieNodeFromCacheOrStorage(
                      TrieNodeKey.of(accountHash, location), nodeHash));
    }
  }
}
