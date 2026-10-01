/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListOverlay;
import org.hyperledger.besu.ethereum.mainnet.parallelization.BlockProcessingExecutors;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.NoOpMerkleTrie;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.immutabletree.ImmutableTreeCache;
import org.hyperledger.besu.ethereum.trie.immutabletree.TreeKind;
import org.hyperledger.besu.ethereum.trie.immutabletree.TreeRole;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateLayerStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.NoOpBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.bal.BonsaiBalWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache.PathBasedWorldStateCacheManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.frontier.CachingFrontierStorageRootTracker;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.frontier.FrontierRootHashTracker;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.frontier.FrontierStorageRootTracker;
import org.hyperledger.besu.ethereum.trie.patricia.ParallelStoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;
import org.hyperledger.besu.plugin.services.worldstate.StateRootCommitter;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.function.Supplier;

import jakarta.validation.constraints.NotNull;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

@SuppressWarnings("rawtypes")
public class BonsaiWorldState extends PathBasedWorldState {

  protected BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader;
  private final BonsaiCodeCache codeCache;
  private final EvmConfiguration evmConfiguration;
  private final FrontierRootHashTracker frontierRootHashTracker;
  // null when state roots are computed on classic tries
  private final BonsaiImmutableTrees immutableTrees;

  public BonsaiWorldState(
      final BonsaiWorldStateProvider archive,
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final EvmConfiguration evmConfiguration,
      final WorldStateConfig worldStateConfig,
      final BonsaiCodeCache codeCache) {
    this(
        worldStateKeyValueStorage,
        archive.getCachedMerkleTrieLoader(),
        archive.getWorldStateCacheManager(),
        archive.getTrieLogManager(),
        evmConfiguration,
        worldStateConfig,
        codeCache,
        archive.getImmutableTreeCache().orElse(null));
  }

  public BonsaiWorldState(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader,
      final PathBasedWorldStateCacheManager worldStateCacheManager,
      final TrieLogManager trieLogManager,
      final EvmConfiguration evmConfiguration,
      final WorldStateConfig worldStateConfig,
      final BonsaiCodeCache codeCache) {
    this(
        worldStateKeyValueStorage,
        bonsaiCachedMerkleTrieLoader,
        worldStateCacheManager,
        trieLogManager,
        evmConfiguration,
        worldStateConfig,
        codeCache,
        null);
  }

  /**
   * Creates a world state.
   *
   * @param worldStateKeyValueStorage the storage it reads and persists to
   * @param bonsaiCachedMerkleTrieLoader the trie node cache and preloader
   * @param worldStateCacheManager the cache of layered world states
   * @param trieLogManager the trie log manager
   * @param evmConfiguration the EVM configuration
   * @param worldStateConfig the world state configuration
   * @param codeCache the code cache
   * @param immutableTreeCache the cache to compute state roots on, or null for classic tries
   */
  public BonsaiWorldState(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader,
      final PathBasedWorldStateCacheManager worldStateCacheManager,
      final TrieLogManager trieLogManager,
      final EvmConfiguration evmConfiguration,
      final WorldStateConfig worldStateConfig,
      final BonsaiCodeCache codeCache,
      final ImmutableTreeCache immutableTreeCache) {
    super(worldStateKeyValueStorage, worldStateCacheManager, trieLogManager, worldStateConfig);
    this.immutableTrees =
        immutableTreeCache == null ? null : new BonsaiImmutableTrees(immutableTreeCache);
    this.bonsaiCachedMerkleTrieLoader = bonsaiCachedMerkleTrieLoader;
    this.worldStateKeyValueStorage = worldStateKeyValueStorage;
    this.evmConfiguration = evmConfiguration;
    final BonsaiWorldStateUpdateAccumulator acc =
        new BonsaiWorldStateUpdateAccumulator(
            this,
            (addr, value) ->
                this.bonsaiCachedMerkleTrieLoader.preLoadAccount(
                    getWorldStateStorage(), worldStateRootHash, addr),
            (addr, value) ->
                this.bonsaiCachedMerkleTrieLoader.preLoadStorageSlot(
                    getWorldStateStorage(), addr, value),
            evmConfiguration,
            codeCache);
    this.setAccumulator(acc);
    final FrontierStorageRootTracker frontierStorageRootTracker =
        worldStateConfig.isTrieDisabled()
            ? FrontierStorageRootTracker.NO_OP
            : new CachingFrontierStorageRootTracker(
                acc,
                (addressHash, baseRoot) ->
                    createFrontierTrie(
                        TreeKind.STORAGE,
                        (location, key) ->
                            bonsaiCachedMerkleTrieLoader.getAccountStorageTrieNode(
                                getWorldStateStorage(), addressHash, location, key),
                        Bytes32.wrap(baseRoot.getBytes())));
    this.frontierRootHashTracker =
        new FrontierRootHashTracker(
            acc,
            rootHash ->
                createFrontierTrie(
                    TreeKind.STATE,
                    (location, hash) ->
                        bonsaiCachedMerkleTrieLoader.getAccountStateTrieNode(
                            getWorldStateStorage(), location, hash),
                    rootHash),
            frontierStorageRootTracker);
    // Keep frontier-derived caches aligned with accumulator resets.
    acc.setCommittedTransactionListener(frontierRootHashTracker);
    this.codeCache = codeCache;
  }

  @Override
  public void persist(final BlockHeader blockHeader, final StateRootCommitter committer) {
    frontierRootHashTracker.reset();
    if (immutableTrees == null) {
      super.persist(blockHeader, committer);
      return;
    }
    final TreeRole role = immutableTreeRole();
    immutableTrees.begin(blockHeader);
    boolean persisted = false;
    try {
      super.persist(blockHeader, committer);
      persisted = true;
    } finally {
      if (persisted) {
        immutableTrees.finish(role, blockHeader, worldStateRootHash);
      } else {
        immutableTrees.discard();
      }
    }
  }

  @Override
  public Hash rootHash() {
    if (immutableTrees == null || !(isStorageFrozen && accumulator.isAccumulatorStateChanged())) {
      return super.rootHash();
    }
    immutableTrees.begin(null);
    boolean computed = false;
    try {
      final Hash root = super.rootHash();
      computed = true;
      return root;
    } finally {
      if (computed) {
        // block building candidates and simulations: registered as forks, never bound
        immutableTrees.finish(null, null, worldStateRootHash);
      } else {
        immutableTrees.discard();
      }
    }
  }

  /**
   * The role the roots this world state persists are bound to: the head world state persists the
   * head, a frozen one validates a payload, and a rolled layer is a short-lived fork.
   */
  private TreeRole immutableTreeRole() {
    if (isStorageFrozen()) {
      return TreeRole.NEW_PAYLOAD;
    }
    return isModifyingHeadWorldState() ? TreeRole.HEAD : null;
  }

  /**
   * Starts collecting the roots this world state's tries compute, for a computation whose result
   * another world state persists. No-op without an immutable tree cache.
   */
  public void beginImmutableTreeResults() {
    if (immutableTrees != null) {
      immutableTrees.begin(null);
    }
  }

  /**
   * Returns the roots collected since {@link #beginImmutableTreeResults()}.
   *
   * @return the roots, or null without an immutable tree cache
   */
  public BonsaiImmutableTrees.Results drainImmutableTreeResults() {
    return immutableTrees == null ? null : immutableTrees.drain();
  }

  /**
   * Adds roots another world state computed for the state this one is persisting, so they are
   * registered with its own if the persist succeeds.
   *
   * @param results the roots, or null
   */
  public void adoptImmutableTreeResults(final BonsaiImmutableTrees.Results results) {
    if (immutableTrees != null) {
      immutableTrees.adopt(results);
    }
  }

  @Override
  public Hash frontierRootHash() {
    return frontierRootHashTracker.frontierRootHash(worldStateRootHash);
  }

  @Override
  public void applyBlockAccessListOverlay(final BlockAccessListOverlay blockAccessListOverlay) {
    setAccumulator(
        new BonsaiBalWorldStateUpdateAccumulator(
            this, evmConfiguration, codeCache, blockAccessListOverlay));
  }

  @Override
  public Optional<Bytes> getCode(@NotNull final Address address, final Hash codeHash) {
    return getWorldStateStorage().getCode(codeHash, address.addressHash());
  }

  @Override
  public BonsaiWorldStateKeyValueStorage getWorldStateStorage() {
    return worldStateKeyValueStorage;
  }

  @Override
  public Account get(final Address address) {
    return getWorldStateStorage()
        .getAccount(address.addressHash())
        .map(bytes -> BonsaiAccount.fromRLP(accumulator, address, bytes, true, codeCache))
        .orElse(null);
  }

  protected Optional<Bytes> getAccountStateTrieNode(final Bytes location, final Bytes32 nodeHash) {
    return getWorldStateStorage().getAccountStateTrieNode(location, nodeHash);
  }

  public Optional<Bytes> getStorageTrieNode(
      final Hash accountHash, final Bytes location, final Bytes32 nodeHash) {
    return getWorldStateStorage().getAccountStorageTrieNode(accountHash, location, nodeHash);
  }

  @Override
  public UInt256 getStorageValue(final Address address, final UInt256 storageKey) {
    return getStorageValueByStorageSlotKey(address, new StorageSlotKey(storageKey))
        .orElse(UInt256.ZERO);
  }

  @Override
  public Optional<UInt256> getStorageValueByStorageSlotKey(
      final Address address, final StorageSlotKey storageSlotKey) {
    return getWorldStateStorage()
        .getStorageValueByStorageSlotKey(address.addressHash(), storageSlotKey)
        .map(UInt256::fromBytes);
  }

  public Optional<UInt256> getStorageValueByStorageSlotKey(
      final Supplier<Optional<Hash>> storageRootSupplier,
      final Address address,
      final StorageSlotKey storageSlotKey) {
    return getWorldStateStorage()
        .getStorageValueByStorageSlotKey(storageRootSupplier, address.addressHash(), storageSlotKey)
        .map(UInt256::fromBytes);
  }

  @Override
  public UInt256 getPriorStorageValue(final Address address, final UInt256 storageKey) {
    return getStorageValue(address, storageKey);
  }

  @Override
  public Map<Bytes32, Bytes> getAllAccountStorage(final Address address, final Hash rootHash) {
    final MerkleTrie<Bytes, Bytes> storageTrie =
        createTrie(
            (location, key) -> getStorageTrieNode(address.addressHash(), location, key),
            Bytes32.wrap(rootHash.getBytes()));
    return storageTrie.entriesFrom(Bytes32.ZERO, Integer.MAX_VALUE);
  }

  @Override
  public MutableWorldState freezeStorage() {
    this.isStorageFrozen = true;
    this.worldStateKeyValueStorage = new BonsaiWorldStateLayerStorage(getWorldStateStorage());
    return this;
  }

  public void disableCacheMerkleTrieLoader() {
    this.bonsaiCachedMerkleTrieLoader = new NoOpBonsaiCachedMerkleTrieLoader();
  }

  /**
   * Frontier receipt computation is inherently sequential (each receipt depends on the prior
   * transaction's state root), so its tries skip the parallel implementation and its ForkJoinPool
   * scheduling overhead.
   */
  private MerkleTrie<Bytes, Bytes> createFrontierTrie(
      final TreeKind kind, final NodeLoader nodeLoader, final Bytes32 rootHash) {
    if (worldStateConfig.isTrieDisabled()) {
      return new NoOpMerkleTrie<>();
    }
    if (immutableTrees != null) {
      return immutableTrees.frontierTrie(kind, rootHash, nodeLoader);
    }
    return new StoredMerklePatriciaTrie<>(
        nodeLoader, rootHash, Function.identity(), Function.identity());
  }

  /**
   * Account state trie rooted at the current world state root. With an immutable tree cache its
   * root is computed on the cached tree of the current root.
   */
  public MerkleTrie<Bytes, Bytes> createAccountStateTrie() {
    final NodeLoader loader =
        (location, hash) ->
            bonsaiCachedMerkleTrieLoader.getAccountStateTrieNode(
                getWorldStateStorage(), location, hash);
    final Bytes32 rootHash = Bytes32.wrap(worldStateRootHash.getBytes());
    if (immutableTrees != null && !worldStateConfig.isTrieDisabled()) {
      return immutableTrees.accountTrie(
          rootHash, loader, parallelPool(BlockProcessingExecutors.accountTrieForkJoinPool()));
    }
    return createTrie(loader, rootHash, BlockProcessingExecutors.accountTrieForkJoinPool());
  }

  /**
   * Storage trie for the given account rooted at the provided storage root. With an immutable tree
   * cache its root is computed on the cached tree of that storage root.
   */
  public MerkleTrie<Bytes, Bytes> createStorageTrie(
      final Hash accountHash, final Hash storageRoot) {
    final NodeLoader loader =
        (location, key) ->
            bonsaiCachedMerkleTrieLoader.getAccountStorageTrieNode(
                getWorldStateStorage(), accountHash, location, key);
    final Bytes32 rootHash = Bytes32.wrap(storageRoot.getBytes());
    if (immutableTrees != null && !worldStateConfig.isTrieDisabled()) {
      return immutableTrees.storageTrie(
          accountHash,
          rootHash,
          loader,
          parallelPool(BlockProcessingExecutors.storageTrieForkJoinPool()));
    }
    return createTrie(loader, rootHash, BlockProcessingExecutors.storageTrieForkJoinPool());
  }

  private ForkJoinPool parallelPool(final ForkJoinPool pool) {
    return worldStateConfig.isParallelStateRootComputationEnabled() ? pool : null;
  }

  private MerkleTrie<Bytes, Bytes> createTrie(final NodeLoader nodeLoader, final Bytes32 rootHash) {
    return createTrie(nodeLoader, rootHash, BlockProcessingExecutors.accountTrieForkJoinPool());
  }

  private MerkleTrie<Bytes, Bytes> createTrie(
      final NodeLoader nodeLoader, final Bytes32 rootHash, final ForkJoinPool forkJoinPool) {
    if (worldStateConfig.isTrieDisabled()) {
      return new NoOpMerkleTrie<>();
    }
    if (worldStateConfig.isParallelStateRootComputationEnabled()) {
      return new ParallelStoredMerklePatriciaTrie<>(
          nodeLoader, rootHash, Function.identity(), Function.identity(), forkJoinPool);
    }
    return new StoredMerklePatriciaTrie<>(
        nodeLoader, rootHash, Function.identity(), Function.identity());
  }

  public Hash hashAndSavePreImage(final Bytes value) {
    // by default do not save has preImages
    return Hash.hash(value);
  }

  @Override
  protected Hash getEmptyTrieHash() {
    return Hash.EMPTY_TRIE_HASH;
  }

  @Override
  public BonsaiCodeCache codeCache() {
    return codeCache;
  }
}
