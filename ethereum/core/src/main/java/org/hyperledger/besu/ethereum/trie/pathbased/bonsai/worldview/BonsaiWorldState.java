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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_BLOCK_HASH_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_ROOT_HASH_KEY;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListOverlay;
import org.hyperledger.besu.ethereum.mainnet.parallelization.BlockProcessingExecutors;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.DefaultStateRootCommitter;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.TrieDisabledStateRootCommitter;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.NoOpMerkleTrie;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.common.StateRootMismatchException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.StorageRootStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiSnapshotWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateLayerStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.StorageSubscriber;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.NoOpBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.bal.BonsaiBalWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache.BonsaiWorldStateCacheManager;
import org.hyperledger.besu.ethereum.trie.patricia.ParallelStoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;
import org.hyperledger.besu.plugin.services.worldstate.StateRootCommitter;
import org.hyperledger.besu.plugin.services.worldstate.StateRootComputation;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.stream.Stream;

import jakarta.validation.constraints.NotNull;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BonsaiWorldState implements MutableWorldState, BonsaiWorldView, StorageSubscriber {

  private static final Logger LOG = LoggerFactory.getLogger(BonsaiWorldState.class);

  protected static final DefaultStateRootCommitter DEFAULT_STATE_ROOT_COMMITTER =
      new DefaultStateRootCommitter();

  /**
   * Resolves the committer used by the no-arg {@link #persist(BlockHeader)} and the frozen {@link
   * #rootHash()} / {@link #frontierRootHash()} recomputes. In flat (trie-disabled) mode the
   * dedicated {@link TrieDisabledStateRootCommitter} collects flat writes without touching any
   * trie; otherwise the default trie-based committer is used.
   */
  protected StateRootCommitter resolveDefaultCommitter() {
    return isTrieDisabled()
        ? TrieDisabledStateRootCommitter.INSTANCE
        : DEFAULT_STATE_ROOT_COMMITTER;
  }

  protected BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage;
  protected final BonsaiWorldStateCacheManager worldStateCacheManager;
  protected final TrieLogManager trieLogManager;
  protected BonsaiWorldStateUpdateAccumulator accumulator;

  protected Hash worldStateRootHash;
  protected Hash worldStateBlockHash;

  // configuration parameters for the world state.
  protected WorldStateConfig worldStateConfig;

  protected boolean isStorageFrozen;

  protected BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader;
  private final BonsaiCodeCache codeCache;
  private final EvmConfiguration evmConfiguration;

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
        codeCache);
  }

  public BonsaiWorldState(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader,
      final BonsaiWorldStateCacheManager worldStateCacheManager,
      final TrieLogManager trieLogManager,
      final EvmConfiguration evmConfiguration,
      final WorldStateConfig worldStateConfig,
      final BonsaiCodeCache codeCache) {
    this.worldStateKeyValueStorage = worldStateKeyValueStorage;
    this.worldStateRootHash =
        Hash.wrap(
            Bytes32.wrap(
                worldStateKeyValueStorage
                    .getWorldStateRootHash()
                    .orElse(getEmptyTrieHash().getBytes())));
    this.worldStateBlockHash = worldStateKeyValueStorage.getWorldStateBlockHash().orElse(Hash.ZERO);
    this.worldStateCacheManager = worldStateCacheManager;
    this.trieLogManager = trieLogManager;
    this.worldStateConfig = worldStateConfig;
    this.isStorageFrozen = false;
    this.bonsaiCachedMerkleTrieLoader = bonsaiCachedMerkleTrieLoader;
    this.evmConfiguration = evmConfiguration;
    this.setAccumulator(
        new BonsaiWorldStateUpdateAccumulator(
            this,
            (addr, value) ->
                this.bonsaiCachedMerkleTrieLoader.preLoadAccount(
                    getWorldStateStorage(), worldStateRootHash, addr),
            (addr, value) ->
                this.bonsaiCachedMerkleTrieLoader.preLoadStorageSlot(
                    getWorldStateStorage(), addr, value),
            evmConfiguration,
            codeCache));
    this.codeCache = codeCache;
  }

  /**
   * Sets the updater strategy for this world state. Called once during construction to solve the
   * chicken-and-egg problem of needing a world-state reference ({@code this}) when constructing the
   * updater.
   *
   * @param accumulator the updater to use (either an accumulator or a BAL-backed updater)
   */
  public void setAccumulator(final BonsaiWorldStateUpdateAccumulator accumulator) {
    this.accumulator = accumulator;
  }

  /**
   * Returns the world state block hash of this world state
   *
   * @return the world state block hash.
   */
  public Hash getWorldStateBlockHash() {
    return worldStateBlockHash;
  }

  /**
   * Returns the world state root hash of this world state
   *
   * @return the world state root hash.
   */
  public Hash getWorldStateRootHash() {
    return worldStateRootHash;
  }

  /**
   * Determines whether the current world state is directly modifying the "head" state of the
   * blockchain. A world state modifying the head directly updates the latest state of the node,
   * while a world state derived from a snapshot or historical view (e.g., layered or snapshot world
   * state) does not directly modify the head
   *
   * @return {@code true} if the current world state is modifying the head, {@code false} otherwise.
   */
  @Override
  public boolean isModifyingHeadWorldState() {
    return isModifyingHeadWorldState(worldStateKeyValueStorage);
  }

  private boolean isModifyingHeadWorldState(
      final WorldStateKeyValueStorage worldStateKeyValueStorage) {
    return !(worldStateKeyValueStorage instanceof BonsaiSnapshotWorldStateKeyValueStorage);
  }

  @Override
  public boolean isStorageFrozen() {
    return isStorageFrozen;
  }

  /**
   * Reset the worldState to this block header
   *
   * @param blockHeader block to use
   */
  public void resetWorldStateTo(final BlockHeader blockHeader) {
    worldStateBlockHash = blockHeader.getBlockHash();
    worldStateRootHash = blockHeader.getStateRoot();
  }

  @Override
  public BonsaiWorldStateKeyValueStorage getWorldStateStorage() {
    return worldStateKeyValueStorage;
  }

  public BonsaiWorldStateUpdateAccumulator getAccumulator() {
    return accumulator;
  }

  public boolean isTrieDisabled() {
    return worldStateConfig.isTrieDisabled();
  }

  @Override
  public MutableWorldState disableTrie() {
    this.worldStateConfig.setTrieDisabled(true);
    return this;
  }

  @Override
  public void persist(final BlockHeader blockHeader) {
    persist(blockHeader, resolveDefaultCommitter());
  }

  @Override
  public void persist(final BlockHeader blockHeader, final StateRootCommitter committer) {
    LOG.atDebug()
        .setMessage("Persist world state for block {}")
        .addArgument(() -> Optional.ofNullable(blockHeader))
        .log();

    boolean success = false;

    final BonsaiWorldStateKeyValueStorage.Updater stateUpdater =
        worldStateKeyValueStorage.updater();
    Runnable saveTrieLog = () -> {};
    Runnable cacheWorldState = () -> {};

    try {
      final StateRootComputation computation = committer.compute(this, blockHeader, accumulator);
      if (!isStorageFrozen()) {
        computation.applyTo(stateUpdater);
      }
      final Hash calculatedRootHash = computation.root();

      if (blockHeader != null) {
        verifyWorldStateRoot(calculatedRootHash, blockHeader);
        saveTrieLog =
            () -> {
              trieLogManager.saveTrieLog(accumulator, calculatedRootHash, blockHeader, this);
            };
        cacheWorldState =
            () -> worldStateCacheManager.addCachedLayer(blockHeader, calculatedRootHash, this);
        stateUpdater
            .getWorldStateTransaction()
            .put(
                TRIE_BRANCH_STORAGE,
                WORLD_BLOCK_HASH_KEY,
                blockHeader.getBlockHash().getBytes().toArrayUnsafe());
        worldStateBlockHash = blockHeader.getBlockHash();
      } else {
        stateUpdater.getWorldStateTransaction().remove(TRIE_BRANCH_STORAGE, WORLD_BLOCK_HASH_KEY);
        worldStateBlockHash = null;
      }

      stateUpdater
          .getWorldStateTransaction()
          .put(
              TRIE_BRANCH_STORAGE,
              WORLD_ROOT_HASH_KEY,
              calculatedRootHash.getBytes().toArrayUnsafe());

      stateUpdater
          .getWorldStateTransaction()
          .put(
              TRIE_BRANCH_STORAGE,
              WORLD_BLOCK_NUMBER_KEY,
              Bytes.ofUnsignedLong(blockHeader == null ? 0L : blockHeader.getNumber())
                  .toArrayUnsafe());
      worldStateRootHash = calculatedRootHash;
      success = true;
    } finally {
      if (success) {
        // commit the trielog transaction ahead of the state, in case of an abnormal shutdown:
        saveTrieLog.run();
        // commit only the composed worldstate, as trielog transaction is already complete:
        stateUpdater.commitComposedOnly();
        if (!isStorageFrozen) {
          // optionally save the committed worldstate state in the cache
          cacheWorldState.run();
        }
        accumulator.reset();
      } else {
        stateUpdater.rollback();
        accumulator.reset();
      }
    }
  }

  protected void verifyWorldStateRoot(final Hash calculatedStateRoot, final BlockHeader header) {
    if (!worldStateConfig.isTrieDisabled() && !calculatedStateRoot.equals(header.getStateRoot())) {
      throw new StateRootMismatchException(header.getStateRoot(), calculatedStateRoot);
    }
  }

  @Override
  public BonsaiWorldStateUpdateAccumulator updater() {
    return accumulator;
  }

  protected static final KeyValueStorageTransaction noOpTx =
      new KeyValueStorageTransaction() {

        @Override
        public void put(final byte[] key, final byte[] value) {
          // no-op
        }

        @Override
        public void remove(final byte[] key) {
          // no-op
        }

        @Override
        public void commit() throws StorageException {
          // no-op
        }

        @Override
        public void rollback() {
          // no-op
        }

        @Override
        public void close() {
          // no-op
        }
      };

  protected static final SegmentedKeyValueStorageTransaction noOpSegmentedTx =
      new SegmentedKeyValueStorageTransaction() {

        @Override
        public void put(
            final SegmentIdentifier segmentIdentifier, final byte[] key, final byte[] value) {
          // no-op
        }

        @Override
        public void remove(final SegmentIdentifier segmentIdentifier, final byte[] key) {
          // no-op
        }

        @Override
        public void commit() throws StorageException {
          // no-op
        }

        @Override
        public void rollback() {
          // no-op
        }

        @Override
        public void close() {
          // no-op
        }
      };

  public Hash blockHash() {
    return worldStateBlockHash;
  }

  @Override
  public Stream<StreamableAccount> streamAccounts(final Bytes32 startKeyHash, final int limit) {
    throw new RuntimeException("storage format do not provide account streaming.");
  }

  @Override
  public UInt256 getPriorStorageValue(final Address address, final UInt256 storageKey) {
    return getStorageValue(address, storageKey);
  }

  protected Optional<Bytes> getAccountStateTrieNode(final Bytes location, final Bytes32 nodeHash) {
    return getWorldStateStorage().getTrieNode(location, nodeHash);
  }

  public Optional<Bytes> getStorageTrieNode(
      final Hash accountHash, final Bytes location, final Bytes32 nodeHash) {
    return getWorldStateStorage()
        .getTrieNode(Bytes.concatenate(accountHash.getBytes(), location), nodeHash);
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
  public void close() {
    try {
      if (!isModifyingHeadWorldState()) {
        this.worldStateKeyValueStorage.close();
        if (isStorageFrozen) {
          closeFrozenStorage();
        }
      }
    } catch (Exception e) {
      // no op
    }
  }

  private void closeFrozenStorage() {
    try {
      final BonsaiWorldStateLayerStorage worldStateLayerStorage =
          (BonsaiWorldStateLayerStorage) worldStateKeyValueStorage;
      if (!isModifyingHeadWorldState(worldStateLayerStorage.getParentWorldStateStorage())) {
        worldStateLayerStorage.getParentWorldStateStorage().close();
      }
    } catch (Exception e) {
      // no op
    }
  }

  @Override
  public Hash frontierRootHash() {
    return resolveDefaultCommitter().compute(this, null, accumulator.copy()).root();
  }

  @Override
  public Hash rootHash() {
    if (isStorageFrozen && accumulator.isAccumulatorStateChanged()) {
      worldStateRootHash = resolveDefaultCommitter().compute(this, null, accumulator.copy()).root();
      accumulator.resetAccumulatorStateChanged();
    }
    return worldStateRootHash;
  }

  /**
   * Configures the current world state to operate in "frozen" mode.
   *
   * <p>In this mode: - Changes (to accounts, code, or slots) are isolated and not applied to the
   * underlying storage. - The state root can be recalculated, and a trie log can be generated, but
   * updates will not affect the world state storage. - All modifications are temporary and will be
   * lost once the world state is discarded.
   *
   * <p>Use Cases: - Calculating the state root after updates without altering the storage. -
   * Generating a trie log.
   *
   * @return The current world state in "frozen" mode.
   */
  @Override
  public MutableWorldState freezeStorage() {
    this.isStorageFrozen = true;
    this.worldStateKeyValueStorage = new BonsaiWorldStateLayerStorage(getWorldStateStorage());
    return this;
  }

  @Override
  public Account get(final Address address) {
    return getWorldStateStorage()
        .getAccount(address.addressHash())
        .map(
            bytes ->
                BonsaiAccount.fromFlatBytes(
                    accumulator,
                    address,
                    bytes,
                    true,
                    codeCache,
                    StorageRootStrategy.forFormat(getWorldStateStorage().getDataStorageFormat())))
        .orElse(null);
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

  @Override
  public Optional<Bytes> getCode(@NotNull final Address address, final Hash codeHash) {
    return getWorldStateStorage().getCode(codeHash, address.addressHash());
  }

  public BonsaiCachedMerkleTrieLoader getBonsaiCachedMerkleTrieLoader() {
    return bonsaiCachedMerkleTrieLoader;
  }

  public WorldStateConfig getWorldStateConfig() {
    return worldStateConfig;
  }

  public EvmConfiguration getEvmConfiguration() {
    return evmConfiguration;
  }

  public void disableCacheMerkleTrieLoader() {
    this.bonsaiCachedMerkleTrieLoader = new NoOpBonsaiCachedMerkleTrieLoader();
  }

  /** Account state trie rooted at the current world state root. */
  public MerkleTrie<Bytes, Bytes> createAccountStateTrie() {
    return createTrie(
        (location, hash) ->
            bonsaiCachedMerkleTrieLoader.getAccountStateTrieNode(
                getWorldStateStorage(), location, hash),
        Bytes32.wrap(worldStateRootHash.getBytes()),
        BlockProcessingExecutors.accountTrieForkJoinPool());
  }

  /** Storage trie for the given account rooted at the provided storage root. */
  public MerkleTrie<Bytes, Bytes> createStorageTrie(
      final Hash accountHash, final Hash storageRoot) {
    return createTrie(
        (location, key) ->
            bonsaiCachedMerkleTrieLoader.getAccountStorageTrieNode(
                getWorldStateStorage(), accountHash, location, key),
        Bytes32.wrap(storageRoot.getBytes()),
        BlockProcessingExecutors.storageTrieForkJoinPool());
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

  /**
   * Attaches a Block Access List overlay to this world state, replacing its accumulator with a
   * BAL-aware one. Must be called after the world state has been resolved (and rolled) to the
   * target block, so that overlay values never interfere with trie-log replay.
   *
   * @param blockAccessListOverlay the overlay to attach
   */
  public void applyBlockAccessListOverlay(final BlockAccessListOverlay blockAccessListOverlay) {
    setAccumulator(
        new BonsaiBalWorldStateUpdateAccumulator(
            this, evmConfiguration, codeCache, blockAccessListOverlay));
  }

  protected Hash getEmptyTrieHash() {
    return Hash.EMPTY_TRIE_HASH;
  }

  @Override
  public BonsaiCodeCache codeCache() {
    return codeCache;
  }

  static Optional<Bytes32> incrementBytes32(final Bytes32 value) {
    final UInt256 incremented = UInt256.fromBytes(value).add(UInt256.ONE);
    return incremented.isZero() ? Optional.empty() : Optional.of(incremented);
  }
}
