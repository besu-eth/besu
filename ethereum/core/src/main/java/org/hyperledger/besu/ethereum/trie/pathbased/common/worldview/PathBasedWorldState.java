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
package org.hyperledger.besu.ethereum.trie.pathbased.common.worldview;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_HASH_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_ROOT_HASH_KEY;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.StateRootCommitter;
import org.hyperledger.besu.ethereum.trie.common.StateRootMismatchException;
import org.hyperledger.besu.ethereum.trie.pathbased.common.StorageSubscriber;
import org.hyperledger.besu.ethereum.trie.pathbased.common.cache.PathBasedCachedWorldStorageManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedLayeredWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedSnapshotWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;

import java.util.Optional;
import java.util.stream.Stream;

import jakarta.validation.constraints.NotNull;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PathBasedWorldState
    implements MutableWorldState, PathBasedWorldView, StorageSubscriber {

  private static final Logger LOG = LoggerFactory.getLogger(PathBasedWorldState.class);

  protected PathBasedWorldStateKeyValueStorage worldStateKeyValueStorage;
  protected final PathBasedCachedWorldStorageManager cachedWorldStorageManager;
  protected final TrieLogManager trieLogManager;
  protected PathBasedWorldStateUpdateAccumulator<?> accumulator;

  protected Hash worldStateRootHash;
  protected Hash worldStateBlockHash;

  // configuration parameters for the world state.
  protected WorldStateConfig worldStateConfig;

  /*
   * Indicates whether the world state is in "frozen" mode.
   *
   * When `isStorageFrozen` is true:
   * - Changes to accounts, code, or slots will not affect the underlying storage.
   * - The state root can still be recalculated, and a trie log can be generated.
   * - All modifications are temporary and will be lost once the world state is discarded.
   */
  protected boolean isStorageFrozen;

  protected PathBasedWorldState(
      final PathBasedWorldStateKeyValueStorage worldStateKeyValueStorage,
      final PathBasedCachedWorldStorageManager cachedWorldStorageManager,
      final TrieLogManager trieLogManager,
      final WorldStateConfig worldStateConfig) {
    this.worldStateKeyValueStorage = worldStateKeyValueStorage;
    this.worldStateRootHash =
        Hash.wrap(
            Bytes32.wrap(
                worldStateKeyValueStorage
                    .getWorldStateRootHash()
                    .orElse(getEmptyTrieHash().getBytes())));
    this.worldStateBlockHash = worldStateKeyValueStorage.getWorldStateBlockHash().orElse(Hash.ZERO);
    this.cachedWorldStorageManager = cachedWorldStorageManager;
    this.trieLogManager = trieLogManager;
    this.worldStateConfig = worldStateConfig;
    this.isStorageFrozen = false;
  }

  /**
   * Having a protected method to override the accumulator solves the chicken-egg problem of needing
   * a worldstate reference (this) when constructing the Accumulator.
   *
   * @param accumulator accumulator to use.
   */
  public void setAccumulator(final PathBasedWorldStateUpdateAccumulator<?> accumulator) {
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
    return !(worldStateKeyValueStorage instanceof PathBasedSnapshotWorldStateKeyValueStorage);
  }

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
  public PathBasedWorldStateKeyValueStorage getWorldStateStorage() {
    return worldStateKeyValueStorage;
  }

  public PathBasedWorldStateUpdateAccumulator<?> getAccumulator() {
    return accumulator;
  }

  @Override
  public MutableWorldState disableTrie() {
    this.worldStateConfig.setTrieDisabled(true);
    return this;
  }

  @Override
  public void persist(final BlockHeader blockHeader, final StateRootCommitter committer) {
    LOG.atDebug()
        .setMessage("Persist world state for block {}")
        .addArgument(() -> Optional.ofNullable(blockHeader))
        .log();

    boolean success = false;

    final PathBasedWorldStateKeyValueStorage.Updater stateUpdater =
        worldStateKeyValueStorage.updater();
    Runnable saveTrieLog = () -> {};
    Runnable cacheWorldState = () -> {};

    try {
      final Hash calculatedRootHash =
          committer.computeRoot(
              buildStateRootSupplier(stateUpdater, blockHeader), this, stateUpdater, blockHeader);

      if (blockHeader != null) {
        verifyWorldStateRoot(calculatedRootHash, blockHeader);
        saveTrieLog =
            () -> {
              trieLogManager.saveTrieLog(accumulator, calculatedRootHash, blockHeader, this);
            };
        cacheWorldState =
            () -> cachedWorldStorageManager.addCachedLayer(blockHeader, calculatedRootHash, this);
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

  /**
   * Builds a lazy supplier that, when invoked, applies all accumulated state changes to the trie
   * and returns the resulting state root hash. This supplier is passed to the {@link
   * StateRootCommitter}, which may invoke it (sync / BAL-verification) or skip it entirely
   * (BAL-trusted mode).
   */
  private java.util.function.Supplier<Hash> buildStateRootSupplier(
      final PathBasedWorldStateKeyValueStorage.Updater stateUpdater,
      final BlockHeader blockHeader) {
    final Optional<PathBasedWorldStateKeyValueStorage.Updater> updaterForTrie =
        isStorageFrozen ? Optional.empty() : Optional.of(stateUpdater);

    return () -> {
      if (blockHeader != null && worldStateConfig.isTrieDisabled()) {
        LOG.atDebug()
            .setMessage("Unsafe state root verification for block header {}")
            .addArgument(blockHeader)
            .log();
        calculateRootHash(updaterForTrie, accumulator);
        return blockHeader.getStateRoot();
      }
      return calculateRootHash(updaterForTrie, accumulator);
    };
  }

  protected void verifyWorldStateRoot(final Hash calculatedStateRoot, final BlockHeader header) {
    if (!worldStateConfig.isTrieDisabled() && !calculatedStateRoot.equals(header.getStateRoot())) {
      throw new StateRootMismatchException(header.getStateRoot(), calculatedStateRoot);
    }
  }

  @Override
  public PathBasedWorldStateUpdateAccumulator<?> updater() {
    return accumulator;
  }

  @Override
  public Hash rootHash() {
    if (isStorageFrozen && accumulator.isAccumulatorStateChanged()) {
      worldStateRootHash = calculateRootHash(Optional.empty(), accumulator.copy());
      accumulator.resetAccumulatorStateChanged();
    }
    return worldStateRootHash;
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
      final PathBasedLayeredWorldStateKeyValueStorage worldStateLayerStorage =
          (PathBasedLayeredWorldStateKeyValueStorage) worldStateKeyValueStorage;
      if (!isModifyingHeadWorldState(worldStateLayerStorage.getParentWorldStateStorage())) {
        worldStateLayerStorage.getParentWorldStateStorage().close();
      }
    } catch (Exception e) {
      // no op
    }
  }

  @Override
  public abstract Hash frontierRootHash();

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
  public abstract MutableWorldState freezeStorage();

  @Override
  public abstract Account get(final Address address);

  @Override
  public abstract UInt256 getStorageValue(final Address address, final UInt256 storageKey);

  @Override
  public abstract Optional<UInt256> getStorageValueByStorageSlotKey(
      final Address address, final StorageSlotKey storageSlotKey);

  @Override
  public abstract Optional<Bytes> getCode(@NotNull final Address address, final Hash codeHash);

  public abstract Hash calculateRootHash(
      final Optional<PathBasedWorldStateKeyValueStorage.Updater> maybeStateUpdater,
      final PathBasedWorldStateUpdateAccumulator<?> worldStateUpdater);

  protected abstract Hash getEmptyTrieHash();
}
