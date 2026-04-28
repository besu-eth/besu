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
package org.hyperledger.besu.ethereum.trie.pathbased.common.provider;

import static org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.proof.WorldStateProof;
import org.hyperledger.besu.ethereum.proof.WorldStateProofProvider;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.pathbased.common.cache.PathBasedCachedWorldStorageManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.worldstate.PathBasedExtraStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldState;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PathBasedWorldStateProvider implements WorldStateArchive {

  private static final Logger LOG = LoggerFactory.getLogger(PathBasedWorldStateProvider.class);

  protected final Blockchain blockchain;

  protected final TrieLogManager trieLogManager;
  protected PathBasedCachedWorldStorageManager cachedWorldStorageManager;
  protected PathBasedWorldState headWorldState;
  protected final PathBasedWorldStateKeyValueStorage worldStateKeyValueStorage;
  protected EvmConfiguration evmConfiguration;
  // Configuration that will be shared by all instances of world state at their creation
  protected final WorldStateConfig worldStateConfig;

  public PathBasedWorldStateProvider(
      final PathBasedWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Blockchain blockchain,
      final PathBasedExtraStorageConfiguration pathBasedExtraStorageConfiguration,
      final ServiceManager pluginContext) {
    this(
        worldStateKeyValueStorage,
        blockchain,
        pathBasedExtraStorageConfiguration,
        new TrieLogManager(
            blockchain,
            worldStateKeyValueStorage,
            pathBasedExtraStorageConfiguration.getMaxLayersToLoad(),
            pluginContext));
  }

  public PathBasedWorldStateProvider(
      final PathBasedWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Blockchain blockchain,
      final PathBasedExtraStorageConfiguration pathBasedExtraStorageConfiguration,
      final TrieLogManager trieLogManager) {
    this.worldStateKeyValueStorage = worldStateKeyValueStorage;
    this.trieLogManager = trieLogManager;
    this.blockchain = blockchain;
    this.worldStateConfig =
        WorldStateConfig.newBuilder()
            .parallelStateRootComputationEnabled(
                pathBasedExtraStorageConfiguration.getParallelStateRootComputationEnabled())
            .build();
  }

  protected void provideCachedWorldStorageManager(
      final PathBasedCachedWorldStorageManager cachedWorldStorageManager) {
    this.cachedWorldStorageManager = cachedWorldStorageManager;
  }

  protected void loadHeadWorldState(final PathBasedWorldState headWorldState) {
    this.headWorldState = headWorldState;
    blockchain
        .getBlockHeader(headWorldState.getWorldStateBlockHash())
        .ifPresent(
            blockHeader ->
                this.cachedWorldStorageManager.addCachedLayer(
                    blockHeader, headWorldState.getWorldStateRootHash(), headWorldState));
  }

  @Override
  public Optional<WorldState> get(final Hash rootHash, final Hash blockHash) {
    return cachedWorldStorageManager
        .getWorldState(blockHash)
        .or(
            () -> {
              if (blockHash.equals(headWorldState.blockHash())) {
                return Optional.of(headWorldState);
              } else {
                return Optional.empty();
              }
            })
        .map(WorldState.class::cast);
  }

  @Override
  public boolean isWorldStateAvailable(final Hash rootHash, final Hash blockHash) {
    return cachedWorldStorageManager.contains(blockHash)
        || headWorldState.blockHash().equals(blockHash)
        || worldStateKeyValueStorage.isWorldStateAvailable(
            Bytes32.wrap(rootHash.getBytes()), blockHash);
  }

  /**
   * Gets a mutable world state based on the provided query parameters.
   *
   * <p>This method checks if the world state is configured to be stateful. If it is, it retrieves
   * the full world state using the provided query parameters. If the world state is not configured
   * to be full, the stateless one will be returned.
   *
   * <p>The method follows these steps: 1. Check if the world state is configured to be stateful. 2.
   * If true, call {@link #getFullWorldState(WorldStateQueryParams)} with the query parameters. 3.
   * If false, throw a RuntimeException indicating that stateless mode is not yet available.
   *
   * @param queryParams the query parameters
   * @return the mutable world state, if available
   * @throws RuntimeException if the world state is not configured to be stateful
   */
  @Override
  public Optional<MutableWorldState> getWorldState(final WorldStateQueryParams queryParams) {
    if (worldStateConfig.isStateful()) {
      return getFullWorldState(queryParams);
    } else {
      throw new RuntimeException("stateless mode is not yet available");
    }
  }

  /**
   * Gets the head world state.
   *
   * <p>This method returns the head world state, which is the most recent state of the world.
   *
   * @return the head world state
   */
  @Override
  public MutableWorldState getWorldState() {
    return headWorldState;
  }

  /**
   * Gets the full world state based on the provided query parameters.
   *
   * <p>This method determines whether to retrieve the full world state from the head or from the
   * cache based on the query parameters. If the query parameters indicate that the world state
   * should update the head, the method retrieves the full world state from the head. Otherwise, it
   * retrieves the full world state from the cache.
   *
   * <p>The method follows these steps: 1. Check if the query parameters indicate that the world
   * state should update the head. 2. If true, call {@link #getFullWorldStateFromHead(Hash)} with
   * the block hash from the query parameters. 3. If false, call {@link
   * #getFullWorldStateFromCache(BlockHeader)} with the block header from the query parameters.
   *
   * @param queryParams the query parameters
   * @return the stateful world state, if available
   */
  protected Optional<MutableWorldState> getFullWorldState(final WorldStateQueryParams queryParams) {
    return queryParams.shouldWorldStateUpdateHead()
        ? getFullWorldStateFromHead(queryParams.getBlockHash())
        : getFullWorldStateFromCache(queryParams.getBlockHeader());
  }

  /**
   * Gets the full world state from the head based on the provided block hash.
   *
   * <p>This method attempts to roll the head world state to the specified block hash. If the block
   * hash matches the block hash of the head world state, the head world state is returned.
   * Otherwise, the method attempts to roll the full world state to the specified block hash.
   *
   * <p>The method follows these steps: 1. Check if the block hash matches the block hash of the
   * head world state. 2. If it matches, return the head world state. 3. If it does not match,
   * attempt to roll the full world state to the specified block hash.
   *
   * @param blockHash the block hash
   * @return the full world state, if available
   */
  private Optional<MutableWorldState> getFullWorldStateFromHead(final Hash blockHash) {
    return rollFullWorldStateToBlockHash(headWorldState, blockHash);
  }

  /**
   * Gets the full world state from the cache based on the provided block header.
   *
   * <p>This method attempts to retrieve the world state from the cache using the block header. If
   * the block header is too old (i.e., the number of blocks between the chain head and the provided
   * block header exceeds the maximum layers to load), a warning is logged and an empty Optional is
   * returned.
   *
   * <p>The method follows these steps: 1. Check if the world state for the given block header is
   * available in the cache. 2. If not, attempt to get the nearest world state from the cache. 3. If
   * still not found, attempt to get the head world state. 4. If a world state is found, roll it to
   * the block hash of the provided block header. 5. Freeze the world state and return it.
   *
   * @param blockHeader the block header
   * @return the full world state, if available
   */
  private Optional<MutableWorldState> getFullWorldStateFromCache(final BlockHeader blockHeader) {
    final BlockHeader chainHeadBlockHeader = blockchain.getChainHeadHeader();
    if (chainHeadBlockHeader.getNumber() - blockHeader.getNumber()
        >= trieLogManager.getMaxLayersToLoad()) {
      LOG.warn(
          "Exceeded the limit of historical blocks that can be loaded ({}). If you need to make older historical queries, configure your `--bonsai-historical-block-limit`.",
          trieLogManager.getMaxLayersToLoad());
      return Optional.empty();
    }
    return cachedWorldStorageManager
        .getWorldState(blockHeader.getBlockHash())
        .or(() -> cachedWorldStorageManager.getNearestWorldState(blockHeader))
        .or(
            () ->
                cachedWorldStorageManager.getHeadWorldState(
                    blockHeaderHash ->
                        blockchain.getBlockHeader(blockHeaderHash).map(BlockHeader.class::cast)))
        .flatMap(
            worldState -> rollFullWorldStateToBlockHash(worldState, blockHeader.getBlockHash()))
        .map(MutableWorldState::freezeStorage);
  }

  private Optional<MutableWorldState> rollFullWorldStateToBlockHash(
      final PathBasedWorldState mutableState, final Hash blockHash) {
    if (blockHash.equals(mutableState.blockHash())) {
      return Optional.of(mutableState);
    } else {
      try {

        final Optional<BlockHeader> maybePersistedHeader =
            blockchain.getBlockHeader(mutableState.blockHash()).map(BlockHeader.class::cast);

        final List<TrieLog> rollBacks = new ArrayList<>();
        final List<TrieLog> rollForwards = new ArrayList<>();
        if (maybePersistedHeader.isEmpty()) {
          trieLogManager.getTrieLogLayer(mutableState.blockHash()).ifPresent(rollBacks::add);
        } else {
          BlockHeader targetHeader = blockchain.getBlockHeader(blockHash).get();
          BlockHeader persistedHeader = maybePersistedHeader.get();
          // roll back from persisted to even with target
          Hash persistedBlockHash = persistedHeader.getBlockHash();
          while (persistedHeader.getNumber() > targetHeader.getNumber()) {
            LOG.debug("Rollback {}", persistedBlockHash);
            rollBacks.add(trieLogManager.getTrieLogLayer(persistedBlockHash).get());
            persistedHeader = blockchain.getBlockHeader(persistedHeader.getParentHash()).get();
            persistedBlockHash = persistedHeader.getBlockHash();
          }
          // roll forward to target
          Hash targetBlockHash = targetHeader.getBlockHash();
          while (persistedHeader.getNumber() < targetHeader.getNumber()) {
            LOG.debug("Rollforward {}", targetBlockHash);
            rollForwards.add(trieLogManager.getTrieLogLayer(targetBlockHash).get());
            targetHeader = blockchain.getBlockHeader(targetHeader.getParentHash()).get();
            targetBlockHash = targetHeader.getBlockHash();
          }

          // roll back in tandem until we hit a shared state
          while (!persistedBlockHash.equals(targetBlockHash)) {
            LOG.debug("Paired Rollback {}", persistedBlockHash);
            LOG.debug("Paired Rollforward {}", targetBlockHash);
            rollForwards.add(trieLogManager.getTrieLogLayer(targetBlockHash).get());
            targetHeader = blockchain.getBlockHeader(targetHeader.getParentHash()).get();

            rollBacks.add(trieLogManager.getTrieLogLayer(persistedBlockHash).get());
            persistedHeader = blockchain.getBlockHeader(persistedHeader.getParentHash()).get();

            targetBlockHash = targetHeader.getBlockHash();
            persistedBlockHash = persistedHeader.getBlockHash();
          }
        }

        // attempt the state rolling
        final PathBasedWorldStateUpdateAccumulator<?> pathBasedUpdater =
            (PathBasedWorldStateUpdateAccumulator<?>) mutableState.updater();
        try {
          for (final TrieLog rollBack : rollBacks) {
            LOG.debug("Attempting Rollback of {}", rollBack.getBlockHash());
            pathBasedUpdater.rollBack(rollBack);
          }
          for (int i = rollForwards.size() - 1; i >= 0; i--) {
            final var forward = rollForwards.get(i);
            LOG.debug("Attempting Rollforward of {}", rollForwards.get(i).getBlockHash());
            pathBasedUpdater.rollForward(forward);
          }
          pathBasedUpdater.commit();

          mutableState.persist(blockchain.getBlockHeader(blockHash).get());

          LOG.debug(
              "Archive rolling finished, {} now at {}",
              mutableState.getWorldStateStorage().getClass().getSimpleName(),
              blockHash);
          return Optional.of(mutableState);
        } catch (final MerkleTrieException re) {
          // need to throw to trigger the heal
          throw re;
        } catch (final Exception e) {
          // if we fail we must clean up the updater
          pathBasedUpdater.reset();
          LOG.atDebug()
              .setMessage("State rolling failed on {} for block hash {}")
              .addArgument(mutableState.getWorldStateStorage().getClass().getSimpleName())
              .addArgument(blockHash)
              .addArgument(e)
              .log();

          return Optional.empty();
        }
      } catch (final RuntimeException re) {
        LOG.info("Archive rolling failed for block hash " + blockHash, re);
        if (re instanceof MerkleTrieException) {
          // need to throw to trigger the heal
          throw re;
        }
        throw new MerkleTrieException(
            "invalid", Optional.of(Address.ZERO), Bytes32.wrap(Hash.EMPTY.getBytes()), Bytes.EMPTY);
      }
    }
  }

  public WorldStateConfig getWorldStateSharedSpec() {
    return worldStateConfig;
  }

  public PathBasedWorldStateKeyValueStorage getWorldStateKeyValueStorage() {
    return worldStateKeyValueStorage;
  }

  public TrieLogManager getTrieLogManager() {
    return trieLogManager;
  }

  public PathBasedCachedWorldStorageManager getCachedWorldStorageManager() {
    return cachedWorldStorageManager;
  }

  @Override
  public void resetArchiveStateTo(final BlockHeader blockHeader) {
    headWorldState.resetWorldStateTo(blockHeader);
    this.cachedWorldStorageManager.reset();
    this.cachedWorldStorageManager.addCachedLayer(
        blockHeader, headWorldState.getWorldStateRootHash(), headWorldState);
  }

  @Override
  public <U> Optional<U> getAccountProof(
      final BlockHeader blockHeader,
      final Address accountAddress,
      final List<UInt256> accountStorageKeys,
      final Function<Optional<WorldStateProof>, ? extends Optional<U>> mapper) {
    try (PathBasedWorldState ws =
        (PathBasedWorldState)
            getWorldState(withBlockHeaderAndNoUpdateNodeHead(blockHeader)).orElse(null)) {
      if (ws != null) {
        final WorldStateProofProvider worldStateProofProvider =
            new WorldStateProofProvider(
                new WorldStateStorageCoordinator(ws.getWorldStateStorage()));
        return mapper.apply(
            worldStateProofProvider.getAccountProof(
                ws.getWorldStateRootHash(), accountAddress, accountStorageKeys));
      }
    } catch (Exception ex) {
      LOG.error(
          "failed proof query for " + blockHeader.getBlockHash().getBytes().toShortHexString(), ex);
    }
    return Optional.empty();
  }

  @Override
  public void close() {
    try {
      worldStateKeyValueStorage.close();
    } catch (Exception e) {
      // no op
    }
  }
}
