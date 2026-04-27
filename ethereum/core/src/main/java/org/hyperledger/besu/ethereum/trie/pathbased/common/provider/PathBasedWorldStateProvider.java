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

  /**
   * Rolls {@code mutableState} to {@code blockHash} by trie logs: builds a rollback/forward hash
   * plan, then applies it in batches (see {@link #applyTrieLogRollPlan}).
   */
  private Optional<MutableWorldState> rollFullWorldStateToBlockHash(
      final PathBasedWorldState mutableState, final Hash blockHash) {
    if (blockHash.equals(mutableState.blockHash())) {
      return Optional.of(mutableState);
    }
    try {
      final List<Hash> rollbackBlockHashes = new ArrayList<>();
      final List<Hash> forwardBlockHashes = new ArrayList<>();
      planTrieLogRollHashes(mutableState, blockHash, rollbackBlockHashes, forwardBlockHashes);
      if (rollbackBlockHashes.isEmpty() && forwardBlockHashes.isEmpty()) {
        return Optional.of(mutableState);
      }
      return applyTrieLogRollPlan(
          mutableState,
          blockHash,
          rollbackBlockHashes,
          forwardBlockHashes,
          trieLogManager.getMaxLayersToLoad());
    } catch (final RuntimeException re) {
      LOG.info("Archive rolling failed for block hash " + blockHash, re);
      if (re instanceof MerkleTrieException) {
        throw re;
      }
      throw new MerkleTrieException(
          "invalid", Optional.of(Address.ZERO), Bytes32.wrap(Hash.EMPTY.getBytes()), Bytes.EMPTY);
    }
  }

  /**
   * Fills {@code rollbackBlockHashes} in head-to-ancestor order (each hash is the next trie-log
   * layer to undo). Fills {@code forwardBlockHashes} from the destination tip toward the fork
   * ancestor; {@link #applyTrieLogRollPlan} consumes that list from the end inward so rollforwards
   * run ancestor-to-tip.
   *
   * <p>When the world state's block is indexed on the chain: align heights on the old branch, then
   * walk both forks in lockstep until a shared hash (common ancestor). When it is not indexed, only
   * a single rollback hash is planned if a trie log exists for that block.
   */
  private void planTrieLogRollHashes(
      final PathBasedWorldState mutableState,
      final Hash destinationBlockHash,
      final List<Hash> rollbackBlockHashes,
      final List<Hash> forwardBlockHashes) {
    final Optional<BlockHeader> maybePersistedHeader =
        blockchain.getBlockHeader(mutableState.blockHash()).map(BlockHeader.class::cast);

    if (maybePersistedHeader.isEmpty()) {
      trieLogManager
          .getTrieLogLayer(mutableState.blockHash())
          .ifPresent(__ -> rollbackBlockHashes.add(mutableState.blockHash()));
      return;
    }

    BlockHeader targetHeader = blockchain.getBlockHeader(destinationBlockHash).get();
    BlockHeader persistedHeader = maybePersistedHeader.get();

    Hash persistedBlockHash = persistedHeader.getBlockHash();
    // Old chain higher than target: undo blocks until the same height as the target header.
    while (persistedHeader.getNumber() > targetHeader.getNumber()) {
      LOG.debug("Rollback {}", persistedBlockHash);
      rollbackBlockHashes.add(persistedBlockHash);
      persistedHeader = blockchain.getBlockHeader(persistedHeader.getParentHash()).get();
      persistedBlockHash = persistedHeader.getBlockHash();
    }

    Hash targetBlockHash = targetHeader.getBlockHash();
    // Target branch longer: record trie-log keys from target tip down toward the common height.
    while (persistedHeader.getNumber() < targetHeader.getNumber()) {
      LOG.debug("Rollforward {}", targetBlockHash);
      forwardBlockHashes.add(targetBlockHash);
      targetHeader = blockchain.getBlockHeader(targetHeader.getParentHash()).get();
      targetBlockHash = targetHeader.getBlockHash();
    }

    // Same height but different forks: paired undo on the old head and redo on the new branch.
    while (!persistedBlockHash.equals(targetBlockHash)) {
      LOG.debug("Paired Rollback {}", persistedBlockHash);
      LOG.debug("Paired Rollforward {}", targetBlockHash);
      forwardBlockHashes.add(targetBlockHash);
      targetHeader = blockchain.getBlockHeader(targetHeader.getParentHash()).get();

      rollbackBlockHashes.add(persistedBlockHash);
      persistedHeader = blockchain.getBlockHeader(persistedHeader.getParentHash()).get();

      targetBlockHash = targetHeader.getBlockHash();
      persistedBlockHash = persistedHeader.getBlockHash();
    }
  }

  /** Block hash of the world state after undoing {@code blockToUndo}'s trie log layer. */
  private Hash rollBackOne(
      final PathBasedWorldStateUpdateAccumulator<?> updater, final Hash blockToUndo) {
    final TrieLog layer = trieLogManager.getTrieLogLayer(blockToUndo).get();
    LOG.debug("Attempting Rollback of {}", layer.getBlockHash());
    updater.rollBack(layer);
    return blockchain.getBlockHeader(layer.getBlockHash()).get().getParentHash();
  }

  /** Block hash of the world state after applying {@code blockToApply}'s trie log layer. */
  private Hash rollForwardOne(
      final PathBasedWorldStateUpdateAccumulator<?> updater, final Hash blockToApply) {
    final TrieLog layer = trieLogManager.getTrieLogLayer(blockToApply).get();
    LOG.debug("Attempting Rollforward of {}", layer.getBlockHash());
    updater.rollForward(layer);
    return layer.getBlockHash();
  }

  /**
   * Applies trie logs in at most {@code maxTrieLogsPerBatch} operations per {@code commit} + {@code
   * persist}. Trie layers are loaded from storage per hash (lists hold hashes only).
   *
   * <p>Within each batch, rollbacks run first (required for correctness), then rollforwards until
   * the batch budget is used. {@code min(batch, opsLeft)} lets the last batch combine tail
   * rollbacks and forwards in a single persist when the total remainder fits in one batch.
   */
  private Optional<MutableWorldState> applyTrieLogRollPlan(
      final PathBasedWorldState mutableState,
      final Hash finalBlockHash,
      final List<Hash> rollbackBlockHashes,
      final List<Hash> forwardBlockHashes,
      final long maxTrieLogsPerBatch) {
    final PathBasedWorldStateUpdateAccumulator<?> updater =
        (PathBasedWorldStateUpdateAccumulator<?>) mutableState.updater();
    final int nRollbacks = rollbackBlockHashes.size();
    final int nForwards = forwardBlockHashes.size();
    int iRollback = 0;
    int iForward = nForwards - 1;
    Hash currentBlockHash = mutableState.blockHash();

    try {
      while (iRollback < nRollbacks || iForward >= 0) {
        final long opsLeft = (long) (nRollbacks - iRollback) + (long) (iForward + 1);
        final int budget = (int) Math.min(maxTrieLogsPerBatch, opsLeft);

        int used = 0;
        while (used < budget && iRollback < nRollbacks) {
          currentBlockHash = rollBackOne(updater, rollbackBlockHashes.get(iRollback++));
          used++;
        }
        while (used < budget && iForward >= 0) {
          currentBlockHash = rollForwardOne(updater, forwardBlockHashes.get(iForward--));
          used++;
        }

        updater.commit();
        mutableState.persist(blockchain.getBlockHeader(currentBlockHash).get());
      }

      if (!currentBlockHash.equals(finalBlockHash)) {
        updater.reset();
        LOG.atDebug()
            .setMessage("State rolling finished at unexpected block hash {} (expected {})")
            .addArgument(currentBlockHash)
            .addArgument(finalBlockHash)
            .log();
        return Optional.empty();
      }

      LOG.debug(
          "Archive rolling finished, {} now at {}",
          mutableState.getWorldStateStorage().getClass().getSimpleName(),
          finalBlockHash);
      return Optional.of(mutableState);
    } catch (final MerkleTrieException re) {
      throw re;
    } catch (final Exception e) {
      updater.reset();
      LOG.atDebug()
          .setMessage("State rolling failed on {} for block hash {}, {}")
          .addArgument(mutableState.getWorldStateStorage().getClass().getSimpleName())
          .addArgument(finalBlockHash)
          .addArgument(e)
          .log();

      return Optional.empty();
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
