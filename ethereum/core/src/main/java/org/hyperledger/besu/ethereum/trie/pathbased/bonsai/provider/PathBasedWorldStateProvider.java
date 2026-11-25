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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider;

import static org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListOverlay;
import org.hyperledger.besu.ethereum.proof.WorldStateProof;
import org.hyperledger.besu.ethereum.proof.WorldStateProofProvider;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.MissingCodeException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.WorldStateConfig;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache.PathBasedWorldStateCacheManager;
import org.hyperledger.besu.ethereum.worldstate.ExtraStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldState;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

public abstract class PathBasedWorldStateProvider implements WorldStateArchive {

  private static final Logger LOG = LoggerFactory.getLogger(PathBasedWorldStateProvider.class);

  protected final Blockchain blockchain;

  protected final TrieLogManager trieLogManager;
  protected PathBasedWorldStateCacheManager worldStateCacheManager;
  protected PathBasedWorldState headWorldState;
  // the head world state is rolled, executed on and persisted in place, so concurrent writers
  // would leave its data out of step with its block hash marker. Fair, so that a fork choice
  // waiting for it is not overtaken by backward sync importing the next block
  private final ReentrantLock headWorldStateLock = new ReentrantLock(true);
  // bounds the changes a roll holds in memory before persisting
  private static final int DEFAULT_TRIE_LOGS_PER_ROLL_BATCH = 512;
  private int trieLogsPerRollBatch = DEFAULT_TRIE_LOGS_PER_ROLL_BATCH;
  protected final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage;
  protected EvmConfiguration evmConfiguration;
  // Configuration that will be shared by all instances of world state at their creation
  protected final WorldStateConfig worldStateConfig;

  public PathBasedWorldStateProvider(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Blockchain blockchain,
      final ExtraStorageConfiguration extraStorageConfiguration,
      final ServiceManager pluginContext) {
    this(
        worldStateKeyValueStorage,
        blockchain,
        extraStorageConfiguration,
        new TrieLogManager(
            blockchain,
            worldStateKeyValueStorage,
            extraStorageConfiguration.getMaxLayersToLoad(),
            pluginContext));
  }

  public PathBasedWorldStateProvider(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Blockchain blockchain,
      final ExtraStorageConfiguration extraStorageConfiguration,
      final TrieLogManager trieLogManager) {
    this.worldStateKeyValueStorage = worldStateKeyValueStorage;
    this.trieLogManager = trieLogManager;
    this.blockchain = blockchain;
    this.worldStateConfig =
        WorldStateConfig.newBuilder()
            .parallelStateRootComputationEnabled(
                extraStorageConfiguration.getParallelStateRootComputationEnabled())
            .build();
  }

  protected void provideWorldStateCacheManager(
      final PathBasedWorldStateCacheManager worldStateCacheManager) {
    this.worldStateCacheManager = worldStateCacheManager;
  }

  protected void loadHeadWorldState(
      final BlockHeader blockHeader, final PathBasedWorldState headWorldState) {
    this.headWorldState = headWorldState;
    this.worldStateCacheManager.addCachedLayer(
        blockHeader, headWorldState.getWorldStateRootHash(), headWorldState);
  }

  @Override
  public Optional<WorldState> get(final Hash rootHash, final Hash blockHash) {
    return worldStateCacheManager
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
    return worldStateCacheManager.contains(blockHash)
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

  @Override
  public <T> T withHeadWorldStateLock(final Supplier<T> action) {
    headWorldStateLock.lock();
    try {
      return action.get();
    } finally {
      headWorldStateLock.unlock();
    }
  }

  @Override
  public boolean requiresBatchedHeadRoll(final BlockHeader blockHeader) {
    try {
      // a move in progress only makes this estimate stale, so it is safe without the lock
      return planRoll(headWorldState.blockHash(), blockHeader, trieLogsPerRollBatch).isEmpty();
    } catch (final RuntimeException e) {
      // a roll that cannot be planned fails, and is reported, when it is attempted
      return false;
    }
  }

  @VisibleForTesting
  public void setTrieLogsPerRollBatch(final int trieLogsPerRollBatch) {
    this.trieLogsPerRollBatch = trieLogsPerRollBatch;
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
   * #getFullWorldStateFromCache(BlockHeader, Optional)} with the block header and optional BAL
   * overlay from the query parameters.
   *
   * @param queryParams the query parameters
   * @return the stateful world state, if available
   */
  protected Optional<MutableWorldState> getFullWorldState(final WorldStateQueryParams queryParams) {
    return queryParams.shouldWorldStateUpdateHead()
        ? getFullWorldStateFromHead(queryParams.getBlockHash())
        : getFullWorldStateFromCache(
            queryParams.getBlockHeader(), queryParams.getBlockAccessListOverlay());
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
    // a failure here keeps the chain head from moving, so it must be visible at the default level
    return withHeadWorldStateLock(
        () ->
            rollFullWorldStateToBlockHash(headWorldState, blockHash, Long.MAX_VALUE, Level.WARN)
                .map(MutableWorldState.class::cast));
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
  private Optional<MutableWorldState> getFullWorldStateFromCache(
      final BlockHeader blockHeader,
      final Optional<BlockAccessListOverlay> maybeBlockAccessListOverlay) {
    final BlockHeader chainHeadBlockHeader = blockchain.getChainHeadHeader();
    if (chainHeadBlockHeader.getNumber() - blockHeader.getNumber()
        >= trieLogManager.getMaxLayersToLoad()) {
      LOG.warn(
          "Exceeded the limit of historical blocks that can be loaded ({}). If you need to make older historical queries, configure your `--bonsai-historical-block-limit`.",
          trieLogManager.getMaxLayersToLoad());
      return Optional.empty();
    }
    return worldStateCacheManager
        .getWorldState(blockHeader.getBlockHash())
        .or(() -> worldStateCacheManager.getNearestWorldState(blockHeader))
        .or(
            () ->
                worldStateCacheManager.getHeadWorldState(
                    blockHeaderHash ->
                        blockchain.getBlockHeader(blockHeaderHash).map(BlockHeader.class::cast)))
        .flatMap(
            worldState ->
                rollFullWorldStateToBlockHash(
                    worldState,
                    blockHeader.getBlockHash(),
                    trieLogManager.getMaxLayersToLoad(),
                    Level.DEBUG))
        .map(
            worldState -> {
              // the BAL overlay is attached only once the world state has been rolled to the
              // target block, so overlay values never interfere with trie-log replay
              maybeBlockAccessListOverlay.ifPresent(worldState::applyBlockAccessListOverlay);
              return worldState;
            })
        .map(MutableWorldState::freezeStorage);
  }

  private BlockHeader headerOrThrow(final Hash blockHash) {
    return blockchain
        .getBlockHeader(blockHash)
        .orElseThrow(
            () -> new IllegalStateException("Missing block header for block hash " + blockHash));
  }

  private TrieLog trieLogOrThrow(final Hash blockHash) {
    return trieLogManager
        .getTrieLogLayer(blockHash)
        .orElseThrow(
            () -> new IllegalStateException("Missing trie log for block hash " + blockHash));
  }

  /**
   * Rolls a world state to a block with trie logs, persisting it after every {@link
   * #trieLogsPerRollBatch} trie logs so a deep roll does not hold all of its changes in memory.
   * Every batch ends at a block, so an interrupted roll leaves the world state at that block.
   *
   * @param maxTrieLogs the roll is refused, returning empty, if it needs more trie logs than this
   */
  protected Optional<PathBasedWorldState> rollFullWorldStateToBlockHash(
      final PathBasedWorldState mutableState,
      final Hash blockHash,
      final long maxTrieLogs,
      final Level rollingFailureLogLevel) {
    if (blockHash.equals(mutableState.blockHash())) {
      return Optional.of(mutableState);
    }
    try {
      final BlockHeader targetHeader = headerOrThrow(blockHash);
      final Optional<List<RollStep>> maybeSteps =
          planRoll(mutableState.blockHash(), targetHeader, maxTrieLogs);
      if (maybeSteps.isEmpty()) {
        LOG.debug(
            "Rolling to block {} needs more than {} trie logs, more than the limit of historical blocks that can be loaded. If you need to make older historical queries, configure your `--bonsai-historical-block-limit`.",
            blockHash,
            maxTrieLogs);
        return Optional.empty();
      }
      return applyRoll(mutableState, targetHeader, maybeSteps.get(), rollingFailureLogLevel);
    } catch (final RuntimeException re) {
      LOG.warn("Archive rolling failed for block hash " + blockHash, re);
      if (re instanceof MerkleTrieException || re instanceof MissingCodeException) {
        // need to throw to trigger the heal
        throw re;
      }
      throw new MerkleTrieException(
          "invalid", Optional.of(Address.ZERO), Bytes32.wrap(Hash.EMPTY.getBytes()), Bytes.EMPTY);
    }
  }

  /** One trie log to apply, and the block whose state applying it leaves. */
  private record RollStep(Hash trieLogBlockHash, boolean isForward, Hash resultingBlockHash) {}

  /**
   * Plans the trie logs that roll a world state at one block to another: roll backs down to the
   * common ancestor, then roll forwards up to the target. Only headers are read.
   *
   * @return the steps in the order to apply them, or empty if there are more than {@code
   *     maxTrieLogs}
   */
  private Optional<List<RollStep>> planRoll(
      final Hash fromBlockHash, final BlockHeader target, final long maxTrieLogs) {
    final List<RollStep> rollBacks = new ArrayList<>();
    final List<RollStep> rollForwards = new ArrayList<>();
    final Optional<BlockHeader> maybeFromHeader =
        blockchain.getBlockHeader(fromBlockHash).map(BlockHeader.class::cast);
    if (maybeFromHeader.isEmpty()) {
      // a block that is not on the chain, such as a locally built one, has no known parent, so
      // only its own changes are undone and the result is checked against the target
      if (trieLogManager.getTrieLogLayer(fromBlockHash).isPresent()) {
        rollBacks.add(new RollStep(fromBlockHash, false, target.getBlockHash()));
      }
      return Optional.of(rollBacks);
    }

    BlockHeader fromHeader = maybeFromHeader.get();
    BlockHeader targetHeader = target;
    while (fromHeader.getNumber() > targetHeader.getNumber()) {
      rollBacks.add(new RollStep(fromHeader.getBlockHash(), false, fromHeader.getParentHash()));
      if (rollBacks.size() > maxTrieLogs) {
        return Optional.empty();
      }
      fromHeader = headerOrThrow(fromHeader.getParentHash());
    }
    while (fromHeader.getNumber() < targetHeader.getNumber()) {
      rollForwards.add(
          new RollStep(targetHeader.getBlockHash(), true, targetHeader.getBlockHash()));
      if (rollBacks.size() + rollForwards.size() > maxTrieLogs) {
        return Optional.empty();
      }
      targetHeader = headerOrThrow(targetHeader.getParentHash());
    }
    // same height on different forks: step both back until they share a block
    while (!fromHeader.getBlockHash().equals(targetHeader.getBlockHash())) {
      rollBacks.add(new RollStep(fromHeader.getBlockHash(), false, fromHeader.getParentHash()));
      rollForwards.add(
          new RollStep(targetHeader.getBlockHash(), true, targetHeader.getBlockHash()));
      if (rollBacks.size() + rollForwards.size() > maxTrieLogs) {
        return Optional.empty();
      }
      fromHeader = headerOrThrow(fromHeader.getParentHash());
      targetHeader = headerOrThrow(targetHeader.getParentHash());
    }
    Collections.reverse(rollForwards);
    rollBacks.addAll(rollForwards);
    return Optional.of(rollBacks);
  }

  private Optional<PathBasedWorldState> applyRoll(
      final PathBasedWorldState mutableState,
      final BlockHeader targetHeader,
      final List<RollStep> steps,
      final Level rollingFailureLogLevel) {
    final PathBasedWorldStateUpdateAccumulator<?> pathBasedUpdater =
        (PathBasedWorldStateUpdateAccumulator<?>) mutableState.updater();
    final boolean logProgress =
        steps.size() > trieLogsPerRollBatch && mutableState.isModifyingHeadWorldState();
    int applied = 0;
    do {
      final List<RollStep> batch =
          steps.subList(applied, Math.min(applied + trieLogsPerRollBatch, steps.size()));
      // loaded outside the try below, so a missing trie log triggers the heal
      final List<TrieLog> trieLogs =
          batch.stream().map(step -> trieLogOrThrow(step.trieLogBlockHash())).toList();
      applied += batch.size();
      // the last batch is checked against the target, also when rolling from a block off the chain
      final BlockHeader batchHeader =
          applied == steps.size()
              ? targetHeader
              : headerOrThrow(batch.getLast().resultingBlockHash());
      try {
        for (int i = 0; i < batch.size(); i++) {
          final TrieLog trieLog = trieLogs.get(i);
          if (batch.get(i).isForward()) {
            LOG.debug("Attempting Rollforward of {}", trieLog.getBlockHash());
            pathBasedUpdater.rollForward(trieLog);
          } else {
            LOG.debug("Attempting Rollback of {}", trieLog.getBlockHash());
            pathBasedUpdater.rollBack(trieLog);
          }
        }
        pathBasedUpdater.commit();
        mutableState.persist(batchHeader);
      } catch (final MerkleTrieException | MissingCodeException e) {
        // the caller heals and rolls the state again, from a clean updater
        pathBasedUpdater.reset();
        throw e;
      } catch (final Exception e) {
        // if we fail we must clean up the updater
        pathBasedUpdater.reset();
        LOG.atLevel(rollingFailureLogLevel)
            .setMessage("State rolling failed on {} for block hash {}")
            .addArgument(mutableState.getWorldStateStorage().getClass().getSimpleName())
            .addArgument(targetHeader.getBlockHash())
            .setCause(e)
            .log();
        return Optional.empty();
      }
      if (logProgress) {
        LOG.info(
            "Rolled head world state to block {}, {} of {} trie logs applied",
            batchHeader.getNumber(),
            applied,
            steps.size());
      }
    } while (applied < steps.size());

    LOG.debug(
        "Archive rolling finished, {} now at {}",
        mutableState.getWorldStateStorage().getClass().getSimpleName(),
        targetHeader.getBlockHash());
    return Optional.of(mutableState);
  }

  public WorldStateConfig getWorldStateSharedSpec() {
    return worldStateConfig;
  }

  public BonsaiWorldStateKeyValueStorage getWorldStateKeyValueStorage() {
    return worldStateKeyValueStorage;
  }

  public TrieLogManager getTrieLogManager() {
    return trieLogManager;
  }

  public PathBasedWorldStateCacheManager getWorldStateCacheManager() {
    return worldStateCacheManager;
  }

  @Override
  public void resetArchiveStateTo(final BlockHeader blockHeader) {
    headWorldStateLock.lock();
    try {
      headWorldState.resetWorldStateTo(blockHeader);
      this.worldStateCacheManager.reset();
      this.worldStateCacheManager.addCachedLayer(
          blockHeader, headWorldState.getWorldStateRootHash(), headWorldState);
    } finally {
      headWorldStateLock.unlock();
    }
  }

  @Override
  public <U> Optional<U> getAccountProof(
      final BlockHeader blockHeader,
      final Address accountAddress,
      final List<UInt256> accountStorageKeys,
      final Function<Optional<WorldStateProof>, ? extends Optional<U>> mapper) {
    // Empty means no world state for the block, including when it cannot be loaded, e.g. a trie
    // log missing for the roll or a cached layer closed while it is being layered on, or a world
    // state without a trie to prove against. Failures building the proof propagate so callers do
    // not mistake them for missing state.
    final Optional<MutableWorldState> maybeWorldState;
    try {
      maybeWorldState = getWorldState(withBlockHeaderAndNoUpdateNodeHead(blockHeader));
    } catch (final RuntimeException e) {
      LOG.atDebug()
          .setMessage("Cannot load world state for proof query at block {}")
          .addArgument(blockHeader::toLogString)
          .setCause(e)
          .log();
      return Optional.empty();
    }
    try (PathBasedWorldState ws = (PathBasedWorldState) maybeWorldState.orElse(null)) {
      if (ws == null) {
        return Optional.empty();
      }
      // with the trie disabled (disableWorldStateTrie, or a historical archive state without
      // archive proofs) only the root hash is kept, so there are no nodes to build a proof from
      if (ws.isTrieDisabled()) {
        LOG.atDebug()
            .setMessage("No trie to prove against at block {}")
            .addArgument(blockHeader::toLogString)
            .log();
        return Optional.empty();
      }
      final WorldStateProofProvider worldStateProofProvider =
          new WorldStateProofProvider(new WorldStateStorageCoordinator(ws.getWorldStateStorage()));
      return mapper.apply(
          worldStateProofProvider.getAccountProof(
              ws.getWorldStateRootHash(),
              ws.getWorldStateBlockHash(),
              accountAddress,
              accountStorageKeys));
    }
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
