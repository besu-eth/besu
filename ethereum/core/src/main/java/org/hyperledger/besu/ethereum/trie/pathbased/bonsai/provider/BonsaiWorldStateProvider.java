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

import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListOverlay;
import org.hyperledger.besu.ethereum.proof.WorldStateProof;
import org.hyperledger.besu.ethereum.proof.WorldStateProofProvider;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.WorldStateConfig;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache.BonsaiWorldStateCacheManager;
import org.hyperledger.besu.ethereum.worldstate.PathBasedExtraStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldState;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BonsaiWorldStateProvider implements WorldStateArchive {

  private static final Logger LOG = LoggerFactory.getLogger(BonsaiWorldStateProvider.class);

  protected final Blockchain blockchain;

  protected final TrieLogManager trieLogManager;
  protected BonsaiWorldStateCacheManager worldStateCacheManager;
  protected BonsaiWorldState headWorldState;
  protected final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage;
  protected EvmConfiguration evmConfiguration;
  // Configuration that will be shared by all instances of world state at their creation
  protected final WorldStateConfig worldStateConfig;

  private final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader;
  private final Optional<Long> amsterdamMilestone;

  public BonsaiWorldStateProvider(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Blockchain blockchain,
      final PathBasedExtraStorageConfiguration pathBasedExtraStorageConfiguration,
      final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader,
      final ServiceManager pluginContext,
      final EvmConfiguration evmConfiguration,
      final BonsaiCodeCache codeCache) {
    this(
        worldStateKeyValueStorage,
        blockchain,
        pathBasedExtraStorageConfiguration,
        bonsaiCachedMerkleTrieLoader,
        pluginContext,
        evmConfiguration,
        codeCache,
        Optional.empty());
  }

  public BonsaiWorldStateProvider(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Blockchain blockchain,
      final PathBasedExtraStorageConfiguration pathBasedExtraStorageConfiguration,
      final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader,
      final ServiceManager pluginContext,
      final EvmConfiguration evmConfiguration,
      final BonsaiCodeCache codeCache,
      final Optional<Long> amsterdamMilestone) {
    this.worldStateKeyValueStorage = worldStateKeyValueStorage;
    this.trieLogManager =
        new TrieLogManager(
            blockchain,
            worldStateKeyValueStorage,
            pathBasedExtraStorageConfiguration.getMaxLayersToLoad(),
            pluginContext);
    this.blockchain = blockchain;
    this.worldStateConfig =
        WorldStateConfig.newBuilder()
            .parallelStateRootComputationEnabled(
                pathBasedExtraStorageConfiguration.getParallelStateRootComputationEnabled())
            .build();
    this.bonsaiCachedMerkleTrieLoader = bonsaiCachedMerkleTrieLoader;
    this.amsterdamMilestone = amsterdamMilestone;
    this.evmConfiguration = evmConfiguration;
    provideWorldStateCacheManager(
        new BonsaiWorldStateCacheManager(
            this, worldStateKeyValueStorage, evmConfiguration, worldStateConfig, codeCache));
    initializeHeadWorldState(
        new BonsaiWorldState(
            this, worldStateKeyValueStorage, evmConfiguration, worldStateConfig, codeCache));
  }

  @VisibleForTesting
  BonsaiWorldStateProvider(
      final BonsaiWorldStateCacheManager bonsaiWorldStateCacheManager,
      final PathBasedExtraStorageConfiguration pathBasedExtraStorageConfiguration,
      final TrieLogManager trieLogManager,
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Blockchain blockchain,
      final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader,
      final EvmConfiguration evmConfiguration,
      final BonsaiCodeCache codeCache) {
    this.worldStateKeyValueStorage = worldStateKeyValueStorage;
    this.trieLogManager = trieLogManager;
    this.blockchain = blockchain;
    this.worldStateConfig =
        WorldStateConfig.newBuilder()
            .parallelStateRootComputationEnabled(
                pathBasedExtraStorageConfiguration.getParallelStateRootComputationEnabled())
            .build();
    this.bonsaiCachedMerkleTrieLoader = bonsaiCachedMerkleTrieLoader;
    this.amsterdamMilestone = Optional.empty();
    this.evmConfiguration = evmConfiguration;
    provideWorldStateCacheManager(bonsaiWorldStateCacheManager);
    initializeHeadWorldState(
        new BonsaiWorldState(
            this, worldStateKeyValueStorage, evmConfiguration, worldStateConfig, codeCache));
  }

  public BonsaiCachedMerkleTrieLoader getCachedMerkleTrieLoader() {
    return bonsaiCachedMerkleTrieLoader;
  }

  protected void provideWorldStateCacheManager(
      final BonsaiWorldStateCacheManager worldStateCacheManager) {
    this.worldStateCacheManager = worldStateCacheManager;
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

  @Override
  public Optional<MutableWorldState> getWorldState(final WorldStateQueryParams queryParams) {
    return getFullWorldState(queryParams);
  }

  @Override
  public MutableWorldState getWorldState() {
    return headWorldState;
  }

  protected Optional<MutableWorldState> getFullWorldState(final WorldStateQueryParams queryParams) {
    return queryParams.shouldWorldStateUpdateHead()
        ? getFullWorldStateFromHead(queryParams.getBlockHash())
        : getFullWorldStateFromCache(
            queryParams.getBlockHeader(), queryParams.getBlockAccessListOverlay());
  }

  private Optional<MutableWorldState> getFullWorldStateFromHead(final Hash blockHash) {
    return rollFullWorldStateToBlockHash(headWorldState, blockHash)
        .map(MutableWorldState.class::cast);
  }

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
            worldState -> rollFullWorldStateToBlockHash(worldState, blockHeader.getBlockHash()))
        .map(
            worldState -> {
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

  private Optional<BonsaiWorldState> rollFullWorldStateToBlockHash(
      final BonsaiWorldState mutableState, final Hash blockHash) {
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
          BlockHeader targetHeader = headerOrThrow(blockHash);
          BlockHeader persistedHeader = maybePersistedHeader.get();
          Hash persistedBlockHash = persistedHeader.getBlockHash();
          while (persistedHeader.getNumber() > targetHeader.getNumber()) {
            LOG.debug("Rollback {}", persistedBlockHash);
            rollBacks.add(trieLogOrThrow(persistedBlockHash));
            persistedHeader = headerOrThrow(persistedHeader.getParentHash());
            persistedBlockHash = persistedHeader.getBlockHash();
          }
          Hash targetBlockHash = targetHeader.getBlockHash();
          while (persistedHeader.getNumber() < targetHeader.getNumber()) {
            LOG.debug("Rollforward {}", targetBlockHash);
            rollForwards.add(trieLogOrThrow(targetBlockHash));
            targetHeader = headerOrThrow(targetHeader.getParentHash());
            targetBlockHash = targetHeader.getBlockHash();
          }
          while (!persistedBlockHash.equals(targetBlockHash)) {
            LOG.debug("Paired Rollback {}", persistedBlockHash);
            LOG.debug("Paired Rollforward {}", targetBlockHash);
            rollForwards.add(trieLogOrThrow(targetBlockHash));
            targetHeader = headerOrThrow(targetHeader.getParentHash());
            rollBacks.add(trieLogOrThrow(persistedBlockHash));
            persistedHeader = headerOrThrow(persistedHeader.getParentHash());
            targetBlockHash = targetHeader.getBlockHash();
            persistedBlockHash = persistedHeader.getBlockHash();
          }
        }

        final BonsaiWorldStateUpdateAccumulator pathBasedUpdater = mutableState.updater();
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
          mutableState.persist(headerOrThrow(blockHash));
          LOG.debug(
              "Archive rolling finished, {} now at {}",
              mutableState.getWorldStateStorage().getClass().getSimpleName(),
              blockHash);
          return Optional.of(mutableState);
        } catch (final MerkleTrieException re) {
          throw re;
        } catch (final Exception e) {
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
        LOG.warn("Archive rolling failed for block hash " + blockHash, re);
        if (re instanceof MerkleTrieException) {
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

  public BonsaiWorldStateKeyValueStorage getWorldStateKeyValueStorage() {
    return worldStateKeyValueStorage;
  }

  public TrieLogManager getTrieLogManager() {
    return trieLogManager;
  }

  public BonsaiWorldStateCacheManager getWorldStateCacheManager() {
    return worldStateCacheManager;
  }

  @Override
  public void resetArchiveStateTo(final BlockHeader blockHeader) {
    headWorldState.resetWorldStateTo(blockHeader);
    this.worldStateCacheManager.reset();
    this.worldStateCacheManager.addCachedLayer(
        blockHeader, headWorldState.getWorldStateRootHash(), headWorldState);
  }

  @Override
  public <U> Optional<U> getAccountProof(
      final BlockHeader blockHeader,
      final Address accountAddress,
      final List<UInt256> accountStorageKeys,
      final Function<Optional<WorldStateProof>, ? extends Optional<U>> mapper) {
    try (BonsaiWorldState ws =
        (BonsaiWorldState)
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

  private void initializeHeadWorldState(final BonsaiWorldState headWorldState) {
    blockchain
        .getBlockHeader(headWorldState.getWorldStateBlockHash())
        .ifPresentOrElse(
            header -> loadHeadWorldState(header, headWorldState),
            () -> this.headWorldState = headWorldState);
  }

  protected void loadHeadWorldState(
      final BlockHeader blockHeader, final BonsaiWorldState headWorldState) {
    this.headWorldState = headWorldState;
    this.worldStateCacheManager.addCachedLayer(
        blockHeader, headWorldState.getWorldStateRootHash(), headWorldState);
    prepareWorldStateForBlock(blockHeader, headWorldState);
  }

  @Override
  public void prepareWorldStateForBlock(
      final BlockHeader blockHeader, final MutableWorldState worldState) {
    if (isAmsterdamActive(blockHeader)) {
      if (worldState instanceof BonsaiWorldState bonsaiWorldState) {
        bonsaiWorldState.disableCacheMerkleTrieLoader();
      }
    }
  }

  private boolean isAmsterdamActive(final BlockHeader blockHeader) {
    return amsterdamMilestone
        .map(milestone -> Long.compareUnsigned(blockHeader.getTimestamp(), milestone) >= 0)
        .orElse(false);
  }
}
