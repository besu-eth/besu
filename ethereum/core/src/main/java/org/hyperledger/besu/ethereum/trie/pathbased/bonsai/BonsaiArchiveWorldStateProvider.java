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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.ARCHIVE_PROOF_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveReadFlatDbStrategyProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BonsaiArchiveWorldStateProvider extends BonsaiWorldStateProvider {

  private static final Logger LOG = LoggerFactory.getLogger(BonsaiArchiveWorldStateProvider.class);

  private final BonsaiWorldStateKeyValueStorage archiveReadStorage;
  private final CodeCache codeCache;
  private final WorldStateConfig archiveWorldStateConfig;
  private final boolean stateProofsEnabled;
  private final long trieNodeCheckpointInterval;
  private volatile LongSupplier archiveMigrationProgressSupplier = () -> -1L;

  public BonsaiArchiveWorldStateProvider(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Blockchain blockchain,
      final DataStorageConfiguration dataStorageConfiguration,
      final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader,
      final ServiceManager pluginContext,
      final EvmConfiguration evmConfiguration,
      final Supplier<WorldStateHealer> worldStateHealerSupplier,
      final CodeCache codeCache,
      final MetricsSystem metricsSystem) {
    super(
        worldStateKeyValueStorage,
        blockchain,
        dataStorageConfiguration.getPathBasedExtraStorageConfiguration(),
        bonsaiCachedMerkleTrieLoader,
        pluginContext,
        evmConfiguration,
        worldStateHealerSupplier,
        codeCache);
    this.codeCache = codeCache;
    this.archiveWorldStateConfig =
        WorldStateConfig.newBuilder(worldStateConfig).trieDisabled(true).build();
    this.stateProofsEnabled =
        dataStorageConfiguration
            .getPathBasedExtraStorageConfiguration()
            .getUnstable()
            .getStateProofsEnabled();
    this.trieNodeCheckpointInterval =
        dataStorageConfiguration
            .getPathBasedExtraStorageConfiguration()
            .getUnstable()
            .getArchiveTrieNodeCheckpointInterval();
    final BonsaiArchiveReadFlatDbStrategyProvider archiveProvider =
        new BonsaiArchiveReadFlatDbStrategyProvider(metricsSystem, dataStorageConfiguration);
    archiveProvider.loadFlatDbStrategy(worldStateKeyValueStorage.getComposedWorldStateStorage());
    this.archiveReadStorage =
        new BonsaiWorldStateKeyValueStorage(
            archiveProvider,
            worldStateKeyValueStorage.getComposedWorldStateStorage(),
            worldStateKeyValueStorage.getTrieLogStorage(),
            worldStateKeyValueStorage.getCacheManager(),
            worldStateKeyValueStorage.getCurrentVersion(),
            stateProofsEnabled
                ? new BonsaiArchiveTrieNodeStrategy(null) // reads archive; migrator owns all writes
                : new BonsaiTrieNodeStrategy());
  }

  @Override
  public Optional<MutableWorldState> getWorldState(final WorldStateQueryParams queryParams) {
    if (isHistoricalQuery(queryParams)) {
      if (stateProofsEnabled) {
        final Optional<BlockHeader> checkpointBlock =
            getCheckpointStateStartBlock(queryParams.getBlockHeader().getBlockHash());
        // Only use the archive proof path if the migration has fully processed the checkpoint
        // block. If migration is still behind the checkpoint, the archive flat-DB and trie-node
        // CFs won't have checkpoint data yet, causing spurious "nonces differ" failures.
        if (checkpointBlock.isPresent()
            && archiveMigrationProgressSupplier.getAsLong() >= checkpointBlock.get().getNumber()) {
          LOG.debug(
              "Returning archive proof state for block {} via checkpoint {}",
              queryParams.getBlockHeader().getNumber(),
              checkpointBlock.get().getNumber());
          return rollArchiveProofWorldStateToBlockHash(
                  newFrozenArchiveWorldState(worldStateConfig),
                  checkpointBlock.get(),
                  queryParams.getBlockHeader().getBlockHash())
              .map(MutableWorldState::freezeStorage);
        }
      }
      LOG.debug(
          "Returning archive state without verifying state root for block {}",
          queryParams.getBlockHeader().getNumber());
      return rollMutableArchiveStateToBlockHash(
          newFrozenArchiveWorldState(archiveWorldStateConfig),
          queryParams.getBlockHeader().getBlockHash());
    }
    return super.getWorldState(queryParams);
  }

  private BonsaiWorldState newFrozenArchiveWorldState(final WorldStateConfig config) {
    final BonsaiWorldState worldState =
        new BonsaiWorldState(this, archiveReadStorage, evmConfiguration, config, codeCache);
    // Freeze before persisting to ensure the historical block number does not affect the database
    worldState.freezeStorage();
    return worldState;
  }

  private Optional<BlockHeader> getCheckpointStateStartBlock(final Hash targetHash) {
    return blockchain
        .getBlockHeader(targetHash)
        .map(BlockHeader::getNumber)
        .flatMap(
            targetNumber -> {
              long nearestCheckpoint =
                  (((targetNumber + trieNodeCheckpointInterval) / trieNodeCheckpointInterval)
                          * trieNodeCheckpointInterval)
                      - 1;
              return blockchain
                  .getBlockHeaderSafe(nearestCheckpoint)
                  .or(() -> blockchain.getBlockHeaderSafe(blockchain.getChainHeadHash()));
            });
  }

  private Optional<MutableWorldState> rollArchiveProofWorldStateToBlockHash(
      final PathBasedWorldState mutableState,
      final BlockHeader checkpointBlock,
      final Hash targetBlockHash) {

    ((BonsaiWorldState) mutableState).resetWorldStateToCheckpoint(checkpointBlock);

    if (targetBlockHash.equals(mutableState.blockHash())) {
      return Optional.of(mutableState);
    }

    try {
      final BlockHeader targetHeader =
          blockchain
              .getBlockHeaderSafe(targetBlockHash)
              .orElseThrow(
                  () ->
                      new MerkleTrieException("target block header not found: " + targetBlockHash));
      final Optional<BlockHeader> maybePersistedHeader =
          blockchain.getBlockHeaderSafe(mutableState.blockHash()).map(BlockHeader.class::cast);

      final List<TrieLog> rollBacks = new ArrayList<>();
      if (maybePersistedHeader.isEmpty()) {
        trieLogManager.getTrieLogLayer(mutableState.blockHash()).ifPresent(rollBacks::add);
      } else {
        BlockHeader persistedHeader = maybePersistedHeader.get();
        Hash persistedBlockHash = persistedHeader.getBlockHash();
        while (persistedHeader.getNumber() > targetHeader.getNumber()) {
          LOG.debug("Rollback {}", persistedBlockHash);
          final Hash blockHashForLog = persistedBlockHash;
          rollBacks.add(
              trieLogManager
                  .getTrieLogLayer(persistedBlockHash)
                  .orElseThrow(
                      () -> new MerkleTrieException("missing trie log for " + blockHashForLog)));
          final Hash parentHash = persistedHeader.getParentHash();
          persistedHeader =
              blockchain
                  .getBlockHeaderSafe(parentHash)
                  .orElseThrow(
                      () -> new MerkleTrieException("missing parent header for " + parentHash));
          persistedBlockHash = persistedHeader.getBlockHash();
        }
      }

      final PathBasedWorldStateUpdateAccumulator<?> diffBasedUpdater =
          (PathBasedWorldStateUpdateAccumulator<?>) mutableState.updater();
      try {
        for (final TrieLog rollBack : rollBacks) {
          LOG.info("Attempting rollback of {}", rollBack.getBlockHash());
          diffBasedUpdater.rollBack(rollBack);
        }
        diffBasedUpdater.commit();

        // overrides suffix selection in putFlatAccountTrieNode/putFlatStorageTrieNode
        final SegmentedKeyValueStorageTransaction tx =
            mutableState.getWorldStateStorage().getComposedWorldStateStorage().startTransaction();
        tx.put(
            TRIE_BRANCH_STORAGE,
            ARCHIVE_PROOF_BLOCK_NUMBER_KEY,
            Bytes.ofUnsignedLong(targetHeader.getNumber()).toArrayUnsafe());
        tx.commit();

        mutableState.persist(targetHeader);

        return Optional.of(mutableState);
      } catch (final MerkleTrieException re) {
        throw re;
      } catch (final Exception e) {
        diffBasedUpdater.reset();
        LOG.atInfo()
            .setMessage("Archive proof state rolling failed on {} for block hash {}: {}")
            .addArgument(mutableState.getWorldStateStorage().getClass().getSimpleName())
            .addArgument(targetBlockHash)
            .addArgument(e)
            .log();
        return Optional.empty();
      }
    } catch (final RuntimeException re) {
      LOG.info("Archive proof rolling failed for block hash {}", targetBlockHash, re);
      if (re instanceof MerkleTrieException) {
        throw re;
      }
      throw new MerkleTrieException("invalid archive proof rollback for " + targetBlockHash);
    }
  }

  /**
   * Sets the supplier used by {@code isHistoricalQuery} to check the highest block number that has
   * been migrated to Bonsai archive storage.
   *
   * <p>Until this is called, the default supplier returns {@code -1}, which denies all
   * archive-backed historical queries and falls back to trie-log rollback via {@code super}.
   *
   * @param supplier returns the highest block number available in Bonsai archive storage
   */
  public void setArchiveMigrationProgressSupplier(final LongSupplier supplier) {
    this.archiveMigrationProgressSupplier = supplier;
  }

  private boolean isHistoricalQuery(final WorldStateQueryParams queryParams) {
    final long queryBlock = queryParams.getBlockHeader().getNumber();
    return worldStateKeyValueStorage.getFlatDbMode().equals(FlatDbMode.ARCHIVE)
        && !queryParams.shouldWorldStateUpdateHead()
        && blockchain.getChainHeadHeader().getNumber() - queryBlock
            >= trieLogManager.getMaxLayersToLoad()
        && archiveMigrationProgressSupplier.getAsLong() >= queryBlock;
  }

  // Archive-specific rollback behaviour. There is no trie-log roll forward/backward, we just roll
  // back the state root, block hash and block number
  protected Optional<MutableWorldState> rollMutableArchiveStateToBlockHash(
      final PathBasedWorldState mutableState, final Hash blockHash) {
    LOG.trace(
        "Rolling mutable archive world state to block hash {}", blockHash.getBytes().toHexString());
    try {
      // Simply persist the block hash/number and state root for this archive state
      mutableState.persist(blockchain.getBlockHeader(blockHash).get());
      LOG.trace(
          "Archive rolling finished, {} now at {}",
          mutableState.getWorldStateStorage().getClass().getSimpleName(),
          blockHash);
      return Optional.of(mutableState);
    } catch (final MerkleTrieException re) {
      // need to throw to trigger the heal
      throw re;
    } catch (final Exception e) {
      LOG.atInfo()
          .setMessage("State rolling failed on {} for block hash {}: {}")
          .addArgument(mutableState.getWorldStateStorage().getClass().getSimpleName())
          .addArgument(blockHash)
          .addArgument(e)
          .log();
      return Optional.empty();
    }
  }
}
