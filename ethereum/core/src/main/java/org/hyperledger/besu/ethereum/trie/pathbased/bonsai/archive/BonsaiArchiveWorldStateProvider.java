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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.proof.WorldStateProof;
import org.hyperledger.besu.ethereum.proof.WorldStateProofProvider;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveHistoryReader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeHistoryStore;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeHistoryStore.ArchivedBlock;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeWriter;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.WorldStateConfig;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ExtraStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;
import org.hyperledger.besu.util.log.LogUtil;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.LongSupplier;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BonsaiArchiveWorldStateProvider extends BonsaiWorldStateProvider {

  private static final Logger LOG = LoggerFactory.getLogger(BonsaiArchiveWorldStateProvider.class);

  /** Blocks a block must be buried under before it is archived on chains without finality. */
  public static final long CONFIRMATION_DEPTH = 128;

  private static final int NOT_CANONICAL_LOG_INTERVAL_SECONDS = 60;

  private final BonsaiWorldStateKeyValueStorage archiveReadStorage;
  private final BonsaiCodeCache codeCache;
  private final WorldStateConfig archiveWorldStateConfig;
  private volatile LongSupplier archiveMigrationProgressSupplier = () -> -1L;

  private final ArchiveNodeHistoryStore archiveHistoryStore;
  private final ArchiveHistoryReader archiveHistoryReader;
  private final Optional<ArchiveTrieNodeStrategy> archiveTrieNodeStrategy;
  private final boolean instantFinality;
  private final AtomicBoolean logNotCanonical = new AtomicBoolean(true);
  private final LongSupplier networkHead;

  public BonsaiArchiveWorldStateProvider(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Blockchain blockchain,
      final DataStorageConfiguration dataStorageConfiguration,
      final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader,
      final ServiceManager pluginContext,
      final EvmConfiguration evmConfiguration,
      final BonsaiCodeCache codeCache,
      final MetricsSystem metricsSystem,
      final Optional<Long> amsterdamMilestone,
      final int trieCaptureParallelism,
      final boolean instantFinality,
      final LongSupplier networkHead) {
    super(
        worldStateKeyValueStorage,
        blockchain,
        dataStorageConfiguration.getExtraStorageConfiguration(),
        bonsaiCachedMerkleTrieLoader,
        pluginContext,
        evmConfiguration,
        codeCache,
        amsterdamMilestone);
    this.codeCache = codeCache;
    this.instantFinality = instantFinality;
    this.networkHead = networkHead;
    this.archiveWorldStateConfig =
        WorldStateConfig.newBuilder(worldStateConfig).trieDisabled(true).build();
    final BonsaiArchiveReadFlatDbStrategyProvider archiveProvider =
        new BonsaiArchiveReadFlatDbStrategyProvider(metricsSystem, dataStorageConfiguration);
    archiveProvider.loadFlatDbStrategy(worldStateKeyValueStorage.getComposedWorldStateStorage());
    this.archiveReadStorage =
        new BonsaiWorldStateKeyValueStorage(
            archiveProvider,
            worldStateKeyValueStorage.getComposedWorldStateStorage(),
            worldStateKeyValueStorage.getTrieLogStorage(),
            worldStateKeyValueStorage.getCacheManager(),
            worldStateKeyValueStorage.getCurrentVersion());
    final SegmentedKeyValueStorage liveStorage =
        worldStateKeyValueStorage.getComposedWorldStateStorage();
    this.archiveHistoryStore = new ArchiveNodeHistoryStore(liveStorage);
    this.archiveHistoryReader = new ArchiveHistoryReader(archiveHistoryStore);
    final ExtraStorageConfiguration.Unstable unstable =
        dataStorageConfiguration.getExtraStorageConfiguration().getUnstable();
    if (unstable.getBonsaiArchiveStateProofsEnabled()) {
      requireCoverageFromGenesis();
      this.archiveTrieNodeStrategy =
          Optional.of(createInlineStrategy(liveStorage, unstable, trieCaptureParallelism));
    } else {
      this.archiveTrieNodeStrategy = Optional.empty();
    }
    archiveTrieNodeStrategy.ifPresent(worldStateKeyValueStorage::setTrieNodeStrategy);
  }

  private void requireCoverageFromGenesis() {
    // A persisted world state without proof history was synced without proofs; history starts at
    // genesis.
    final boolean statePersisted =
        blockchain.getChainHeadBlockNumber() > 0
            || worldStateKeyValueStorage.getWorldStateBlockHash().isPresent();
    if (statePersisted && archiveHistoryStore.getArchivedBlock().isEmpty()) {
      throw new IllegalStateException(
          "Bonsai archive state proofs need trie-node history from genesis, but this database has"
              + " no proof history. Proofs require a full sync from genesis with archive state"
              + " proofs enabled.");
    }
  }

  private ArchiveTrieNodeStrategy createInlineStrategy(
      final SegmentedKeyValueStorage liveStorage,
      final ExtraStorageConfiguration.Unstable unstable,
      final int trieCaptureParallelism) {
    final ExecutorService trieCapturePool = Executors.newFixedThreadPool(trieCaptureParallelism);
    return new ArchiveTrieNodeStrategy(
        new BonsaiTrieNodeStrategy(),
        new ArchiveTrieNodeWriter(
            archiveHistoryStore,
            trieCapturePool,
            unstable.getBonsaiArchiveShallowCheckpointInterval(),
            unstable.getBonsaiArchiveDeepCheckpointInterval()),
        archiveHistoryStore,
        liveStorage,
        this::isFinal);
  }

  // Without a finalized block, a block counts as final once it is buried under the confirmation
  // depth. Genesis can't be reorged.
  @VisibleForTesting
  boolean isFinal(final long block) {
    if (instantFinality || block == 0L) {
      return true;
    }
    return blockchain
        .getFinalized()
        .flatMap(blockchain::getBlockHeader)
        .map(finalized -> block <= finalized.getNumber())
        .orElseGet(() -> block <= networkHead.getAsLong() - CONFIRMATION_DEPTH);
  }

  @Override
  public void close() {
    archiveTrieNodeStrategy.ifPresent(ArchiveTrieNodeStrategy::close);
    super.close();
  }

  @Override
  public Optional<MutableWorldState> getWorldState(final WorldStateQueryParams queryParams) {
    if (isHistoricalQuery(queryParams)) {
      LOG.debug(
          "Returning archive state without verifying state root for block {}",
          queryParams.getBlockHeader().getNumber());
      final BonsaiArchiveWorldState archiveWorldState =
          new BonsaiArchiveWorldState(
              this, archiveReadStorage, evmConfiguration, archiveWorldStateConfig, codeCache);
      // Freeze before persisting: BonsaiArchiveWorldState.freezeStorage() wraps in
      // BonsaiArchiveWorldStateLayerStorage, which passes the LayeredKeyValueStorage (holding the
      // historical WORLD_BLOCK_NUMBER_KEY) to the flat-DB strategy rather than the raw RocksDB
      // parent, ensuring archive reads use the queried block number, not the current HEAD.
      archiveWorldState.freezeStorage();
      return rollMutableArchiveStateToBlockHash(
          archiveWorldState, queryParams.getBlockHeader().getBlockHash());
    }
    return super.getWorldState(queryParams);
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

  @Override
  public <U> Optional<U> getAccountProof(
      final BlockHeader blockHeader,
      final Address accountAddress,
      final List<UInt256> accountStorageKeys,
      final Function<Optional<WorldStateProof>, ? extends Optional<U>> mapper) {
    final long blockNumber = blockHeader.getNumber();
    final Optional<ArchivedBlock> last = archiveHistoryStore.getArchivedBlock();
    // Archived nodes are the canonical chain's, so a fork block at an archived height isn't served.
    if (last.filter(archived -> blockNumber <= archived.number()).isEmpty()
        || !isCanonicalBlock(blockHeader)
        || !isCanonical(last.get())) {
      return super.getAccountProof(blockHeader, accountAddress, accountStorageKeys, mapper);
    }
    final WorldStateStorageCoordinator coordinator =
        new BonsaiArchiveReadWorldStateStorageCoordinator(
            archiveReadStorage, archiveHistoryReader, blockNumber);
    final WorldStateProofProvider proofProvider = new WorldStateProofProvider(coordinator);
    return mapper.apply(
        proofProvider.getAccountProof(
            blockHeader.getStateRoot(),
            blockHeader.getBlockHash(),
            accountAddress,
            accountStorageKeys));
  }

  private boolean isCanonicalBlock(final BlockHeader blockHeader) {
    return blockchain
        .getBlockHashByNumber(blockHeader.getNumber())
        .filter(blockHeader.getBlockHash()::equals)
        .isPresent();
  }

  // A reorg deeper than the confirmation depth replaced archived blocks, so archived nodes may
  // belong to blocks that are no longer canonical. Never serve them.
  @VisibleForTesting
  boolean isCanonical(final ArchivedBlock last) {
    if (blockchain.getBlockHashByNumber(last.number()).filter(last.hash()::equals).isPresent()) {
      return true;
    }
    LogUtil.throttledLog(
        () ->
            LOG.error(
                "Archived proof history ends at block {} ({}), which is no longer canonical. "
                    + "Proofs for archived blocks are refused; proof history needs a resync.",
                last.number(),
                last.hash()),
        logNotCanonical,
        NOT_CANONICAL_LOG_INTERVAL_SECONDS);
    return false;
  }

  // Archive-specific rollback behaviour. There is no trie-log roll forward/backward, we just roll
  // back the state root, block hash and block number
  protected Optional<MutableWorldState> rollMutableArchiveStateToBlockHash(
      final PathBasedWorldState mutableState, final Hash blockHash) {
    if (LOG.isTraceEnabled()) {
      LOG.trace(
          "Rolling mutable archive world state to block hash {}",
          blockHash.getBytes().toHexString());
    }
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
