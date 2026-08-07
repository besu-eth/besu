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
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.common.code.PathBasedCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BonsaiArchiveWorldStateProvider extends BonsaiWorldStateProvider {

  private static final Logger LOG = LoggerFactory.getLogger(BonsaiArchiveWorldStateProvider.class);

  private final BonsaiWorldStateKeyValueStorage archiveReadStorage;
  private final PathBasedCodeCache codeCache;
  private final WorldStateConfig archiveWorldStateConfig;
  private volatile LongSupplier archiveMigrationProgressSupplier = () -> -1L;

  /**
   * Null unless archive format + flag enabled. Wired by {@code BesuControllerBuilder} at
   * construction time so the same instance is shared with the walker.
   */
  private final TrieNodeHistoryReader trieHistoryReader;

  /**
   * Live reference shared with the walker; {@code volatile} internally so walker advances are
   * immediately visible to proof-query threads without reloading. Null if off.
   */
  private final TrieNodeHistoryProgress trieHistoryProgress;

  public BonsaiArchiveWorldStateProvider(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Blockchain blockchain,
      final DataStorageConfiguration dataStorageConfiguration,
      final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader,
      final ServiceManager pluginContext,
      final EvmConfiguration evmConfiguration,
      final Supplier<WorldStateHealer> worldStateHealerSupplier,
      final PathBasedCodeCache codeCache,
      final MetricsSystem metricsSystem,
      final TrieNodeHistoryReader trieNodeHistoryReader,
      final TrieNodeHistoryProgress trieNodeHistoryProgress) {
    super(
        worldStateKeyValueStorage,
        blockchain,
        dataStorageConfiguration.getPathBasedExtraStorageConfiguration(),
        requireNonNullLoader(bonsaiCachedMerkleTrieLoader),
        pluginContext,
        evmConfiguration,
        worldStateHealerSupplier,
        codeCache);
    this.codeCache = codeCache;
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
    // Both null when the feature flag is off; getAccountProof delegates entirely to super.
    this.trieHistoryReader = trieNodeHistoryReader;
    this.trieHistoryProgress = trieNodeHistoryProgress;
  }

  /**
   * Validates that {@code loader} is non-null before it reaches {@code super()}, where a null would
   * be silently stored and only surface as a NullPointerException inside trie-log rollback lambdas
   * — making misconfiguration look like proof unavailability.
   *
   * <p>This method is called as an argument to {@code super()} so that the check runs before the
   * superclass constructor stores the value.
   */
  private static BonsaiCachedMerkleTrieLoader requireNonNullLoader(
      final BonsaiCachedMerkleTrieLoader loader) {
    return Objects.requireNonNull(
        loader,
        "bonsaiCachedMerkleTrieLoader must not be null; "
            + "pass NoOpBonsaiCachedMerkleTrieLoader if no preloading is desired");
  }

  @Override
  public <U> Optional<U> getAccountProof(
      final BlockHeader blockHeader,
      final Address accountAddress,
      final List<UInt256> accountStorageKeys,
      final Function<Optional<WorldStateProof>, ? extends Optional<U>> mapper) {
    final boolean outsideReorgWindow =
        blockchain.getChainHeadBlockNumber() - blockHeader.getNumber()
            >= trieLogManager.getMaxLayersToLoad();
    if (trieHistoryProgress != null
        && outsideReorgWindow
        && trieHistoryProgress.covers(blockHeader.getNumber())) {
      final ArchiveProofNodeLoader loader =
          new ArchiveProofNodeLoader(
              trieHistoryReader,
              worldStateKeyValueStorage.getComposedWorldStateStorage(),
              blockHeader.getNumber());
      final WorldStateStorageCoordinator historyCoordinator =
          new HistoryBackedWorldStateStorageCoordinator(worldStateKeyValueStorage, loader);
      final WorldStateProofProvider proofProvider = new WorldStateProofProvider(historyCoordinator);
      try {
        return mapper.apply(
            proofProvider.getAccountProof(
                blockHeader.getStateRoot(), accountAddress, accountStorageKeys));
      } catch (final Exception e) {
        LOG.error(
            "Trie-node history reconstruction failed for block {}; falling back to the trie-log path",
            blockHeader.getNumber(),
            e);
        // fall through to super
      }
    }
    return super.getAccountProof(blockHeader, accountAddress, accountStorageKeys, mapper);
  }

  /**
   * Routes trie-node reads through {@link ArchiveProofNodeLoader} instead of live storage, so
   * {@link WorldStateProofProvider}'s trie walk reconstructs historical nodes without needing the
   * trie disabled or any other world-state-level change.
   */
  private static final class HistoryBackedWorldStateStorageCoordinator
      extends WorldStateStorageCoordinator {
    private final ArchiveProofNodeLoader loader;

    HistoryBackedWorldStateStorageCoordinator(
        final WorldStateKeyValueStorage delegate, final ArchiveProofNodeLoader loader) {
      super(delegate);
      this.loader = loader;
    }

    @Override
    public boolean isWorldStateAvailable(final Bytes32 nodeHash, final Hash blockHash) {
      // Availability is already gated by TrieNodeHistoryProgress.covers() before this class is
      // ever constructed (see getAccountProof above) — always available from here on.
      return true;
    }

    @Override
    public Optional<Bytes> getAccountStateTrieNode(final Bytes location, final Bytes32 nodeHash) {
      return loader.accountNodeLoader().getNode(location, nodeHash);
    }

    @Override
    public Optional<Bytes> getAccountStorageTrieNode(
        final Hash accountHash, final Bytes location, final Bytes32 nodeHash) {
      return loader
          .storageNodeLoader(Bytes32.wrap(accountHash.getBytes()))
          .getNode(location, nodeHash);
    }
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
