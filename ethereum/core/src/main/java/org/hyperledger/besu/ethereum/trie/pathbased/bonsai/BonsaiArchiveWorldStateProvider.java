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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveReadFlatDbStrategyProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.Optional;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BonsaiArchiveWorldStateProvider extends BonsaiWorldStateProvider {

  private static final Logger LOG = LoggerFactory.getLogger(BonsaiArchiveWorldStateProvider.class);

  private final BonsaiWorldStateKeyValueStorage archiveReadStorage;
  private final CodeCache codeCache;
  private final WorldStateConfig archiveWorldStateConfig;
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
    final BonsaiArchiveReadFlatDbStrategyProvider archiveProvider =
        new BonsaiArchiveReadFlatDbStrategyProvider(metricsSystem, dataStorageConfiguration);
    archiveProvider.loadFlatDbStrategy(worldStateKeyValueStorage.getComposedWorldStateStorage());
    this.archiveReadStorage =
        new BonsaiWorldStateKeyValueStorage(
            archiveProvider,
            worldStateKeyValueStorage.getComposedWorldStateStorage(),
            worldStateKeyValueStorage.getTrieLogStorage());
  }

  @Override
  public Optional<MutableWorldState> getWorldState(final WorldStateQueryParams queryParams) {
    if (isHistoricalQuery(queryParams)) {
      LOG.debug(
          "Returning archive state without verifying state root for block {}",
          queryParams.getBlockHeader().getNumber());
      final BonsaiWorldState archiveWorldState =
          new BonsaiWorldState(
              this, archiveReadStorage, evmConfiguration, archiveWorldStateConfig, codeCache);
      // Freeze before the persisting to ensure that the historical block number which is needed for
      // Bonsai archive does not affect the database
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
