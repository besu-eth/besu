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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache.BonsaiWorldStateCacheManager;
import org.hyperledger.besu.ethereum.worldstate.ExtraStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateRepairBudget;
import org.hyperledger.besu.ethereum.worldstate.WorldStateRepairResult;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BonsaiWorldStateProvider extends PathBasedWorldStateProvider {

  private static final Logger LOG = LoggerFactory.getLogger(BonsaiWorldStateProvider.class);

  private final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader;
  private final Optional<Long> amsterdamMilestone;
  private volatile CodeFetcher codeFetcher = codeHash -> Optional.empty();
  private volatile MissingTrieNodeFetcher missingTrieNodeFetcher =
      (request, budget) -> Optional.empty();

  public BonsaiWorldStateProvider(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Blockchain blockchain,
      final ExtraStorageConfiguration extraStorageConfiguration,
      final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader,
      final ServiceManager pluginContext,
      final EvmConfiguration evmConfiguration,
      final BonsaiCodeCache codeCache) {
    this(
        worldStateKeyValueStorage,
        blockchain,
        extraStorageConfiguration,
        bonsaiCachedMerkleTrieLoader,
        pluginContext,
        evmConfiguration,
        codeCache,
        Optional.empty());
  }

  public BonsaiWorldStateProvider(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Blockchain blockchain,
      final ExtraStorageConfiguration extraStorageConfiguration,
      final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader,
      final ServiceManager pluginContext,
      final EvmConfiguration evmConfiguration,
      final BonsaiCodeCache codeCache,
      final Optional<Long> amsterdamMilestone) {
    super(worldStateKeyValueStorage, blockchain, extraStorageConfiguration, pluginContext);
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
      final ExtraStorageConfiguration extraStorageConfiguration,
      final TrieLogManager trieLogManager,
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Blockchain blockchain,
      final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader,
      final EvmConfiguration evmConfiguration,
      final BonsaiCodeCache codeCache) {
    super(worldStateKeyValueStorage, blockchain, extraStorageConfiguration, trieLogManager);
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

  private void initializeHeadWorldState(final BonsaiWorldState headWorldState) {
    blockchain
        .getBlockHeader(headWorldState.getWorldStateBlockHash())
        .ifPresentOrElse(
            header -> loadHeadWorldState(header, headWorldState),
            () -> this.headWorldState = headWorldState);
  }

  @Override
  protected void loadHeadWorldState(
      final BlockHeader blockHeader, final PathBasedWorldState headWorldState) {
    super.loadHeadWorldState(blockHeader, headWorldState);
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

  public void setCodeFetcher(final CodeFetcher codeFetcher) {
    this.codeFetcher = codeFetcher;
  }

  public void setMissingTrieNodeFetcher(final MissingTrieNodeFetcher fetcher) {
    this.missingTrieNodeFetcher = java.util.Objects.requireNonNull(fetcher);
  }

  @Override
  public WorldStateRepairResult repairWorldState(
      final Hash blockHash, final Hash stateRoot, final WorldStateRepairBudget budget) {
    return new BonsaiWorldStateRepair(this, missingTrieNodeFetcher, budget)
        .repair(blockHash, stateRoot);
  }

  @Override
  public boolean healMissingCode(final Address address, final Hash codeHash) {
    // the block already failed as a local fault, a failed heal must not change that
    try {
      // checked before asking the peers for a code that could not be stored
      if (wouldReplaceHeadCode(address, codeHash)) {
        LOG.warn(
            "Not healing the code {} of account {}, the head has another one", codeHash, address);
        return false;
      }
      // only the code of the stored hash is written, so a peer cannot change what is executed
      final Optional<Bytes> code =
          codeFetcher.fetch(codeHash).filter(fetched -> Hash.hash(fetched).equals(codeHash));
      if (code.isEmpty()) {
        LOG.warn("Unable to heal the missing code {} of account {}", codeHash, address);
        return false;
      }
      return withHeadWorldStateLock(() -> storeHeadAccountCode(address, codeHash, code.get()));
    } catch (final RuntimeException e) {
      LOG.warn("Unable to heal the missing code {} of account {}", codeHash, address, e);
      return false;
    }
  }

  // a code stored by its hash replaces nothing, but stored by account it replaces the head one
  private boolean wouldReplaceHeadCode(final Address address, final Hash codeHash) {
    if (getWorldStateKeyValueStorage().getFlatDbStrategy().isCodeByCodeHash()) {
      return false;
    }
    final Account headAccount = getWorldState().get(address);
    return headAccount == null || !headAccount.getCodeHash().equals(codeHash);
  }

  private boolean storeHeadAccountCode(
      final Address address, final Hash codeHash, final Bytes code) {
    // checked again, the head may have moved during the fetch
    if (wouldReplaceHeadCode(address, codeHash)) {
      LOG.warn(
          "Not healing the code {} of account {}, the head moved to another one",
          codeHash,
          address);
      return false;
    }
    final BonsaiWorldStateKeyValueStorage.Updater updater =
        getWorldStateKeyValueStorage().updater();
    updater.putCode(address.addressHash(), codeHash, code);
    updater.commit();
    // the cached world states are snapshots taken before the code was stored
    worldStateCacheManager.reset();
    LOG.info("Healed the missing code {} of account {}", codeHash, address);
    return true;
  }

  private boolean isAmsterdamActive(final BlockHeader blockHeader) {
    return amsterdamMilestone
        .map(milestone -> Long.compareUnsigned(blockHeader.getTimestamp(), milestone) >= 0)
        .orElse(false);
  }
}
