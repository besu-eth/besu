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

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache.BonsaiWorldStateCacheManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.code.PathBasedCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.worldstate.PathBasedExtraStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;

public class BonsaiWorldStateProvider extends PathBasedWorldStateProvider {

  private final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader;
  private final Optional<Long> amsterdamMilestone;

  public BonsaiWorldStateProvider(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Blockchain blockchain,
      final PathBasedExtraStorageConfiguration pathBasedExtraStorageConfiguration,
      final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader,
      final ServiceManager pluginContext,
      final EvmConfiguration evmConfiguration,
      final PathBasedCodeCache codeCache) {
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
      final PathBasedCodeCache codeCache,
      final Optional<Long> amsterdamMilestone) {
    super(worldStateKeyValueStorage, blockchain, pathBasedExtraStorageConfiguration, pluginContext);
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
      final PathBasedCodeCache codeCache) {
    super(
        worldStateKeyValueStorage, blockchain, pathBasedExtraStorageConfiguration, trieLogManager);
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

  private boolean isAmsterdamActive(final BlockHeader blockHeader) {
    return amsterdamMilestone
        .map(milestone -> Long.compareUnsigned(blockHeader.getTimestamp(), milestone) >= 0)
        .orElse(false);
  }
}
