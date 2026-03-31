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
package org.hyperledger.besu.ethereum.trie.pathbased.bintrie;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.cache.BinTrieCachedWorldStorageManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.storage.BinTrieWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.worldview.BinTrieWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.worldstate.PathBasedExtraStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.ServiceManager;

import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;

/**
 * World state provider for BinTrie storage format. This class manages the lifecycle and access to
 * BinTrie-based world states.
 */
public class BinTrieWorldStateProvider extends PathBasedWorldStateProvider {

  public BinTrieWorldStateProvider(
      final BinTrieWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Blockchain blockchain,
      final PathBasedExtraStorageConfiguration pathBasedExtraStorageConfiguration,
      final ServiceManager pluginContext,
      final EvmConfiguration evmConfiguration,
      final CodeCache codeCache) {
    super(worldStateKeyValueStorage, blockchain, pathBasedExtraStorageConfiguration, pluginContext);
    this.evmConfiguration = evmConfiguration;
    provideCachedWorldStorageManager(
        new BinTrieCachedWorldStorageManager(
            this, worldStateKeyValueStorage, evmConfiguration, worldStateConfig, codeCache));
    loadHeadWorldState(
        new BinTrieWorldState(
            this, worldStateKeyValueStorage, evmConfiguration, worldStateConfig, codeCache));
  }

  @VisibleForTesting
  BinTrieWorldStateProvider(
      final BinTrieCachedWorldStorageManager binTrieCachedWorldStorageManager,
      final PathBasedExtraStorageConfiguration pathBasedExtraStorageConfiguration,
      final TrieLogManager trieLogManager,
      final BinTrieWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Blockchain blockchain,
      final EvmConfiguration evmConfiguration,
      final CodeCache codeCache) {
    super(
        worldStateKeyValueStorage, blockchain, pathBasedExtraStorageConfiguration, trieLogManager);
    this.evmConfiguration = evmConfiguration;
    provideCachedWorldStorageManager(binTrieCachedWorldStorageManager);
    loadHeadWorldState(
        new BinTrieWorldState(
            this, worldStateKeyValueStorage, evmConfiguration, worldStateConfig, codeCache));
  }

  @Override
  public void heal(final Optional<Address> maybeAccountToRepair, final Bytes location) {
    // BinTrie does not support healing in the same way as Bonsai
    // This is a no-op for now
  }
}
