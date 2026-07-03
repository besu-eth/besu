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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache;

import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListOverlay;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiSnapshotWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateLayerStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.code.PathBasedCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.cache.PathBasedWorldStateCacheManager;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class BonsaiWorldStateCacheManager extends PathBasedWorldStateCacheManager {
  private final PathBasedCodeCache codeCache;

  public BonsaiWorldStateCacheManager(
      final BonsaiWorldStateProvider archive,
      final PathBasedWorldStateKeyValueStorage worldStateKeyValueStorage,
      final EvmConfiguration evmConfiguration,
      final WorldStateConfig worldStateConfig,
      final PathBasedCodeCache codeCache) {
    super(
        archive,
        worldStateKeyValueStorage,
        new ConcurrentHashMap<>(),
        evmConfiguration,
        worldStateConfig);

    this.codeCache = codeCache;
  }

  @Override
  public PathBasedWorldState createWorldState(
      final PathBasedWorldStateProvider archive,
      final PathBasedWorldStateKeyValueStorage worldStateKeyValueStorage,
      final EvmConfiguration evmConfiguration) {
    return createWorldState(archive, worldStateKeyValueStorage, evmConfiguration, Optional.empty());
  }

  @Override
  public PathBasedWorldState createWorldState(
      final PathBasedWorldStateProvider archive,
      final PathBasedWorldStateKeyValueStorage worldStateKeyValueStorage,
      final EvmConfiguration evmConfiguration,
      final Optional<BlockAccessListOverlay> maybeBlockAccessListOverlay) {
    return new BonsaiWorldState(
        (BonsaiWorldStateProvider) archive,
        (BonsaiWorldStateKeyValueStorage) worldStateKeyValueStorage,
        evmConfiguration,
        WorldStateConfig.newBuilder(worldStateConfig).build(),
        codeCache,
        maybeBlockAccessListOverlay);
  }

  @Override
  public PathBasedWorldStateKeyValueStorage createLayeredKeyValueStorage(
      final PathBasedWorldStateKeyValueStorage worldStateKeyValueStorage) {
    return new BonsaiWorldStateLayerStorage(
        (BonsaiWorldStateKeyValueStorage) worldStateKeyValueStorage);
  }

  @Override
  public PathBasedWorldStateKeyValueStorage createSnapshotKeyValueStorage(
      final PathBasedWorldStateKeyValueStorage worldStateKeyValueStorage) {
    return new BonsaiSnapshotWorldStateKeyValueStorage(
        (BonsaiWorldStateKeyValueStorage) worldStateKeyValueStorage);
  }
}
