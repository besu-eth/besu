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

import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiSnapshotWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateLayerStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.WorldStateConfig;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.util.concurrent.ConcurrentHashMap;

public class BonsaiWorldStateCacheManager extends PathBasedWorldStateCacheManager {
  private final BonsaiCodeCache codeCache;

  public BonsaiWorldStateCacheManager(
      final BonsaiWorldStateProvider archive,
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final EvmConfiguration evmConfiguration,
      final WorldStateConfig worldStateConfig,
      final BonsaiCodeCache codeCache) {
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
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final EvmConfiguration evmConfiguration) {
    return new BonsaiWorldState(
        (BonsaiWorldStateProvider) archive,
        worldStateKeyValueStorage,
        evmConfiguration,
        WorldStateConfig.newBuilder(worldStateConfig).build(),
        codeCache);
  }

  @Override
  public BonsaiWorldStateKeyValueStorage createLayeredKeyValueStorage(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage) {
    return new BonsaiWorldStateLayerStorage(worldStateKeyValueStorage);
  }

  @Override
  public BonsaiWorldStateKeyValueStorage createSnapshotKeyValueStorage(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage) {
    return new BonsaiSnapshotWorldStateKeyValueStorage(worldStateKeyValueStorage);
  }
}
