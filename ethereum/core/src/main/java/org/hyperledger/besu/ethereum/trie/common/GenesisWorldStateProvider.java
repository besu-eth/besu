/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.trie.common;

import static org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig.createStatefulConfigWithTrie;

import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.WorldStatePreimageKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.forest.worldview.ForestMutableWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.NoOpBonsaiCachedWorldStorageManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.NoOpTrieLogManager;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.Objects;

public class GenesisWorldStateProvider {

  /**
   * Creates a Genesis world state based on the provided data storage format.
   *
   * @param dataStorageConfiguration the data storage configuration to use
   * @return a mutable world state for the Genesis block
   */
  public static MutableWorldState createGenesisWorldState(
      final DataStorageConfiguration dataStorageConfiguration, final CodeCache codeCache) {

    if (Objects.requireNonNull(dataStorageConfiguration).getDataStorageFormat()
        == DataStorageFormat.BONSAI) {
      return createGenesisBonsaiWorldState(
          DataStorageConfiguration.DEFAULT_BONSAI_CONFIG, codeCache);
    } else if (Objects.requireNonNull(dataStorageConfiguration).getDataStorageFormat()
        == DataStorageFormat.X_BONSAI_ARCHIVE) {
      return createGenesisBonsaiWorldState(
          DataStorageConfiguration.DEFAULT_BONSAI_ARCHIVE_CONFIG, codeCache);
    } else {
      return createGenesisForestWorldState();
    }
  }

  /**
   * Creates a Genesis world state using the Bonsai data storage format.
   *
   * @return a mutable world state for the Genesis block
   */
  private static MutableWorldState createGenesisBonsaiWorldState(
      final DataStorageConfiguration storageConfiguration, final CodeCache codeCache) {
    final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader =
        new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem());
    final BonsaiWorldStateKeyValueStorage bonsaiWorldStateKeyValueStorage =
        new BonsaiWorldStateKeyValueStorage(
            new KeyValueStorageProvider(
                segmentIdentifiers -> new SegmentedInMemoryKeyValueStorage(),
                () -> new InMemoryKeyValueStorage(),
                new NoOpMetricsSystem()),
            new NoOpMetricsSystem(),
            storageConfiguration);
    return new BonsaiWorldState(
        bonsaiWorldStateKeyValueStorage,
        bonsaiCachedMerkleTrieLoader,
        new NoOpBonsaiCachedWorldStorageManager(
            bonsaiWorldStateKeyValueStorage, EvmConfiguration.DEFAULT, codeCache),
        new NoOpTrieLogManager(),
        EvmConfiguration.DEFAULT,
        createStatefulConfigWithTrie(),
        codeCache);
  }

  /**
   * Creates a Genesis world state using the Forest data storage format.
   *
   * @return a mutable world state for the Genesis block
   */
  private static MutableWorldState createGenesisForestWorldState() {
    final ForestWorldStateKeyValueStorage stateStorage =
        new ForestWorldStateKeyValueStorage(new InMemoryKeyValueStorage());
    final WorldStatePreimageKeyValueStorage preimageStorage =
        new WorldStatePreimageKeyValueStorage(new InMemoryKeyValueStorage());
    return new ForestMutableWorldState(stateStorage, preimageStorage, EvmConfiguration.DEFAULT);
  }
}
