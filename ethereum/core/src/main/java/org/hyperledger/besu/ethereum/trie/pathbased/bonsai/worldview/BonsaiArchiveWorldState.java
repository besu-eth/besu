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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview;

import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiArchiveWorldStateLayerStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

/**
 * Bonsai world state used to serve archive historical queries. It behaves like {@link
 * BonsaiWorldState} except that when frozen it wraps its storage in a {@link
 * BonsaiArchiveWorldStateLayerStorage} so flat reads use the historical block context held in the
 * layer (written by {@code persist(blockHeader)}), rather than the live HEAD context that the
 * default {@code BonsaiWorldStateLayerStorage} would resolve from the underlying RocksDB.
 */
public class BonsaiArchiveWorldState extends BonsaiWorldState {

  public BonsaiArchiveWorldState(
      final BonsaiWorldStateProvider archive,
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final EvmConfiguration evmConfiguration,
      final WorldStateConfig worldStateConfig,
      final CodeCache codeCache) {
    super(archive, worldStateKeyValueStorage, evmConfiguration, worldStateConfig, codeCache);
  }

  @Override
  public MutableWorldState freezeStorage() {
    this.isStorageFrozen = true;
    this.worldStateKeyValueStorage =
        new BonsaiArchiveWorldStateLayerStorage(getWorldStateStorage());
    return this;
  }
}
