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
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.data.BlockHeader;

import java.util.Optional;
import java.util.function.Function;

public class NoOpBonsaiCachedWorldStorageManager extends BonsaiCachedWorldStorageManager {

  public NoOpBonsaiCachedWorldStorageManager(
      final BonsaiWorldStateKeyValueStorage bonsaiWorldStateKeyValueStorage,
      final EvmConfiguration evmConfiguration,
      final CodeCache codeCache) {
    super(
        null,
        bonsaiWorldStateKeyValueStorage,
        evmConfiguration,
        WorldStateConfig.createStatefulConfigWithTrie(),
        codeCache);
  }

  @Override
  public synchronized void addCachedLayer(
      final BlockHeader blockHeader,
      final Hash worldStateRootHash,
      final PathBasedWorldState forWorldState) {
    // no cache
  }

  @Override
  public boolean contains(final Hash blockHash) {
    return false;
  }

  @Override
  public Optional<PathBasedWorldState> getWorldState(final Hash blockHash) {
    return Optional.empty();
  }

  @Override
  public Optional<PathBasedWorldState> getNearestWorldState(final BlockHeader blockHeader) {
    return Optional.empty();
  }

  @Override
  public Optional<PathBasedWorldState> getHeadWorldState(
      final Function<Hash, Optional<BlockHeader>> hashBlockHeaderFunction) {
    return Optional.empty();
  }

  @Override
  public void reset() {
    // world states are not re-used
  }
}
