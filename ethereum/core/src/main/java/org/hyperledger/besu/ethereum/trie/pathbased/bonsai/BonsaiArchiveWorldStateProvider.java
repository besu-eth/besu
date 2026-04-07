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
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.ethereum.worldstate.PathBasedExtraStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.ServiceManager;

import java.util.Optional;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BonsaiArchiveWorldStateProvider extends BonsaiWorldStateProvider {

  private static final Logger LOG = LoggerFactory.getLogger(BonsaiArchiveWorldStateProvider.class);

  public BonsaiArchiveWorldStateProvider(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Blockchain blockchain,
      final PathBasedExtraStorageConfiguration pathBasedExtraStorageConfiguration,
      final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader,
      final ServiceManager pluginContext,
      final EvmConfiguration evmConfiguration,
      final Supplier<WorldStateHealer> worldStateHealerSupplier,
      final CodeCache codeCache) {
    super(
        worldStateKeyValueStorage,
        blockchain,
        pathBasedExtraStorageConfiguration,
        bonsaiCachedMerkleTrieLoader,
        pluginContext,
        evmConfiguration,
        worldStateHealerSupplier,
        codeCache);
  }

  @Override
  public Optional<MutableWorldState> getWorldState(final WorldStateQueryParams queryParams) {
    // If not in archive mode then the migration is not yet complete, so fallback to
    // the regular BonsaiWorldStateProvider
    if (!worldStateKeyValueStorage.getFlatDbMode().equals(FlatDbMode.ARCHIVE)) {
      return super.getWorldState(queryParams);
    }

    if (queryParams.shouldWorldStateUpdateHead()) {
      return getFullWorldState(queryParams);
    } else {
      // If we are creating a world state for a historic/archive block, we have 2 options:
      // 1. Roll back and create a layered world state. We can do this as far back as 512 blocks by
      // default, and we end up with a full state trie & flat DB at the desired block
      // 2. Rely entirely on the flat DB, which is less safe because we can't check the world state
      // root is correct but at least gives us the ability to serve historic state. The rollback
      // step in this case is minimal - take the chain head state and reset the block hash and
      // number for
      // archive flat DB queries
      final BlockHeader chainHeadBlockHeader = blockchain.getChainHeadHeader();
      if (chainHeadBlockHeader.getNumber() - queryParams.getBlockHeader().getNumber()
          >= trieLogManager.getMaxLayersToLoad()) {
        LOG.debug(
            "Returning archive state without verifying state root {}",
            trieLogManager.getMaxLayersToLoad());
        return cachedWorldStorageManager
            .getWorldState(chainHeadBlockHeader.getHash())
            .map(MutableWorldState::disableTrie)
            .flatMap(
                worldState ->
                    rollMutableArchiveStateToBlockHash( // This is a tiny action for archive
                        // state
                        (PathBasedWorldState) worldState,
                        queryParams.getBlockHeader().getBlockHash()))
            .map(MutableWorldState::freezeStorage);
      }
      return super.getWorldState(queryParams);
    }
  }

  // Archive-specific rollback behaviour. There is no trie-log roll forward/backward, we just roll
  // back the state root, block hash and block number
  protected Optional<MutableWorldState> rollMutableArchiveStateToBlockHash(
      final PathBasedWorldState mutableState, final Hash blockHash) {
    LOG.trace(
        "Rolling mutable archive world state to block hash " + blockHash.getBytes().toHexString());
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
