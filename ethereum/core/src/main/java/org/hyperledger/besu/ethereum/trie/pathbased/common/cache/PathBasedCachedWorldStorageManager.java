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
package org.hyperledger.besu.ethereum.trie.pathbased.common.cache;

import static org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.StorageSubscriber;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedLayeredWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.data.BlockHeader;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PathBasedCachedWorldStorageManager implements StorageSubscriber {
  public static final long RETAINED_LAYERS = 512; // at least 256 + typical rollbacks
  private static final Logger LOG =
      LoggerFactory.getLogger(PathBasedCachedWorldStorageManager.class);
  private final PathBasedWorldStateProvider archive;
  private final EvmConfiguration evmConfiguration;
  protected final WorldStateConfig worldStateConfig;
  private final Cache<Hash, BlockHeader> stateRootToBlockHeaderCache =
      Caffeine.newBuilder()
          .maximumSize(RETAINED_LAYERS)
          .expireAfterWrite(100, TimeUnit.MINUTES)
          .build();

  private final PathBasedWorldStateKeyValueStorage rootWorldStateStorage;
  private final Map<Hash, PathBasedCachedWorldView> cachedWorldStatesByHash;

  protected PathBasedCachedWorldStorageManager(
      final PathBasedWorldStateProvider archive,
      final PathBasedWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Map<Hash, PathBasedCachedWorldView> cachedWorldStatesByHash,
      final EvmConfiguration evmConfiguration,
      final WorldStateConfig worldStateConfig) {
    worldStateKeyValueStorage.subscribe(this);
    this.rootWorldStateStorage = worldStateKeyValueStorage;
    this.cachedWorldStatesByHash = cachedWorldStatesByHash;
    this.archive = archive;
    this.evmConfiguration = evmConfiguration;
    this.worldStateConfig = worldStateConfig;
  }

  public synchronized void addCachedLayer(
      final BlockHeader blockHeader,
      final Hash worldStateRootHash,
      final PathBasedWorldState forWorldState) {
    final Optional<PathBasedCachedWorldView> cachedPathBasedWorldView =
        Optional.ofNullable(this.cachedWorldStatesByHash.get(blockHeader.getBlockHash()));
    if (cachedPathBasedWorldView.isPresent()) {
      // only replace if it is a layered storage
      if (forWorldState.isModifyingHeadWorldState()
          && cachedPathBasedWorldView.get().getWorldStateStorage()
              instanceof PathBasedLayeredWorldStateKeyValueStorage) {
        LOG.atDebug()
            .setMessage("updating layered world state for block {}, state root hash {}")
            .addArgument(blockHeader::toLogString)
            .addArgument(() -> worldStateRootHash.getBytes().toShortHexString())
            .log();
        cachedPathBasedWorldView
            .get()
            .updateWorldStateStorage(
                createSnapshotKeyValueStorage(forWorldState.getWorldStateStorage()));
      }
    } else {
      LOG.atDebug()
          .setMessage("adding layered world state for block {}, state root hash {}")
          .addArgument(blockHeader::toLogString)
          .addArgument(() -> worldStateRootHash.getBytes().toShortHexString())
          .log();
      if (forWorldState.isModifyingHeadWorldState()) {
        cachedWorldStatesByHash.put(
            blockHeader.getBlockHash(),
            new PathBasedCachedWorldView(
                blockHeader, createSnapshotKeyValueStorage(forWorldState.getWorldStateStorage())));
      } else {
        // otherwise, add the layer to the cache
        cachedWorldStatesByHash.put(
            blockHeader.getBlockHash(),
            new PathBasedCachedWorldView(
                blockHeader,
                ((PathBasedLayeredWorldStateKeyValueStorage) forWorldState.getWorldStateStorage())
                    .clone()));
      }
      // add stateroot -> blockHeader cache entry
      stateRootToBlockHeaderCache.put(blockHeader.getStateRoot(), blockHeader);
    }
    scrubCachedLayers(blockHeader.getNumber());
  }

  private synchronized void scrubCachedLayers(final long newMaxHeight) {
    if (cachedWorldStatesByHash.size() > RETAINED_LAYERS) {
      final long waterline = newMaxHeight - RETAINED_LAYERS;
      cachedWorldStatesByHash.values().stream()
          .filter(layer -> layer.getBlockNumber() < waterline)
          .toList()
          .forEach(
              layer -> {
                cachedWorldStatesByHash.remove(layer.getBlockHash());
                layer.close();
              });
    }
  }

  public Optional<PathBasedWorldState> getWorldState(final Hash blockHash) {
    if (cachedWorldStatesByHash.containsKey(blockHash)) {
      // return a new worldstate using worldstate storage and an isolated copy of the updater
      return Optional.ofNullable(cachedWorldStatesByHash.get(blockHash))
          .map(
              cached ->
                  createWorldState(
                      archive,
                      createLayeredKeyValueStorage(cached.getWorldStateStorage()),
                      evmConfiguration));
    }
    LOG.atDebug()
        .setMessage("did not find worldstate in cache for {}")
        .addArgument(blockHash.getBytes().toShortHexString())
        .log();

    return Optional.empty();
  }

  public Optional<PathBasedWorldState> getNearestWorldState(final BlockHeader blockHeader) {
    LOG.atDebug()
        .setMessage("getting nearest worldstate for {}")
        .addArgument(blockHeader::toLogString)
        .log();

    return Optional.ofNullable(
            cachedWorldStatesByHash.get(blockHeader.getParentHash())) // search parent block
        .map(PathBasedCachedWorldView::getWorldStateStorage)
        .or(
            () -> {
              // or else search the nearest state in the cache
              LOG.atDebug()
                  .setMessage("searching cache for nearest worldstate for {}")
                  .addArgument(blockHeader::toLogString)
                  .log();

              final List<PathBasedCachedWorldView> cachedPathBasedWorldViews =
                  new ArrayList<>(cachedWorldStatesByHash.values());
              return cachedPathBasedWorldViews.stream()
                  .sorted(
                      Comparator.comparingLong(
                          view -> Math.abs(blockHeader.getNumber() - view.getBlockNumber())))
                  .map(PathBasedCachedWorldView::getWorldStateStorage)
                  .findFirst();
            })
        .map(
            storage ->
                createWorldState( // wrap the state in a layered worldstate
                    archive, createLayeredKeyValueStorage(storage), evmConfiguration));
  }

  public Optional<PathBasedWorldState> getHeadWorldState(
      final Function<Hash, Optional<BlockHeader>> hashBlockHeaderFunction) {

    LOG.atDebug().setMessage("getting head worldstate").log();

    return rootWorldStateStorage
        .getWorldStateBlockHash()
        .flatMap(hashBlockHeaderFunction)
        .flatMap(
            blockHeader -> {
              // add the head to the cache
              addCachedLayer(
                  blockHeader,
                  blockHeader.getStateRoot(),
                  createWorldState(archive, rootWorldStateStorage, evmConfiguration));
              return getWorldState(blockHeader.getBlockHash());
            });
  }

  public boolean contains(final Hash blockHash) {
    return cachedWorldStatesByHash.containsKey(blockHash);
  }

  public void reset() {
    this.cachedWorldStatesByHash.clear();
  }

  public void primeRootToBlockHashCache(final Blockchain blockchain, final int numEntries) {
    // prime the stateroot-to-blockhash cache
    long head = blockchain.getChainHeadHeader().getNumber();
    for (long i = head; i > Math.max(0, head - numEntries); i--) {
      blockchain
          .getBlockHeader(i)
          .ifPresent(header -> stateRootToBlockHeaderCache.put(header.getStateRoot(), header));
    }
  }

  /**
   * Returns the worldstate for the supplied root hash. If the worldstate is not already in cache,
   * this method will attempt to fetch it and add it to the cache. synchronized to prevent
   * concurrent loads/adds to the cache of the same root hash.
   *
   * @param rootHash rootHash to supply worldstate storage for
   * @return Optional worldstate storage
   */
  public synchronized Optional<PathBasedWorldStateKeyValueStorage> getStorageByRootHash(
      final Hash rootHash) {
    return Optional.ofNullable(stateRootToBlockHeaderCache.getIfPresent(rootHash))
        .flatMap(
            header ->
                Optional.ofNullable(cachedWorldStatesByHash.get(header.getBlockHash()))
                    .map(PathBasedCachedWorldView::getWorldStateStorage)
                    .or(
                        () -> {
                          // if not cached already, maybe fetch and cache this worldstate
                          var maybeWorldState =
                              archive
                                  .getWorldState(withBlockHeaderAndNoUpdateNodeHead(header))
                                  .map(BonsaiWorldState.class::cast);
                          maybeWorldState.ifPresent(
                              ws -> addCachedLayer(header, header.getStateRoot(), ws));
                          return maybeWorldState.map(BonsaiWorldState::getWorldStateStorage);
                        }));
  }

  @Override
  public void onClearStorage() {
    this.cachedWorldStatesByHash.clear();
  }

  @Override
  public void onClearFlatDatabaseStorage() {
    this.cachedWorldStatesByHash.clear();
  }

  @Override
  public void onClearTrieLog() {
    this.cachedWorldStatesByHash.clear();
  }

  @Override
  public void onClearTrie() {
    this.cachedWorldStatesByHash.clear();
  }

  @Override
  public void onCloseStorage() {
    this.cachedWorldStatesByHash.clear();
  }

  public abstract PathBasedWorldState createWorldState(
      final PathBasedWorldStateProvider archive,
      final PathBasedWorldStateKeyValueStorage worldStateKeyValueStorage,
      final EvmConfiguration evmConfiguration);

  public abstract PathBasedWorldStateKeyValueStorage createLayeredKeyValueStorage(
      final PathBasedWorldStateKeyValueStorage worldStateKeyValueStorage);

  public abstract PathBasedWorldStateKeyValueStorage createSnapshotKeyValueStorage(
      final PathBasedWorldStateKeyValueStorage worldStateKeyValueStorage);
}
