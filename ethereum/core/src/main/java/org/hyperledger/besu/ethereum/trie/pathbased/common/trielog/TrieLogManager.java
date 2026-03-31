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
package org.hyperledger.besu.ethereum.trie.pathbased.common.trielog;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.trielog.BinTrieTrieLogFactoryImpl;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogFactoryImpl;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.TrieLogService;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.plugin.services.trielogs.TrieLogEvent;
import org.hyperledger.besu.plugin.services.trielogs.TrieLogFactory;
import org.hyperledger.besu.plugin.services.trielogs.TrieLogProvider;
import org.hyperledger.besu.util.Subscribers;

import java.util.List;
import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrieLogManager {
  private static final Logger LOG = LoggerFactory.getLogger(TrieLogManager.class);
  public static final long LOG_RANGE_LIMIT = 1000; // restrict trielog range queries to 1k logs
  protected final Blockchain blockchain;
  protected final PathBasedWorldStateKeyValueStorage rootWorldStateStorage;

  protected final long maxLayersToLoad;
  protected final Subscribers<TrieLogEvent.TrieLogObserver> trieLogObservers = Subscribers.create();

  protected final TrieLogFactory trieLogFactory;

  public TrieLogManager(
      final Blockchain blockchain,
      final PathBasedWorldStateKeyValueStorage worldStateKeyValueStorage,
      final long maxLayersToLoad,
      final ServiceManager pluginContext) {
    this.blockchain = blockchain;
    this.rootWorldStateStorage = worldStateKeyValueStorage;
    this.maxLayersToLoad = maxLayersToLoad;
    this.trieLogFactory =
        worldStateKeyValueStorage == null
            ? new TrieLogFactoryImpl()
            : setupTrieLogFactory(worldStateKeyValueStorage.getDataStorageFormat(), pluginContext);
  }

  public synchronized void saveTrieLog(
      final PathBasedWorldStateUpdateAccumulator<?> localUpdater,
      final Hash forWorldStateRootHash,
      final BlockHeader forBlockHeader,
      final PathBasedWorldState forWorldState) {
    // do not overwrite a trielog layer that already exists in the database.
    // if it's only in memory we need to save it
    // for example, in case of reorg we don't replace a trielog layer
    if (rootWorldStateStorage.getTrieLog(forBlockHeader.getBlockHash()).isEmpty()) {
      final PathBasedWorldStateKeyValueStorage.Updater stateUpdater =
          forWorldState.getWorldStateStorage().updater();
      boolean success = false;
      try {
        final TrieLog trieLog = prepareTrieLog(forBlockHeader, localUpdater);
        persistTrieLog(forBlockHeader, forWorldStateRootHash, trieLog, stateUpdater);

        // notify trie log added observers, synchronously
        trieLogObservers.forEach(o -> o.onTrieLogAdded(new TrieLogAddedEvent(trieLog)));

        success = true;
      } finally {
        if (success) {
          stateUpdater.commitTrieLogOnly();
        } else {
          stateUpdater.rollback();
        }
      }
    }
  }

  public TrieLogFactory getTrieLogFactory() {
    return trieLogFactory;
  }

  private TrieLog prepareTrieLog(
      final BlockHeader blockHeader, final PathBasedWorldStateUpdateAccumulator<?> localUpdater) {
    LOG.atDebug()
        .setMessage("Adding layered world state for {}")
        .addArgument(blockHeader::toLogString)
        .log();
    final TrieLog trieLog = trieLogFactory.create(localUpdater, blockHeader);
    trieLog.freeze();
    return trieLog;
  }

  private void persistTrieLog(
      final BlockHeader blockHeader,
      final Hash worldStateRootHash,
      final TrieLog trieLog,
      final PathBasedWorldStateKeyValueStorage.Updater stateUpdater) {
    LOG.atDebug()
        .setMessage("Persisting trie log for block hash {} and world state root {}")
        .addArgument(blockHeader::toLogString)
        .addArgument(() -> worldStateRootHash.getBytes().toHexString())
        .log();

    stateUpdater
        .getTrieLogStorageTransaction()
        .put(
            blockHeader.getBlockHash().getBytes().toArrayUnsafe(),
            trieLogFactory.serialize(trieLog));
  }

  public long getMaxLayersToLoad() {
    return maxLayersToLoad;
  }

  public Optional<TrieLog> getTrieLogLayer(final Hash blockHash) {
    return rootWorldStateStorage.getTrieLog(blockHash).map(trieLogFactory::deserialize);
  }

  public synchronized long subscribe(final TrieLogEvent.TrieLogObserver sub) {
    return trieLogObservers.subscribe(sub);
  }

  public synchronized void unsubscribe(final long id) {
    trieLogObservers.unsubscribe(id);
  }

  private TrieLogFactory setupTrieLogFactory(
      final DataStorageFormat dataStorageFormat, final ServiceManager pluginContext) {
    // if we have a TrieLogService from pluginContext, use it.
    var trieLogServicez =
        Optional.ofNullable(pluginContext)
            .flatMap(context -> context.getService(TrieLogService.class));

    if (trieLogServicez.isPresent()) {
      var trieLogService = trieLogServicez.get();
      // push the TrieLogProvider into the TrieLogService
      trieLogService.configureTrieLogProvider(getTrieLogProvider());

      // configure plugin observers:
      trieLogService.getObservers().forEach(trieLogObservers::subscribe);

      // return the TrieLogFactory implementation from the TrieLogService
      if (trieLogService.getTrieLogFactory().isPresent()) {
        return trieLogService.getTrieLogFactory().get();
      }
    }
    // Choose factory based on data storage format
    if (dataStorageFormat.isBinTrieFormat()) {
      return new BinTrieTrieLogFactoryImpl();
    }
    // Default to Bonsai TrieLogFactoryImpl
    return new TrieLogFactoryImpl();
  }

  private TrieLogProvider getTrieLogProvider() {
    return new TrieLogProvider() {
      @Override
      public Optional<Bytes> getRawTrieLogLayer(final Hash blockHash) {
        return rootWorldStateStorage.getTrieLog(blockHash).map(Bytes::wrap);
      }

      @Override
      public Optional<Bytes> getRawTrieLogLayer(final long blockNumber) {
        return TrieLogManager.this
            .blockchain
            .getBlockHeader(blockNumber)
            .map(BlockHeader::getBlockHash)
            .flatMap(this::getRawTrieLogLayer);
      }

      @Override
      public void saveRawTrieLogLayer(
          final Hash blockHash, final long blockNumber, final Bytes trieLog) {
        final PathBasedWorldStateKeyValueStorage.Updater updater = rootWorldStateStorage.updater();
        updater
            .getTrieLogStorageTransaction()
            .put(blockHash.getBytes().toArrayUnsafe(), trieLog.toArrayUnsafe());
        updater.commitTrieLogOnly();
        // TODO maybe find a way to have a clean and complete trielog for observers
        trieLogObservers.forEach(
            o ->
                o.onTrieLogAdded(
                    new TrieLogAddedEvent(
                        new TrieLogLayer().setBlockHash(blockHash).setBlockNumber(blockNumber))));
      }

      @Override
      public Optional<TrieLog> getTrieLogLayer(final Hash blockHash) {
        return TrieLogManager.this.getTrieLogLayer(blockHash);
      }

      @Override
      public Optional<TrieLog> getTrieLogLayer(final long blockNumber) {
        return TrieLogManager.this
            .blockchain
            .getBlockHeader(blockNumber)
            .map(BlockHeader::getBlockHash)
            .flatMap(TrieLogManager.this::getTrieLogLayer);
      }

      @Override
      public List<TrieLogRangeTuple> getTrieLogsByRange(
          final long fromBlockNumber, final long toBlockNumber) {
        return rangeAsStream(fromBlockNumber, toBlockNumber)
            .map(blockchain::getBlockHeader)
            .map(
                headerOpt ->
                    headerOpt.flatMap(
                        header ->
                            TrieLogManager.this
                                .getTrieLogLayer(header.getBlockHash())
                                .map(
                                    layer ->
                                        new TrieLogRangeTuple(
                                            header.getBlockHash(), header.getNumber(), layer))))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .toList();
      }

      Stream<Long> rangeAsStream(final long fromBlockNumber, final long toBlockNumber) {
        if (Math.abs(toBlockNumber - fromBlockNumber) > LOG_RANGE_LIMIT) {
          throw new IllegalArgumentException("Requested Range too large");
        }
        long left = Math.min(fromBlockNumber, toBlockNumber);
        long right = Math.max(fromBlockNumber, toBlockNumber);
        return LongStream.range(left, right).boxed();
      }
    };
  }
}
