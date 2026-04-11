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
package org.hyperledger.besu.cli.subcommands.storage;

import static com.google.common.base.Preconditions.checkArgument;
import static org.hyperledger.besu.cli.options.storage.PathBasedExtraStorageOptions.MAX_LAYERS_TO_LOAD;
import static org.hyperledger.besu.cli.options.storage.PathBasedExtraStorageOptions.TRIE_LOG_PRUNING_WINDOW_SIZE;
import static org.hyperledger.besu.controller.BesuController.DATABASE_PATH;
import static org.hyperledger.besu.ethereum.worldstate.PathBasedExtraStorageConfiguration.DEFAULT_TRIE_LOG_PRUNING_WINDOW_SIZE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogFactoryImpl;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.PathBasedExtraStorageConfiguration;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helper class for counting and pruning trie logs */
public class TrieLogHelper {
  private static final String TRIE_LOG_FILE = "trieLogsToRetain";
  private static final long BATCH_SIZE = 20_000;
  private static final int ROCKSDB_MAX_INSERTS_PER_TRANSACTION = 1000;
  private static final Logger LOG = LoggerFactory.getLogger(TrieLogHelper.class);

  /** Default Constructor. */
  public TrieLogHelper() {}

  boolean prune(
      final DataStorageConfiguration config,
      final PathBasedWorldStateKeyValueStorage rootWorldStateStorage,
      final MutableBlockchain blockchain,
      final Path dataDirectoryPath) {

    final String batchFileNameBase =
        dataDirectoryPath.resolve(DATABASE_PATH).resolve(TRIE_LOG_FILE).toString();

    validatePruneConfiguration(config);

    final long layersToRetain = config.getPathBasedExtraStorageConfiguration().getMaxLayersToLoad();

    final long chainHeight = blockchain.getChainHeadBlockNumber();

    final long lastBlockNumberToRetainTrieLogsFor = chainHeight - layersToRetain + 1;

    if (!validatePruneRequirements(
        blockchain,
        chainHeight,
        lastBlockNumberToRetainTrieLogsFor,
        rootWorldStateStorage,
        layersToRetain)) {
      return false;
    }

    final long numberOfBatches = calculateNumberOfBatches(layersToRetain);
    LOG.info("Retain {} trie logs, processing in {} batches", layersToRetain, numberOfBatches);

    processTrieLogBatches(
        rootWorldStateStorage,
        blockchain,
        chainHeight,
        lastBlockNumberToRetainTrieLogsFor,
        numberOfBatches,
        batchFileNameBase);

    // Should only be layersToRetain left but loading extra just in case of an unforeseen bug
    final long countAfterPrune =
        rootWorldStateStorage
            .streamTrieLogKeys(layersToRetain + DEFAULT_TRIE_LOG_PRUNING_WINDOW_SIZE)
            .count();
    if (countAfterPrune == layersToRetain) {
      if (deleteFiles(batchFileNameBase, numberOfBatches)) {
        return true;
      } else {
        throw new IllegalStateException(
            "There was an error deleting the trie log backup files. Please ensure besu is working before deleting them manually.");
      }
    } else {
      throw new IllegalStateException(
          String.format(
              "Remaining trie logs (%d) did not match %s (%d). Trie logs backup files (in %s) have not been deleted, it is safe to rerun the subcommand.",
              countAfterPrune, MAX_LAYERS_TO_LOAD, layersToRetain, batchFileNameBase));
    }
  }

  private void processTrieLogBatches(
      final PathBasedWorldStateKeyValueStorage rootWorldStateStorage,
      final MutableBlockchain blockchain,
      final long chainHeight,
      final long lastBlockNumberToRetainTrieLogsFor,
      final long numberOfBatches,
      final String batchFileNameBase) {

    for (long batchNumber = 1; batchNumber <= numberOfBatches; batchNumber++) {
      final String batchFileName = batchFileNameBase + "-" + batchNumber;
      final long firstBlockOfBatch = chainHeight - ((batchNumber - 1) * BATCH_SIZE);
      final long lastBlockOfBatch =
          Math.max(chainHeight - (batchNumber * BATCH_SIZE), lastBlockNumberToRetainTrieLogsFor);
      final List<Hash> trieLogKeys =
          getTrieLogKeysForBlocks(blockchain, firstBlockOfBatch, lastBlockOfBatch);

      LOG.info("Saving trie logs to retain in file {} (batch {})...", batchFileName, batchNumber);
      saveTrieLogBatches(batchFileName, rootWorldStateStorage, trieLogKeys);
    }

    LOG.info("Clear trie logs...");
    rootWorldStateStorage.clearTrieLog();

    for (long batchNumber = 1; batchNumber <= numberOfBatches; batchNumber++) {
      restoreTrieLogBatches(rootWorldStateStorage, batchNumber, batchFileNameBase);
    }
  }

  private void saveTrieLogBatches(
      final String batchFileName,
      final PathBasedWorldStateKeyValueStorage rootWorldStateStorage,
      final List<Hash> trieLogKeys) {

    saveTrieLogsAsRlpInFile(trieLogKeys, rootWorldStateStorage, batchFileName);
  }

  private void restoreTrieLogBatches(
      final PathBasedWorldStateKeyValueStorage rootWorldStateStorage,
      final long batchNumber,
      final String batchFileNameBase) {

    LOG.info("Restoring trie logs retained from batch {}...", batchNumber);
    recreateTrieLogs(rootWorldStateStorage, batchNumber, batchFileNameBase);
  }

  private boolean deleteFiles(final String batchFileNameBase, final long numberOfBatches) {

    LOG.info("Deleting files...");

    try {
      for (long batchNumber = 1; batchNumber <= numberOfBatches; batchNumber++) {
        File file = new File(batchFileNameBase + "-" + batchNumber);
        if (file.exists()) {
          file.delete();
        }
      }
      return true;
    } catch (Exception e) {
      LOG.error("Error deleting files", e);
      return false;
    }
  }

  private List<Hash> getTrieLogKeysForBlocks(
      final MutableBlockchain blockchain,
      final long firstBlockOfBatch,
      final long lastBlockOfBatch) {
    final List<Hash> trieLogKeys = new ArrayList<>();
    for (long i = firstBlockOfBatch; i >= lastBlockOfBatch; i--) {
      final Optional<BlockHeader> header = blockchain.getBlockHeader(i);
      header.ifPresentOrElse(
          blockHeader -> trieLogKeys.add(blockHeader.getHash()),
          () -> LOG.error("Error retrieving block"));
    }
    return trieLogKeys;
  }

  private long calculateNumberOfBatches(final long layersToRetain) {
    return layersToRetain / BATCH_SIZE + ((layersToRetain % BATCH_SIZE == 0) ? 0 : 1);
  }

  private boolean validatePruneRequirements(
      final MutableBlockchain blockchain,
      final long chainHeight,
      final long lastBlockNumberToRetainTrieLogsFor,
      final PathBasedWorldStateKeyValueStorage rootWorldStateStorage,
      final long layersToRetain) {

    if (lastBlockNumberToRetainTrieLogsFor < 0) {
      throw new IllegalArgumentException(
          "Trying to retain more trie logs than chain length ("
              + chainHeight
              + "), skipping pruning");
    }

    // Need to ensure we're loading at least layersToRetain if they exist
    // plus extra threshold to account forks and orphans
    final long clampedCountBeforePruning =
        rootWorldStateStorage
            .streamTrieLogKeys(layersToRetain + DEFAULT_TRIE_LOG_PRUNING_WINDOW_SIZE)
            .count();
    if (clampedCountBeforePruning < layersToRetain) {
      throw new IllegalArgumentException(
          String.format(
              "Trie log count (%d) is less than retention limit (%d), skipping pruning",
              clampedCountBeforePruning, layersToRetain));
    }

    final Optional<Hash> finalizedBlockHash = blockchain.getFinalized();

    if (finalizedBlockHash.isEmpty()) {
      throw new RuntimeException("No finalized block present, can't safely run trie log prune");
    } else {
      final Hash finalizedHash = finalizedBlockHash.get();
      final Optional<BlockHeader> finalizedBlockHeader = blockchain.getBlockHeader(finalizedHash);
      if (finalizedBlockHeader.isPresent()
          && finalizedBlockHeader.get().getNumber() < lastBlockNumberToRetainTrieLogsFor) {
        throw new IllegalArgumentException(
            "Trying to prune more layers than the finalized block height, skipping pruning");
      }
    }
    return true;
  }

  private void recreateTrieLogs(
      final PathBasedWorldStateKeyValueStorage rootWorldStateStorage,
      final long batchNumber,
      final String batchFileNameBase) {
    // process in chunk to avoid OOM
    final String batchFileName = batchFileNameBase + "-" + batchNumber;
    IdentityHashMap<byte[], byte[]> trieLogsToRetain = readTrieLogsAsRlpFromFile(batchFileName);
    final int chunkSize = ROCKSDB_MAX_INSERTS_PER_TRANSACTION;
    List<byte[]> keys = new ArrayList<>(trieLogsToRetain.keySet());

    for (int startIndex = 0; startIndex < keys.size(); startIndex += chunkSize) {
      processTransactionChunk(startIndex, chunkSize, keys, trieLogsToRetain, rootWorldStateStorage);
    }
  }

  private void processTransactionChunk(
      final int startIndex,
      final int chunkSize,
      final List<byte[]> keys,
      final IdentityHashMap<byte[], byte[]> trieLogsToRetain,
      final PathBasedWorldStateKeyValueStorage rootWorldStateStorage) {

    var updater = rootWorldStateStorage.updater();
    int endIndex = Math.min(startIndex + chunkSize, keys.size());

    for (int i = startIndex; i < endIndex; i++) {
      byte[] key = keys.get(i);
      byte[] value = trieLogsToRetain.get(key);
      updater.getTrieLogStorageTransaction().put(key, value);
      LOG.info("Key({}): {}", i, Bytes32.wrap(key).toShortHexString());
    }

    updater.getTrieLogStorageTransaction().commit();
  }

  @VisibleForTesting
  void validatePruneConfiguration(final DataStorageConfiguration config) {
    final PathBasedExtraStorageConfiguration subStorageConfiguration =
        config.getPathBasedExtraStorageConfiguration();
    checkArgument(
        subStorageConfiguration.getMaxLayersToLoad()
            >= PathBasedExtraStorageConfiguration.MINIMUM_TRIE_LOG_RETENTION_LIMIT,
        String.format(
            MAX_LAYERS_TO_LOAD + " minimum value is %d",
            PathBasedExtraStorageConfiguration.MINIMUM_TRIE_LOG_RETENTION_LIMIT));
    checkArgument(
        subStorageConfiguration.getTrieLogPruningWindowSize() > 0,
        String.format(
            TRIE_LOG_PRUNING_WINDOW_SIZE + "=%d must be greater than 0",
            subStorageConfiguration.getTrieLogPruningWindowSize()));
    checkArgument(
        subStorageConfiguration.getTrieLogPruningWindowSize()
            > subStorageConfiguration.getMaxLayersToLoad(),
        String.format(
            TRIE_LOG_PRUNING_WINDOW_SIZE + "=%d must be greater than " + MAX_LAYERS_TO_LOAD + "=%d",
            subStorageConfiguration.getTrieLogPruningWindowSize(),
            subStorageConfiguration.getMaxLayersToLoad()));
  }

  private void saveTrieLogsAsRlpInFile(
      final List<Hash> trieLogsKeys,
      final PathBasedWorldStateKeyValueStorage rootWorldStateStorage,
      final String batchFileName) {
    File file = new File(batchFileName);
    if (file.exists()) {
      LOG.warn("File already exists {}, skipping file creation", batchFileName);
      return;
    }

    final IdentityHashMap<byte[], byte[]> trieLogs =
        getTrieLogs(trieLogsKeys, rootWorldStateStorage);
    final Bytes rlp =
        RLP.encode(
            o ->
                o.writeList(
                    trieLogs.entrySet(), (val, out) -> out.writeRaw(Bytes.wrap(val.getValue()))));
    try {
      Files.write(file.toPath(), rlp.toArrayUnsafe());
    } catch (IOException e) {
      LOG.error(e.getMessage());
      throw new RuntimeException(e);
    }
  }

  IdentityHashMap<byte[], byte[]> readTrieLogsAsRlpFromFile(final String batchFileName) {
    try {
      final Bytes file = Bytes.wrap(Files.readAllBytes(Path.of(batchFileName)));
      final BytesValueRLPInput input = new BytesValueRLPInput(file, false);

      input.enterList();
      final IdentityHashMap<byte[], byte[]> trieLogs = new IdentityHashMap<>();
      while (!input.isEndOfCurrentList()) {
        final Bytes trieLogBytes = input.currentListAsBytes();
        TrieLogLayer trieLogLayer =
            TrieLogFactoryImpl.readFrom(new BytesValueRLPInput(Bytes.wrap(trieLogBytes), false));
        trieLogs.put(
            trieLogLayer.getBlockHash().getBytes().toArrayUnsafe(), trieLogBytes.toArrayUnsafe());
      }
      input.leaveList();

      return trieLogs;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private IdentityHashMap<byte[], byte[]> getTrieLogs(
      final List<Hash> trieLogKeys,
      final PathBasedWorldStateKeyValueStorage rootWorldStateStorage) {
    IdentityHashMap<byte[], byte[]> trieLogsToRetain = new IdentityHashMap<>();

    LOG.info("Obtaining trielogs from db, this may take a few minutes...");
    trieLogKeys.forEach(
        hash ->
            rootWorldStateStorage
                .getTrieLog(hash)
                .ifPresent(
                    trieLog -> trieLogsToRetain.put(hash.getBytes().toArrayUnsafe(), trieLog)));
    return trieLogsToRetain;
  }

  TrieLogCount getCount(
      final PathBasedWorldStateKeyValueStorage rootWorldStateStorage,
      final int limit,
      final Blockchain blockchain) {
    final AtomicInteger total = new AtomicInteger();
    final AtomicInteger canonicalCount = new AtomicInteger();
    final AtomicInteger forkCount = new AtomicInteger();
    final AtomicInteger orphanCount = new AtomicInteger();
    rootWorldStateStorage
        .streamTrieLogKeys(limit)
        .map(Bytes32::wrap)
        .map(Hash::wrap)
        .forEach(
            hash -> {
              total.getAndIncrement();
              blockchain
                  .getBlockHeader(hash)
                  .ifPresentOrElse(
                      (header) -> {
                        long number = header.getNumber();
                        final Optional<BlockHeader> headerByNumber =
                            blockchain.getBlockHeader(number);
                        if (headerByNumber.isPresent()
                            && headerByNumber.get().getHash().equals(hash)) {
                          canonicalCount.getAndIncrement();
                        } else {
                          forkCount.getAndIncrement();
                        }
                      },
                      orphanCount::getAndIncrement);
            });

    return new TrieLogCount(total.get(), canonicalCount.get(), forkCount.get(), orphanCount.get());
  }

  void printCount(final PrintWriter out, final TrieLogCount count) {
    out.printf(
        "trieLog count: %s\n - canonical count: %s\n - fork count: %s\n - orphaned count: %s\n",
        count.total, count.canonicalCount, count.forkCount, count.orphanCount);
  }

  void importTrieLog(
      final PathBasedWorldStateKeyValueStorage rootWorldStateStorage, final Path trieLogFilePath) {

    var trieLog = readTrieLogsAsRlpFromFile(trieLogFilePath.toString());

    var updater = rootWorldStateStorage.updater();
    trieLog.forEach((key, value) -> updater.getTrieLogStorageTransaction().put(key, value));
    updater.getTrieLogStorageTransaction().commit();
  }

  void exportTrieLog(
      final PathBasedWorldStateKeyValueStorage rootWorldStateStorage,
      final List<Hash> trieLogHash,
      final Path directoryPath)
      throws IOException {
    final String trieLogFile = directoryPath.toString();

    saveTrieLogsAsRlpInFile(trieLogHash, rootWorldStateStorage, trieLogFile);
  }

  record TrieLogCount(int total, int canonicalCount, int forkCount, int orphanCount) {}
}
