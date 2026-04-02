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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsaiarchive;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_ARCHIVE;

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveFlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.util.log.LogUtil;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Migrates a Bonsai flat DB node to Bonsai archive format without requiring a full resync.
 *
 * <p>Migration replays trie logs sequentially from block 0 (or the last saved checkpoint) to the
 * current chain head, writing archive-keyed entries into the archive column families. Progress is
 * persisted atomically with each block's data so the migration can safely resume after a restart.
 *
 * <p>The chain head target is updated in real time as new blocks arrive, so the migrator chases the
 * head until it converges. Once all blocks are processed, the flat DB mode is atomically switched
 * to {@link org.hyperledger.besu.ethereum.worldstate.FlatDbMode#ARCHIVE}.
 */
public class BonsaiFlatDbToArchiveMigrator implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(BonsaiFlatDbToArchiveMigrator.class);
  private static final int LOG_INTERVAL_SECONDS = 60;

  private static final byte[] MIGRATION_PROGRESS_KEY =
      "ARCHIVE_MIGRATION_PROGRESS".getBytes(StandardCharsets.UTF_8);

  private final BonsaiWorldStateKeyValueStorage worldStateStorage;
  private final TrieLogManager trieLogManager;
  private final Blockchain blockchain;
  private final ExecutorService executorService;
  private final BonsaiArchiveFlatDbStrategy archiveStrategy;
  private final AtomicLong migratedBlockNumber = new AtomicLong(0);
  private final AtomicBoolean shouldLogProgress = new AtomicBoolean(true);
  protected final AtomicBoolean migrationRunning = new AtomicBoolean(false);

  /**
   * Creates a new BonsaiFlatDbToArchiveMigrator.
   *
   * @param worldStateStorage the Bonsai world state storage
   * @param trieLogManager the trie log manager for reading trie logs
   * @param blockchain the blockchain for reading block headers
   * @param executorService the executor service for running migration on a separate thread
   * @param metricsSystem the metrics system for tracking migration progress
   * @param archiveStrategy the archive flat DB strategy for writing archive keys
   */
  public BonsaiFlatDbToArchiveMigrator(
      final BonsaiWorldStateKeyValueStorage worldStateStorage,
      final TrieLogManager trieLogManager,
      final Blockchain blockchain,
      final ExecutorService executorService,
      final MetricsSystem metricsSystem,
      final BonsaiArchiveFlatDbStrategy archiveStrategy) {
    this.worldStateStorage = worldStateStorage;
    this.trieLogManager = trieLogManager;
    this.blockchain = blockchain;
    this.executorService = executorService;
    this.archiveStrategy = archiveStrategy;
    metricsSystem.createLongGauge(
        BesuMetricCategory.BLOCKCHAIN,
        "bonsai_archive_migration_block",
        "The current block the Bonsai archive migration has reached",
        migratedBlockNumber::get);
  }

  /**
   * Migrates Bonsai flat DB to Bonsai archive format.
   *
   * @return a CompletableFuture that completes when migration finishes
   */
  public CompletableFuture<Void> migrate() {
    if (!migrationRunning.compareAndSet(false, true)) {
      LOG.warn("Bonsai migration already in progress, ignoring");
      return CompletableFuture.completedFuture(null);
    }

    final Instant migrationStartTime = Instant.now();
    final long lastProcessedBlock = getMigrationProgress().orElse(-1L);
    final long startBlock = lastProcessedBlock + 1;
    migratedBlockNumber.set(Math.max(0, lastProcessedBlock));

    final AtomicLong target = new AtomicLong(blockchain.getChainHeadBlockNumber());
    final long blockObserverId =
        blockchain.observeBlockAdded(event -> target.set(event.getHeader().getNumber()));

    LOG.info("Starting Bonsai Archive migration from block {}", startBlock);
    return CompletableFuture.runAsync(() -> migrateBlocks(startBlock, target), executorService)
        .whenComplete(
            (result, ex) -> {
              blockchain.removeObserver(blockObserverId);
              if (ex != null) {
                migrationRunning.set(false);
                LOG.error("Bonsai to Bonsai archive migration failed", ex);
              }
            })
        .thenRun(
            () -> {
              worldStateStorage.upgradeToArchiveFlatDbMode();
              logCompletion(startBlock, target.get(), migrationStartTime);
              migrationRunning.set(false);
            });
  }

  private void migrateBlocks(final long startBlock, final AtomicLong target) {
    for (long blockNumber = startBlock; blockNumber <= target.get(); blockNumber++) {
      final Optional<TrieLog> maybeTrieLog =
          blockchain
              .getBlockHeader(blockNumber)
              .flatMap(header -> trieLogManager.getTrieLogLayer(header.getHash()));
      if (maybeTrieLog.isPresent()) {
        final SegmentedKeyValueStorageTransaction tx =
            worldStateStorage.getComposedWorldStateStorage().startLowPriorityTransaction();
        processBlock(maybeTrieLog.get(), blockNumber, tx);
        migratedBlockNumber.incrementAndGet();
        saveProgress(blockNumber, tx);
        tx.commit();
        logProgress(blockNumber, target.get());
      } else if (blockNumber > 0) {
        throw new IllegalStateException("No trie log found for block " + blockNumber);
      }
    }
  }

  @Override
  public void close() {
    executorService.shutdownNow();
    try {
      if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
        LOG.warn("Migration executor did not terminate within 10 seconds");
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void logProgress(final long blockNumber, final long endBlock) {
    LogUtil.throttledLog(
        () -> {
          long progressPercent = endBlock > 0 ? (blockNumber * 100) / endBlock : 0;
          LOG.info(
              "Bonsai Archive migration progress: {}% (block {}/{})",
              progressPercent, blockNumber, endBlock);
        },
        shouldLogProgress,
        LOG_INTERVAL_SECONDS);
  }

  private void logCompletion(
      final long startBlock, final long endBlock, final Instant migrationStartTime) {
    final Duration migrationDuration = Duration.between(migrationStartTime, Instant.now());
    final String formattedDuration =
        DurationFormatUtils.formatDurationWords(migrationDuration.toMillis(), true, true);
    LOG.info(
        "Bonsai Archive migration completed. Processed {} blocks in {}.",
        endBlock - startBlock + 1,
        formattedDuration);
  }

  private void processBlock(
      final TrieLog trieLog, final long blockNumber, final SegmentedKeyValueStorageTransaction tx) {
    final BonsaiContext context = new BonsaiContext(blockNumber);
    processAccountChanges(trieLog, context, tx);
    processStorageChanges(trieLog, context, tx);
  }

  private void processAccountChanges(
      final TrieLog trieLog,
      final BonsaiContext context,
      final SegmentedKeyValueStorageTransaction tx) {
    trieLog
        .getAccountChanges()
        .forEach(
            (address, accountChange) -> {
              if (accountChange.getUpdated() != null) {
                final BytesValueRLPOutput out = new BytesValueRLPOutput();
                accountChange.getUpdated().writeTo(out);
                archiveStrategy.putFlatAccount(context, tx, address.addressHash(), out.encoded());
              } else {
                archiveStrategy.removeFlatAccount(context, tx, address.addressHash());
              }
            });
  }

  private void processStorageChanges(
      final TrieLog trieLog,
      final BonsaiContext context,
      final SegmentedKeyValueStorageTransaction tx) {
    trieLog
        .getStorageChanges()
        .forEach(
            (address, storageMap) ->
                storageMap.forEach(
                    (slotKey, storageChange) -> {
                      if (storageChange.getUpdated() != null) {
                        archiveStrategy.putFlatAccountStorageValueByStorageSlotHash(
                            context,
                            tx,
                            address.addressHash(),
                            slotKey.getSlotHash(),
                            storageChange.getUpdated().toBytes());
                      } else {
                        archiveStrategy.removeFlatAccountStorageValueByStorageSlotHash(
                            context, tx, address.addressHash(), slotKey.getSlotHash());
                      }
                    }));
  }

  @VisibleForTesting
  protected Optional<Long> getMigrationProgress() {
    return worldStateStorage
        .getComposedWorldStateStorage()
        .get(ACCOUNT_INFO_STATE_ARCHIVE, MIGRATION_PROGRESS_KEY)
        .map(Bytes::wrap)
        .map(Bytes::toLong);
  }

  private void saveProgress(final long blockNumber, final SegmentedKeyValueStorageTransaction tx) {
    tx.put(
        ACCOUNT_INFO_STATE_ARCHIVE,
        MIGRATION_PROGRESS_KEY,
        Bytes.ofUnsignedLong(blockNumber).toArrayUnsafe());
  }
}
