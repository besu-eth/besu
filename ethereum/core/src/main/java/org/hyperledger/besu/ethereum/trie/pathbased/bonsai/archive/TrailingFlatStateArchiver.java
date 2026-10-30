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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_ARCHIVE;

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogManager;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.util.log.LogUtil;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Populates the Bonsai archive column families by replaying trie logs, running from node boot.
 *
 * <p>The archiver replays trie logs sequentially from block 0 (or the last saved checkpoint) and
 * trails the chain head by {@code trieLogManager.getMaxLayersToLoad()} blocks, writing
 * archive-keyed entries into the archive column families. Progress is persisted atomically with
 * each block's data so archiving can safely resume after a restart.
 */
public class TrailingFlatStateArchiver implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(TrailingFlatStateArchiver.class);
  private static final int LOG_INTERVAL_SECONDS = 60;

  private static final byte[] PROGRESS_KEY =
      "ARCHIVE_FLAT_STATE_PROGRESS".getBytes(StandardCharsets.UTF_8);

  private final BonsaiWorldStateKeyValueStorage worldStateStorage;
  private final TrieLogManager trieLogManager;
  private final Blockchain blockchain;
  private final ScheduledExecutorService executorService;
  private final BonsaiArchiveFlatDbStrategy archiveStrategy;
  private final AtomicBoolean shouldLogProgress = new AtomicBoolean(true);
  protected final AtomicLong archivedBlockNumber = new AtomicLong(-1);
  protected final AtomicLong targetBlockNumber = new AtomicLong(0);
  protected final AtomicBoolean catchUpRunning = new AtomicBoolean(false);
  protected OptionalLong blockObserverId = OptionalLong.empty();
  protected volatile boolean failed = false;

  /**
   * Creates a new TrailingFlatStateArchiver.
   *
   * @param worldStateStorage the Bonsai world state storage
   * @param trieLogManager the trie log manager for reading trie logs
   * @param blockchain the blockchain for reading block headers
   * @param executorService a dedicated executor for archiving; shutting it down stops the archiver
   * @param metricsSystem the metrics system for tracking archiving progress
   * @param archiveStrategy the archive flat DB strategy for writing archive keys
   */
  public TrailingFlatStateArchiver(
      final BonsaiWorldStateKeyValueStorage worldStateStorage,
      final TrieLogManager trieLogManager,
      final Blockchain blockchain,
      final ScheduledExecutorService executorService,
      final MetricsSystem metricsSystem,
      final BonsaiArchiveFlatDbStrategy archiveStrategy) {
    this.worldStateStorage = worldStateStorage;
    this.trieLogManager = trieLogManager;
    this.blockchain = blockchain;
    this.executorService = executorService;
    this.archiveStrategy = archiveStrategy;
    metricsSystem.createLongGauge(
        BesuMetricCategory.BLOCKCHAIN,
        "bonsai_archive_flat_state_block",
        "The highest block the Bonsai flat state archiver has archived",
        archivedBlockNumber::get);
  }

  /**
   * Starts archiving blocks to the bonsai archive, trailing the chain head by the trie log
   * retention window. Resumes from the saved progress and registers a head observer to keep
   * following the chain.
   */
  public synchronized void start() {
    if (executorService.isShutdown() || blockObserverId.isPresent()) {
      return;
    }
    archivedBlockNumber.set(getSavedProgress().orElse(-1L));
    blockObserverId =
        OptionalLong.of(
            blockchain.observeBlockAdded(
                event -> {
                  if (event.isNewCanonicalHead()) {
                    advanceTarget(event.getHeader().getNumber());
                  }
                }));
    advanceTarget(blockchain.getChainHeadBlockNumber());
  }

  private void advanceTarget(final long headBlockNumber) {
    targetBlockNumber.accumulateAndGet(archiveTarget(headBlockNumber), Math::max);
    scheduleCatchUpIfNeeded();
  }

  private void scheduleCatchUpIfNeeded() {
    if (failed || !catchUpRunning.compareAndSet(false, true)) {
      return;
    }
    try {
      executorService.submit(this::catchUp);
    } catch (final RejectedExecutionException e) {
      catchUpRunning.set(false);
      LOG.debug(
          "Bonsai flat state archiver executor shut down; skipping archiving up to block {}",
          targetBlockNumber.get());
    }
  }

  private void catchUp() {
    try {
      archiveBlocks();
    } catch (final RuntimeException ex) {
      if (executorService.isShutdown()) {
        LOG.debug(
            "Bonsai flat state archiving interrupted by close at block {}",
            archivedBlockNumber.get() + 1,
            ex);
      } else {
        failed = true;
        LOG.error(
            "Bonsai flat state archiving stopped at block {}; archive will remain behind until restart",
            archivedBlockNumber.get() + 1,
            ex);
      }
    } finally {
      catchUpRunning.set(false);
      if (archivedBlockNumber.get() < targetBlockNumber.get()) {
        scheduleCatchUpIfNeeded();
      }
    }
  }

  private void archiveBlocks() {
    for (long blockNumber = archivedBlockNumber.get() + 1;
        blockNumber <= targetBlockNumber.get() && !executorService.isShutdown();
        blockNumber++) {
      final TrieLog trieLog = trieLogAt(blockNumber);
      final SegmentedKeyValueStorageTransaction tx =
          worldStateStorage.getComposedWorldStateStorage().startBatchTransaction();
      processBlock(trieLog, blockNumber, tx);
      saveProgress(blockNumber, tx);
      tx.commit();
      archivedBlockNumber.set(blockNumber);
      logProgress(blockNumber, targetBlockNumber.get());
    }
  }

  private TrieLog trieLogAt(final long blockNumber) {
    return blockchain
        .getBlockHeader(blockNumber)
        .flatMap(header -> trieLogManager.getTrieLogLayer(header.getHash()))
        .orElseThrow(() -> new IllegalStateException("No trie log found for block " + blockNumber));
  }

  private long archiveTarget(final long blockNumber) {
    return Math.max(0, blockNumber - trieLogManager.getMaxLayersToLoad());
  }

  /**
   * Returns the current archived block.
   *
   * @return the highest block number that has been archived
   */
  public long getArchivedBlockNumber() {
    return archivedBlockNumber.get();
  }

  @Override
  public synchronized void close() {
    blockObserverId.ifPresent(blockchain::removeObserver);
    blockObserverId = OptionalLong.empty();
    executorService.shutdownNow();
    try {
      if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
        LOG.warn("Archiving executor did not terminate within 10 seconds");
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void logProgress(final long blockNumber, final long endBlock) {
    LogUtil.throttledLog(
        () -> LOG.info("Bonsai flat state archived to block {} of {}", blockNumber, endBlock),
        shouldLogProgress,
        LOG_INTERVAL_SECONDS);
  }

  private void processBlock(
      final TrieLog trieLog, final long blockNumber, final SegmentedKeyValueStorageTransaction tx) {
    final BonsaiArchiveContext context = new BonsaiArchiveContext(blockNumber);
    processAccountChanges(trieLog, context, tx);
    processStorageChanges(trieLog, context, tx);
  }

  private void processAccountChanges(
      final TrieLog trieLog,
      final BonsaiArchiveContext context,
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
      final BonsaiArchiveContext context,
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
  protected Optional<Long> getSavedProgress() {
    return worldStateStorage
        .getComposedWorldStateStorage()
        .get(ACCOUNT_INFO_STATE_ARCHIVE, PROGRESS_KEY)
        .map(Bytes::wrap)
        .map(Bytes::toLong);
  }

  private void saveProgress(final long blockNumber, final SegmentedKeyValueStorageTransaction tx) {
    tx.put(
        ACCOUNT_INFO_STATE_ARCHIVE,
        PROGRESS_KEY,
        Bytes.ofUnsignedLong(blockNumber).toArrayUnsafe());
  }
}
