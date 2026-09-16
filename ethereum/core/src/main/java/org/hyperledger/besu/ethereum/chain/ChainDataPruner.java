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
package org.hyperledger.besu.ethereum.chain;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;
import org.hyperledger.besu.util.log.LogUtil;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChainDataPruner implements BlockAddedObserver {
  private static final Logger LOG = LoggerFactory.getLogger(ChainDataPruner.class);
  private static final int LOG_PRUNING_PROGRESS_REPEAT_DELAY_SECONDS = 300;

  /**
   * Catch-up cap per job, as a multiple of {@code chainPruningFrequency}. Default frequency is 256,
   * so one job deletes at most 768 blocks rather than the full lag (e.g. after snap-sync).
   */
  private static final int PRUNE_BATCH_FREQUENCY_MULTIPLIER = 3;

  /**
   * When frequency is 0 (tests / “prune whenever lag exists”), still bound work per transaction so
   * a single job cannot scan millions of keys.
   */
  private static final long MIN_CATCH_UP_BLOCKS_PER_JOB = 1024;

  public static final int MAX_PRUNING_THREAD_QUEUE_SIZE = 16;

  private final BlockchainStorage blockchainStorage;
  private final Runnable unsubscribeRunnable;
  private final ChainDataPrunerStorage prunerStorage;
  private final long mergeBlock;
  private final PruningMode pruningMode;
  private final ChainPrunerConfiguration config;
  private final ExecutorService pruningExecutor;
  private final AtomicBoolean logPreMergePruningProgress = new AtomicBoolean(true);
  private final AtomicBoolean logChainPruningProgress = new AtomicBoolean(true);

  public ChainDataPruner(
      final BlockchainStorage blockchainStorage,
      final Runnable unsubscribeRunnable,
      final ChainDataPrunerStorage prunerStorage,
      final long mergeBlock,
      final PruningMode pruningMode,
      final ChainPrunerConfiguration config,
      final ExecutorService pruningExecutor) {
    this.blockchainStorage = blockchainStorage;
    this.unsubscribeRunnable = unsubscribeRunnable;
    this.prunerStorage = prunerStorage;
    this.mergeBlock = mergeBlock;
    this.pruningMode = pruningMode;
    this.config = config;
    this.pruningExecutor = pruningExecutor;
  }

  @Override
  public void onBlockAdded(final BlockAddedEvent event) {
    switch (pruningMode) {
      case CHAIN_PRUNING -> chainPrunerAction(event);
      case PRE_MERGE_PRUNING -> {
        if (event.isNewCanonicalHead()) preMergePruningAction();
      }
    }
  }

  private void chainPrunerAction(final BlockAddedEvent event) {
    final BlockHeader header = event.getHeader();
    // Validate on every event, including non-canonical forks: a fork below the mark means
    // retained may be too small. Canonical heads then share pruneForSyncedHead with snap-sync.
    final long storedBlockPruningMark = prunerStorage.getChainPruningMark().orElse(1L);
    final long storedBalPruningMark = prunerStorage.getBalPruningMark().orElse(1L);
    validatePruningMarks(
        header.getNumber(),
        storedBlockPruningMark,
        storedBalPruningMark,
        header.getBalHash().isPresent());
    recordForkBlock(event, header.getNumber());
    if (!event.isNewCanonicalHead()) {
      return;
    }
    pruneForSyncedHead(header);
  }

  /**
   * Drives catch-up chain/BAL pruning from the snap-sync pipeline ({@code ImportSyncBlocksStep}),
   * which uses {@code DefaultBlockchain#unsafeImportSyncBodiesAndReceipts} and therefore bypasses
   * {@code BlockAddedEvent} observers. Also used by {@link #chainPrunerAction} after a new
   * canonical head. Called with the new chain head header. No-op unless chain pruning is enabled;
   * pre-merge-only pruning is driven by the observer path.
   */
  public void pruneForSyncedHead(final BlockHeader header) {
    if (pruningMode != PruningMode.CHAIN_PRUNING) {
      return;
    }
    // Never default the mark to the current head: during snap, the first BlockAddedEvent is the
    // tip and that used to persist a mark that skipped all historical blocks (issue #11131).
    final long storedBlockPruningMark = prunerStorage.getChainPruningMark().orElse(1L);
    final long storedBalPruningMark = prunerStorage.getBalPruningMark().orElse(1L);
    validatePruningMarks(
        header.getNumber(),
        storedBlockPruningMark,
        storedBalPruningMark,
        header.getBalHash().isPresent());
    try {
      pruningExecutor.submit(
          () -> pruneChainAndBalData(header, storedBlockPruningMark, storedBalPruningMark));
    } catch (final RejectedExecutionException e) {
      LOG.debug(
          "Chain pruning task rejected for head {}; will retry on the next head update",
          header.getNumber());
    }
  }

  private void validatePruningMarks(
      final long blockNumber,
      final long storedPruningMark,
      final long storedBalPruningMark,
      final boolean isBalHashPresent) {
    if (config.isBlockPruningEnabled() && blockNumber < storedPruningMark) {
      LOG.warn(
          "Block number {} is less than pruning mark {} - chain-pruning-blocks-retained may be too small",
          blockNumber,
          storedPruningMark);
    }
    if (config.isBalPruningEnabled() && isBalHashPresent && blockNumber < storedBalPruningMark) {
      LOG.warn(
          "Block number {} is less than BAL pruning mark {} - chain-pruning-bals-retained may be too small",
          blockNumber,
          storedBalPruningMark);
    }
  }

  private void recordForkBlock(final BlockAddedEvent event, final long blockNumber) {
    prunerStorage.addForkBlock(blockNumber, event.getHeader().getHash());
  }

  private void pruneChainAndBalData(
      final BlockHeader header,
      final long storedBlockPruningMark,
      final long storedBalPruningMark) {

    final long blockPruningMark = header.getNumber() - config.chainPruningBlocksRetained();
    final long balPruningMark = header.getNumber() - config.chainPruningBalsRetained();

    final boolean shouldPruneBlock =
        config.isBlockPruningEnabled() && shouldPrune(blockPruningMark, storedBlockPruningMark);
    final boolean shouldPruneBal =
        config.isBalPruningEnabled() && shouldPrune(balPruningMark, storedBalPruningMark);

    if (!shouldPruneBlock && !shouldPruneBal) {
      if (config.isBalPruningEnabled() && header.getBalHash().isEmpty()) {
        final KeyValueStorageTransaction tx = prunerStorage.startTransaction();
        prunerStorage.setBalPruningMark(tx, header.getNumber());
        tx.commit();
      }
      return;
    }

    final KeyValueStorageTransaction pruningTransaction = prunerStorage.startTransaction();
    long currentChainMark = storedBlockPruningMark;
    long currentBalMark = storedBalPruningMark;

    final BlockchainStorage.Updater updater = blockchainStorage.updater();
    // When chain pruning is active, BAL is also active (mode ALL)
    // When only BAL pruning is active (mode BAL), we prune from storedBalPruningMark to
    // balPruningMark
    final long startBlock = shouldPruneBlock ? storedBlockPruningMark : storedBalPruningMark;
    final long targetEnd = shouldPruneBlock ? blockPruningMark : balPruningMark;
    final long endBlock = cappedEndBlock(startBlock, targetEnd);

    for (long blockNum = startBlock; blockNum <= endBlock; blockNum++) {
      if (blockNum < 1) {
        continue;
      }
      // In mode ALL: prune chain data up to blockPruningMark, BAL data up to balPruningMark
      // In mode BAL: only prune BAL data up to balPruningMark
      final boolean pruneChainAtBlock = shouldPruneBlock && blockNum <= blockPruningMark;
      final boolean pruneBalAtBlock = shouldPruneBal && blockNum <= balPruningMark;

      if (!pruneChainAtBlock && !pruneBalAtBlock) {
        continue;
      }

      final Collection<Hash> forkBlocks = hashesToPrune(blockNum);

      for (final Hash blockHash : forkBlocks) {
        if (pruneChainAtBlock) {
          LOG.debug("Pruning chain data at block {}", blockNum);
          removeChainData(updater, blockHash);
        }
        if (pruneBalAtBlock) {
          LOG.debug("Pruning BAL data at block {}", blockNum);
          updater.removeBlockAccessList(blockHash);
        }
      }

      if (pruneChainAtBlock) {
        updater.removeBlockHash(blockNum);
        currentChainMark = blockNum;
        prunerStorage.removeForkBlocks(pruningTransaction, blockNum);
      }

      if (pruneBalAtBlock) {
        currentBalMark = blockNum;
        // In BAL-only mode, remove fork blocks when pruning BAL data
        if (!config.isBlockPruningEnabled()) {
          prunerStorage.removeForkBlocks(pruningTransaction, blockNum);
        }
      }
    }
    updater.commit();

    prunerStorage.setChainPruningMark(pruningTransaction, currentChainMark);
    if (header.getBalHash().isEmpty() && !config.isBlockPruningEnabled()) {
      // BAL not activated yet; only advance the BAL mark in BAL-only mode
      currentBalMark = header.getNumber();
    }
    prunerStorage.setBalPruningMark(pruningTransaction, currentBalMark);
    pruningTransaction.commit();
    final long loggedMark = shouldPruneBlock ? currentChainMark : currentBalMark;
    LogUtil.throttledLog(
        () -> LOG.info("Pruned chain data up to block {}", loggedMark),
        logChainPruningProgress,
        LOG_PRUNING_PROGRESS_REPEAT_DELAY_SECONDS);
  }

  private Collection<Hash> hashesToPrune(final long blockNum) {
    final Collection<Hash> forkBlocks = prunerStorage.getForkBlocks(blockNum);
    if (forkBlocks.isEmpty()) {
      // Snap / unsafe import never records fork hashes via BlockAddedEvent
      blockchainStorage.getBlockHash(blockNum).ifPresent(forkBlocks::add);
    }
    return forkBlocks;
  }

  private boolean shouldPrune(final long newMark, final long currentMark) {
    return (newMark - currentMark) >= config.chainPruningFrequency();
  }

  private long cappedEndBlock(final long startBlock, final long targetEnd) {
    final long frequency = config.chainPruningFrequency();
    final long maxBlocks =
        frequency > 0 ? frequency * PRUNE_BATCH_FREQUENCY_MULTIPLIER : MIN_CATCH_UP_BLOCKS_PER_JOB;
    return Math.min(targetEnd, startBlock + maxBlocks - 1);
  }

  private void removeChainData(final BlockchainStorage.Updater updater, final Hash blockHash) {
    updater.removeBlockHeader(blockHash);
    updater.removeBlockBody(blockHash);
    updater.removeTransactionReceipts(blockHash);
    updater.removeTotalDifficulty(blockHash);
    removeTransactionLocations(updater, blockHash);
  }

  private void removeTransactionLocations(
      final BlockchainStorage.Updater updater, final Hash blockHash) {
    blockchainStorage
        .getBlockBody(blockHash)
        .ifPresent(
            blockBody ->
                blockBody
                    .getTransactions()
                    .forEach(t -> updater.removeTransactionLocation(t.getHash())));
  }

  private void preMergePruningAction() {
    pruningExecutor.submit(
        () -> {
          try {
            Thread.sleep(1000);
            final long storedBlockPruningMark = prunerStorage.getChainPruningMark().orElse(1L);
            final long expectedNewPruningMark =
                Math.min(
                    storedBlockPruningMark + config.preMergePruningBlocksQuantity(), mergeBlock);
            LOG.debug(
                "Attempting to prune blocks {} to {}",
                storedBlockPruningMark,
                expectedNewPruningMark);
            final KeyValueStorageTransaction pruningTransaction = prunerStorage.startTransaction();
            final BlockchainStorage.Updater updater = blockchainStorage.updater();
            for (long blockNumber = storedBlockPruningMark;
                blockNumber < expectedNewPruningMark;
                blockNumber++) {
              blockchainStorage
                  .getBlockHash(blockNumber)
                  .ifPresent(
                      (blockHash) -> {
                        updater.removeBlockBody(blockHash);
                        updater.removeTransactionReceipts(blockHash);
                        blockchainStorage
                            .getBlockBody(blockHash)
                            .ifPresent(
                                blockBody ->
                                    blockBody
                                        .getTransactions()
                                        .forEach(
                                            t -> updater.removeTransactionLocation(t.getHash())));
                      });
            }
            updater.commit();
            prunerStorage.setChainPruningMark(pruningTransaction, expectedNewPruningMark);
            pruningTransaction.commit();
            LOG.debug("Pruned pre-merge blocks up to {}", expectedNewPruningMark);
            LogUtil.throttledLog(
                () -> LOG.info("Pruned pre-merge blocks up to {}", expectedNewPruningMark),
                logPreMergePruningProgress,
                LOG_PRUNING_PROGRESS_REPEAT_DELAY_SECONDS);
            if (expectedNewPruningMark == mergeBlock) {
              LOG.info("Done pruning pre-merge blocks.");
              LOG.debug("Unsubscribing from block added event observation");
              unsubscribeRunnable.run();
            }
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        });
  }

  public enum PruningMode {
    CHAIN_PRUNING,
    PRE_MERGE_PRUNING
  }

  /** Enum for chain pruning strategy. */
  public enum ChainPruningStrategy {
    /** Prune both blocks and BALs. */
    ALL,
    /** Prune only BALs. */
    BAL,
    /** Pruning disabled. */
    NONE
  }
}
