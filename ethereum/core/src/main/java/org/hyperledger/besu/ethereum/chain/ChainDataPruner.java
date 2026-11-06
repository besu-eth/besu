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
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongPredicate;
import java.util.function.Predicate;

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

  private static final long ABSENT_PRUNING_MARK = -1;

  public static final int MAX_PRUNING_THREAD_QUEUE_SIZE = 16;

  private final BlockchainStorage blockchainStorage;
  private final ChainDataPrunerStorage prunerStorage;
  private final ChainPrunerConfiguration config;
  private final boolean pruneFromHead;
  private final long minimumHeadDistance;
  private final Predicate<BlockHeader> isBalEnabled;

  /**
   * Whether, with blocks kept from the given one a pruning run later, a back-fill still has bodies
   * to store below the ones a sync stored. Block pruning finds no boundary until it has none.
   */
  private final LongPredicate bodiesMissing;

  private final ExecutorService pruningExecutor;
  private final AtomicBoolean logChainPruningProgress = new AtomicBoolean(true);
  private final boolean prunesBlockData;

  /** Persisted progress, or -1 until existing data establishes a starting boundary. */
  private volatile long lastPrunedBlock;

  private volatile long lastPrunedBal;

  public ChainDataPruner(
      final BlockchainStorage blockchainStorage,
      final ChainDataPrunerStorage prunerStorage,
      final ChainPrunerConfiguration config,
      final boolean pruneFromHead,
      final long minimumHeadDistance,
      final Predicate<BlockHeader> isBalEnabled,
      final LongPredicate bodiesMissing,
      final ExecutorService pruningExecutor) {
    this.blockchainStorage = blockchainStorage;
    this.prunerStorage = prunerStorage;
    this.config = config;
    this.pruneFromHead = pruneFromHead;
    this.minimumHeadDistance = minimumHeadDistance;
    this.isBalEnabled = isBalEnabled;
    this.bodiesMissing = bodiesMissing;
    this.pruningExecutor = pruningExecutor;
    this.prunesBlockData = config.isBlockPruningEnabled();
    this.lastPrunedBlock = prunerStorage.getBlockPruningMark().orElse(ABSENT_PRUNING_MARK);
    this.lastPrunedBal = prunerStorage.getBalPruningMark().orElse(ABSENT_PRUNING_MARK);
  }

  @Override
  public void onBlockAdded(final BlockAddedEvent event) {
    chainPrunerAction(event);
  }

  /**
   * Returns the lowest block whose body and receipts a back-fill can store at this chain head: the
   * lowest one still retained a pruning run later, so that a run starting while the back-fill
   * stores a block never passes over it, and never a block pruning already removed: no later run
   * would find it below the completed mark.
   */
  public long lowestToBackfill(final long chainHead) {
    return Math.max(lowestRetained(chainHead + config.chainPruningFrequency()), lowestUnpruned());
  }

  // the lowest block pruning has not removed; genesis is never pruned
  private long lowestUnpruned() {
    final long lastPruned = lastPrunedBlock;
    return lastPruned <= 0 ? 0 : lastPruned + 1;
  }

  // the lowest block whose body and receipts stay in place at this chain head
  private long lowestRetained(final long chainHead) {
    // pruning only the block access lists keeps every body
    if (!config.isBlockPruningEnabled()) {
      return 0;
    }
    return Math.max(0, chainHead - config.chainPruningBlocksRetained() + 1);
  }

  private void chainPrunerAction(final BlockAddedEvent event) {
    final BlockHeader header = event.getHeader();
    recordForkBlock(event, header.getNumber());
    if (!event.isNewCanonicalHead()) {
      return;
    }
    pruneForSyncedHead(header);
  }

  /**
   * The block both retention windows end at: the head for BFT, whose imported blocks are final,
   * otherwise the persisted safe block, or empty while it is unknown.
   */
  private OptionalLong pruningReference(final long headBlockNumber) {
    if (pruneFromHead) {
      return OptionalLong.of(headBlockNumber);
    }
    return blockchainStorage
        .getSafeBlock()
        .flatMap(blockchainStorage::getBlockHeader)
        .map(safe -> OptionalLong.of(safe.getNumber()))
        .orElseGet(OptionalLong::empty);
  }

  /** The highest block a pass may prune: its retention below the reference, never near the head. */
  private long pruningTarget(
      final long reference, final long headBlockNumber, final long retained) {
    return Math.min(reference - retained, headBlockNumber - minimumHeadDistance);
  }

  private boolean pruneBals(final BlockHeader header) {
    return config.pruningMode() != ChainPruningStrategy.NONE && isBalEnabled.test(header);
  }

  /**
   * Submits a pruning job for a new canonical head, unless nothing is due.
   *
   * @param header the new chain head header
   */
  void pruneForSyncedHead(final BlockHeader header) {
    final boolean pruneBals = pruneBals(header);
    if (prunesBlockData) {
      validatePruningMark(header.getNumber(), lastPrunedBlock, "blocks");
    }
    if (pruneBals) {
      validatePruningMark(header.getNumber(), lastPrunedBal, "bals");
    }
    final OptionalLong reference = pruningReference(header.getNumber());
    if (reference.isEmpty()) {
      return;
    }
    final boolean blockWork =
        prunesBlockData
            && needsPruningJob(
                pruningTarget(
                    reference.getAsLong(), header.getNumber(), config.chainPruningBlocksRetained()),
                lastPrunedBlock);
    final boolean balWork =
        pruneBals
            && needsPruningJob(
                pruningTarget(
                    reference.getAsLong(), header.getNumber(), config.chainPruningBalsRetained()),
                lastPrunedBal);
    if (!blockWork && !balWork) {
      return;
    }
    submitJob();
  }

  private void submitJob() {
    try {
      pruningExecutor.submit(this::runPruningJob);
    } catch (final RejectedExecutionException e) {
      LOG.debug("Chain pruning task rejected; will retry on the next head update");
    }
  }

  /**
   * The earliest retained body and receipt advertised in a snapshot of the eth/69 block range.
   * Subsequent head updates may advance this range; publishing it does not gate pruning.
   *
   * @param headBlockNumber the current chain head number
   * @return the earliest retained block number, or empty if this pruner never removes bodies
   */
  public Optional<Long> getEarliestRetainedBlock(final long headBlockNumber) {
    if (!prunesBlockData) {
      return Optional.empty();
    }
    // Block pruning cannot pass this head's block retention window.
    final long bound = headBlockNumber - config.chainPruningBlocksRetained() + 1;
    // Covers retention being raised after blocks were already deleted.
    return Optional.of(Math.max(bound, lowestUnpruned()));
  }

  private void validatePruningMark(
      final long blockNumber, final long lastPruned, final String dataKind) {
    if (blockNumber < lastPruned) {
      LOG.warn(
          "Block number {} is less than the {} pruning mark {} - --Xchain-pruning-{}-retained may be too small",
          blockNumber,
          dataKind,
          lastPruned,
          dataKind);
    }
  }

  /** Indexes a block hash by height so non-canonical blocks can be found when pruning. */
  private void recordForkBlock(final BlockAddedEvent event, final long blockNumber) {
    prunerStorage.addForkBlock(blockNumber, event.getHeader().getHash());
  }

  private long resolveLastPruned(final boolean pruneBlocks, final long headBlockNumber) {
    final long lastPruned = pruneBlocks ? lastPrunedBlock : lastPrunedBal;
    if (lastPruned != ABSENT_PRUNING_MARK) {
      return lastPruned;
    }
    if (pruneBlocks && bodiesMissing.test(lowestToBackfill(headBlockNumber))) {
      // a back-fill still stores bodies below the stored ones, out of order: no boundary yet
      LOG.debug("Block pruning waits for the back-fill to store the bodies it misses");
      return ABSENT_PRUNING_MARK;
    }
    final Optional<Long> firstWithData =
        pruneBlocks
            ? BlockDataBoundary.firstBlockWithBody(
                BlockDataBoundary.blockDataIn(blockchainStorage), headBlockNumber)
            : BlockDataBoundary.firstBlockWithData(
                blockNumber ->
                    blockchainStorage
                        .getBlockHash(blockNumber)
                        .flatMap(blockchainStorage::getBlockHeader)
                        .filter(isBalEnabled)
                        .flatMap(header -> blockchainStorage.getBlockAccessList(header.getHash()))
                        .isPresent(),
                headBlockNumber);
    if (firstWithData.isEmpty()) {
      return ABSENT_PRUNING_MARK;
    }
    final long initialMark = Math.max(0, firstWithData.get() - 1);
    final KeyValueStorageTransaction transaction = prunerStorage.startTransaction();
    boolean commitAttempted = false;
    try {
      setPruningMark(transaction, pruneBlocks, initialMark);
      commitAttempted = true;
      transaction.commit();
      updateProgress(pruneBlocks, initialMark);
    } finally {
      if (!commitAttempted) {
        transaction.rollback();
      }
    }
    return initialMark;
  }

  private void runPruningJob() {
    try {
      pruneToCurrentHead();
    } catch (final RuntimeException e) {
      LOG.warn("Chain pruning job failed; retrying on the next head", e);
    }
  }

  // reads the head and safe block as they are when the job runs, not when it was queued
  private void pruneToCurrentHead() {
    final Optional<BlockHeader> head =
        blockchainStorage.getChainHead().flatMap(blockchainStorage::getBlockHeader);
    if (head.isEmpty()) {
      return;
    }
    final long headBlockNumber = head.get().getNumber();
    final boolean pruneBals = pruneBals(head.get());
    final OptionalLong reference = pruningReference(headBlockNumber);
    if (reference.isEmpty()) {
      return;
    }
    final long blockTarget =
        pruningTarget(reference.getAsLong(), headBlockNumber, config.chainPruningBlocksRetained());
    final long balTarget =
        pruningTarget(reference.getAsLong(), headBlockNumber, config.chainPruningBalsRetained());
    final long blockStart =
        prunesBlockData
            ? initializePruningMark(true, headBlockNumber, blockTarget)
            : ABSENT_PRUNING_MARK;
    final long balStart =
        pruneBals ? initializePruningMark(false, headBlockNumber, balTarget) : ABSENT_PRUNING_MARK;
    runPruningPass(true, headBlockNumber, blockTarget, blockStart);
    runPruningPass(false, headBlockNumber, balTarget, balStart);
  }

  private long initializePruningMark(
      final boolean pruneBlocks, final long headBlockNumber, final long target) {
    try {
      return target > 0 ? resolveLastPruned(pruneBlocks, headBlockNumber) : ABSENT_PRUNING_MARK;
    } catch (final RuntimeException e) {
      LOG.warn(
          "{} pruning initialization for head {} failed; retrying on the next head",
          pruneBlocks ? "Block" : "BAL",
          headBlockNumber,
          e);
      return ABSENT_PRUNING_MARK;
    }
  }

  private void runPruningPass(
      final boolean pruneBlocks,
      final long headBlockNumber,
      final long target,
      final long lastPruned) {
    try {
      if (lastPruned == ABSENT_PRUNING_MARK || !shouldPrune(target, lastPruned)) {
        return;
      }
      pruneData(pruneBlocks, lastPruned + 1, cappedEndBlock(lastPruned + 1, target));
    } catch (final RuntimeException e) {
      LOG.warn(
          "{} pruning pass for head {} failed; retrying on the next head",
          pruneBlocks ? "Block" : "BAL",
          headBlockNumber,
          e);
    }
  }

  private void pruneData(final boolean pruneBlocks, final long startBlock, final long endBlock) {
    final KeyValueStorageTransaction transaction = prunerStorage.startTransaction();
    boolean metadataCommitAttempted = false;
    try {
      final BlockchainStorage.Updater updater = blockchainStorage.updater();
      boolean dataCommitAttempted = false;
      try {
        for (long blockNum = startBlock; blockNum <= endBlock; blockNum++) {
          final Optional<Hash> canonicalHash = blockchainStorage.getBlockHash(blockNum);
          for (final Hash blockHash : hashesToPrune(blockNum, canonicalHash)) {
            if (pruneBlocks) {
              removeChainData(
                  updater, blockHash, canonicalHash.filter(blockHash::equals).isPresent());
            } else {
              updater.removeBlockAccessList(blockHash);
            }
          }
          // BAL-only must retain the index for a later switch to ALL. In ALL, whichever pass
          // finishes a height last can discard it; pre-activation heights need no BAL pass.
          if (prunesBlockData
              && (pruneBlocks
                  ? blockNum <= lastPrunedBal
                      || canonicalHash
                          .flatMap(blockchainStorage::getBlockHeader)
                          .map(header -> !isBalEnabled.test(header))
                          .orElse(false)
                  : blockNum <= lastPrunedBlock)) {
            prunerStorage.removeForkBlocks(transaction, blockNum);
          }
        }
        // Storage commits complete their transactions even when they throw.
        dataCommitAttempted = true;
        updater.commit();
      } finally {
        if (!dataCommitAttempted) {
          updater.rollback();
        }
      }
      setPruningMark(transaction, pruneBlocks, endBlock);
      metadataCommitAttempted = true;
      transaction.commit();
      updateProgress(pruneBlocks, endBlock);
    } finally {
      if (!metadataCommitAttempted) {
        transaction.rollback();
      }
    }

    LogUtil.throttledLog(
        () -> LOG.info("Pruned {} data up to block {}", pruneBlocks ? "block" : "BAL", endBlock),
        logChainPruningProgress,
        LOG_PRUNING_PROGRESS_REPEAT_DELAY_SECONDS);
  }

  private void setPruningMark(
      final KeyValueStorageTransaction transaction, final boolean pruneBlocks, final long mark) {
    if (pruneBlocks) {
      prunerStorage.setBlockPruningMark(transaction, mark);
    } else {
      prunerStorage.setBalPruningMark(transaction, mark);
    }
  }

  private void updateProgress(final boolean pruneBlocks, final long mark) {
    if (pruneBlocks) {
      lastPrunedBlock = mark;
    } else {
      lastPrunedBal = mark;
    }
  }

  private Collection<Hash> hashesToPrune(final long blockNum, final Optional<Hash> canonicalHash) {
    final Collection<Hash> forkBlocks = prunerStorage.getForkBlocks(blockNum);
    // Snap / unsafe import may omit the canonical hash even when forks were indexed.
    canonicalHash.filter(hash -> !forkBlocks.contains(hash)).ifPresent(forkBlocks::add);
    return forkBlocks;
  }

  private boolean needsPruningJob(final long target, final long lastPruned) {
    return lastPruned == ABSENT_PRUNING_MARK ? target > 0 : shouldPrune(target, lastPruned);
  }

  private boolean shouldPrune(final long target, final long lastPruned) {
    // At least one block to prune, even with frequency 0.
    return target > lastPruned && (target - lastPruned) >= config.chainPruningFrequency();
  }

  private long cappedEndBlock(final long startBlock, final long targetEnd) {
    final long frequency = config.chainPruningFrequency();
    final long maxBlocks =
        frequency > 0 ? frequency * PRUNE_BATCH_FREQUENCY_MULTIPLIER : MIN_CATCH_UP_BLOCKS_PER_JOB;
    return Math.min(targetEnd, startBlock + maxBlocks - 1);
  }

  private void removeChainData(
      final BlockchainStorage.Updater updater, final Hash blockHash, final boolean isCanonical) {
    // eth/69 assumes headers are available from genesis, so the canonical header stays. Forks at
    // the same height carry no such requirement and would otherwise accumulate.
    if (!isCanonical) {
      updater.removeBlockHeader(blockHash);
    }
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
