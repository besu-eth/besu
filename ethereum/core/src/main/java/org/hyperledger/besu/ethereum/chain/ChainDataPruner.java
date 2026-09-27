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
  private final ChainDataPrunerStorage prunerStorage;
  private final ChainPrunerConfiguration config;
  private final ExecutorService pruningExecutor;
  private final AtomicBoolean logChainPruningProgress = new AtomicBoolean(true);
  private boolean derivedStartResolved = false;

  public ChainDataPruner(
      final BlockchainStorage blockchainStorage,
      final ChainDataPrunerStorage prunerStorage,
      final ChainPrunerConfiguration config,
      final ExecutorService pruningExecutor) {
    this.blockchainStorage = blockchainStorage;
    this.prunerStorage = prunerStorage;
    this.config = config;
    this.pruningExecutor = pruningExecutor;
  }

  @Override
  public void onBlockAdded(final BlockAddedEvent event) {
    chainPrunerAction(event);
  }

  /**
   * Returns the lowest block whose body and receipts a back-fill can store at this chain head: the
   * lowest one still retained a pruning run later, so that a run starting while the back-fill
   * stores a block never passes over it.
   */
  public long lowestToBackfill(final long chainHead) {
    return lowestRetained(chainHead + config.chainPruningFrequency());
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
    // Validate on every event, including non-canonical forks: a fork below the mark means
    // retained may be too small. Canonical heads then share pruneForSyncedHead with snap-sync.
    final long storedPruningMark = prunerStorage.getChainPruningMark().orElse(1L);
    validatePruningMark(header.getNumber(), storedPruningMark);
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
   * canonical head. Called with the new chain head header.
   */
  public void pruneForSyncedHead(final BlockHeader header) {
    // Never default the mark to the current head: during snap, the first BlockAddedEvent is the
    // tip and that used to persist a mark that skipped all historical blocks (issue #11131).
    final long storedPruningMark = prunerStorage.getChainPruningMark().orElse(1L);
    validatePruningMark(header.getNumber(), storedPruningMark);
    try {
      pruningExecutor.submit(() -> pruneChainAndBalData(header, storedPruningMark));
    } catch (final RejectedExecutionException e) {
      LOG.debug(
          "Chain pruning task rejected for head {}; will retry on the next head update",
          header.getNumber());
    }
  }

  /**
   * The lowest block number this pruner guarantees to keep bodies and receipts for. Advertised as
   * the eth/69 block range.
   *
   * @param headBlockNumber the current chain head number
   * @return the earliest retained block number, or empty if this pruner never removes bodies
   */
  public Optional<Long> getEarliestRetainedBlock(final long headBlockNumber) {
    if (!config.isBlockPruningEnabled()) {
      return Optional.empty();
    }
    // pruneChainAndBalData deletes to head - retained - pruneSlackBlocks, so the window
    // itself is always still backed by data.
    final long bound = headBlockNumber - config.chainPruningBlocksRetained() + 1;
    // Covers retention being raised after blocks were already deleted.
    final long lowestUnpruned =
        prunerStorage.getChainPruningMark().map(mark -> mark + 1).orElse(0L);
    return Optional.of(Math.max(bound, lowestUnpruned));
  }

  private void validatePruningMark(final long blockNumber, final long storedPruningMark) {
    if (blockNumber < storedPruningMark) {
      LOG.warn(
          "Block number {} is less than the pruning mark {} - --Xchain-pruning-blocks-retained may be too small",
          blockNumber,
          storedPruningMark);
    }
  }

  /** Indexes a block hash by height so non-canonical blocks can be found when pruning. */
  private void recordForkBlock(final BlockAddedEvent event, final long blockNumber) {
    prunerStorage.addForkBlock(blockNumber, event.getHeader().getHash());
  }

  /**
   * The block to start the next pruning batch from. The data tells us where to begin, our own
   * progress tells us where to continue: on the first job of this process, a mark left behind by
   * BAL-only pruning says nothing about where block data starts, so it is replaced with the first
   * block the data itself still holds a body for. Every job after that trusts the mark, which by
   * then reflects this pruner's own writes.
   *
   * @param storedPruningMark the persisted pruning mark
   * @param headBlockNumber the current chain head number, bounding the derivation search
   * @return the block number to start the next pruning batch from
   */
  private long startBlock(final long storedPruningMark, final long headBlockNumber) {
    if (!config.isBlockPruningEnabled()) {
      return storedPruningMark;
    }
    if (!derivedStartResolved) {
      derivedStartResolved = true;
      final Optional<Long> derived =
          BlockDataBoundary.firstBlockWithBody(
              BlockDataBoundary.blockDataIn(blockchainStorage), headBlockNumber);
      if (derived.isPresent()) {
        return derived.get();
      }
    }
    return storedPruningMark;
  }

  private void pruneChainAndBalData(final BlockHeader header, final long storedPruningMark) {
    final long pruningMark =
        header.getNumber() - config.chainPruningBlocksRetained() - config.pruneSlackBlocks();

    if (!shouldPrune(pruningMark, storedPruningMark)) {
      return;
    }
    // A chain with no access lists yet has nothing for BAL-only pruning to delete. Return without
    // advancing the mark: with a single mark, moving it here would make a later switch to ALL skip
    // everything below, silently.
    if (!config.isBlockPruningEnabled() && header.getBalHash().isEmpty()) {
      return;
    }

    final long startBlock = startBlock(storedPruningMark, header.getNumber());
    final KeyValueStorageTransaction pruningTransaction = prunerStorage.startTransaction();
    final BlockchainStorage.Updater updater = blockchainStorage.updater();
    long currentMark = startBlock;
    final long endBlock = cappedEndBlock(startBlock, pruningMark);

    for (long blockNum = startBlock; blockNum <= endBlock; blockNum++) {
      if (blockNum < 1) {
        continue;
      }
      final Collection<Hash> hashes = hashesToPrune(blockNum);
      final Optional<Hash> canonicalHash = blockchainStorage.getBlockHash(blockNum);
      for (final Hash blockHash : hashes) {
        if (config.isBlockPruningEnabled()) {
          removeChainData(updater, blockHash, canonicalHash.filter(blockHash::equals).isPresent());
        }
        updater.removeBlockAccessList(blockHash);
      }
      prunerStorage.removeForkBlocks(pruningTransaction, blockNum);
      currentMark = blockNum;
    }
    updater.commit();
    prunerStorage.setChainPruningMark(pruningTransaction, currentMark);
    pruningTransaction.commit();

    final long loggedMark = currentMark;
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
