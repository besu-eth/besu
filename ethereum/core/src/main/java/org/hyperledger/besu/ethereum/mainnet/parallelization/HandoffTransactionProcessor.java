/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.mainnet.parallelization;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BlockAccessListBuilder;
import org.hyperledger.besu.ethereum.mainnet.parallelization.BlockReadWriteSets.RawWrite;
import org.hyperledger.besu.ethereum.mainnet.parallelization.BlockReadWriteSets.ReadWriteSet;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.worldstate.JournaledAccount;
import org.hyperledger.besu.evm.worldstate.UpdateTrackingAccount;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.units.bigints.UInt256;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processes the blocks the BAL processor does not, like {@link
 * OptimisticConcurrentTransactionProcessor}, and runs a transaction again before block processing
 * reaches it once the values its run read have changed.
 *
 * <p>Each finished run leaves a read/write set, which {@link BlockReadWriteSets} indexes by slot. A
 * refresh worker runs the transactions whose read/write sets the writes before them invalidate
 * again, nearest to block processing first, with the slots they read seeded with the values the
 * transactions before them leave. Block processing takes a refreshed result when the block holds
 * every seed, and never waits for a refresh. It reports the writes of the transactions it executes
 * itself, which invalidates the read/write sets that read those slots at other values.
 */
public class HandoffTransactionProcessor extends OptimisticConcurrentTransactionProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(HandoffTransactionProcessor.class);

  // tests: block processing runs the refreshes itself before each transaction, so that they happen
  // on the writes of the transactions before it rather than whenever the worker gets to them
  private static volatile boolean refreshSynchronously = false;

  @VisibleForTesting
  static void refreshSynchronously(final boolean synchronously) {
    refreshSynchronously = synchronously;
  }

  private record BlockRun(
      ProtocolContext protocolContext,
      BlockHeader blockHeader,
      List<Transaction> transactions,
      Address miningBeneficiary,
      BlockHashLookup blockHashLookup,
      Wei blobGasPrice,
      Optional<BlockAccessListBuilder> blockAccessListBuilder,
      Optional<BlockHeader> maybeParentHeader) {}

  private final Optional<Counter> refreshCounter;
  private final Optional<Counter> refreshedTransactionCounter;

  private volatile @Nullable BlockReadWriteSets readWriteSets;
  private volatile @Nullable BlockRun blockRun;

  /**
   * Creates the processor of one block.
   *
   * @param transactionProcessor the processor of single transactions
   * @param refreshCounter counts the refreshes the block runs
   * @param refreshedTransactionCounter counts the transactions whose refreshed result is taken
   */
  public HandoffTransactionProcessor(
      final MainnetTransactionProcessor transactionProcessor,
      final Optional<Counter> refreshCounter,
      final Optional<Counter> refreshedTransactionCounter) {
    super(transactionProcessor);
    this.refreshCounter = refreshCounter;
    this.refreshedTransactionCounter = refreshedTransactionCounter;
  }

  @Override
  public void runAsyncBlock(
      final ProtocolContext protocolContext,
      final BlockHeader blockHeader,
      final List<Transaction> transactions,
      final Address miningBeneficiary,
      final BlockHashLookup blockHashLookup,
      final Wei blobGasPrice,
      final Executor executor,
      final Optional<BlockAccessListBuilder> blockAccessListBuilder,
      final Optional<BlockHeader> maybeParentHeader) {
    final BlockReadWriteSets sets = new BlockReadWriteSets(transactions.size());
    final BlockRun run =
        new BlockRun(
            protocolContext,
            blockHeader,
            transactions,
            miningBeneficiary,
            blockHashLookup,
            blobGasPrice,
            blockAccessListBuilder,
            maybeParentHeader);
    // set before the runs start, as each stores its read/write set when it ends
    readWriteSets = sets;
    blockRun = run;

    super.runAsyncBlock(
        protocolContext,
        blockHeader,
        transactions,
        miningBeneficiary,
        blockHashLookup,
        blobGasPrice,
        executor,
        blockAccessListBuilder,
        maybeParentHeader);

    // block processing reaches the first two transactions before a refresh could help them
    if (maybeParentHeader.isPresent() && transactions.size() > 2 && !refreshSynchronously) {
      try {
        BlockProcessingExecutors.refreshExecutor().execute(() -> refreshInvalidated(sets, run));
      } catch (final RejectedExecutionException e) {
        LOG.debug("Could not start refreshing the block's runs", e);
      }
    }
  }

  @Override
  protected void onRunFinished(
      final int transactionLocation,
      final ParallelizedTransactionContext context,
      final @Nullable Map<Address, Map<StorageSlotKey, UInt256>> seeds) {
    final BlockReadWriteSets sets = readWriteSets;
    final BlockRun run = blockRun;
    // a refresh stores its read/write set itself
    if (seeds != null
        || sets == null
        || run == null
        || context == null
        || context.transactionProcessingResult().isInvalid()) {
      return;
    }

    sets.store(transactionLocation, ReadWriteSet.of(context, run.miningBeneficiary(), null), true);
  }

  @Override
  public Optional<TransactionProcessingResult> getProcessingResult(
      final MutableWorldState worldState,
      final Address miningBeneficiary,
      final Transaction transaction,
      final int transactionLocation,
      final Optional<Counter> confirmedParallelizedTransactionCounter,
      final Optional<Counter> conflictingButCachedTransactionCounter) {
    final BlockReadWriteSets sets = readWriteSets;
    if (sets == null) {
      return super.getProcessingResult(
          worldState,
          miningBeneficiary,
          transaction,
          transactionLocation,
          confirmedParallelizedTransactionCounter,
          conflictingButCachedTransactionCounter);
    }

    final BlockRun run = blockRun;
    if (refreshSynchronously && run != null) {
      sets.applyReported();
      int position;
      while ((position = sets.takeInvalidated(transactionLocation - 1)) >= 0) {
        refresh(sets, run, position);
      }
    }

    sets.blockProcessingReached(transactionLocation);
    final ReadWriteSet latest = sets.latest(transactionLocation);
    if (latest != null && latest.refreshed) {
      // the refresh started from later values than the runs the optimistic processor started
      reachedWithoutRuns(transactionLocation);
      final Optional<TransactionProcessingResult> result =
          takeOrReject(
              worldState,
              miningBeneficiary,
              transaction,
              latest.context,
              confirmedParallelizedTransactionCounter,
              conflictingButCachedTransactionCounter);
      if (result.isPresent()) {
        refreshedTransactionCounter.ifPresent(Counter::inc);
        sets.reportTaken(transactionLocation);
      }

      return result;
    }

    final Optional<TransactionProcessingResult> result =
        super.getProcessingResult(
            worldState,
            miningBeneficiary,
            transaction,
            transactionLocation,
            confirmedParallelizedTransactionCounter,
            conflictingButCachedTransactionCounter);
    if (result.isPresent()) {
      sets.reportTaken(transactionLocation);
    }

    return result;
  }

  @Override
  public void onExecutedSequentially(final int location, final WorldUpdater transactionUpdater) {
    final BlockReadWriteSets sets = readWriteSets;
    if (sets == null) {
      return;
    }

    // read from the updater on this thread before the commit, hashed on the refresh worker
    final List<RawWrite> writes = new ArrayList<>();
    final Set<Address> cleared = new HashSet<>();
    for (final Account account : transactionUpdater.getTouchedAccounts()) {
      if (account instanceof MutableAccount mutable) {
        final Address address = mutable.getAddress();
        if (wasStorageCleared(mutable)) {
          cleared.add(address);
        }

        for (final Map.Entry<UInt256, UInt256> slot : mutable.getUpdatedStorage().entrySet()) {
          writes.add(new RawWrite(address, slot.getKey(), slot.getValue()));
        }
      }
    }

    cleared.addAll(transactionUpdater.getDeletedAccountAddresses());
    sets.reportExecuted(location, writes, cleared);
  }

  // the stacked and the journaled updater each track a cleared storage on their own accounts
  private static boolean wasStorageCleared(final MutableAccount account) {
    return (account instanceof UpdateTrackingAccount<?> tracked && tracked.getStorageWasCleared())
        || (account instanceof JournaledAccount journaled && journaled.getStorageWasCleared());
  }

  @Override
  public void abort() {
    final BlockReadWriteSets sets = readWriteSets;
    if (sets != null) {
      sets.finish();
    }

    readWriteSets = null;
    blockRun = null;
    super.abort();
  }

  /**
   * Refreshes the read/write sets the block's earlier transactions invalidate, nearest to block
   * processing first, until block processing reaches the block's last transaction. The transaction
   * block processing reaches next is left to it.
   */
  private void refreshInvalidated(final BlockReadWriteSets sets, final BlockRun run) {
    try {
      while (!sets.isFinished()) {
        sets.applyReported();
        final int processing = sets.blockProcessingAt();
        if (processing >= sets.size() - 1) {
          return;
        }

        final int position = sets.takeInvalidated(processing + 1);
        if (position >= 0) {
          refresh(sets, run, position);
          continue;
        }

        sets.waitForWork();
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (final RuntimeException e) {
      LOG.debug("Error refreshing the block's runs", e);
    }
  }

  private void refresh(final BlockReadWriteSets sets, final BlockRun run, final int position) {
    final ReadWriteSet invalidated = sets.latest(position);
    // a chain member runs on its predecessor's result, which a refresh on the parent state lacks
    if (invalidated == null
        || !invalidated.refreshable
        || invalidated.context.chainPredecessorAccumulator() != null) {
      return;
    }

    final Map<Address, Map<StorageSlotKey, UInt256>> seeds = sets.seedsFor(position, invalidated);
    if (seeds == null || sets.blockProcessingAt() >= position) {
      return;
    }

    final ParallelizedTransactionContext context;
    try {
      context =
          runSeeded(
              run.protocolContext(),
              run.blockHeader(),
              position,
              run.transactions().get(position),
              run.miningBeneficiary(),
              run.blockHashLookup(),
              run.blobGasPrice(),
              run.blockAccessListBuilder(),
              run.maybeParentHeader(),
              seeds);
    } catch (final RuntimeException e) {
      LOG.debug("Error refreshing the run of transaction {}", position, e);
      return;
    }

    refreshCounter.ifPresent(Counter::inc);
    if (context == null || context.transactionProcessingResult().isInvalid()) {
      return;
    }

    sets.store(position, ReadWriteSet.of(context, run.miningBeneficiary(), context.seeds()), false);
  }
}
