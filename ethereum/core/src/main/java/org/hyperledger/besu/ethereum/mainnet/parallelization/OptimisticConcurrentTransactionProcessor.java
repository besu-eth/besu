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
package org.hyperledger.besu.ethereum.mainnet.parallelization;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.AccessLocationTracker;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BlockAccessListBuilder;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.evm.worldstate.WorldView;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import com.google.common.annotations.VisibleForTesting;

/**
 * Optimizes transaction processing by executing transactions in parallel within a given block.
 * Transactions are executed optimistically in a non-blocking manner. After execution, the class
 * checks for potential conflicts among transactions to ensure data integrity before applying the
 * results to the world state.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class OptimisticConcurrentTransactionProcessor extends ParallelBlockTransactionProcessor {

  private final MainnetTransactionProcessor transactionProcessor;

  private final TransactionCollisionDetector transactionCollisionDetector;

  private final Executor warmUpExecutor;

  // Index the serial loop has reached. A speculative run for that index or an earlier one can no
  // longer be used, so workers skip it instead of competing with the import for cores.
  private final AtomicInteger serialLoopPosition = new AtomicInteger(-1);
  // chained runs of transactions whose sender was recovered only after they ran alone
  private volatile AtomicReferenceArray<CompletableFuture<ParallelizedTransactionContext>>
      lateChainFutures = new AtomicReferenceArray<>(0);
  // senders recovered per task, few enough to spread over the executor's threads
  private static final int SENDER_RECOVERY_BATCH = 8;

  /**
   * Constructs a PreloadConcurrentTransactionProcessor with a specified transaction processor. This
   * processor is responsible for the individual processing of transactions.
   *
   * @param transactionProcessor The transaction processor for processing individual transactions.
   */
  public OptimisticConcurrentTransactionProcessor(
      final MainnetTransactionProcessor transactionProcessor) {
    this(
        transactionProcessor,
        new TransactionCollisionDetector(),
        BlockProcessingExecutors.ioExecutor());
  }

  @VisibleForTesting
  public OptimisticConcurrentTransactionProcessor(
      final MainnetTransactionProcessor transactionProcessor,
      final TransactionCollisionDetector transactionCollisionDetector) {
    this(transactionProcessor, transactionCollisionDetector, warmUp -> {});
  }

  @VisibleForTesting
  OptimisticConcurrentTransactionProcessor(
      final MainnetTransactionProcessor transactionProcessor,
      final TransactionCollisionDetector transactionCollisionDetector,
      final Executor warmUpExecutor) {
    this.transactionProcessor = transactionProcessor;
    this.transactionCollisionDetector = transactionCollisionDetector;
    this.warmUpExecutor = warmUpExecutor;
  }

  /**
   * Transactions of one sender are chained on one worker so that each sees the nonce and balance
   * left by the previous one; run alone against the parent state they would fail the nonce check
   * before touching any state. A chained result is reused when the block, at its turn, holds the
   * state the chain assumed; otherwise it still turns a cold re-execution into a warm one. Senders
   * the transaction pool did not know are recovered first, in parallel.
   */
  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
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

    final Map<Address, List<Integer>> bySender = new HashMap<>();
    final List<Integer> unknownSenders = new ArrayList<>();
    for (int i = 0; i < transactions.size(); i++) {
      final int txIndex = i;
      transactions
          .get(i)
          .getSenderIfKnown()
          .ifPresentOrElse(
              sender -> bySender.computeIfAbsent(sender, k -> new ArrayList<>(1)).add(txIndex),
              () -> unknownSenders.add(txIndex));
    }

    futures = new CompletableFuture[transactions.size()];
    lateChainFutures = new AtomicReferenceArray<>(transactions.size());
    for (int i = 0; i < transactions.size(); i++) {
      final Transaction transaction = transactions.get(i);
      final List<Integer> chain = transaction.getSenderIfKnown().map(bySender::get).orElse(null);
      if (chain == null || chain.size() == 1) {
        final int txIndex = i;
        futures[i] =
            CompletableFuture.supplyAsync(
                () ->
                    serialLoopHasPassed(txIndex)
                        ? null
                        : runTransaction(
                            protocolContext,
                            blockHeader,
                            txIndex,
                            transaction,
                            miningBeneficiary,
                            blockHashLookup,
                            blobGasPrice,
                            blockAccessListBuilder,
                            maybeParentHeader,
                            null),
                executor);
      } else if (chain.get(0) == i) {
        final List<CompletableFuture<ParallelizedTransactionContext>> chainFutures =
            new ArrayList<>(chain.size());
        for (final int member : chain) {
          futures[member] = new CompletableFuture<>();
          chainFutures.add(futures[member]);
        }
        executor.execute(
            () ->
                runChain(
                    protocolContext,
                    blockHeader,
                    transactions,
                    chain,
                    chainFutures,
                    miningBeneficiary,
                    blockHashLookup,
                    blobGasPrice,
                    blockAccessListBuilder,
                    maybeParentHeader));
        // a member runs only after its predecessors, so what it reads is fetched meanwhile
        for (int k = 1; k < chain.size(); k++) {
          final int member = chain.get(k);
          final CompletableFuture<ParallelizedTransactionContext> future = chainFutures.get(k);
          warmUpExecutor.execute(
              () ->
                  warmUp(
                      protocolContext,
                      blockHeader,
                      member,
                      transactions.get(member),
                      future,
                      miningBeneficiary,
                      blockHashLookup,
                      blobGasPrice,
                      maybeParentHeader));
        }
      }
    }
    if (!unknownSenders.isEmpty()) {
      chainUnknownSenders(
          protocolContext,
          blockHeader,
          transactions,
          unknownSenders,
          miningBeneficiary,
          blockHashLookup,
          blobGasPrice,
          executor,
          blockAccessListBuilder,
          maybeParentHeader);
    }
  }

  /**
   * The transaction pool knows the senders of the transactions it saw, but not those sent to block
   * builders directly, so those run alone at first and the later ones of a sender fail their nonce
   * check. Their senders are recovered meanwhile on the I/O executor, and the transactions that
   * turn out to share a sender run again chained on the result of the first one, which ran alone
   * correctly.
   */
  private void chainUnknownSenders(
      final ProtocolContext protocolContext,
      final BlockHeader blockHeader,
      final List<Transaction> transactions,
      final List<Integer> unknownSenders,
      final Address miningBeneficiary,
      final BlockHashLookup blockHashLookup,
      final Wei blobGasPrice,
      final Executor executor,
      final Optional<BlockAccessListBuilder> blockAccessListBuilder,
      final Optional<BlockHeader> maybeParentHeader) {
    final Map<Integer, CompletableFuture<ParallelizedTransactionContext>> aloneRuns =
        new HashMap<>();
    for (final int txIndex : unknownSenders) {
      aloneRuns.put(txIndex, futures[txIndex]);
    }
    final List<CompletableFuture<Void>> recoveries = new ArrayList<>();
    for (int from = 0; from < unknownSenders.size(); from += SENDER_RECOVERY_BATCH) {
      final List<Integer> batch =
          unknownSenders.subList(
              from, Math.min(unknownSenders.size(), from + SENDER_RECOVERY_BATCH));
      recoveries.add(
          CompletableFuture.runAsync(
              () -> batch.forEach(txIndex -> recoverSender(transactions.get(txIndex))),
              warmUpExecutor));
    }
    CompletableFuture.allOf(recoveries.toArray(CompletableFuture[]::new))
        .thenRunAsync(
            () -> {
              final Map<Address, List<Integer>> bySender = new HashMap<>();
              for (final int txIndex : unknownSenders) {
                transactions
                    .get(txIndex)
                    .getSenderIfKnown()
                    .ifPresent(
                        sender ->
                            bySender.computeIfAbsent(sender, k -> new ArrayList<>(1)).add(txIndex));
              }
              for (final List<Integer> chain : bySender.values()) {
                if (chain.size() < 2) {
                  continue;
                }
                final List<CompletableFuture<ParallelizedTransactionContext>> chainFutures =
                    new ArrayList<>(chain.size());
                chainFutures.add(aloneRuns.get(chain.get(0)));
                for (int k = 1; k < chain.size(); k++) {
                  final CompletableFuture<ParallelizedTransactionContext> future =
                      new CompletableFuture<>();
                  lateChainFutures.set(chain.get(k), future);
                  chainFutures.add(future);
                }
                chainFutures
                    .get(0)
                    .whenCompleteAsync(
                        (first, failure) ->
                            runChainAfter(
                                first,
                                protocolContext,
                                blockHeader,
                                transactions,
                                chain,
                                chainFutures,
                                miningBeneficiary,
                                blockHashLookup,
                                blobGasPrice,
                                blockAccessListBuilder,
                                maybeParentHeader),
                        executor);
              }
            },
            warmUpExecutor);
  }

  private static void recoverSender(final Transaction transaction) {
    try {
      transaction.getSender();
    } catch (final IllegalStateException | IllegalArgumentException e) {
      // an unrecoverable signature leaves the transaction unchained, its own run reports it
    }
  }

  /** Runs the rest of a chain on the result of its first member, which ran alone. */
  private void runChainAfter(
      final ParallelizedTransactionContext first,
      final ProtocolContext protocolContext,
      final BlockHeader blockHeader,
      final List<Transaction> transactions,
      final List<Integer> chain,
      final List<CompletableFuture<ParallelizedTransactionContext>> chainFutures,
      final Address miningBeneficiary,
      final BlockHashLookup blockHashLookup,
      final Wei blobGasPrice,
      final Optional<BlockAccessListBuilder> blockAccessListBuilder,
      final Optional<BlockHeader> maybeParentHeader) {
    PathBasedWorldStateUpdateAccumulator<?> previous =
        first == null || first.transactionProcessingResult().isInvalid()
            ? null
            : first.transactionAccumulator();
    for (int k = 1; k < chain.size(); k++) {
      final int txIndex = chain.get(k);
      final CompletableFuture<ParallelizedTransactionContext> future = chainFutures.get(k);
      if (previous == null || future.isCancelled() || serialLoopHasPassed(txIndex)) {
        for (int j = k; j < chain.size(); j++) {
          chainFutures.get(j).complete(null);
        }
        return;
      }
      final ParallelizedTransactionContext context =
          runTransaction(
              protocolContext,
              blockHeader,
              txIndex,
              transactions.get(txIndex),
              miningBeneficiary,
              blockHashLookup,
              blobGasPrice,
              blockAccessListBuilder,
              maybeParentHeader,
              previous);
      future.complete(context);
      previous = context == null ? null : context.transactionAccumulator();
    }
  }

  /**
   * Executes a chained transaction against the parent state only for what it reads, so that its run
   * on top of its predecessors finds that state cached. Its nonce and balance checks are relaxed as
   * the predecessors have not run, and its result is discarded.
   */
  private void warmUp(
      final ProtocolContext protocolContext,
      final BlockHeader blockHeader,
      final int transactionLocation,
      final Transaction transaction,
      final CompletableFuture<ParallelizedTransactionContext> future,
      final Address miningBeneficiary,
      final BlockHashLookup blockHashLookup,
      final Wei blobGasPrice,
      final Optional<BlockHeader> maybeParentHeader) {
    if (future.isDone()
        || serialLoopHasPassed(transactionLocation)
        || maybeParentHeader.isEmpty()) {
      return;
    }
    final BonsaiWorldState ws =
        getWorldState(protocolContext, maybeParentHeader.get()).orElse(null);
    if (ws == null) {
      return;
    }
    try {
      ws.disableCacheMerkleTrieLoader();
      transactionProcessor.processTransaction(
          ws.updater().updater(),
          blockHeader,
          transaction.detachedCopy(),
          miningBeneficiary,
          OperationTracer.NO_TRACING,
          blockHashLookup.forkForParallelWorker(),
          TransactionValidationParams.transactionSimulatorAllowExceedingBalanceAndFutureNonce(),
          blobGasPrice,
          Optional.empty());
    } catch (final RuntimeException e) {
      // only fetching state, the chained run reports any failure
    } finally {
      ws.close();
    }
  }

  private void runChain(
      final ProtocolContext protocolContext,
      final BlockHeader blockHeader,
      final List<Transaction> transactions,
      final List<Integer> chain,
      final List<CompletableFuture<ParallelizedTransactionContext>> chainFutures,
      final Address miningBeneficiary,
      final BlockHashLookup blockHashLookup,
      final Wei blobGasPrice,
      final Optional<BlockAccessListBuilder> blockAccessListBuilder,
      final Optional<BlockHeader> maybeParentHeader) {
    PathBasedWorldStateUpdateAccumulator<?> previous = null;
    for (int k = 0; k < chain.size(); k++) {
      final int txIndex = chain.get(k);
      final CompletableFuture<ParallelizedTransactionContext> future = chainFutures.get(k);
      // Once the serial loop has reached a member, the rest of the chain would run without the
      // state that member leaves behind, so the whole remainder is dropped.
      if (future.isCancelled() || serialLoopHasPassed(txIndex)) {
        for (int j = k; j < chain.size(); j++) {
          chainFutures.get(j).complete(null);
        }
        return;
      }
      final ParallelizedTransactionContext context =
          runTransaction(
              protocolContext,
              blockHeader,
              txIndex,
              transactions.get(txIndex),
              miningBeneficiary,
              blockHashLookup,
              blobGasPrice,
              blockAccessListBuilder,
              maybeParentHeader,
              previous);
      future.complete(context);
      if (context == null) {
        for (int j = k + 1; j < chain.size(); j++) {
          chainFutures.get(j).complete(null);
        }
        return;
      }
      previous = context.transactionAccumulator();
    }
  }

  private boolean serialLoopHasPassed(final int txIndex) {
    return txIndex <= serialLoopPosition.get();
  }

  @Override
  @VisibleForTesting
  protected ParallelizedTransactionContext runTransaction(
      final ProtocolContext protocolContext,
      final BlockHeader blockHeader,
      final int transactionLocation,
      final Transaction transaction,
      final Address miningBeneficiary,
      final BlockHashLookup blockHashLookup,
      final Wei blobGasPrice,
      final Optional<BlockAccessListBuilder> blockAccessListBuilder,
      final Optional<BlockHeader> maybeParentHeader) {
    return runTransaction(
        protocolContext,
        blockHeader,
        transactionLocation,
        transaction,
        miningBeneficiary,
        blockHashLookup,
        blobGasPrice,
        blockAccessListBuilder,
        maybeParentHeader,
        null);
  }

  private ParallelizedTransactionContext runTransaction(
      final ProtocolContext protocolContext,
      final BlockHeader blockHeader,
      final int transactionLocation,
      final Transaction transaction,
      final Address miningBeneficiary,
      final BlockHashLookup blockHashLookup,
      final Wei blobGasPrice,
      final Optional<BlockAccessListBuilder> blockAccessListBuilder,
      final Optional<BlockHeader> maybeParentHeader,
      final PathBasedWorldStateUpdateAccumulator<?> previousInChain) {

    if (maybeParentHeader.isEmpty()) {
      return null;
    }
    final BonsaiWorldState ws =
        getWorldState(protocolContext, maybeParentHeader.get()).orElse(null);
    if (ws == null) {
      return null;
    }

    try {
      ws.disableCacheMerkleTrieLoader();
      final ParallelizedTransactionContext.Builder contextBuilder =
          new ParallelizedTransactionContext.Builder();
      final PathBasedWorldStateUpdateAccumulator<?> roundWorldStateUpdater =
          (PathBasedWorldStateUpdateAccumulator<?>) ws.updater();
      if (previousInChain != null) {
        ((PathBasedWorldStateUpdateAccumulator) roundWorldStateUpdater)
            .importStateChangesFromSource(previousInChain);
        contextBuilder.chainPredecessorAccumulator(previousInChain);
      }
      final WorldUpdater transactionUpdater = roundWorldStateUpdater.updater();
      final Optional<AccessLocationTracker> transactionLocationTracker =
          blockAccessListBuilder.map(
              b ->
                  BlockAccessListBuilder.createTransactionAccessLocationTracker(
                      transactionLocation));
      final TransactionProcessingResult result =
          transactionProcessor.processTransaction(
              transactionUpdater,
              blockHeader,
              transaction.detachedCopy(),
              miningBeneficiary,
              new OperationTracer() {
                @Override
                public boolean tracesOperations() {
                  return false;
                }

                @Override
                public void traceBeforeRewardTransaction(
                    final WorldView worldView,
                    final org.hyperledger.besu.datatypes.Transaction tx,
                    final Wei miningReward) {
                  /*
                   * This part checks if the mining beneficiary's account was accessed before increasing its balance for rewards.
                   * Indeed, if the transaction has interacted with the address to read or modify it,
                   * it means that the value is necessary for the proper execution of the transaction and will therefore be considered in collision detection.
                   * If this is not the case, we can ignore this address during conflict detection.
                   */
                  if (transactionCollisionDetector
                      .getAddressesTouchedByTransaction(
                          transaction, Optional.of(roundWorldStateUpdater))
                      .contains(miningBeneficiary)) {
                    contextBuilder.isMiningBeneficiaryTouchedPreRewardByTransaction(true);
                  }
                  contextBuilder.miningBeneficiaryReward(miningReward);
                }
              },
              blockHashLookup.forkForParallelWorker(),
              TransactionValidationParams.processingBlock(),
              blobGasPrice,
              transactionLocationTracker);

      // commit the accumulator in order to apply all the modifications
      transactionUpdater.commit();
      roundWorldStateUpdater.commit();

      contextBuilder
          .transactionAccumulator(ws.getAccumulator())
          .transactionProcessingResult(result);

      final ParallelizedTransactionContext parallelizedTransactionContext = contextBuilder.build();
      if (!parallelizedTransactionContext.isMiningBeneficiaryTouchedPreRewardByTransaction()) {
        /*
         * If the address of the mining beneficiary has been touched only for adding rewards,
         * we remove it from the accumulator to avoid a false positive collision. Its code and
         * storage are removed too, as the merge would otherwise overwrite the block's ones while
         * the collision detection skips it. It stays in the deleted accounts, so a deletion still
         * collides. The balance will be increased during the sequential processing.
         */
        roundWorldStateUpdater.getAccountsToUpdate().remove(miningBeneficiary);
        roundWorldStateUpdater.getCodeToUpdate().remove(miningBeneficiary);
        roundWorldStateUpdater.getStorageToUpdate().remove(miningBeneficiary);
        roundWorldStateUpdater.getStorageToClear().remove(miningBeneficiary);
      }
      return parallelizedTransactionContext;
    } catch (Exception ex) {
      // no op as failing to get worldstate
      return null;
    } finally {
      if (ws != null) ws.close();
    }
  }

  /**
   * Applies the results of parallelized transactions to the world state after checking for
   * conflicts.
   *
   * <p>If a transaction was executed optimistically without any detected conflicts, its result is
   * directly applied to the world state. If there is a conflict, this method does not apply the
   * transaction's modifications directly to the world state. Instead, it caches the data read from
   * the database during the transaction's execution. This cached data is then used to optimize the
   * replay of the transaction by reducing the need for additional reads from the disk, thereby
   * making the replay process faster. This approach ensures that the integrity of the world state
   * is maintained while optimizing the performance of transaction processing.
   *
   * @param worldState Mutable world state intended for applying transaction results.
   * @param miningBeneficiary Address of the beneficiary for mining rewards.
   * @param transaction Transaction for which the result is to be applied.
   * @param transactionLocation Index of the transaction within the block.
   * @param confirmedParallelizedTransactionCounter Metric counter for confirmed parallelized
   *     transactions
   * @param conflictingButCachedTransactionCounter Metric counter for conflicting but cached
   *     transactions
   * @return Optional containing the transaction processing result if applied, or empty if the
   *     transaction needs to be replayed due to a conflict.
   */
  @Override
  public Optional<TransactionProcessingResult> getProcessingResult(
      final MutableWorldState worldState,
      final Address miningBeneficiary,
      final Transaction transaction,
      final int transactionLocation,
      final Optional<Counter> confirmedParallelizedTransactionCounter,
      final Optional<Counter> conflictingButCachedTransactionCounter) {

    serialLoopPosition.set(transactionLocation);
    final CompletableFuture<ParallelizedTransactionContext> aloneOrChained =
        removeFuture(transactionLocation);
    // a chained run replaces the run alone that failed its nonce check
    final CompletableFuture<ParallelizedTransactionContext> lateChained =
        transactionLocation < lateChainFutures.length()
            ? lateChainFutures.getAndSet(transactionLocation, null)
            : null;
    final CompletableFuture<ParallelizedTransactionContext> future =
        lateChained != null ? lateChained : aloneOrChained;

    if (future != null && future.isDone()) {
      final ParallelizedTransactionContext parallelizedTransactionContext = future.resultNow();
      if (parallelizedTransactionContext == null) {
        return Optional.empty();
      }

      final PathBasedWorldState pathBasedWorldState = (PathBasedWorldState) worldState;
      final PathBasedWorldStateUpdateAccumulator blockAccumulator =
          (PathBasedWorldStateUpdateAccumulator) pathBasedWorldState.updater();
      final PathBasedWorldStateUpdateAccumulator<?> transactionAccumulator =
          parallelizedTransactionContext.transactionAccumulator();
      final TransactionProcessingResult transactionProcessingResult =
          parallelizedTransactionContext.transactionProcessingResult();
      // a reverted execution is as repeatable as a successful one when nothing it read changed;
      // an invalid result is replayed anyway, and its sender may not even be recoverable
      if (!transactionProcessingResult.isInvalid()
          && !transactionCollisionDetector.hasCollision(
              transaction, miningBeneficiary, parallelizedTransactionContext, blockAccumulator)) {
        final Wei reward = parallelizedTransactionContext.miningBeneficiaryReward();
        if (!reward.isZero() || !transactionProcessor.getClearEmptyAccounts()) {
          final MutableAccount miningBeneficiaryAccount =
              blockAccumulator.getOrCreate(miningBeneficiary);
          miningBeneficiaryAccount.incrementBalance(reward);

          if (!reward.isZero()) {
            final Wei miningBeneficiaryPostBalance = miningBeneficiaryAccount.getBalance();
            transactionProcessingResult
                .getPartialBlockAccessView()
                .ifPresent(
                    partialBlockAccessView ->
                        partialBlockAccessView.accountChanges().stream()
                            .filter(
                                accountChanges ->
                                    accountChanges.getAddress().equals(miningBeneficiary))
                            .findFirst()
                            .ifPresent(
                                accountChanges ->
                                    accountChanges.setPostBalance(miningBeneficiaryPostBalance)));
          }
        }

        blockAccumulator.importStateChangesFromSource(transactionAccumulator);

        if (confirmedParallelizedTransactionCounter.isPresent()) {
          confirmedParallelizedTransactionCounter.get().inc();
          transactionProcessingResult.setIsProcessedInParallel(Optional.of(Boolean.TRUE));
          transactionProcessingResult.accumulator = transactionAccumulator;
        }
        return Optional.of(transactionProcessingResult);
      } else {
        blockAccumulator.importPriorStateFromSource(transactionAccumulator);
        if (conflictingButCachedTransactionCounter.isPresent())
          conflictingButCachedTransactionCounter.get().inc();
        // If there is a conflict, we return an empty result to signal the block processor to
        // re-execute the transaction.
        return Optional.empty();
      }
    }
    if (future != null) {
      future.cancel(true);
    }
    return Optional.empty();
  }
}
