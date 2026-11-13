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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Optimizes transaction processing by executing transactions in parallel within a given block.
 * Transactions are executed optimistically in a non-blocking manner. After execution, the class
 * checks for potential conflicts among transactions to ensure data integrity before applying the
 * results to the world state.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class OptimisticConcurrentTransactionProcessor extends ParallelBlockTransactionProcessor {

  private static final Logger LOG =
      LoggerFactory.getLogger(OptimisticConcurrentTransactionProcessor.class);

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
  // A member's result holds everything the members before it changed, which the serial loop
  // checks and merges again, so a chain costs the square of its length. Members past the cap run
  // alone.
  @VisibleForTesting static final int MAX_CHAIN_LENGTH = 64;

  // a warm-up runs before the member's predecessors, which may pay its nonce and gas
  @VisibleForTesting
  static final TransactionValidationParams WARM_UP_VALIDATION =
      TransactionValidationParams.transactionSimulatorAllowUnderpricedAndFutureNonce();

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

    // looked up once, as the transaction pool may recover a sender while the runs are set up
    final Address[] senders = new Address[transactions.size()];
    final Map<Address, List<Integer>> bySender = new HashMap<>();
    final List<Integer> unknownSenders = new ArrayList<>();
    for (int i = 0; i < transactions.size(); i++) {
      senders[i] = transactions.get(i).getSenderIfKnown().orElse(null);
      if (senders[i] == null) {
        unknownSenders.add(i);
      } else {
        bySender.computeIfAbsent(senders[i], k -> new ArrayList<>(1)).add(i);
      }
    }

    futures = new CompletableFuture[transactions.size()];
    lateChainFutures = new AtomicReferenceArray<>(transactions.size());
    // queued ahead of the runs, as chaining the unknown senders waits for every recovery
    final CompletableFuture<Void> recovered =
        recoverSenders(transactions, unknownSenders, executor);
    for (int i = 0; i < transactions.size(); i++) {
      final Transaction transaction = transactions.get(i);
      final List<Integer> chain = senders[i] == null ? null : capped(bySender.get(senders[i]), 0);
      if (chain == null || chain.size() == 1 || i > chain.get(chain.size() - 1)) {
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
                    0,
                    null,
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
          senders,
          bySender,
          unknownSenders,
          recovered,
          miningBeneficiary,
          blockHashLookup,
          blobGasPrice,
          executor,
          blockAccessListBuilder,
          maybeParentHeader);
    }
  }

  /** Recovers the unknown senders on the block's workers, a batch per task. */
  private static CompletableFuture<Void> recoverSenders(
      final List<Transaction> transactions,
      final List<Integer> unknownSenders,
      final Executor executor) {
    final List<CompletableFuture<Void>> recoveries = new ArrayList<>();
    for (int from = 0; from < unknownSenders.size(); from += SENDER_RECOVERY_BATCH) {
      final List<Integer> batch =
          unknownSenders.subList(
              from, Math.min(unknownSenders.size(), from + SENDER_RECOVERY_BATCH));
      recoveries.add(
          CompletableFuture.runAsync(
              () -> batch.forEach(txIndex -> recoverSender(transactions.get(txIndex))), executor));
    }
    return CompletableFuture.allOf(recoveries.toArray(CompletableFuture[]::new));
  }

  /**
   * The transaction pool knows the senders of the transactions it saw, but not those sent to block
   * builders directly, so those run alone at first and the later ones of a sender fail their nonce
   * check. Once their senders are recovered, the transactions of a sender, together with those of
   * it the pool knew, run again chained from the last result that already ran on the state before
   * it: the result of the last known transaction before the first unknown one, or the first unknown
   * one's own run.
   *
   * @param senders the sender of each transaction known when the runs were set up, else null
   * @param knownBySender the transactions of each sender known then, in block order
   */
  private void chainUnknownSenders(
      final ProtocolContext protocolContext,
      final BlockHeader blockHeader,
      final List<Transaction> transactions,
      final Address[] senders,
      final Map<Address, List<Integer>> knownBySender,
      final List<Integer> unknownSenders,
      final CompletableFuture<Void> recovered,
      final Address miningBeneficiary,
      final BlockHashLookup blockHashLookup,
      final Wei blobGasPrice,
      final Executor executor,
      final Optional<BlockAccessListBuilder> blockAccessListBuilder,
      final Optional<BlockHeader> maybeParentHeader) {
    // the serial loop takes the runs out of the futures meanwhile
    final CompletableFuture<ParallelizedTransactionContext>[] runs = futures.clone();
    // run where the last recovery ends rather than queued behind the block's runs
    recovered.thenRun(
        () -> {
          final Map<Address, List<Integer>> bySender = new HashMap<>();
          for (final int txIndex : unknownSenders) {
            transactions
                .get(txIndex)
                .getSenderIfKnown()
                .ifPresent(
                    sender ->
                        bySender
                            .computeIfAbsent(
                                sender,
                                s -> new ArrayList<>(knownBySender.getOrDefault(s, List.of())))
                            .add(txIndex));
          }
          final List<List<Integer>> lateChains = new ArrayList<>();
          for (final List<Integer> transactionsOfSender : bySender.values()) {
            transactionsOfSender.sort(null);
            int firstUnknown = 0;
            while (senders[transactionsOfSender.get(firstUnknown)] != null) {
              firstUnknown++;
            }
            final List<Integer> chain = capped(transactionsOfSender, Math.max(0, firstUnknown - 1));
            if (chain.size() > 1) {
              lateChains.add(chain);
            }
          }
          // in block order, the order they are needed in
          lateChains.sort(Comparator.comparing(chain -> chain.get(0)));
          for (final List<Integer> chain : lateChains) {
            final List<CompletableFuture<ParallelizedTransactionContext>> chainFutures =
                new ArrayList<>(chain.size());
            chainFutures.add(runs[chain.get(0)]);
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
        });
  }

  /** The chain of a sender's transactions from the given one on, at most the maximum length. */
  private static List<Integer> capped(final List<Integer> transactionsOfSender, final int from) {
    return transactionsOfSender.subList(
        from, Math.min(transactionsOfSender.size(), from + MAX_CHAIN_LENGTH));
  }

  private static void recoverSender(final Transaction transaction) {
    try {
      transaction.getSender();
    } catch (final IllegalStateException | IllegalArgumentException e) {
      // an unrecoverable signature leaves the transaction unchained, its own run reports it
    }
  }

  /** Runs the rest of a chain on the result of its first member. */
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
    if (first == null || first.transactionProcessingResult().isInvalid()) {
      dropFrom(chainFutures, 1);
      return;
    }
    runChain(
        protocolContext,
        blockHeader,
        transactions,
        chain,
        chainFutures,
        1,
        first.transactionAccumulator(),
        miningBeneficiary,
        blockHashLookup,
        blobGasPrice,
        blockAccessListBuilder,
        maybeParentHeader);
  }

  /**
   * Executes a chained transaction against the parent state only for what it reads, so that its run
   * on top of its predecessors finds that state cached. Its nonce and gas payment checks are
   * relaxed as the predecessors have not run, and its result is discarded.
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
    // a member dropped from its chain is executed by the serial loop, which the warm-up still helps
    final boolean ranInChain = future.state() == Future.State.SUCCESS && future.resultNow() != null;
    if (ranInChain || serialLoopHasPassed(transactionLocation) || maybeParentHeader.isEmpty()) {
      return;
    }
    try {
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
            WARM_UP_VALIDATION,
            blobGasPrice,
            Optional.empty());
      } finally {
        ws.close();
      }
    } catch (final RuntimeException e) {
      // only fetching state, the chained run reports any failure
    }
  }

  /**
   * Runs the members of a chain from the given one on, each on the state the previous one left.
   *
   * @param from the first member to run
   * @param previous the state the member before it left, or null if it is the first of the chain
   */
  private void runChain(
      final ProtocolContext protocolContext,
      final BlockHeader blockHeader,
      final List<Transaction> transactions,
      final List<Integer> chain,
      final List<CompletableFuture<ParallelizedTransactionContext>> chainFutures,
      final int from,
      final PathBasedWorldStateUpdateAccumulator<?> previous,
      final Address miningBeneficiary,
      final BlockHashLookup blockHashLookup,
      final Wei blobGasPrice,
      final Optional<BlockAccessListBuilder> blockAccessListBuilder,
      final Optional<BlockHeader> maybeParentHeader) {
    PathBasedWorldStateUpdateAccumulator<?> state = previous;
    for (int k = from; k < chain.size(); k++) {
      final int txIndex = chain.get(k);
      final CompletableFuture<ParallelizedTransactionContext> future = chainFutures.get(k);
      // The rest of the chain needs the state this member leaves. Running a member the serial
      // loop already reached only to pass that state on would leave the chain behind the serial
      // loop still, so the rest is dropped. A member overtaken while it runs passes its state on,
      // as that costs nothing more.
      if (future.isCancelled() || serialLoopHasPassed(txIndex)) {
        dropFrom(chainFutures, k);
        return;
      }
      final ParallelizedTransactionContext context;
      try {
        context =
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
                state);
      } catch (final Throwable t) {
        // completed with the error so that the serial loop reports it, as for a run alone
        future.completeExceptionally(t);
        dropFrom(chainFutures, k + 1);
        return;
      }
      future.complete(context);
      // an invalid member leaves the sender's nonce as it was, so the next would fail its check
      if (context == null || context.transactionProcessingResult().isInvalid()) {
        dropFrom(chainFutures, k + 1);
        return;
      }
      state = context.transactionAccumulator();
    }
  }

  /** Leaves the members from the given one on without a result, so they run in the serial loop. */
  private static void dropFrom(
      final List<CompletableFuture<ParallelizedTransactionContext>> chainFutures, final int from) {
    for (int k = from; k < chainFutures.size(); k++) {
      chainFutures.get(k).complete(null);
    }
  }

  private boolean serialLoopHasPassed(final int txIndex) {
    return txIndex <= serialLoopPosition.get();
  }

  /**
   * Also stops what the futures of the block do not cover: the late chains and the chains and
   * warm-ups not started yet skip every transaction once the block is done with.
   */
  @Override
  public void abort() {
    serialLoopPosition.set(Integer.MAX_VALUE);
    super.abort();
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
    // on the block's transaction, whose lock the background recovery takes too, so that the copy
    // executed below carries the sender instead of recovering it a second time
    recoverSender(transaction);
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
                  if (transactionCollisionDetector.isAddressTouchedByTransaction(
                      transaction, roundWorldStateUpdater, miningBeneficiary)) {
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
      return precomputeLogsBloom(parallelizedTransactionContext);
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

    if (future != null && future.state() == Future.State.FAILED) {
      LOG.debug(
          "Parallel execution of transaction {} failed, executing it sequentially",
          transaction.getHash(),
          future.exceptionNow());
    }
    if (future != null && future.state() == Future.State.SUCCESS) {
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
      // a reverted or halted execution is as repeatable as a successful one when nothing it read
      // changed; an invalid result is replayed anyway, and its sender may not even be recoverable
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

        confirmedParallelizedTransactionCounter.ifPresent(Counter::inc);
        transactionProcessingResult.setIsProcessedInParallel(Optional.of(Boolean.TRUE));
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
