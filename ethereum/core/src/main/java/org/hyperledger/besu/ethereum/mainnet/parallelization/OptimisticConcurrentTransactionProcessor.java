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
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.AccessLocationTracker;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BlockAccessListBuilder;
import org.hyperledger.besu.ethereum.mainnet.parallelization.prefetch.RunaheadPrefetcher;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache.BenchReads;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiValue;
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
import java.util.TreeSet;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
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

  // which transactions a worker has started, and which run in a chain
  private volatile AtomicIntegerArray started = new AtomicIntegerArray(0);
  private volatile AtomicIntegerArray inChain = new AtomicIntegerArray(0);
  private volatile long[] gasLimits = new long[0];
  // the first pass of each transaction's run ahead, which a worker may wait for before its run
  private volatile CompletableFuture<?>[] runaheads = new CompletableFuture<?>[0];
  // transactions a worker has run or the serial loop has taken, whose run ahead can stop
  private volatile AtomicIntegerArray finished = new AtomicIntegerArray(0);
  private static final long RUNAHEAD_WAIT_MS = Long.getLong("besu.t14.runaheadWaitMs", 0L);
  // chained runs of transactions whose sender was recovered only after they ran alone
  private volatile AtomicReferenceArray<CompletableFuture<ParallelizedTransactionContext>>
      lateChainFutures = new AtomicReferenceArray<>(0);
  // senders recovered per task, few enough to spread over the executor's threads
  private static final int SENDER_RECOVERY_BATCH = 8;

  // a long transaction submitted first runs alongside the rest of the block instead of after it
  private static final long HEAVY_FIRST_GAS = Long.getLong("besu.t14.heavyFirstGas", 0L);

  // none: the serial loop runs a transaction whose run has not finished itself; chained: it waits
  // for a running chain member; heavy: also for a running transaction with a large gas limit;
  // all: it waits for any running transaction
  private static final String WAIT_RUNNING = System.getProperty("besu.t14.waitRunning", "none");
  private static final long WAIT_HEAVY_GAS = Long.getLong("besu.t14.waitHeavyGas", 2_000_000L);
  private static final long WAIT_RUNNING_MS = Long.getLong("besu.t14.waitRunningMs", 500L);

  // transactions at or above this gas limit are run ahead to load their slots, 0 turns it off
  private static final long RUNAHEAD_GAS = Long.getLong("besu.t14.runaheadGas", 0L);

  // experiment: a transaction whose run touched what the run of an earlier one changed runs again
  // on
  // a worker on top of that one's result, so that the serial loop finds a result it can take
  // instead of running the transaction itself
  private static final boolean RERUN_DEPENDENCIES =
      Boolean.getBoolean("besu.t14.rerunDependencies");
  // a transaction runs again at most this often, as each later writer it touched can start a run
  private static final int MAX_RERUNS = Integer.getInteger("besu.t14.maxReruns", 3);
  private volatile Dependencies dependencies;

  // replay benchmarks: log each worker execution's time
  private static final boolean LOG_TX_TIMES = Boolean.getBoolean("besu.bench.logTxTimes");
  // replay benchmarks: log why a worker result could not be taken
  private static final boolean LOG_CONFLICTS = Boolean.getBoolean("besu.bench.logConflicts");
  private volatile long benchBlock;

  // workers warm the trie paths of what they touch, so the state root finds them loaded
  private static final boolean WORKER_PRELOAD = Boolean.getBoolean("besu.t14.workerPreload");

  // a result whose transaction did not depend on a balance the block changed carries its balance
  // change over instead of conflicting (#11809); false turns it off for comparisons
  private static final boolean CARRY_OVER =
      Boolean.parseBoolean(System.getProperty("besu.t14.carryOver", "true"));

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

    if (LOG_TX_TIMES) {
      benchBlock = blockHeader.getNumber();
      LOG.info(
          "Bench block start {} txs {} t {}",
          blockHeader.getNumber(),
          transactions.size(),
          System.nanoTime() / 1000);
    }
    final CompletableFuture<?>[] runaheadFirstPasses =
        new CompletableFuture<?>[transactions.size()];
    runaheads = runaheadFirstPasses;
    final AtomicIntegerArray finishedRuns = new AtomicIntegerArray(transactions.size());
    finished = finishedRuns;
    if (RUNAHEAD_GAS > 0 && maybeParentHeader.isPresent()) {
      for (int i = 0; i < transactions.size(); i++) {
        final Transaction transaction = transactions.get(i);
        if (transaction.getGasLimit() >= RUNAHEAD_GAS) {
          final int location = i;
          runaheadFirstPasses[i] =
              RunaheadPrefetcher.runAhead(
                  transactionProcessor,
                  protocolContext,
                  maybeParentHeader.get(),
                  blockHeader,
                  transaction,
                  miningBeneficiary,
                  blockHashLookup,
                  blobGasPrice,
                  WARM_UP_VALIDATION,
                  () -> finishedRuns.get(location) == 1);
        }
      }
    }

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
    started = new AtomicIntegerArray(transactions.size());
    inChain = new AtomicIntegerArray(transactions.size());
    gasLimits = new long[transactions.size()];
    for (int i = 0; i < transactions.size(); i++) {
      gasLimits[i] = transactions.get(i).getGasLimit();
    }
    dependencies =
        RERUN_DEPENDENCIES
            ? new Dependencies(
                protocolContext,
                blockHeader,
                transactions,
                miningBeneficiary,
                blockHashLookup,
                blobGasPrice,
                executor,
                blockAccessListBuilder,
                maybeParentHeader)
            : null;
    // queued ahead of the runs, as chaining the unknown senders waits for every recovery
    final CompletableFuture<Void> recovered =
        recoverSenders(transactions, unknownSenders, executor);
    for (final int i : submissionOrder(transactions)) {
      final Transaction transaction = transactions.get(i);
      final List<Integer> chain = senders[i] == null ? null : bySender.get(senders[i]);
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
          inChain.set(member, 1);
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

  /** Whether the serial loop waits for the run of the transaction rather than running it itself. */
  private boolean shouldWaitFor(final int transactionLocation) {
    if (transactionLocation >= started.length() || started.get(transactionLocation) == 0) {
      return false;
    }
    return switch (WAIT_RUNNING) {
      case "all" -> true;
      case "chained" -> inChain.get(transactionLocation) == 1;
      case "heavy" ->
          inChain.get(transactionLocation) == 1
              || (transactionLocation < gasLimits.length
                  && gasLimits[transactionLocation] >= WAIT_HEAVY_GAS);
      default -> false;
    };
  }

  /**
   * Block order, or with heavyFirstGas set the transactions at or above it first, largest first.
   */
  private static int[] submissionOrder(final List<Transaction> transactions) {
    final int n = transactions.size();
    final int[] order = new int[n];
    if (HEAVY_FIRST_GAS <= 0) {
      for (int i = 0; i < n; i++) {
        order[i] = i;
      }
      return order;
    }
    final List<Integer> heavy = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      if (transactions.get(i).getGasLimit() >= HEAVY_FIRST_GAS) {
        heavy.add(i);
      }
    }
    heavy.sort(
        (x, y) ->
            Long.compare(transactions.get(y).getGasLimit(), transactions.get(x).getGasLimit()));
    final boolean[] taken = new boolean[n];
    int k = 0;
    for (final int i : heavy) {
      order[k++] = i;
      taken[i] = true;
    }
    for (int i = 0; i < n; i++) {
      if (!taken[i]) {
        order[k++] = i;
      }
    }
    return order;
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
            final List<Integer> chain =
                transactionsOfSender.subList(
                    Math.max(0, firstUnknown - 1), transactionsOfSender.size());
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
              inChain.set(chain.get(k), 1);
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

  private record SlotKey(Address address, StorageSlotKey slot) {}

  /**
   * What the runs of a block's transactions touched and changed. A transaction that touched what
   * earlier ones changed runs again on top of the latest of them, if that one's result includes the
   * others, as the serial loop would otherwise find the transaction's result colliding and run it
   * itself. A run on top of another is checked like a chained run: it holds if the block reaches it
   * with the values it started from.
   */
  private final class Dependencies {
    // marks the runs of this class, whose results it records itself
    private static final ThreadLocal<Boolean> RERUNNING = ThreadLocal.withInitial(() -> false);

    private final ProtocolContext protocolContext;
    private final BlockHeader blockHeader;
    private final List<Transaction> transactions;
    private final Address miningBeneficiary;
    private final BlockHashLookup blockHashLookup;
    private final Wei blobGasPrice;
    private final Executor executor;
    private final Optional<BlockAccessListBuilder> blockAccessListBuilder;
    private final Optional<BlockHeader> maybeParentHeader;
    // guarded by this
    private final Map<Object, TreeSet<Integer>> writers = new HashMap<>();
    private final Map<Object, TreeSet<Integer>> readers = new HashMap<>();
    private final ParallelizedTransactionContext[] results;
    private final List<?>[] touchedKeys;
    // the transactions whose changes each latest result includes
    private final java.util.BitSet[] ancestry;
    // how often each transaction got a new result, and which result of its base its re-run used
    private final int[] resultVersion;
    private final int[] predecessor;
    private final int[] baseVersion;
    private final int[] reruns;
    // the latest re-run of each transaction, and whether it started
    private final AtomicReferenceArray<CompletableFuture<ParallelizedTransactionContext>>
        rerunFutures;
    private final AtomicIntegerArray rerunStarted;

    Dependencies(
        final ProtocolContext protocolContext,
        final BlockHeader blockHeader,
        final List<Transaction> transactions,
        final Address miningBeneficiary,
        final BlockHashLookup blockHashLookup,
        final Wei blobGasPrice,
        final Executor executor,
        final Optional<BlockAccessListBuilder> blockAccessListBuilder,
        final Optional<BlockHeader> maybeParentHeader) {
      this.protocolContext = protocolContext;
      this.blockHeader = blockHeader;
      this.transactions = transactions;
      this.miningBeneficiary = miningBeneficiary;
      this.blockHashLookup = blockHashLookup;
      this.blobGasPrice = blobGasPrice;
      this.executor = executor;
      this.blockAccessListBuilder = blockAccessListBuilder;
      this.maybeParentHeader = maybeParentHeader;
      final int n = transactions.size();
      this.results = new ParallelizedTransactionContext[n];
      this.touchedKeys = new List<?>[n];
      this.ancestry = new java.util.BitSet[n];
      this.resultVersion = new int[n];
      this.predecessor = new int[n];
      java.util.Arrays.fill(predecessor, -1);
      this.baseVersion = new int[n];
      this.reruns = new int[n];
      this.rerunFutures = new AtomicReferenceArray<>(n);
      this.rerunStarted = new AtomicIntegerArray(n);
    }

    /** The re-run the serial loop takes: one that finished or started, else none. */
    CompletableFuture<ParallelizedTransactionContext> rerunToTake(final int index) {
      if (index >= rerunFutures.length()) {
        return null;
      }
      final CompletableFuture<ParallelizedTransactionContext> rerun = rerunFutures.get(index);
      if (rerun == null || (!rerun.isDone() && rerunStarted.get(index) == 0)) {
        return null;
      }
      return rerun;
    }

    /** Records the result of a run other than a re-run of this class. */
    void finished(final int index, final ParallelizedTransactionContext context) {
      if (RERUNNING.get()) {
        return;
      }
      final java.util.BitSet itself = new java.util.BitSet();
      itself.set(index);
      record(index, context, itself);
    }

    private synchronized void record(
        final int index,
        final ParallelizedTransactionContext context,
        final java.util.BitSet includes) {
      if (index >= results.length
          || context == null
          || context.transactionProcessingResult().isInvalid()) {
        return;
      }
      results[index] = context;
      ancestry[index] = includes;
      resultVersion[index]++;
      final List<Object> changed = new ArrayList<>();
      final List<Object> touched = new ArrayList<>();
      collect(context.transactionAccumulator(), changed, touched);
      touchedKeys[index] = touched;
      for (final Object key : touched) {
        readers.computeIfAbsent(key, k -> new TreeSet<>()).add(index);
      }
      // the later transactions that touched what this one changed may run again on top of it
      final java.util.BitSet later = new java.util.BitSet();
      for (final Object key : changed) {
        writers.computeIfAbsent(key, k -> new TreeSet<>()).add(index);
        final TreeSet<Integer> keyReaders = readers.get(key);
        if (keyReaders != null) {
          for (final int reader : keyReaders.tailSet(index, false)) {
            later.set(reader);
          }
        }
      }
      consider(index);
      for (int reader = later.nextSetBit(0); reader >= 0; reader = later.nextSetBit(reader + 1)) {
        consider(reader);
      }
    }

    /** Runs the transaction again on top of the latest earlier writer, if its result can hold. */
    private void consider(final int index) {
      final List<?> touched = touchedKeys[index];
      if (touched == null) {
        return;
      }
      final java.util.BitSet earlierWriters = new java.util.BitSet();
      for (final Object key : touched) {
        final TreeSet<Integer> keyWriters = writers.get(key);
        if (keyWriters != null) {
          for (final int writer : keyWriters.headSet(index, false)) {
            earlierWriters.set(writer);
          }
        }
      }
      if (earlierWriters.isEmpty()) {
        return;
      }
      final int latest = earlierWriters.previousSetBit(index - 1);
      if (ancestry[latest] == null) {
        return;
      }
      // on top of the latest writer only, the changes of another earlier writer would be missing
      earlierWriters.andNot(ancestry[latest]);
      if (earlierWriters.isEmpty()) {
        rerunOnTop(index, latest);
      }
    }

    private void collect(
        final PathBasedWorldStateUpdateAccumulator<?> accumulator,
        final List<Object> changed,
        final List<Object> touched) {
      for (final var account : accumulator.getAccountsToUpdate().entrySet()) {
        final Address address = account.getKey();
        if (address.equals(miningBeneficiary)) {
          continue;
        }
        touched.add(address);
        final BonsaiAccount prior = account.getValue().getPrior();
        final BonsaiAccount updated = account.getValue().getUpdated();
        if (!sameDetails(prior, updated)) {
          changed.add(address);
        }
      }
      changed.addAll(accumulator.getStorageToClear());
      for (final var storage : accumulator.getStorageToUpdate().entrySet()) {
        final Address address = storage.getKey();
        if (address.equals(miningBeneficiary)) {
          continue;
        }
        for (final var slot : storage.getValue().entrySet()) {
          final SlotKey key = new SlotKey(address, slot.getKey());
          touched.add(key);
          if (slot.getValue().isChanged()) {
            changed.add(key);
          }
        }
      }
    }

    private static boolean sameDetails(final BonsaiAccount prior, final BonsaiAccount updated) {
      return (prior == null && updated == null)
          || (prior != null
              && updated != null
              && prior.getNonce() == updated.getNonce()
              && prior.getBalance().equals(updated.getBalance())
              && prior.getCodeHash().equals(updated.getCodeHash()));
    }

    /** Runs the transaction again on top of the result of an earlier one it depends on. */
    private void rerunOnTop(final int index, final int earlier) {
      if (earlier < predecessor[index]
          || (earlier == predecessor[index] && resultVersion[earlier] == baseVersion[index])
          || reruns[index] >= MAX_RERUNS
          || inChain.get(index) == 1
          || serialLoopHasPassed(index)
          || results[earlier] == null) {
        return;
      }
      final Transaction transaction = transactions.get(index);
      final PathBasedWorldStateUpdateAccumulator<?> onTopOf =
          results[earlier].transactionAccumulator();
      final java.util.BitSet includes = (java.util.BitSet) ancestry[earlier].clone();
      includes.set(index);
      predecessor[index] = earlier;
      baseVersion[index] = resultVersion[earlier];
      final int run = ++reruns[index];
      if (LOG_TX_TIMES) {
        LOG.info(
            "Bench rerun block {} idx {} after {} rerun {}",
            blockHeader.getNumber(),
            index,
            earlier,
            run);
      }
      final CompletableFuture<ParallelizedTransactionContext> rerun = new CompletableFuture<>();
      rerunFutures.set(index, rerun);
      try {
        executor.execute(
            () -> {
              if (serialLoopHasPassed(index)) {
                rerun.complete(null);
                return;
              }
              rerunStarted.set(index, 1);
              final ParallelizedTransactionContext context;
              RERUNNING.set(true);
              try {
                context =
                    runTransaction(
                        protocolContext,
                        blockHeader,
                        index,
                        transaction,
                        miningBeneficiary,
                        blockHashLookup,
                        blobGasPrice,
                        blockAccessListBuilder,
                        maybeParentHeader,
                        onTopOf);
              } catch (final Throwable t) {
                rerun.completeExceptionally(t);
                return;
              } finally {
                RERUNNING.set(false);
              }
              rerun.complete(context);
              synchronized (this) {
                // a later re-run of the transaction replaces this one
                if (reruns[index] == run) {
                  record(index, context, includes);
                }
              }
            });
      } catch (final java.util.concurrent.RejectedExecutionException e) {
        rerun.complete(null);
      }
    }
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
    final AtomicIntegerArray startedRuns = started;
    if (transactionLocation < startedRuns.length()) {
      startedRuns.set(transactionLocation, 1);
    }
    final AtomicIntegerArray finishedRuns = finished;
    final Dependencies blockDependencies = dependencies;
    final CompletableFuture<?>[] firstPasses = runaheads;
    if (RUNAHEAD_WAIT_MS > 0
        && previousInChain == null
        && transactionLocation < firstPasses.length
        && firstPasses[transactionLocation] != null) {
      // a run that starts once the run ahead loaded what it reads waits for memory, not the disk
      try {
        firstPasses[transactionLocation].get(RUNAHEAD_WAIT_MS, TimeUnit.MILLISECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (final ExecutionException | TimeoutException | CancellationException e) {
        // runs anyway
      }
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
      if (!WORKER_PRELOAD) {
        ws.disableCacheMerkleTrieLoader();
      }
      final ParallelizedTransactionContext.Builder contextBuilder =
          new ParallelizedTransactionContext.Builder();
      final PathBasedWorldStateUpdateAccumulator<?> roundWorldStateUpdater =
          (PathBasedWorldStateUpdateAccumulator<?>) ws.updater();
      if (CARRY_OVER) {
        roundWorldStateUpdater.recordBalanceObservations();
      }
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
      final long benchStart = LOG_TX_TIMES ? System.nanoTime() : 0L;
      if (LOG_TX_TIMES) {
        BenchReads.take();
      }
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
                public boolean allowsJournal() {
                  return true;
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

      if (LOG_TX_TIMES) {
        final long[] reads = BenchReads.take();
        LOG.info(
            "Bench tx par block {} idx {} hash {} to {} gas {} us {} chained {} t {} db {} {}",
            blockHeader.getNumber(),
            transactionLocation,
            transaction.getHash().toHexString(),
            transaction.getTo().map(a -> a.toHexString()).orElse("create"),
            transaction.getGasLimit() - result.getGasRemaining(),
            (System.nanoTime() - benchStart) / 1000,
            previousInChain != null,
            benchStart / 1000,
            reads[0],
            reads[1]);
      }
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
      final ParallelizedTransactionContext finishedContext =
          precomputeLogsBloom(parallelizedTransactionContext);
      if (blockDependencies != null) {
        blockDependencies.finished(transactionLocation, finishedContext);
      }
      return finishedContext;
    } catch (Exception ex) {
      // no op as failing to get worldstate
      return null;
    } finally {
      if (ws != null) ws.close();
      if (transactionLocation < finishedRuns.length()) {
        finishedRuns.set(transactionLocation, 1);
      }
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
    final Dependencies blockDependencies = dependencies;
    final CompletableFuture<ParallelizedTransactionContext> rerun =
        blockDependencies == null ? null : blockDependencies.rerunToTake(transactionLocation);
    final CompletableFuture<ParallelizedTransactionContext> future =
        rerun != null ? rerun : lateChained != null ? lateChained : aloneOrChained;
    if (rerun != null && !rerun.isDone()) {
      // a re-run that started runs a transaction whose earlier result does not hold
      try {
        rerun.get(WAIT_RUNNING_MS, TimeUnit.MILLISECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (final ExecutionException | TimeoutException | CancellationException e) {
        // handled below as a run that did not finish
      }
    } else if (future != null && !future.isDone() && shouldWaitFor(transactionLocation)) {
      final long waitStart = LOG_TX_TIMES ? System.nanoTime() : 0L;
      try {
        future.get(WAIT_RUNNING_MS, TimeUnit.MILLISECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (final ExecutionException | TimeoutException | CancellationException e) {
        // handled below as a run that did not finish
      }
      if (LOG_TX_TIMES) {
        LOG.info(
            "Bench wait block {} idx {} us {} t {}",
            benchBlock,
            transactionLocation,
            (System.nanoTime() - waitStart) / 1000,
            waitStart / 1000);
      }
    }

    final AtomicIntegerArray finishedRuns = finished;
    if (transactionLocation < finishedRuns.length()) {
      finishedRuns.set(transactionLocation, 1);
    }
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
      final List<Address> balancesCarried = CARRY_OVER ? new ArrayList<>(0) : null;
      // a reverted or halted execution is as repeatable as a successful one when nothing it read
      // changed; an invalid result is replayed anyway, and its sender may not even be recoverable
      if (!transactionProcessingResult.isInvalid()
          && !transactionCollisionDetector.hasCollision(
              transaction,
              miningBeneficiary,
              parallelizedTransactionContext,
              blockAccumulator,
              balancesCarried)) {
        final Wei reward = parallelizedTransactionContext.miningBeneficiaryReward();
        // a result that touched the beneficiary before its reward already holds the reward
        if (!parallelizedTransactionContext.isMiningBeneficiaryTouchedPreRewardByTransaction()
            && (!reward.isZero() || !transactionProcessor.getClearEmptyAccounts())) {
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

        if (balancesCarried == null || balancesCarried.isEmpty()) {
          blockAccumulator.importStateChangesFromSource(transactionAccumulator);
        } else {
          importCarryingBalances(
              blockAccumulator,
              transactionAccumulator,
              balancesCarried,
              transactionProcessingResult);
        }

        confirmedParallelizedTransactionCounter.ifPresent(Counter::inc);
        transactionProcessingResult.setIsProcessedInParallel(Optional.of(Boolean.TRUE));
        return Optional.of(transactionProcessingResult);
      } else {
        if (LOG_CONFLICTS) {
          LOG.info(
              "Bench conflict idx {} hash {} to {} gas {} reason {}",
              transactionLocation,
              transaction.getHash().toHexString(),
              transaction.getTo().map(a -> a.toHexString()).orElse("create"),
              transaction.getGasLimit() - transactionProcessingResult.getGasRemaining(),
              transactionProcessingResult.isInvalid()
                  ? "invalid"
                  : transactionCollisionDetector.collisionReason(
                      transaction,
                      miningBeneficiary,
                      parallelizedTransactionContext,
                      blockAccumulator,
                      CARRY_OVER));
        }
        blockAccumulator.importPriorStateFromSource(transactionAccumulator);
        if (conflictingButCachedTransactionCounter.isPresent())
          conflictingButCachedTransactionCounter.get().inc();
        // If there is a conflict, we return an empty result to signal the block processor to
        // re-execute the transaction.
        return Optional.empty();
      }
    }
    if (LOG_CONFLICTS) {
      LOG.info(
          "Bench conflict idx {} hash {} to {} gas - reason {}",
          transactionLocation,
          transaction.getHash().toHexString(),
          transaction.getTo().map(a -> a.toHexString()).orElse("create"),
          future == null
              ? "no-run"
              : future.state() == Future.State.FAILED ? "failed" : "unfinished");
    }
    if (future != null) {
      future.cancel(true);
    }
    return Optional.empty();
  }

  /**
   * Takes over a result whose transaction changed balances that earlier transactions of the block
   * changed too, without depending on them: each such balance ends at the block's balance plus the
   * transaction's own change, in the state and in the block access list.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static void importCarryingBalances(
      final PathBasedWorldStateUpdateAccumulator blockAccumulator,
      final PathBasedWorldStateUpdateAccumulator<?> transactionAccumulator,
      final List<Address> balancesCarried,
      final TransactionProcessingResult transactionProcessingResult) {
    final Wei[] carried = new Wei[balancesCarried.size()];
    for (int i = 0; i < carried.length; i++) {
      final Address address = balancesCarried.get(i);
      final BonsaiValue<? extends BonsaiAccount> inTransaction =
          transactionAccumulator.getAccountsToUpdate().get(address);
      final Wei inBlock =
          ((BonsaiValue<? extends BonsaiAccount>)
                  blockAccumulator.getAccountsToUpdate().get(address))
              .getUpdated()
              .getBalance();
      final Wei before = inTransaction.getPrior().getBalance();
      final Wei after = inTransaction.getUpdated().getBalance();
      carried[i] =
          after.compareTo(before) >= 0
              ? inBlock.add(after.subtract(before))
              : inBlock.subtract(before.subtract(after));
    }
    blockAccumulator.importStateChangesFromSource(transactionAccumulator);
    for (int i = 0; i < carried.length; i++) {
      final Address address = balancesCarried.get(i);
      final Wei postBalance = carried[i];
      ((BonsaiValue<? extends BonsaiAccount>) blockAccumulator.getAccountsToUpdate().get(address))
          .getUpdated()
          .setBalance(postBalance);
      transactionProcessingResult
          .getPartialBlockAccessView()
          .ifPresent(
              view ->
                  view.accountChanges().stream()
                      .filter(
                          changes ->
                              changes.getAddress().equals(address)
                                  && changes.getPostBalance().isPresent())
                      .findFirst()
                      .ifPresent(changes -> changes.setPostBalance(postBalance)));
    }
  }
}
