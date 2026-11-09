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
package org.hyperledger.besu.ethereum.blockcreation;

import static com.google.common.base.Preconditions.checkArgument;
import static org.hyperledger.besu.ethereum.mainnet.feemarket.ExcessBlobGasCalculator.calculateExcessBlobGasForParent;
import static org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.manager.MonitoredExecutors;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.Histogram;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.plugin.services.metrics.OperationTimer;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes pending transactions against the head state between blocks, so that the state the next
 * block is likely to read is in the caches when the block arrives. The executions are discarded;
 * only their reads matter.
 *
 * <p>Warming starts when the head moves and runs in passes until a block arrives, block building
 * starts, the gas budget is used up, or the next slot is over. If the block that arrived does not
 * become the head, warming resumes where it stopped. When the pool has new transactions, and at
 * least once a second, a pass takes the pool's candidates in the order block building takes them
 * and executes the senders it has not warmed yet, each sender's transactions in nonce order on its
 * own throwaway copy of the head state.
 */
public class MempoolPrewarmer implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(MempoolPrewarmer.class);

  private static final long PASS_INTERVAL_MILLIS = 250;
  // the longest a pass for an earlier head holds up the coordinator
  private static final long STOP_CHECK_MILLIS = 10;
  // the pool announces the transactions added to it, but not those it promotes as the head moves
  private static final long POOL_COPY_INTERVAL_MILLIS = 1_000;
  // blocks arrive after the start of their slot, and transactions sent in that time are still in
  // them
  private static final long DEADLINE_AFTER_SLOT_MILLIS = 1_000;
  // a sender with a long queue would otherwise take the whole budget of a pass
  private static final int MAX_TRANSACTIONS_PER_SENDER = 16;
  private static final TransactionValidationParams VALIDATION_PARAMS =
      TransactionValidationParams.transactionSimulatorAllowExceedingBalanceAndFutureNonce();

  private final ProtocolContext protocolContext;
  private final ProtocolSchedule protocolSchedule;
  private final TransactionPool transactionPool;
  private final MiningConfiguration miningConfiguration;
  private final int gasLimitMultiplier;
  private final Clock clock;
  private final ScheduledExecutorService coordinator;
  private final ExecutorService executors;
  private final AtomicLong generation = new AtomicLong();
  private final AtomicLong poolAdditions = new AtomicLong();
  private final AtomicInteger runningExecutions = new AtomicInteger();
  private volatile long blockStartedNanos = System.nanoTime();
  private final long poolSubscription;
  @Nullable private volatile Warming warming;
  // the last prewarming a payload stopped, which its block counts against once the block is known
  private final AtomicReference<Warming> stoppedForPayload = new AtomicReference<>();

  private final Counter executedTransactions;
  private final Counter invalidTransactions;
  private final Counter executedGas;
  private final Counter startedWarmings;
  private final Counter usedUpBudgets;
  private final Counter prewarmedBlockTransactions;
  private final Counter missedBlockTransactions;
  private final OperationTimer poolCopyTimer;
  private final Counter executionsRunningAtBlock;
  private final Histogram executionAfterBlock;

  /**
   * Creates a prewarmer.
   *
   * @param protocolContext the protocol context
   * @param protocolSchedule the protocol schedule
   * @param transactionPool the pool whose transactions are executed
   * @param miningConfiguration the mining configuration, for the pending block header
   * @param threads the number of threads that execute transactions
   * @param gasLimitMultiplier the gas budget for a head, as a multiple of its gas limit
   * @param metricsSystem the metrics system
   */
  public MempoolPrewarmer(
      final ProtocolContext protocolContext,
      final ProtocolSchedule protocolSchedule,
      final TransactionPool transactionPool,
      final MiningConfiguration miningConfiguration,
      final int threads,
      final int gasLimitMultiplier,
      final MetricsSystem metricsSystem) {
    this(
        protocolContext,
        protocolSchedule,
        transactionPool,
        miningConfiguration,
        threads,
        gasLimitMultiplier,
        metricsSystem,
        Clock.systemUTC());
  }

  @VisibleForTesting
  MempoolPrewarmer(
      final ProtocolContext protocolContext,
      final ProtocolSchedule protocolSchedule,
      final TransactionPool transactionPool,
      final MiningConfiguration miningConfiguration,
      final int threads,
      final int gasLimitMultiplier,
      final MetricsSystem metricsSystem,
      final Clock clock) {
    checkArgument(threads > 0, "threads must be positive");
    checkArgument(gasLimitMultiplier > 0, "gasLimitMultiplier must be positive");
    this.protocolContext = protocolContext;
    this.protocolSchedule = protocolSchedule;
    this.transactionPool = transactionPool;
    this.miningConfiguration = miningConfiguration;
    this.gasLimitMultiplier = gasLimitMultiplier;
    this.clock = clock;
    this.coordinator =
        MonitoredExecutors.newScheduledThreadPool(
            MempoolPrewarmer.class.getSimpleName() + "-Passes", 1, metricsSystem);
    this.executors =
        MonitoredExecutors.newFixedThreadPool(
            MempoolPrewarmer.class.getSimpleName() + "-Executions", threads, metricsSystem);
    this.poolSubscription =
        transactionPool.subscribePendingTransactions(
            transaction -> poolAdditions.incrementAndGet());

    final LabelledMetric<Counter> transactions =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.BLOCK_PROCESSING,
            "mempool_prewarm_transactions_total",
            "Pending transactions executed to prewarm the state, by whether they were valid on the"
                + " head state",
            "result");
    this.executedTransactions = transactions.labels("executed");
    this.invalidTransactions = transactions.labels("invalid");
    this.executedGas =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCK_PROCESSING,
            "mempool_prewarm_gas_total",
            "Gas used by the pending transactions executed to prewarm the state, which is charged to"
                + " the gas budget for their head");
    this.startedWarmings =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCK_PROCESSING,
            "mempool_prewarm_started_total",
            "Heads prewarming started for, each with its own gas budget");
    this.usedUpBudgets =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCK_PROCESSING,
            "mempool_prewarm_budget_used_up_total",
            "Heads whose prewarming used up its gas budget");
    final LabelledMetric<Counter> blockTransactions =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.BLOCK_PROCESSING,
            "mempool_prewarm_block_transactions_total",
            "Transactions of blocks that arrived while the state was prewarmed for their parent, by"
                + " whether they had been prewarmed",
            "prewarmed");
    this.prewarmedBlockTransactions = blockTransactions.labels("true");
    this.missedBlockTransactions = blockTransactions.labels("false");
    this.poolCopyTimer =
        metricsSystem.createTimer(
            BesuMetricCategory.BLOCK_PROCESSING,
            "mempool_prewarm_pool_copy_duration_seconds",
            "Time a prewarming pass takes to copy the pool's candidates and group them by sender");
    this.executionsRunningAtBlock =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCK_PROCESSING,
            "mempool_prewarm_executions_running_at_block_total",
            "Prewarm executions still running when the execution of an arriving block started");
    this.executionAfterBlock =
        metricsSystem.createHistogram(
            BesuMetricCategory.BLOCK_PROCESSING,
            "mempool_prewarm_execution_after_block_seconds",
            "How long the prewarm executions still running when the execution of an arriving block"
                + " started ran on",
            new double[] {0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5});
  }

  /**
   * Starts prewarming for a new head, stopping any prewarming for an earlier one. Prewarming that
   * stopped while the head stayed resumes with the gas budget it has left, until the next slot is
   * over.
   *
   * @param head the new head
   */
  public void start(final BlockHeader head) {
    if (!transactionPool.isEnabled()) {
      // the pool offers nothing to warm until it is enabled again
      stop(Optional.empty());
      return;
    }
    final Warming current = warming;
    final Warming next;
    if (current != null
        && current.head().getHash().equals(head.getHash())
        && clock.millis() < current.deadline()) {
      if (isCurrent(current) || current.building().get()) {
        return;
      }
      // the budget caps the work for a head, however often a block that does not become the head
      // stops it
      next = current.resumed(generation.incrementAndGet());
    } else {
      final long timestamp;
      try {
        timestamp = slotTimestamp(head);
      } catch (final RuntimeException e) {
        LOG.debug("Cannot prewarm for head {}", head.toLogString(), e);
        return;
      }
      final long gasLimit = head.getGasLimit();
      next =
          new Warming(
              generation.incrementAndGet(),
              head,
              timestamp,
              gasLimit > Long.MAX_VALUE / gasLimitMultiplier
                  ? Long.MAX_VALUE
                  : gasLimit * gasLimitMultiplier);
    }
    warming = next;
    try {
      coordinator.execute(() -> warm(next));
    } catch (final RejectedExecutionException e) {
      LOG.debug("Not prewarming for head {}, the prewarmer is closed", head.toLogString());
    }
  }

  /**
   * Stops prewarming, as a payload arrived and is about to be checked. Executions already running
   * stop when their transaction next enters or leaves a call.
   */
  public void stopForPayload() {
    final Warming stopped = warming;
    final long running = generation.getAndIncrement();
    if (stopped != null && stopped.generation() == running) {
      stoppedForPayload.set(stopped);
    }
  }

  /**
   * Stops prewarming, as a block arrived or mining stops. Executions already running stop when
   * their transaction next enters or leaves a call.
   *
   * @param arrivingBlock the block that arrived, if one did
   */
  public void stop(final Optional<Block> arrivingBlock) {
    final Warming current = warming;
    final long running = generation.getAndIncrement();
    final Warming stoppedEarlier = stoppedForPayload.getAndSet(null);
    // the block's execution starts now, so these executions run alongside it
    if (arrivingBlock.isPresent()) {
      blockStartedNanos = System.nanoTime();
      executionsRunningAtBlock.inc(runningExecutions.get());
    }
    // a block counts once, and only against prewarming that ran until it or its payload arrived
    final Warming stopped =
        current != null && current.generation() == running ? current : stoppedEarlier;
    if (stopped == null) {
      return;
    }
    arrivingBlock
        .filter(block -> block.getHeader().getParentHash().equals(stopped.head().getHash()))
        .ifPresent(block -> countPrewarmed(stopped, block));
  }

  /**
   * Stops prewarming, as block building starts on the head and executes the pool's transactions
   * itself. Prewarming does not start again for the same head before the next slot is over.
   */
  public void stopForBlockBuilding() {
    final Warming current = warming;
    if (current != null) {
      current.building().set(true);
    }
    generation.incrementAndGet();
  }

  /**
   * Stops prewarming for good and waits for the running pass and executions to end, so that none of
   * them reads the chain or the state after its storage is closed. An execution that makes no calls
   * runs until its gas runs out, so its gas limit bounds the wait.
   */
  @Override
  public void close() {
    generation.incrementAndGet();
    transactionPool.unsubscribePendingTransactions(poolSubscription);
    coordinator.shutdownNow();
    executors.shutdown();
    try {
      coordinator.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
      executors.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @VisibleForTesting
  Set<Hash> prewarmedTransactions() {
    final Warming current = warming;
    return current == null ? Set.of() : Set.copyOf(current.warmed());
  }

  private boolean isCurrent(final Warming warming) {
    return warming.generation() == generation.get();
  }

  private boolean isWarming(final Warming warming) {
    return isCurrent(warming)
        && !warming.budgetUsedUp().get()
        && clock.millis() < warming.deadline();
  }

  /**
   * Takes the gas limit of a transaction from the budget for a head, so that the executions
   * together stay within it. What a transaction does not use is given back once it ends. Once a
   * transaction does not fit, prewarming for the head is over.
   */
  private boolean reserve(final Warming warming, final long gas) {
    final AtomicLong budget = warming.gasBudget();
    for (long left = budget.get(); left >= gas; left = budget.get()) {
      if (budget.compareAndSet(left, left - gas)) {
        return true;
      }
    }
    if (warming.budgetUsedUp().compareAndSet(false, true)) {
      usedUpBudgets.inc();
      LOG.atDebug()
          .setMessage("Prewarming for head {} used up its gas budget")
          .addArgument(warming.head()::toLogString)
          .log();
    }
    return false;
  }

  /**
   * The timestamp of the first slot after the head that has not started yet, so that a head kept
   * through a missed slot is warmed for the slot after it.
   */
  private long slotTimestamp(final BlockHeader head) {
    final long slotSeconds =
        Math.max(1, protocolSchedule.getByBlockHeader(head).getSlotDuration().toSeconds());
    final long sinceHeadMillis = clock.millis() - TimeUnit.SECONDS.toMillis(head.getTimestamp());
    final long slots =
        Math.max(1, Math.ceilDiv(sinceHeadMillis, TimeUnit.SECONDS.toMillis(slotSeconds)));
    return head.getTimestamp() + slots * slotSeconds;
  }

  private void countPrewarmed(final Warming stopped, final Block block) {
    final List<Transaction> transactions = block.getBody().getTransactions();
    final long prewarmed =
        transactions.stream().filter(tx -> stopped.warmed().contains(tx.getHash())).count();
    prewarmedBlockTransactions.inc(prewarmed);
    missedBlockTransactions.inc(transactions.size() - prewarmed);
    LOG.atDebug()
        .setMessage("Prewarmed {} of the {} transactions of block {}, {} for its parent in total")
        .addArgument(prewarmed)
        .addArgument(transactions::size)
        .addArgument(block::toLogString)
        .addArgument(() -> stopped.warmed().size())
        .log();
  }

  private void warm(final Warming warming) {
    final Passes passes;
    try {
      // until the pool has caught up with the head, it still offers what the head included
      passes = new Passes(warming, execution(warming), transactionsOf(warming.head()));
    } catch (final RuntimeException e) {
      LOG.debug("Cannot prewarm for head {}", warming.head().toLogString(), e);
      // a later start for the same head tries again
      generation.compareAndSet(warming.generation(), warming.generation() + 1);
      return;
    }
    if (warming.started().compareAndSet(false, true)) {
      startedWarmings.inc();
    }
    pass(passes);
  }

  private void pass(final Passes passes) {
    final Warming warming = passes.warming;
    if (!isWarming(warming)) {
      return;
    }
    final List<Future<?>> executions = new ArrayList<>();
    try {
      for (final List<Transaction> transactions : sendersToWarm(passes)) {
        executions.add(executors.submit(() -> execute(warming, passes.execution, transactions)));
      }
    } catch (final RejectedExecutionException e) {
      return;
    } catch (final RuntimeException e) {
      // the next pass tries again
      LOG.debug("Prewarming pass for head {} failed", warming.head().toLogString(), e);
    }
    // a pass ends before the next one starts, so a slow pass cannot pile up work
    for (final Future<?> future : executions) {
      if (!awaitExecution(warming, future)) {
        return;
      }
    }
    try {
      // waiting for the next pass leaves the coordinator free to warm for the next head
      coordinator.schedule(() -> pass(passes), PASS_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
    } catch (final RejectedExecutionException e) {
      LOG.debug(
          "Prewarming for head {} ends, the prewarmer is closed", warming.head().toLogString());
    }
  }

  /**
   * Waits for an execution while its warming is current. An execution that makes no calls runs on
   * after a stop, and would otherwise hold up the warming for the next head.
   *
   * @return whether the warming is still current
   */
  private boolean awaitExecution(final Warming warming, final Future<?> future) {
    while (isCurrent(warming)) {
      try {
        future.get(STOP_CHECK_MILLIS, TimeUnit.MILLISECONDS);
        return true;
      } catch (final TimeoutException e) {
        // looks again whether the warming is still current
      } catch (final ExecutionException e) {
        LOG.trace("Prewarming failed", e);
        return true;
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return false;
  }

  private Execution execution(final Warming warming) {
    final BlockHeader head = warming.head();
    final ProtocolSpec protocolSpec =
        protocolSchedule.getForNextBlockHeader(head, warming.timestamp());
    final ProcessableBlockHeader header =
        TransactionSimulator.simulatePendingBlockHeader(
            protocolSchedule, miningConfiguration, head, warming.timestamp());
    final Wei blobGasPrice =
        protocolSpec
            .getFeeMarket()
            .blobGasPricePerGas(calculateExcessBlobGasForParent(protocolSpec, head));
    final BlockHashLookup blockHashLookup =
        protocolSpec
            .getPreExecutionProcessor()
            .createBlockHashLookup(protocolContext.getBlockchain(), header);
    return new Execution(header, protocolSpec, blobGasPrice, blockHashLookup);
  }

  private Set<Hash> transactionsOf(final BlockHeader head) {
    return protocolContext
        .getBlockchain()
        .getBlockBody(head.getHash())
        .map(
            body ->
                body.getTransactions().stream()
                    .map(Transaction::getHash)
                    .collect(Collectors.toUnmodifiableSet()))
        .orElse(Set.of());
  }

  private List<List<Transaction>> sendersToWarm(final Passes passes) {
    final long additions = poolAdditions.get();
    final long now = clock.millis();
    if (additions == passes.copiedAdditions && now - passes.copiedAt < POOL_COPY_INTERVAL_MILLIS) {
      return List.of();
    }
    // copying holds the pool's lock, and a busy pool takes a while to group by sender
    try (OperationTimer.TimingContext ignored = poolCopyTimer.startTimer()) {
      return copySendersToWarm(passes, additions, now);
    }
  }

  private List<List<Transaction>> copySendersToWarm(
      final Passes passes, final long additions, final long now) {
    final Warming warming = passes.warming;
    final List<PendingTransaction> candidates = new ArrayList<>();
    // returning no results leaves the pool as it is
    transactionPool.selectTransactions(
        pendingTransactions -> {
          candidates.addAll(pendingTransactions);
          return Map.of();
        });
    passes.copiedAdditions = additions;
    passes.copiedAt = now;
    final Map<Address, List<Transaction>> bySender = new LinkedHashMap<>();
    for (final PendingTransaction candidate : candidates) {
      if (passes.included.contains(candidate.getHash())) {
        continue;
      }
      bySender
          .computeIfAbsent(candidate.getSender(), sender -> new ArrayList<>())
          .add(candidate.getTransaction());
    }
    final Optional<Wei> baseFee = passes.execution.header().getBaseFee();
    final List<List<Transaction>> senders = new ArrayList<>();
    // the transactions warmed before read what is cached already, so these senders go last
    final List<List<Transaction>> sendersAgain = new ArrayList<>();
    for (final List<Transaction> transactions : bySender.values()) {
      transactions.sort(Comparator.comparingLong(Transaction::getNonce));
      final List<Transaction> warmable = new ArrayList<>();
      for (final Transaction transaction : transactions) {
        // the next block takes neither a transaction below its base fee nor the sender's later
        // ones, and executing a blob transaction verifies its KZG proofs again
        if (warmable.size() == MAX_TRANSACTIONS_PER_SENDER
            || transaction.getType().supportsBlob()
            || baseFee.filter(fee -> transaction.getMaxGasPrice().compareTo(fee) < 0).isPresent()) {
          break;
        }
        warmable.add(transaction);
      }
      final List<Hash> hashes = warmable.stream().map(Transaction::getHash).toList();
      if (warming.attempted().containsAll(hashes)) {
        continue;
      }
      // a sender with new transactions is warmed again from its first one, whose writes they read
      (hashes.stream().anyMatch(warming.attempted()::contains) ? sendersAgain : senders)
          .add(warmable);
    }
    senders.addAll(sendersAgain);
    return senders;
  }

  private void execute(
      final Warming warming, final Execution execution, final List<Transaction> transactions) {
    if (!isWarming(warming)) {
      return;
    }
    // a sender that fails is not tried again until it has new transactions
    transactions.forEach(transaction -> warming.attempted().add(transaction.getHash()));
    try (final MutableWorldState worldState = headWorldState(warming.head())) {
      if (worldState instanceof BonsaiWorldState bonsaiWorldState) {
        // the state root of these executions is never computed
        bonsaiWorldState.disableCacheMerkleTrieLoader();
      }
      final ProtocolSpec protocolSpec = execution.protocolSpec();
      final MainnetTransactionProcessor transactionProcessor =
          protocolSpec.getTransactionProcessor();
      // the senders share the hashes found, each walking the chain on its own
      final BlockHashLookup blockHashLookup = execution.blockHashLookup().forkForParallelWorker();
      final WorldUpdater updater = worldState.updater();
      final OperationTracer tracer = new StoppingTracer(warming);
      for (final Transaction transaction : transactions) {
        // runs for a sender's new transactions are charged again, so the budget caps the work
        if (!isWarming(warming) || !reserve(warming, transaction.getGasLimit())) {
          break;
        }
        final WorldUpdater transactionUpdater = updater.updater();
        final long startedNanos = System.nanoTime();
        runningExecutions.incrementAndGet();
        final TransactionProcessingResult result;
        try {
          result =
              transactionProcessor.processTransaction(
                  transactionUpdater,
                  execution.header(),
                  transaction,
                  execution.header().getCoinbase(),
                  tracer,
                  blockHashLookup,
                  VALIDATION_PARAMS,
                  execution.blobGasPrice());
        } finally {
          runningExecutions.decrementAndGet();
          final long blockStarted = blockStartedNanos;
          if (blockStarted - startedNanos > 0) {
            executionAfterBlock.observe((System.nanoTime() - blockStarted) / 1e9);
          }
        }
        if (!isCurrent(warming)) {
          // the tracer may have stopped the transaction halfway
          break;
        }
        if (result.isInvalid()) {
          invalidTransactions.inc();
          warming.gasBudget().addAndGet(transaction.getGasLimit());
          continue;
        }
        // the sender's next transaction reads what this one wrote, as in a block
        transactionUpdater.commit();
        updater.commit();
        updater.markTransactionBoundary();
        executedTransactions.inc();
        warming.warmed().add(transaction.getHash());
        executedGas.inc(transaction.getGasLimit() - result.getGasRemaining());
        warming.gasBudget().addAndGet(result.getGasRemaining());
      }
    } catch (final Exception e) {
      LOG.atTrace()
          .setMessage("Prewarming the transactions of {} failed")
          .addArgument(() -> transactions.getFirst().getSender())
          .setCause(e)
          .log();
    }
  }

  private MutableWorldState headWorldState(final BlockHeader head) {
    return protocolContext
        .getWorldStateArchive()
        .getWorldState(withBlockHeaderAndNoUpdateNodeHead(head))
        .orElseThrow(() -> new IllegalStateException("No world state for " + head.toLogString()));
  }

  private record Execution(
      ProcessableBlockHeader header,
      ProtocolSpec protocolSpec,
      Wei blobGasPrice,
      BlockHashLookup blockHashLookup) {}

  /** What the passes of one warming carry from one pass to the next. They run one at a time. */
  private static final class Passes {
    private final Warming warming;
    private final Execution execution;
    private final Set<Hash> included;
    private long copiedAdditions = -1;
    private long copiedAt;

    private Passes(final Warming warming, final Execution execution, final Set<Hash> included) {
      this.warming = warming;
      this.execution = execution;
      this.included = included;
    }
  }

  /**
   * Stops an execution once its warming is over. It looks when a call starts or ends rather than at
   * every operation, so that EVM v2 leaves out its per-operation tracer calls. EVM v1 makes them
   * for every tracer but {@link OperationTracer#NO_TRACING}.
   */
  private final class StoppingTracer implements OperationTracer {
    private final Warming warming;

    private StoppingTracer(final Warming warming) {
      this.warming = warming;
    }

    @Override
    public boolean tracesOperations() {
      return false;
    }

    @Override
    public void traceContextEnter(final MessageFrame frame) {
      stopIfOver();
    }

    @Override
    public void traceContextReEnter(final MessageFrame frame) {
      stopIfOver();
    }

    @Override
    public void traceContextExit(final MessageFrame frame) {
      stopIfOver();
    }

    @Override
    public void tracePrecompileCall(
        final MessageFrame frame, final long gasRequirement, final Bytes output) {
      stopIfOver();
    }

    private void stopIfOver() {
      if (!isCurrent(warming)) {
        // the transaction processor ends an interrupted transaction as invalid
        throw new RuntimeException(new InterruptedException("Prewarming stopped"));
      }
    }
  }

  /**
   * Prewarming for one head, for the slot that starts at {@code timestamp}. The transactions of a
   * sender that were tried are in {@code attempted}, those that ran in {@code warmed}.
   */
  private record Warming(
      long generation,
      BlockHeader head,
      long timestamp,
      AtomicLong gasBudget,
      AtomicBoolean budgetUsedUp,
      AtomicBoolean building,
      AtomicBoolean started,
      Set<Hash> attempted,
      Set<Hash> warmed) {

    private Warming(
        final long generation, final BlockHeader head, final long timestamp, final long gasBudget) {
      this(
          generation,
          head,
          timestamp,
          new AtomicLong(gasBudget),
          new AtomicBoolean(),
          new AtomicBoolean(),
          new AtomicBoolean(),
          ConcurrentHashMap.newKeySet(),
          ConcurrentHashMap.newKeySet());
    }

    /**
     * A second after the slot warmed for starts, as blocks arrive after the start of their slot.
     */
    private long deadline() {
      return TimeUnit.SECONDS.toMillis(timestamp) + DEADLINE_AFTER_SLOT_MILLIS;
    }

    /**
     * This prewarming under a new generation, sharing its budget and what ran. The senders that did
     * not run are tried again, as the stop may have cut them short; executions still running stop
     * at their next call.
     */
    private Warming resumed(final long generation) {
      final Set<Hash> ran = ConcurrentHashMap.newKeySet();
      ran.addAll(warmed);
      return new Warming(
          generation, head, timestamp, gasBudget, budgetUsedUp, building, started, ran, warmed);
    }
  }
}
