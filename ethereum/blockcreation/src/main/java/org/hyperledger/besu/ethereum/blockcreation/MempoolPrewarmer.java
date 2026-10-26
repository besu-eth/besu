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

import static org.hyperledger.besu.ethereum.mainnet.feemarket.ExcessBlobGasCalculator.calculateExcessBlobGasForParent;
import static org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes pending transactions against the head state between blocks, so that the state the next
 * block is likely to read is in the caches when the block arrives. The executions are discarded;
 * only their reads matter.
 *
 * <p>Warming starts when the head moves and runs in passes until a block arrives, block building
 * starts, the gas budget is used up, or the next slot is over. Each pass takes the pool's
 * candidates in the order block building takes them and executes the senders it has not warmed yet,
 * each sender's transactions in nonce order on its own throwaway copy of the head state. The trie
 * paths of the state the executions change are preloaded as well, for the next block's state root.
 */
public class MempoolPrewarmer implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(MempoolPrewarmer.class);

  private static final long PASS_INTERVAL_MILLIS = 250;
  // blocks arrive after the start of their slot, and transactions sent in that time are still in
  // them
  private static final long DEADLINE_AFTER_SLOT_MILLIS = 1_000;
  // a sender with a long queue would otherwise take the whole budget of a pass
  private static final int MAX_TRANSACTIONS_PER_SENDER = 16;
  private static final long CLOSE_TIMEOUT_SECONDS = 5;
  private static final TransactionValidationParams VALIDATION_PARAMS =
      TransactionValidationParams.transactionSimulatorAllowExceedingBalanceAndFutureNonce();

  private final ProtocolContext protocolContext;
  private final ProtocolSchedule protocolSchedule;
  private final TransactionPool transactionPool;
  private final MiningConfiguration miningConfiguration;
  private final int gasLimitMultiplier;
  private final Clock clock;
  private final ExecutorService coordinator;
  private final ExecutorService executors;
  private final AtomicLong generation = new AtomicLong();
  @Nullable private volatile Warming warming;
  private volatile boolean closed;

  private final Counter executedTransactions;
  private final Counter invalidTransactions;
  private final Counter prewarmedBlockTransactions;
  private final Counter missedBlockTransactions;

  /**
   * Creates a prewarmer.
   *
   * @param protocolContext the protocol context
   * @param protocolSchedule the protocol schedule
   * @param transactionPool the pool whose transactions are executed
   * @param miningConfiguration the mining configuration, for the pending block header
   * @param threads the number of threads that execute transactions
   * @param gasLimitMultiplier the gas executed between two blocks, as a multiple of the gas limit
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
    this.protocolContext = protocolContext;
    this.protocolSchedule = protocolSchedule;
    this.transactionPool = transactionPool;
    this.miningConfiguration = miningConfiguration;
    this.gasLimitMultiplier = gasLimitMultiplier;
    this.clock = clock;
    this.coordinator =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat("besu-mempool-prewarm-passes")
                .setDaemon(true)
                .build());
    this.executors =
        Executors.newFixedThreadPool(
            threads,
            new ThreadFactoryBuilder()
                .setNameFormat("besu-mempool-prewarm-%d")
                .setDaemon(true)
                .build());

    final LabelledMetric<Counter> transactions =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.BLOCK_PROCESSING,
            "mempool_prewarm_transactions_total",
            "Pending transactions executed to prewarm the state, by whether they were valid on the"
                + " head state",
            "result");
    this.executedTransactions = transactions.labels("executed");
    this.invalidTransactions = transactions.labels("invalid");
    final LabelledMetric<Counter> blockTransactions =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.BLOCK_PROCESSING,
            "mempool_prewarm_block_transactions_total",
            "Transactions of blocks that arrived while the state was prewarmed for their parent, by"
                + " whether they had been prewarmed",
            "prewarmed");
    this.prewarmedBlockTransactions = blockTransactions.labels("true");
    this.missedBlockTransactions = blockTransactions.labels("false");
  }

  /**
   * Starts prewarming for a new head, stopping any prewarming for an earlier one.
   *
   * @param head the new head
   */
  public void start(final BlockHeader head) {
    if (!transactionPool.isEnabled()) {
      return;
    }
    final Warming current = warming;
    if (current != null
        && current.head().getHash().equals(head.getHash())
        && (isCurrent(current) || current.building().get())
        && clock.millis() < current.deadline()) {
      return;
    }
    final long deadline;
    try {
      deadline = deadline(head);
    } catch (final RuntimeException e) {
      LOG.debug("Cannot prewarm for head {}", head.toLogString(), e);
      return;
    }
    final long gasLimit = head.getGasLimit();
    final Warming next =
        new Warming(
            generation.incrementAndGet(),
            head,
            deadline,
            gasLimit > Long.MAX_VALUE / gasLimitMultiplier
                ? Long.MAX_VALUE
                : gasLimit * gasLimitMultiplier);
    warming = next;
    try {
      coordinator.execute(() -> warm(next));
    } catch (final RejectedExecutionException e) {
      LOG.debug("Not prewarming for head {}, the prewarmer is closed", head.toLogString());
    }
  }

  /**
   * Stops prewarming, as a block arrived or mining stops. Executions already running finish their
   * current transaction.
   *
   * @param arrivingBlock the block that arrived, if one did
   */
  public void stop(final Optional<Block> arrivingBlock) {
    final Warming stopped = warming;
    final long running = generation.getAndIncrement();
    // a block counts once, and only against prewarming that ran until it arrived
    if (stopped == null || stopped.generation() != running) {
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
   * Stops prewarming for good and waits until the running executions have finished their current
   * transaction, so that none of them reads the state after its storage is closed.
   */
  @Override
  public void close() {
    closed = true;
    generation.incrementAndGet();
    coordinator.shutdownNow();
    executors.shutdown();
    try {
      if (!executors.awaitTermination(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        LOG.debug("Prewarming executions still running after {} s", CLOSE_TIMEOUT_SECONDS);
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @VisibleForTesting
  Set<Hash> prewarmedTransactions() {
    final Warming current = warming;
    return current == null ? Set.of() : Set.copyOf(current.warmed());
  }

  @VisibleForTesting
  Set<Address> preloadedAccounts() {
    final Warming current = warming;
    return current == null ? Set.of() : Set.copyOf(current.preloadedAccounts());
  }

  private boolean isCurrent(final Warming warming) {
    return warming.generation() == generation.get();
  }

  private boolean isWarming(final Warming warming) {
    return isCurrent(warming)
        && warming.gasBudget().get() > 0
        && clock.millis() < warming.deadline();
  }

  /**
   * A second after the start of the first slot after the head that has not started yet, so that a
   * head kept through a missed slot is warmed for the slot after it as well.
   */
  private long deadline(final BlockHeader head) {
    final long slotMillis =
        Math.max(1, protocolSchedule.getByBlockHeader(head).getSlotDuration().toMillis());
    final long headMillis = TimeUnit.SECONDS.toMillis(head.getTimestamp());
    final long slots = Math.max(1, Math.ceilDiv(clock.millis() - headMillis, slotMillis));
    return headMillis + slots * slotMillis + DEADLINE_AFTER_SLOT_MILLIS;
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
    final Execution execution;
    try {
      execution = execution(warming.head());
    } catch (final RuntimeException e) {
      LOG.debug("Cannot prewarm for head {}", warming.head().toLogString(), e);
      return;
    }
    // until the pool has caught up with the head, it still offers what the head included
    final Set<Hash> included = transactionsOf(warming.head());
    while (isWarming(warming)) {
      final List<Future<?>> executions = new ArrayList<>();
      try {
        for (final List<Transaction> transactions : sendersToWarm(warming, execution, included)) {
          executions.add(executors.submit(() -> execute(warming, execution, transactions)));
        }
      } catch (final RejectedExecutionException e) {
        return;
      }
      // a pass ends before the next one starts, so a slow pass cannot pile up work
      for (final Future<?> future : executions) {
        try {
          future.get();
        } catch (final ExecutionException e) {
          LOG.trace("Prewarming failed", e);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
      try {
        Thread.sleep(PASS_INTERVAL_MILLIS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  private Execution execution(final BlockHeader head) {
    final long timestamp =
        head.getTimestamp() + protocolSchedule.getByBlockHeader(head).getSlotDuration().toSeconds();
    final ProtocolSpec protocolSpec = protocolSchedule.getForNextBlockHeader(head, timestamp);
    final ProcessableBlockHeader header =
        BlockHeaderBuilder.createPending(
                protocolSpec,
                head,
                miningConfiguration,
                timestamp,
                Optional.empty(),
                Optional.empty(),
                Optional.empty())
            .buildProcessableBlockHeader();
    final Wei blobGasPrice =
        protocolSpec
            .getFeeMarket()
            .blobGasPricePerGas(calculateExcessBlobGasForParent(protocolSpec, head));
    return new Execution(header, protocolSpec, blobGasPrice);
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

  private List<List<Transaction>> sendersToWarm(
      final Warming warming, final Execution execution, final Set<Hash> included) {
    final List<PendingTransaction> candidates = new ArrayList<>();
    // returning no results leaves the pool as it is
    transactionPool.selectTransactions(
        pendingTransactions -> {
          candidates.addAll(pendingTransactions);
          return Map.of();
        });
    final Map<Address, List<Transaction>> bySender = new LinkedHashMap<>();
    for (final PendingTransaction candidate : candidates) {
      if (included.contains(candidate.getHash())) {
        continue;
      }
      bySender
          .computeIfAbsent(candidate.getSender(), sender -> new ArrayList<>())
          .add(candidate.getTransaction());
    }
    final Optional<Wei> baseFee = execution.header().getBaseFee();
    final List<List<Transaction>> senders = new ArrayList<>();
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
      // a sender with new transactions is warmed again from its first one, whose writes they read
      if (!warmable.isEmpty()
          && !warming
              .attempted()
              .containsAll(warmable.stream().map(Transaction::getHash).toList())) {
        senders.add(warmable);
      }
    }
    return senders;
  }

  private void execute(
      final Warming warming, final Execution execution, final List<Transaction> transactions) {
    if (!isWarming(warming)) {
      return;
    }
    transactions.forEach(transaction -> warming.attempted().add(transaction.getHash()));
    final Optional<MutableWorldState> maybeWorldState =
        protocolContext
            .getWorldStateArchive()
            .getWorldState(withBlockHeaderAndNoUpdateNodeHead(warming.head()));
    if (maybeWorldState.isEmpty()) {
      return;
    }
    try (final MutableWorldState worldState = maybeWorldState.get()) {
      if (worldState instanceof BonsaiWorldState bonsaiWorldState) {
        // the state root of these executions is never computed
        bonsaiWorldState.disableCacheMerkleTrieLoader();
      }
      final ProtocolSpec protocolSpec = execution.protocolSpec();
      final MainnetTransactionProcessor transactionProcessor =
          protocolSpec.getTransactionProcessor();
      final BlockHashLookup blockHashLookup =
          protocolSpec
              .getPreExecutionProcessor()
              .createBlockHashLookup(protocolContext.getBlockchain(), execution.header());
      final WorldUpdater updater = worldState.updater();
      for (final Transaction transaction : transactions) {
        if (!isWarming(warming)) {
          break;
        }
        final WorldUpdater transactionUpdater = updater.updater();
        final TransactionProcessingResult result =
            transactionProcessor.processTransaction(
                transactionUpdater,
                execution.header(),
                transaction,
                execution.header().getCoinbase(),
                blockHashLookup,
                VALIDATION_PARAMS,
                execution.blobGasPrice());
        if (result.isInvalid()) {
          invalidTransactions.inc();
          continue;
        }
        // the sender's next transaction reads what this one wrote
        transactionUpdater.commit();
        executedTransactions.inc();
        warming.warmed().add(transaction.getHash());
        warming.gasBudget().addAndGet(result.getGasRemaining() - transaction.getGasLimit());
      }
      // closing waits only for the executions, not for preloads they start
      if (!closed) {
        preloadChangedPaths(warming, updater);
      }
    } catch (final Exception e) {
      LOG.atTrace()
          .setMessage("Prewarming the transactions of {} failed")
          .addArgument(() -> transactions.getFirst().getSender())
          .setCause(e)
          .log();
    }
  }

  /**
   * Preloads the trie paths of the accounts and slots the executions changed, from the head state,
   * so the next block's state root finds them cached. Each path is preloaded once per head.
   */
  private void preloadChangedPaths(final Warming warming, final WorldUpdater updater) {
    if (!(updater instanceof PathBasedWorldStateUpdateAccumulator<?> accumulator)
        || !(protocolContext.getWorldStateArchive() instanceof BonsaiWorldStateProvider archive)) {
      return;
    }
    // the world state is thrown away, committing only moves the changes where they can be read
    accumulator.commit();
    final BonsaiCachedMerkleTrieLoader loader = archive.getCachedMerkleTrieLoader();
    final BonsaiWorldStateKeyValueStorage storage = archive.getWorldStateKeyValueStorage();
    final Set<Address> changedAccounts = new HashSet<>();
    accumulator
        .getStorageToUpdate()
        .forEach(
            (address, slots) ->
                slots.forEach(
                    (slotKey, slot) -> {
                      if (!slot.isUnchanged()) {
                        changedAccounts.add(address);
                        if (warming.preloadedSlots().add(new ChangedSlot(address, slotKey))) {
                          loader.preLoadStorageSlot(storage, address, slotKey);
                        }
                      }
                    }));
    accumulator
        .getAccountsToUpdate()
        .forEach(
            (address, value) -> {
              if (changesTrieValue(value.getPrior(), value.getUpdated())) {
                changedAccounts.add(address);
              }
            });
    for (final Address address : changedAccounts) {
      if (warming.preloadedAccounts().add(address)) {
        loader.preLoadAccount(storage, warming.head().getStateRoot(), address);
      }
    }
  }

  /** Whether the account trie stores a different value for the two versions of an account. */
  private static boolean changesTrieValue(final BonsaiAccount prior, final BonsaiAccount updated) {
    if (prior == null || updated == null) {
      return prior != updated;
    }
    return prior.getNonce() != updated.getNonce()
        || !prior.getBalance().equals(updated.getBalance())
        || !prior.getStorageRoot().equals(updated.getStorageRoot())
        || !prior.getCodeHash().equals(updated.getCodeHash());
  }

  private record Execution(
      ProcessableBlockHeader header, ProtocolSpec protocolSpec, Wei blobGasPrice) {}

  private record ChangedSlot(Address address, StorageSlotKey slotKey) {}

  /**
   * Prewarming for one head. The transactions of a sender that were tried are in {@code attempted},
   * those that ran in {@code warmed}.
   */
  private record Warming(
      long generation,
      BlockHeader head,
      long deadline,
      AtomicLong gasBudget,
      AtomicBoolean building,
      Set<Hash> attempted,
      Set<Hash> warmed,
      Set<Address> preloadedAccounts,
      Set<ChangedSlot> preloadedSlots) {

    private Warming(
        final long generation, final BlockHeader head, final long deadline, final long gasBudget) {
      this(
          generation,
          head,
          deadline,
          new AtomicLong(gasBudget),
          new AtomicBoolean(),
          ConcurrentHashMap.newKeySet(),
          ConcurrentHashMap.newKeySet(),
          ConcurrentHashMap.newKeySet(),
          ConcurrentHashMap.newKeySet());
    }
  }
}
