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
package org.hyperledger.besu.ethereum.mainnet.parallelization.prefetch;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.RunaheadCalls;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs a long transaction ahead of its execution to load the storage slots it reads.
 *
 * <p>A transaction that reads hundreds of slots nobody read recently waits for one disk read after
 * the other. The run ahead executes it on a {@link RunaheadWorldView} of the parent state, where a
 * slot that is not in memory reads as empty while a background thread loads it and the trie nodes
 * on its path. The slots a transaction reads mostly follow from its input rather than from the
 * values it reads, so the run ahead reaches them long before the execution does, and their reads
 * run side by side. Its results are dropped.
 */
public final class RunaheadPrefetcher {

  private static final Logger LOG = LoggerFactory.getLogger(RunaheadPrefetcher.class);

  private static final ExecutorService RUNS =
      Executors.newFixedThreadPool(
          Integer.getInteger("besu.runahead.threads", 4), daemonThreads("besu-runahead"));

  private static final ExecutorService LOADS =
      Executors.newFixedThreadPool(
          Integer.getInteger("besu.runahead.loadThreads", 32), daemonThreads("besu-runahead-load"));

  private static final boolean LOG_RUNS = Boolean.getBoolean("besu.bench.logRunahead");

  private static final int MAX_PASSES = Integer.getInteger("besu.runahead.passes", 4);

  // one run per placeholder, side by side: checks that a slot is set pass with one, checks that it
  // is not with zero
  private static final List<Optional<UInt256>> PLACEHOLDERS =
      Arrays.stream(System.getProperty("besu.runahead.placeholder", "1").split(","))
          .map(String::trim)
          .map(
              v ->
                  v.equals("0")
                      ? Optional.<UInt256>empty()
                      : Optional.of(
                          UInt256.fromHexString(
                              v.equals("max")
                                  ? "0x" + "ff".repeat(32)
                                  : "0x" + Long.toHexString(Long.parseLong(v)))))
          .toList();

  private static final long LOAD_WAIT_NANOS =
      Long.getLong("besu.runahead.loadWaitMicros", 5_000L) * 1_000L;

  // experiment: load the trie nodes on a slot's or account's path on their own threads, after its
  // value, so that the next pass waits for the values only; the nodes only matter for the state
  // root
  private static final boolean TRIE_AFTER = Boolean.getBoolean("besu.runahead.trieAfter");

  private static final ExecutorService TRIE_LOADS =
      Executors.newFixedThreadPool(
          Integer.getInteger("besu.runahead.trieLoadThreads", 8),
          daemonThreads("besu-runahead-trie"));

  // experiment: skip the run ahead of a transaction when the last one with the same sender, target
  // and function loaded fewer values per millisecond than this, since such a run (a long
  // computation) only competes with the execution for a core
  private static final double MIN_LOADS_PER_MS =
      Double.parseDouble(System.getProperty("besu.runahead.minLoadsPerMs", "0"));

  // a kind of transaction that is skipped still runs ahead now and then, in case it starts loading
  private static final int PROBE_EVERY = Integer.getInteger("besu.runahead.probeEvery", 8);

  // runs shorter than this cost too little to be worth skipping
  private static final long MIN_SKIPPED_RUN_MICROS = 2_000L;

  private static final int MAX_UNPRODUCTIVE_KINDS = 4_096;

  private static final Map<Bytes, AtomicInteger> UNPRODUCTIVE = new ConcurrentHashMap<>();

  // experiment: run the passes side by side, each starting this long after the one before while
  // the passes keep finding slots, instead of one after the other. A pass then reads the values
  // the one before it loaded a moment earlier and finds the slots that depend on them, so a chain
  // of
  // dependent reads is found in about one pass instead of one pass per link. 0 runs them in turn.
  private static final long STAGGER_NANOS =
      Long.getLong("besu.runahead.staggerMicros", 0L) * 1_000L;

  private static final ScheduledExecutorService STAGGER =
      Executors.newSingleThreadScheduledExecutor(daemonThreads("besu-runahead-stagger"));

  static {
    RunaheadCalls.Switch.enable();
  }

  private RunaheadPrefetcher() {}

  /**
   * Starts running a transaction ahead of its execution, in the background.
   *
   * @param transactionProcessor the processor that executes the transaction
   * @param protocolContext the protocol context, for the parent world state
   * @param parentHeader the header of the parent block
   * @param blockHeader the header of the block the transaction is in
   * @param transaction the transaction
   * @param miningBeneficiary the block's coinbase account
   * @param blockHashLookup the block hash lookup, forked for the run
   * @param blobGasPrice the blob gas price of the block
   * @param validationParams the validation that lets the transaction run on the parent state
   * @param stopped whether the transaction was executed, after which further passes are wasted
   * @return completes once the first pass and its loads are done
   */
  public static CompletableFuture<Void> runAhead(
      final MainnetTransactionProcessor transactionProcessor,
      final ProtocolContext protocolContext,
      final BlockHeader parentHeader,
      final BlockHeader blockHeader,
      final Transaction transaction,
      final Address miningBeneficiary,
      final BlockHashLookup blockHashLookup,
      final Wei blobGasPrice,
      final TransactionValidationParams validationParams,
      final BooleanSupplier stopped) {
    final Bytes kind = MIN_LOADS_PER_MS > 0 ? kindOf(transaction) : null;
    if (kind != null) {
      final AtomicInteger skips = UNPRODUCTIVE.get(kind);
      if (skips != null && skips.incrementAndGet() % PROBE_EVERY != 0) {
        if (LOG_RUNS) {
          LOG.info(
              "Bench runahead block {} hash {} skipped",
              blockHeader.getNumber(),
              transaction.getHash().toHexString());
        }
        return CompletableFuture.completedFuture(null);
      }
    }
    final CompletableFuture<Void> firstPass = new CompletableFuture<>();
    for (final Optional<UInt256> placeholder : PLACEHOLDERS) {
      final BlockHashLookup lookup = blockHashLookup.forkForParallelWorker();
      if (STAGGER_NANOS > 0) {
        new StaggeredRun(
                transactionProcessor,
                protocolContext,
                parentHeader,
                blockHeader,
                transaction,
                miningBeneficiary,
                lookup,
                blobGasPrice,
                validationParams,
                placeholder,
                kind,
                stopped,
                firstPass)
            .start();
        continue;
      }
      try {
        RUNS.execute(
            () ->
                run(
                    transactionProcessor,
                    protocolContext,
                    parentHeader,
                    blockHeader,
                    transaction,
                    miningBeneficiary,
                    lookup,
                    blobGasPrice,
                    validationParams,
                    placeholder,
                    kind,
                    stopped,
                    firstPass));
      } catch (final RejectedExecutionException e) {
        LOG.trace("Run ahead of {} not started", transaction.getHash(), e);
        firstPass.complete(null);
      }
    }
    return firstPass;
  }

  private static void run(
      final MainnetTransactionProcessor transactionProcessor,
      final ProtocolContext protocolContext,
      final BlockHeader parentHeader,
      final BlockHeader blockHeader,
      final Transaction transaction,
      final Address miningBeneficiary,
      final BlockHashLookup blockHashLookup,
      final Wei blobGasPrice,
      final TransactionValidationParams validationParams,
      final Optional<UInt256> placeholder,
      final Bytes kind,
      final BooleanSupplier stopped,
      final CompletableFuture<Void> firstPass) {
    if (stopped.getAsBoolean()) {
      // queued until its transaction was executed
      firstPass.complete(null);
      return;
    }
    final BonsaiWorldState worldState =
        protocolContext
            .getWorldStateArchive()
            .getWorldState(WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(parentHeader))
            .map(BonsaiWorldState.class::cast)
            .orElse(null);
    if (worldState == null) {
      firstPass.complete(null);
      return;
    }
    final Loads loads = new Loads(worldState);
    final long start = System.nanoTime();
    String outcome = "?";
    int passes = 0;
    final StringBuilder passLog = LOG_RUNS ? new StringBuilder() : null;
    try {
      final RunaheadWorldView view = new RunaheadWorldView(worldState, loads, placeholder);
      final EvmConfiguration evmConfiguration =
          ((PathBasedWorldStateUpdateAccumulator<?>) worldState.updater()).getEvmConfiguration();
      while (passes < MAX_PASSES) {
        if (stopped.getAsBoolean()) {
          outcome += " stopped";
          break;
        }
        passes++;
        final long passStart = System.nanoTime();
        final int requestedBefore = loads.requested.size();
        final BonsaiWorldStateUpdateAccumulator accumulator =
            new BonsaiWorldStateUpdateAccumulator(
                view,
                (address, value) -> {},
                (address, value) -> {},
                evmConfiguration,
                worldState.codeCache());
        RunaheadCalls.CURRENT.set(new Calls());
        final TransactionProcessingResult result;
        try {
          result =
              transactionProcessor.processTransaction(
                  accumulator.updater(),
                  blockHeader,
                  transaction.detachedCopy(),
                  miningBeneficiary,
                  OperationTracer.NO_TRACING,
                  blockHashLookup,
                  validationParams,
                  blobGasPrice,
                  Optional.empty());
        } finally {
          RunaheadCalls.CURRENT.remove();
        }
        outcome =
            result.isInvalid()
                ? "invalid " + result.getValidationResult().getErrorMessage()
                : result.isSuccessful() ? "success" : "failed " + result.getRevertReason();
        if (passLog != null) {
          passLog
              .append(passLog.isEmpty() ? "" : ",")
              .append((System.nanoTime() - passStart) / 1000)
              .append('/')
              .append(loads.requested.size() - requestedBefore);
        }
        if (loads.requested.size() == requestedBefore) {
          // a further pass would read the same as this one
          break;
        }
        // a slot read as empty can stop a pass early, the next one reads its value and gets further
        final long waitStart = System.nanoTime();
        loads.awaitLoads(LOAD_WAIT_NANOS);
        if (passLog != null) {
          passLog.append('+').append((System.nanoTime() - waitStart) / 1000);
        }
        firstPass.complete(null);
      }
    } catch (final RuntimeException e) {
      // it only loads state, the execution reports any failure
      outcome = "exception " + e;
      LOG.trace("Run ahead of {} stopped", transaction.getHash(), e);
    } finally {
      final long micros = (System.nanoTime() - start) / 1000;
      if (kind != null) {
        recordProductivity(kind, loads.requested.size(), micros);
      }
      if (LOG_RUNS) {
        LOG.info(
            "Bench runahead block {} hash {} placeholder {} us {} passes {} loads {} outcome {} pass_us_loads {} t {}",
            blockHeader.getNumber(),
            transaction.getHash().toHexString(),
            placeholder.map(UInt256::toShortHexString).orElse("0"),
            micros,
            passes,
            loads.requested.size(),
            outcome,
            passLog,
            start / 1000);
      }
      firstPass.complete(null);
      loads.runDone();
    }
  }

  /** A run ahead whose passes run side by side, each a little after the one before. */
  private static final class StaggeredRun {
    private final MainnetTransactionProcessor transactionProcessor;
    private final ProtocolContext protocolContext;
    private final BlockHeader parentHeader;
    private final BlockHeader blockHeader;
    private final Transaction transaction;
    private final Address miningBeneficiary;
    private final BlockHashLookup blockHashLookup;
    private final Wei blobGasPrice;
    private final TransactionValidationParams validationParams;
    private final Optional<UInt256> placeholder;
    private final Bytes kind;
    private final BooleanSupplier stopped;
    private final CompletableFuture<Void> firstPass;
    private final long start = System.nanoTime();
    // the passes running and the next start scheduled; the last of them to end finishes the run
    private final AtomicInteger outstanding = new AtomicInteger(1);
    private final AtomicInteger passes = new AtomicInteger();
    private final AtomicInteger runningPasses = new AtomicInteger();
    private final Queue<String> passLog = new ConcurrentLinkedQueue<>();
    private volatile String outcome = "?";
    private volatile boolean stoppedEarly;
    private BonsaiWorldState worldState;
    private Loads loads;
    private EvmConfiguration evmConfiguration;

    StaggeredRun(
        final MainnetTransactionProcessor transactionProcessor,
        final ProtocolContext protocolContext,
        final BlockHeader parentHeader,
        final BlockHeader blockHeader,
        final Transaction transaction,
        final Address miningBeneficiary,
        final BlockHashLookup blockHashLookup,
        final Wei blobGasPrice,
        final TransactionValidationParams validationParams,
        final Optional<UInt256> placeholder,
        final Bytes kind,
        final BooleanSupplier stopped,
        final CompletableFuture<Void> firstPass) {
      this.transactionProcessor = transactionProcessor;
      this.protocolContext = protocolContext;
      this.parentHeader = parentHeader;
      this.blockHeader = blockHeader;
      this.transaction = transaction;
      this.miningBeneficiary = miningBeneficiary;
      this.blockHashLookup = blockHashLookup;
      this.blobGasPrice = blobGasPrice;
      this.validationParams = validationParams;
      this.placeholder = placeholder;
      this.kind = kind;
      this.stopped = stopped;
      this.firstPass = firstPass;
    }

    void start() {
      try {
        RUNS.execute(this::firstPassThenSchedule);
      } catch (final RejectedExecutionException e) {
        firstPass.complete(null);
      }
    }

    private void firstPassThenSchedule() {
      if (stopped.getAsBoolean()) {
        firstPass.complete(null);
        return;
      }
      worldState =
          protocolContext
              .getWorldStateArchive()
              .getWorldState(WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(parentHeader))
              .map(BonsaiWorldState.class::cast)
              .orElse(null);
      if (worldState == null) {
        firstPass.complete(null);
        return;
      }
      loads = new Loads(worldState);
      evmConfiguration =
          ((PathBasedWorldStateUpdateAccumulator<?>) worldState.updater()).getEvmConfiguration();
      runningPasses.incrementAndGet();
      try {
        scheduleNext(1, 0);
        runPass(0);
      } finally {
        firstPass.complete(null);
        ended();
      }
    }

    /** Starts the given pass after the stagger if the passes found new slots in the meantime. */
    private void scheduleNext(final int pass, final int requestedBefore) {
      if (pass >= MAX_PASSES) {
        return;
      }
      outstanding.incrementAndGet();
      try {
        STAGGER.schedule(
            () -> {
              try {
                final int requestedNow = loads.requested.size();
                if (stopped.getAsBoolean()) {
                  return;
                }
                // a pass started while loads are in flight waits for them and gets further
                if (requestedNow <= requestedBefore && loads.pendingValues() == 0) {
                  // a pass still running may find more, so the next pass waits for it
                  if (runningPasses.get() > 0) {
                    scheduleNext(pass, requestedNow);
                  }
                  return;
                }
                scheduleNext(pass + 1, requestedNow);
                outstanding.incrementAndGet();
                runningPasses.incrementAndGet();
                try {
                  RUNS.execute(
                      () -> {
                        try {
                          runPass(pass);
                        } finally {
                          ended();
                        }
                      });
                } catch (final RejectedExecutionException e) {
                  runningPasses.decrementAndGet();
                  ended();
                }
              } finally {
                ended();
              }
            },
            STAGGER_NANOS,
            TimeUnit.NANOSECONDS);
      } catch (final RejectedExecutionException e) {
        ended();
      }
    }

    /** Runs a pass, which counts as running from when it was submitted until it ends. */
    private void runPass(final int pass) {
      try {
        runPassBody(pass);
      } finally {
        runningPasses.decrementAndGet();
      }
    }

    private void runPassBody(final int pass) {
      if (stopped.getAsBoolean()) {
        stoppedEarly = true;
        return;
      }
      passes.incrementAndGet();
      final long passStart = System.nanoTime();
      final RunaheadWorldView view = new RunaheadWorldView(worldState, loads, placeholder);
      final BonsaiWorldStateUpdateAccumulator accumulator =
          new BonsaiWorldStateUpdateAccumulator(
              view,
              (address, value) -> {},
              (address, value) -> {},
              evmConfiguration,
              worldState.codeCache());
      RunaheadCalls.CURRENT.set(new Calls());
      try {
        final TransactionProcessingResult result =
            transactionProcessor.processTransaction(
                accumulator.updater(),
                blockHeader,
                transaction.detachedCopy(),
                miningBeneficiary,
                OperationTracer.NO_TRACING,
                blockHashLookup.forkForParallelWorker(),
                validationParams,
                blobGasPrice,
                Optional.empty());
        outcome =
            result.isInvalid()
                ? "invalid " + result.getValidationResult().getErrorMessage()
                : result.isSuccessful() ? "success" : "failed " + result.getRevertReason();
      } catch (final RuntimeException e) {
        // it only loads state, the execution reports any failure
        outcome = "exception " + e;
        LOG.trace("Run ahead of {} stopped", transaction.getHash(), e);
      } finally {
        RunaheadCalls.CURRENT.remove();
        if (LOG_RUNS) {
          passLog.add(
              pass
                  + "@"
                  + (passStart - start) / 1000
                  + ":"
                  + (System.nanoTime() - passStart) / 1000
                  + "/"
                  + view.requestedLoads());
        }
      }
    }

    private void ended() {
      if (outstanding.decrementAndGet() > 0) {
        return;
      }
      final long micros = (System.nanoTime() - start) / 1000;
      if (kind != null) {
        recordProductivity(kind, loads.requested.size(), micros);
      }
      if (LOG_RUNS) {
        LOG.info(
            "Bench runahead block {} hash {} placeholder {} us {} passes {} loads {} outcome {} pass_us_loads {} t {}",
            blockHeader.getNumber(),
            transaction.getHash().toHexString(),
            placeholder.map(UInt256::toShortHexString).orElse("0"),
            micros,
            passes.get(),
            loads.requested.size(),
            stoppedEarly ? outcome + " stopped" : outcome,
            String.join(",", passLog),
            start / 1000);
      }
      loads.runDone();
    }
  }

  private static Bytes kindOf(final Transaction transaction) {
    final Bytes payload = transaction.getPayload();
    return Bytes.concatenate(
        transaction.getSender().getBytes(),
        transaction.getTo().map(Address::getBytes).orElse(Bytes.EMPTY),
        payload.slice(0, Math.min(4, payload.size())));
  }

  private static void recordProductivity(final Bytes kind, final int loads, final long micros) {
    if (micros < MIN_SKIPPED_RUN_MICROS) {
      return;
    }
    if (loads < MIN_LOADS_PER_MS * micros / 1000.0) {
      if (UNPRODUCTIVE.size() >= MAX_UNPRODUCTIVE_KINDS) {
        UNPRODUCTIVE.clear();
      }
      UNPRODUCTIVE.putIfAbsent(kind, new AtomicInteger());
    } else {
      UNPRODUCTIVE.remove(kind);
    }
  }

  /** Predicts the output of a skipped call from the last call with the same function selector. */
  private static final class Calls implements RunaheadCalls {
    // a call that returns nothing yet reads as returning one, like a set slot
    private static final Bytes DEFAULT_OUTPUT = Bytes32.leftPad(Bytes.of(1));
    private final Map<Bytes, Bytes> outputs = new HashMap<>();
    private boolean lookingUp;

    @Override
    public void lookingUpCallTarget(final boolean lookingUp) {
      this.lookingUp = lookingUp;
    }

    @Override
    public boolean isLookingUpCallTarget() {
      return lookingUp;
    }

    @Override
    public Bytes predictedReturn(final Bytes input) {
      return outputs.getOrDefault(selector(input), DEFAULT_OUTPUT);
    }

    @Override
    public void recordReturn(final Bytes input, final Bytes output) {
      outputs.put(selector(input), output.copy());
    }

    private static Bytes selector(final Bytes input) {
      return input.size() >= 4 ? input.slice(0, 4).copy() : Bytes.EMPTY;
    }
  }

  /** The loads of one run ahead; the parent world state closes once the run and they are done. */
  static final class Loads {
    private final BonsaiWorldState worldState;
    // the run itself holds one
    private final AtomicInteger pending = new AtomicInteger(1);
    // the loads whose values are not in memory yet, which the next pass waits for
    private final AtomicInteger pendingValues = new AtomicInteger();
    private final Set<Bytes> requested = ConcurrentHashMap.newKeySet();
    // the loads whose values are not in memory yet, which a pass can wait for
    private final Map<Bytes, CompletableFuture<Void>> inFlight = new ConcurrentHashMap<>();
    private final Object idle = new Object();

    Loads(final BonsaiWorldState worldState) {
      this.worldState = worldState;
    }

    static Bytes slotKey(final Address address, final StorageSlotKey storageSlotKey) {
      return Bytes.concatenate(
          address.addressHash().getBytes(), storageSlotKey.getSlotHash().getBytes());
    }

    static Bytes accountKey(final Address address) {
      return address.getBytes();
    }

    boolean load(final Address address, final StorageSlotKey storageSlotKey) {
      final Bytes key = slotKey(address, storageSlotKey);
      if (!requested.add(key)) {
        return false;
      }
      submit(
          key,
          () -> worldState.getStorageValueByStorageSlotKey(address, storageSlotKey),
          () -> worldState.loadStorageTriePath(address, storageSlotKey));
      return true;
    }

    boolean loadAccount(final Address address) {
      final Bytes key = accountKey(address);
      if (!requested.add(key)) {
        return false;
      }
      submit(
          key,
          () -> {
            final Account account = worldState.get(address);
            if (account != null) {
              account.getCode();
            }
          },
          () -> worldState.loadAccountTriePath(address));
      return true;
    }

    /**
     * Waits for a load that is in flight.
     *
     * @return whether the load was in flight and its value is in memory now
     */
    boolean awaitInFlight(final Bytes key, final long timeoutNanos) {
      final CompletableFuture<Void> load = inFlight.get(key);
      if (load == null) {
        return false;
      }
      try {
        load.get(timeoutNanos, TimeUnit.NANOSECONDS);
        return true;
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      } catch (final java.util.concurrent.ExecutionException
          | java.util.concurrent.TimeoutException e) {
        return false;
      }
    }

    int pendingValues() {
      return pendingValues.get();
    }

    private void submit(final Bytes key, final Runnable value, final Runnable triePath) {
      final CompletableFuture<Void> loaded = new CompletableFuture<>();
      inFlight.put(key, loaded);
      pending.incrementAndGet();
      pendingValues.incrementAndGet();
      try {
        LOADS.execute(
            () -> {
              boolean triePathQueued = false;
              try {
                try {
                  value.run();
                  if (!TRIE_AFTER) {
                    triePath.run();
                  }
                } catch (final RuntimeException e) {
                  LOG.trace("A run ahead's load failed", e);
                } finally {
                  inFlight.remove(key);
                  loaded.complete(null);
                  valueDone();
                }
                triePathQueued = TRIE_AFTER && queueTriePath(triePath);
              } finally {
                if (!triePathQueued) {
                  release();
                }
              }
            });
      } catch (final RejectedExecutionException e) {
        inFlight.remove(key);
        loaded.complete(null);
        valueDone();
        release();
      }
    }

    /** Queues a trie path load, which releases the world state once done. */
    private boolean queueTriePath(final Runnable triePath) {
      try {
        TRIE_LOADS.execute(
            () -> {
              try {
                triePath.run();
              } catch (final RuntimeException e) {
                LOG.trace("A run ahead's trie path load failed", e);
              } finally {
                release();
              }
            });
        return true;
      } catch (final RejectedExecutionException e) {
        return false;
      }
    }

    void runDone() {
      release();
    }

    /** Waits until the values requested so far are in memory, or the timeout passed. */
    void awaitLoads(final long timeoutNanos) {
      final long deadline = System.nanoTime() + timeoutNanos;
      synchronized (idle) {
        while (pendingValues.get() > 0) {
          final long left = deadline - System.nanoTime();
          if (left <= 0) {
            return;
          }
          try {
            idle.wait(left / 1_000_000L, (int) (left % 1_000_000L));
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
        }
      }
    }

    private void valueDone() {
      if (pendingValues.decrementAndGet() == 0) {
        synchronized (idle) {
          idle.notifyAll();
        }
      }
    }

    private void release() {
      if (pending.decrementAndGet() == 0) {
        worldState.close();
      }
    }
  }

  private static ThreadFactory daemonThreads(final String prefix) {
    final AtomicInteger counter = new AtomicInteger();
    return runnable -> {
      final Thread thread = new Thread(runnable, prefix + "-" + counter.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    };
  }
}
