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
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tuweni.bytes.Bytes;
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
   */
  public static void runAhead(
      final MainnetTransactionProcessor transactionProcessor,
      final ProtocolContext protocolContext,
      final BlockHeader parentHeader,
      final BlockHeader blockHeader,
      final Transaction transaction,
      final Address miningBeneficiary,
      final BlockHashLookup blockHashLookup,
      final Wei blobGasPrice,
      final TransactionValidationParams validationParams) {
    final BlockHashLookup lookup = blockHashLookup.forkForParallelWorker();
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
                  validationParams));
    } catch (final RejectedExecutionException e) {
      LOG.trace("Run ahead of {} not started", transaction.getHash(), e);
    }
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
      final TransactionValidationParams validationParams) {
    final BonsaiWorldState worldState =
        protocolContext
            .getWorldStateArchive()
            .getWorldState(WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(parentHeader))
            .map(BonsaiWorldState.class::cast)
            .orElse(null);
    if (worldState == null) {
      return;
    }
    final Loads loads = new Loads(worldState);
    final long start = System.nanoTime();
    String outcome = "?";
    try {
      final BonsaiWorldStateUpdateAccumulator accumulator =
          new BonsaiWorldStateUpdateAccumulator(
              new RunaheadWorldView(worldState, loads),
              (address, value) -> {},
              (address, value) -> {},
              ((PathBasedWorldStateUpdateAccumulator<?>) worldState.updater())
                  .getEvmConfiguration(),
              worldState.codeCache());
      final var result =
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
      outcome =
          result.isInvalid()
              ? "invalid " + result.getValidationResult().getErrorMessage()
              : result.isSuccessful() ? "success" : "failed " + result.getRevertReason();
    } catch (final RuntimeException e) {
      // it only loads state, the execution reports any failure
      outcome = "exception " + e;
      LOG.trace("Run ahead of {} stopped", transaction.getHash(), e);
    } finally {
      if (LOG_RUNS) {
        LOG.info(
            "Bench runahead block {} hash {} us {} loads {} outcome {}",
            blockHeader.getNumber(),
            transaction.getHash().toHexString(),
            (System.nanoTime() - start) / 1000,
            loads.requested.size(),
            outcome);
      }
      loads.runDone();
    }
  }

  /** The loads of one run ahead; the parent world state closes once the run and they are done. */
  static final class Loads {
    private final BonsaiWorldState worldState;
    // the run itself holds one
    private final AtomicInteger pending = new AtomicInteger(1);
    private final Set<Bytes> requested = ConcurrentHashMap.newKeySet();

    Loads(final BonsaiWorldState worldState) {
      this.worldState = worldState;
    }

    void load(final Address address, final StorageSlotKey storageSlotKey) {
      if (!requested.add(
          Bytes.concatenate(
              address.addressHash().getBytes(), storageSlotKey.getSlotHash().getBytes()))) {
        return;
      }
      pending.incrementAndGet();
      try {
        LOADS.execute(
            () -> {
              try {
                worldState.getStorageValueByStorageSlotKey(address, storageSlotKey);
                worldState.loadStorageTriePath(address, storageSlotKey);
              } catch (final RuntimeException e) {
                LOG.trace("Load of a slot of {} failed", address, e);
              } finally {
                release();
              }
            });
      } catch (final RejectedExecutionException e) {
        release();
      }
    }

    void runDone() {
      release();
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
