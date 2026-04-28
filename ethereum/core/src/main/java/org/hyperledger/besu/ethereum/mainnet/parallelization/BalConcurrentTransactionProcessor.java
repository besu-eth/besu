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
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ExecutionStats;
import org.hyperledger.besu.ethereum.mainnet.ExecutionStatsHolder;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.SlowBlockTracer;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.AccessLocationTracker;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BlockAccessListBuilder;
import org.hyperledger.besu.ethereum.mainnet.systemcall.BlockProcessingContext;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.metrics.Counter;

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"unchecked", "rawtypes"})
public class BalConcurrentTransactionProcessor extends ParallelBlockTransactionProcessor {

  private static final Logger LOG =
      LoggerFactory.getLogger(BalConcurrentTransactionProcessor.class);

  private final MainnetTransactionProcessor transactionProcessor;
  private final BlockAccessList blockAccessList;
  private final Duration balProcessingTimeout;
  private final BlockProcessingContext blockProcessingContext;

  public BalConcurrentTransactionProcessor(
      final MainnetTransactionProcessor transactionProcessor,
      final BlockAccessList blockAccessList,
      final BalConfiguration balConfiguration,
      final BlockProcessingContext blockProcessingContext) {
    this.transactionProcessor = transactionProcessor;
    this.blockAccessList = blockAccessList;
    this.balProcessingTimeout = balConfiguration.getBalProcessingTimeout();
    this.blockProcessingContext = blockProcessingContext;
  }

  @Override
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

    if (maybeParentHeader.isEmpty()) {
      return null;
    }
    final BonsaiWorldState ws =
        getWorldState(protocolContext, maybeParentHeader.get()).orElse(null);
    if (ws == null) {
      return null;
    }

    final ExecutionStats workerStats =
        (blockProcessingContext != null
                && BackgroundTracerFactory.hasMetricsTracer(
                    blockProcessingContext.getOperationTracer()))
            ? new ExecutionStats()
            : null;
    if (workerStats != null) {
      ExecutionStatsHolder.set(workerStats);
      ws.setStateMetricsCollector(workerStats);
    }

    try {
      ws.disableCacheMerkleTrieLoader();
      final ParallelizedTransactionContext.Builder ctxBuilder =
          new ParallelizedTransactionContext.Builder();

      final PathBasedWorldStateUpdateAccumulator<?> blockUpdater =
          (PathBasedWorldStateUpdateAccumulator<?>) ws.updater();

      applyWritesFromPriorTransactions(
          blockAccessList, (long) transactionLocation + 1L, blockUpdater);
      blockUpdater.commit();

      final WorldUpdater txUpdater = blockUpdater.updater();
      final Optional<AccessLocationTracker> txTracker =
          blockAccessListBuilder.map(
              b ->
                  BlockAccessListBuilder.createTransactionAccessLocationTracker(
                      transactionLocation));

      // Create a background tracer for parallel execution metrics collection
      final OperationTracer backgroundTracer =
          BackgroundTracerFactory.createBackgroundTracer(blockProcessingContext);

      final TransactionProcessingResult result =
          transactionProcessor.processTransaction(
              txUpdater,
              blockHeader,
              transaction.detachedCopy(),
              miningBeneficiary,
              backgroundTracer,
              blockHashLookup,
              TransactionValidationParams.processingBlock(),
              blobGasPrice,
              txTracker);

      txUpdater.commit();
      blockUpdater.commit();

      // TODO: We should pass transaction accumulator
      ctxBuilder
          .transactionAccumulator(blockUpdater)
          .transactionProcessingResult(result)
          .backgroundTracer(backgroundTracer)
          .workerExecutionStats(workerStats);

      return ctxBuilder.build();
    } finally {
      if (workerStats != null) {
        ExecutionStatsHolder.clear();
      }
      ws.close();
    }
  }

  @Override
  // TODO: Throw instead of returning Optional.empty()?
  public Optional<TransactionProcessingResult> getProcessingResult(
      final MutableWorldState worldState,
      final Address miningBeneficiary,
      final Transaction transaction,
      final int txIndex,
      final Optional<Counter> confirmedParallelizedTransactionCounter,
      final Optional<Counter> conflictingButCachedTransactionCounter) {

    final CompletableFuture<ParallelizedTransactionContext> future = futures[txIndex];
    if (future != null) {
      try {
        final ParallelizedTransactionContext ctx =
            balProcessingTimeout.isNegative()
                ? future.join()
                : future.get(balProcessingTimeout.toNanos(), TimeUnit.NANOSECONDS);

        if (ctx == null) {
          LOG.trace("Transaction context for transaction {} is empty.", txIndex);
          return Optional.empty();
        }

        final PathBasedWorldState pathWs = (PathBasedWorldState) worldState;
        final PathBasedWorldStateUpdateAccumulator blockAccumulator =
            (PathBasedWorldStateUpdateAccumulator) pathWs.updater();

        final PathBasedWorldStateUpdateAccumulator<?> txAccumulator = ctx.transactionAccumulator();
        final TransactionProcessingResult result = ctx.transactionProcessingResult();

        blockAccumulator.importStateChangesFromSource(txAccumulator);

        // Consolidate EVM op metrics from the background tracer
        ctx.backgroundTracer()
            .ifPresent(
                backgroundTracer ->
                    BackgroundTracerFactory.consolidateTracerResults(
                        backgroundTracer, blockProcessingContext));

        // Consolidate state-layer metrics (account/storage reads, cache stats) from the worker
        ctx.workerExecutionStats()
            .ifPresent(
                workerStats ->
                    BackgroundTracerFactory.findSlowBlockTracer(
                            blockProcessingContext != null
                                ? blockProcessingContext.getOperationTracer()
                                : OperationTracer.NO_TRACING)
                        .map(SlowBlockTracer::getExecutionStats)
                        .ifPresent(mainStats -> mainStats.mergeStateCountsFrom(workerStats)));

        confirmedParallelizedTransactionCounter.ifPresent(Counter::inc);
        result.setIsProcessedInParallel(Optional.of(Boolean.TRUE));
        result.accumulator = txAccumulator;

        return Optional.of(result);
      } catch (final TimeoutException e) {
        LOG.error(
            "Timed out waiting {}ms for transaction {} processing result.",
            balProcessingTimeout.toMillis(),
            txIndex);
        return Optional.empty();
      } catch (final Exception e) {
        LOG.error(
            "Error integrating transaction processing result for transaction {}.", txIndex, e);
        return Optional.empty();
      }
    }

    LOG.error("No future found for transaction {}.", txIndex);
    return Optional.empty();
  }

  private void applyWritesFromPriorTransactions(
      final BlockAccessList blockAccessList,
      final long balIndex,
      final PathBasedWorldStateUpdateAccumulator<?> worldStateUpdater) {
    for (var accountChanges : blockAccessList.accountChanges()) {
      final Address address = accountChanges.address();
      MutableAccount account = null;

      final var latestBalance = findLatestBalanceChange(accountChanges.balanceChanges(), balIndex);
      if (latestBalance != null) {
        if (account == null) {
          account = worldStateUpdater.getOrCreate(address);
        }
        account.setBalance(latestBalance.postBalance());
      }

      final var latestNonce = findLatestNonceChange(accountChanges.nonceChanges(), balIndex);
      if (latestNonce != null) {
        if (account == null) {
          account = worldStateUpdater.getOrCreate(address);
        }
        account.setNonce(latestNonce.newNonce());
      }

      final var latestCode = findLatestCodeChange(accountChanges.codeChanges(), balIndex);
      if (latestCode != null) {
        if (account == null) {
          account = worldStateUpdater.getOrCreate(address);
        }
        account.setCode(latestCode.newCode());
      }

      for (var slotChanges : accountChanges.storageChanges()) {
        final UInt256 slotKey = slotChanges.slot().getSlotKey().orElseThrow();

        final var latestStorage = findLatestStorageChange(slotChanges.changes(), balIndex);

        if (latestStorage != null) {
          if (account == null) {
            account = worldStateUpdater.getOrCreate(address);
          }
          account.setStorageValue(
              slotKey, latestStorage.newValue() != null ? latestStorage.newValue() : UInt256.ZERO);
        }
      }
    }
  }

  private BlockAccessList.BalanceChange findLatestBalanceChange(
      final Collection<BlockAccessList.BalanceChange> changes, final long maxIndex) {
    BlockAccessList.BalanceChange latest = null;
    long latestIndex = -1L;
    for (var change : changes) {
      final long txIndex = change.txIndex();
      if (txIndex < maxIndex && txIndex > latestIndex) {
        latest = change;
        latestIndex = txIndex;
      }
    }
    return latest;
  }

  private BlockAccessList.NonceChange findLatestNonceChange(
      final Collection<BlockAccessList.NonceChange> changes, final long maxIndex) {
    BlockAccessList.NonceChange latest = null;
    long latestIndex = -1L;
    for (var change : changes) {
      final long txIndex = change.txIndex();
      if (txIndex < maxIndex && txIndex > latestIndex) {
        latest = change;
        latestIndex = txIndex;
      }
    }
    return latest;
  }

  private BlockAccessList.CodeChange findLatestCodeChange(
      final Collection<BlockAccessList.CodeChange> changes, final long maxIndex) {
    BlockAccessList.CodeChange latest = null;
    long latestIndex = -1L;
    for (var change : changes) {
      final long txIndex = change.txIndex();
      if (txIndex < maxIndex && txIndex > latestIndex) {
        latest = change;
        latestIndex = txIndex;
      }
    }
    return latest;
  }

  private BlockAccessList.StorageChange findLatestStorageChange(
      final Collection<BlockAccessList.StorageChange> changes, final long maxIndex) {
    BlockAccessList.StorageChange latest = null;
    long latestIndex = -1L;
    for (var change : changes) {
      final long txIndex = change.txIndex();
      if (txIndex < maxIndex && txIndex > latestIndex) {
        latest = change;
        latestIndex = txIndex;
      }
    }
    return latest;
  }
}
