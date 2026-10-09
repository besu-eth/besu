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
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BlockProcessingMetrics;
import org.hyperledger.besu.ethereum.mainnet.BlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.MiningBeneficiaryCalculator;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpecBuilder;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.AccessLocationTracker;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BlockAccessListBuilder;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListAccountLookup;
import org.hyperledger.besu.ethereum.mainnet.systemcall.BlockProcessingContext;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainnetParallelBlockProcessor extends MainnetBlockProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(MainnetParallelBlockProcessor.class);

  private static final Executor cpuExecutor = BlockProcessingExecutors.cpuExecutor();

  private final Executor executor;
  // Reruns the block sequentially if parallel processing fails (not for conflicting transactions,
  // which processTransaction already reruns). The rerun is a plain MainnetBlockProcessor, so shared
  // rules must come from the ProtocolSpec or be final in AbstractBlockProcessor.
  private final Optional<BlockProcessor> sequentialBlockProcessor;
  private final Optional<Counter> confirmedParallelizedTransactionCounter;
  private final Optional<Counter> conflictingButCachedTransactionCounter;

  public MainnetParallelBlockProcessor(
      final MainnetTransactionProcessor transactionProcessor,
      final TransactionReceiptFactory transactionReceiptFactory,
      final MiningBeneficiaryCalculator miningBeneficiaryCalculator,
      final ProtocolSchedule protocolSchedule,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    this(
        transactionProcessor,
        transactionReceiptFactory,
        miningBeneficiaryCalculator,
        protocolSchedule,
        balConfiguration,
        metricsSystem,
        new BlockProcessingMetrics(metricsSystem));
  }

  private MainnetParallelBlockProcessor(
      final MainnetTransactionProcessor transactionProcessor,
      final TransactionReceiptFactory transactionReceiptFactory,
      final MiningBeneficiaryCalculator miningBeneficiaryCalculator,
      final ProtocolSchedule protocolSchedule,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem,
      final BlockProcessingMetrics blockProcessingMetrics) {
    this(
        transactionProcessor,
        transactionReceiptFactory,
        miningBeneficiaryCalculator,
        protocolSchedule,
        balConfiguration,
        metricsSystem,
        blockProcessingMetrics,
        cpuExecutor,
        Optional.of(
            new MainnetBlockProcessor(
                transactionProcessor,
                transactionReceiptFactory,
                miningBeneficiaryCalculator,
                protocolSchedule,
                balConfiguration,
                blockProcessingMetrics)));
  }

  @VisibleForTesting
  public MainnetParallelBlockProcessor(
      final MainnetTransactionProcessor transactionProcessor,
      final TransactionReceiptFactory transactionReceiptFactory,
      final MiningBeneficiaryCalculator miningBeneficiaryCalculator,
      final ProtocolSchedule protocolSchedule,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem,
      final Executor executor,
      final Optional<BlockProcessor> sequentialBlockProcessor) {
    this(
        transactionProcessor,
        transactionReceiptFactory,
        miningBeneficiaryCalculator,
        protocolSchedule,
        balConfiguration,
        metricsSystem,
        new BlockProcessingMetrics(metricsSystem),
        executor,
        sequentialBlockProcessor);
  }

  private MainnetParallelBlockProcessor(
      final MainnetTransactionProcessor transactionProcessor,
      final TransactionReceiptFactory transactionReceiptFactory,
      final MiningBeneficiaryCalculator miningBeneficiaryCalculator,
      final ProtocolSchedule protocolSchedule,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem,
      final BlockProcessingMetrics blockProcessingMetrics,
      final Executor executor,
      final Optional<BlockProcessor> sequentialBlockProcessor) {
    super(
        transactionProcessor,
        transactionReceiptFactory,
        miningBeneficiaryCalculator,
        protocolSchedule,
        balConfiguration,
        blockProcessingMetrics);
    this.executor = executor;
    this.sequentialBlockProcessor = sequentialBlockProcessor;
    this.confirmedParallelizedTransactionCounter =
        Optional.of(
            metricsSystem.createCounter(
                BesuMetricCategory.BLOCK_PROCESSING,
                "parallelized_transactions_counter",
                "Counter for the number of parallelized transactions during block processing"));

    this.conflictingButCachedTransactionCounter =
        Optional.of(
            metricsSystem.createCounter(
                BesuMetricCategory.BLOCK_PROCESSING,
                "conflicted_transactions_counter",
                "Counter for the number of conflicted transactions during block processing"));
  }

  @Override
  public BlockProcessingResult processBlock(
      final ProtocolContext protocolContext,
      final Blockchain blockchain,
      final MutableWorldState worldState,
      final Block block,
      final Optional<BlockAccessList> blockAccessList) {
    final BlockProcessingResult blockProcessingResult =
        super.processBlock(protocolContext, blockchain, worldState, block, blockAccessList);
    if (blockProcessingResult.isFailed() && sequentialBlockProcessor.isPresent()) {
      LOG.info(
          "Parallel transaction processing failure. Falling back to non-parallel processing for block #{} ({})",
          block.getHeader().getNumber(),
          block.getHash());
      if (worldState instanceof BonsaiWorldState) {
        ((BonsaiWorldStateUpdateAccumulator) worldState.updater()).reset();
      }
      return sequentialBlockProcessor
          .get()
          .processBlock(protocolContext, blockchain, worldState, block, blockAccessList);
    }
    return blockProcessingResult;
  }

  @Override
  protected Optional<ParallelBlockTransactionProcessor> startParallelExecution(
      final ProtocolContext protocolContext,
      final BlockHeader blockHeader,
      final List<Transaction> transactions,
      final Address miningBeneficiary,
      final BlockHashLookup blockHashLookup,
      final Wei blobGasPrice,
      final Optional<BlockAccessListBuilder> blockAccessListBuilder,
      final Optional<BlockAccessListAccountLookup> blockAccessListLookup,
      final Optional<BlockHeader> maybeParentHeader) {
    if (!(protocolContext.getWorldStateArchive() instanceof PathBasedWorldStateProvider)) {
      return Optional.empty();
    }

    final ParallelBlockTransactionProcessor parallelProcessor;

    if (balConfiguration.isPerfectParallelizationEnabled() && blockAccessListLookup.isPresent()) {
      parallelProcessor =
          new BalConcurrentTransactionProcessor(
              transactionProcessor, blockAccessListLookup.get(), balConfiguration);
    } else {
      parallelProcessor = new OptimisticConcurrentTransactionProcessor(transactionProcessor);
    }

    parallelProcessor.runAsyncBlock(
        protocolContext,
        blockHeader,
        transactions,
        miningBeneficiary,
        blockHashLookup,
        blobGasPrice,
        executor,
        blockAccessListBuilder,
        maybeParentHeader);

    return Optional.of(parallelProcessor);
  }

  @Override
  protected final TransactionProcessingResult processTransaction(
      final Optional<ParallelBlockTransactionProcessor> parallelProcessor,
      final BlockProcessingContext blockProcessingContext,
      final WorldUpdater transactionUpdater,
      final Wei blobGasPrice,
      final Address miningBeneficiary,
      final Transaction transaction,
      final int location,
      final BlockHashLookup blockHashLookup,
      final Optional<AccessLocationTracker> accessLocationTracker) {
    return parallelProcessor
        .flatMap(
            processor ->
                processor.getProcessingResult(
                    blockProcessingContext.getWorldState(),
                    miningBeneficiary,
                    transaction,
                    location,
                    confirmedParallelizedTransactionCounter,
                    conflictingButCachedTransactionCounter))
        // No usable parallel result (a conflict, or nothing ran in parallel): process it
        // sequentially.
        .orElseGet(
            () ->
                processTransactionSequentially(
                    blockProcessingContext,
                    transactionUpdater,
                    blobGasPrice,
                    miningBeneficiary,
                    transaction,
                    blockHashLookup,
                    accessLocationTracker));
  }

  public static class ParallelBlockProcessorBuilder
      implements ProtocolSpecBuilder.BlockProcessorBuilder {

    final MetricsSystem metricsSystem;

    public ParallelBlockProcessorBuilder(final MetricsSystem metricsSystem) {
      this.metricsSystem = metricsSystem;
    }

    @Override
    public BlockProcessor apply(
        final MainnetTransactionProcessor transactionProcessor,
        final TransactionReceiptFactory transactionReceiptFactory,
        final MiningBeneficiaryCalculator miningBeneficiaryCalculator,
        final ProtocolSchedule protocolSchedule,
        final BalConfiguration balConfiguration) {
      return new MainnetParallelBlockProcessor(
          transactionProcessor,
          transactionReceiptFactory,
          miningBeneficiaryCalculator,
          protocolSchedule,
          balConfiguration,
          metricsSystem);
    }
  }
}
