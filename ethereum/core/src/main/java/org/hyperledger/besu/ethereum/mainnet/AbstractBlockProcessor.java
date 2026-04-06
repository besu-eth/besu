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
package org.hyperledger.besu.ethereum.mainnet;

import static org.hyperledger.besu.ethereum.mainnet.feemarket.ExcessBlobGasCalculator.calculateExcessBlobGasForParent;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.Request;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.AbstractBlockProcessor.PreprocessingFunction.NoPreprocessing;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.AccessLocationTracker;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BlockAccessListBuilder;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListFactory;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.PartialBlockAccessView;
import org.hyperledger.besu.ethereum.mainnet.parallelization.PreprocessingContext;
import org.hyperledger.besu.ethereum.mainnet.requests.RequestProcessingContext;
import org.hyperledger.besu.ethereum.mainnet.requests.RequestProcessorCoordinator;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.StateRootCommitter;
import org.hyperledger.besu.ethereum.mainnet.systemcall.BlockProcessingContext;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.common.StateRootMismatchException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.worldstate.StackedUpdater;
import org.hyperledger.besu.evm.worldstate.WorldState;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.BlockImportTracerProvider;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.tracer.BlockAwareOperationTracer;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractBlockProcessor implements BlockProcessor {

  public interface TransactionReceiptFactory {
    TransactionReceipt create(
        TransactionType transactionType,
        TransactionProcessingResult result,
        WorldState worldState,
        long gasUsed);

    TransactionReceipt create(
        TransactionType transactionType, TransactionProcessingResult result, long gasUsed);
  }

  private static final Logger LOG = LoggerFactory.getLogger(AbstractBlockProcessor.class);

  static final int MAX_GENERATION = 6;

  protected final MainnetTransactionProcessor transactionProcessor;

  protected final AbstractBlockProcessor.TransactionReceiptFactory transactionReceiptFactory;

  final Wei blockReward;

  protected final boolean skipZeroBlockRewards;
  private final ProtocolSchedule protocolSchedule;
  protected final BalConfiguration balConfiguration;
  private final BlockProcessingMetrics blockProcessingMetrics;

  protected final MiningBeneficiaryCalculator miningBeneficiaryCalculator;
  private BlockImportTracerProvider blockImportTracerProvider = null;

  protected AbstractBlockProcessor(
      final MainnetTransactionProcessor transactionProcessor,
      final TransactionReceiptFactory transactionReceiptFactory,
      final Wei blockReward,
      final MiningBeneficiaryCalculator miningBeneficiaryCalculator,
      final boolean skipZeroBlockRewards,
      final ProtocolSchedule protocolSchedule,
      final BalConfiguration balConfiguration) {
    this(
        transactionProcessor,
        transactionReceiptFactory,
        blockReward,
        miningBeneficiaryCalculator,
        skipZeroBlockRewards,
        protocolSchedule,
        balConfiguration,
        new NoOpMetricsSystem());
  }

  protected AbstractBlockProcessor(
      final MainnetTransactionProcessor transactionProcessor,
      final TransactionReceiptFactory transactionReceiptFactory,
      final Wei blockReward,
      final MiningBeneficiaryCalculator miningBeneficiaryCalculator,
      final boolean skipZeroBlockRewards,
      final ProtocolSchedule protocolSchedule,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    this.transactionProcessor = transactionProcessor;
    this.transactionReceiptFactory = transactionReceiptFactory;
    this.blockReward = blockReward;
    this.miningBeneficiaryCalculator = miningBeneficiaryCalculator;
    this.skipZeroBlockRewards = skipZeroBlockRewards;
    this.protocolSchedule = protocolSchedule;
    this.balConfiguration = balConfiguration;
    this.blockProcessingMetrics = new BlockProcessingMetrics(metricsSystem);
  }

  private BlockAwareOperationTracer getBlockImportTracer(
      final ProtocolContext protocolContext, final BlockHeader header) {

    if (blockImportTracerProvider == null) {
      // fetch from context once, and keep.
      blockImportTracerProvider =
          Optional.ofNullable(protocolContext.getPluginServiceManager())
              .flatMap(serviceManager -> serviceManager.getService(BlockImportTracerProvider.class))
              // if block import tracer provider is not specified by plugin, default to no tracing
              .orElse(
                  ignored -> {
                    if (Boolean.getBoolean("besu.debug.traceBlocks")
                        || "true".equalsIgnoreCase(System.getenv("BESU_TRACE_BLOCKS"))) {
                      return new BlockAwareJsonTracer();
                    }
                    LOG.trace("Block Import uses NO_TRACING");
                    return BlockAwareOperationTracer.NO_TRACING;
                  });
    }

    return blockImportTracerProvider.getBlockImportTracer(header);
  }

  /**
   * Processes the block with no privateMetadata and no preprocessor.
   *
   * @param protocolContext the current context of the protocol
   * @param blockchain the blockchain to append the block to
   * @param worldState the world state to apply changes to
   * @param block the block to process
   * @return the block processing result
   */
  @Override
  public BlockProcessingResult processBlock(
      final ProtocolContext protocolContext,
      final Blockchain blockchain,
      final MutableWorldState worldState,
      final Block block) {
    return processBlock(
        protocolContext, blockchain, worldState, block, Optional.empty(), new NoPreprocessing());
  }

  @Override
  public BlockProcessingResult processBlock(
      final ProtocolContext protocolContext,
      final Blockchain blockchain,
      final MutableWorldState worldState,
      final Block block,
      final PreprocessingFunction preprocessingBlockFunction) {
    return processBlock(
        protocolContext,
        blockchain,
        worldState,
        block,
        Optional.empty(),
        preprocessingBlockFunction);
  }

  @Override
  public BlockProcessingResult processBlock(
      final ProtocolContext protocolContext,
      final Blockchain blockchain,
      final MutableWorldState worldState,
      final Block block,
      final Optional<BlockAccessList> blockAccessList) {
    return processBlock(
        protocolContext, blockchain, worldState, block, blockAccessList, new NoPreprocessing());
  }

  @Override
  public BlockProcessingResult processBlock(
      final ProtocolContext protocolContext,
      final Blockchain blockchain,
      final MutableWorldState worldState,
      final Block block,
      final Optional<BlockAccessList> blockAccessList,
      final PreprocessingFunction preprocessingBlockFunction) {
    final List<TransactionReceipt> receipts = new ArrayList<>();
    // EIP-7778: Track two separate cumulative gas values
    // cumulativeBlockGasUsed: For block gas limit enforcement (uses protocol-specific strategy)
    //   - Pre-Amsterdam: gasLimit - gasRemaining (post-refund)
    //   - Amsterdam+: pre-refund gas (prevents block gas limit circumvention via refunds)
    // cumulativeReceiptGasUsed: For receipt cumulativeGasUsed field (always post-refund)
    long cumulativeBlockGasUsed = 0;
    long cumulativeReceiptGasUsed = 0;
    long currentBlobGasUsed = 0;

    var blockHeader = block.getHeader();
    var blockBody = block.getBody();
    var ommers = blockBody.getOmmers();
    var transactions = blockBody.getTransactions();
    var maybeWithdrawals = blockBody.getWithdrawals();

    final ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(blockHeader);
    final BlockHashLookup blockHashLookup =
        protocolSpec.getPreExecutionProcessor().createBlockHashLookup(blockchain, blockHeader);

    final BlockAwareOperationTracer blockTracer =
        getBlockImportTracer(protocolContext, blockHeader);

    final Address miningBeneficiary = miningBeneficiaryCalculator.calculateBeneficiary(blockHeader);

    LOG.trace("traceStartBlock for {}", blockHeader.getNumber());
    blockTracer.traceStartBlock(worldState, blockHeader, miningBeneficiary);

    final StateRootCommitter stateRootCommitter =
        blockProcessingMetrics.wrapStateRootCommitter(
            protocolSpec
                .getStateRootCommitterFactory()
                .forBlock(protocolContext, blockHeader, blockAccessList));

    final Optional<BlockAccessListBuilder> blockAccessListBuilder =
        protocolSpec
            .getBlockAccessListFactory()
            .map(BlockAccessListFactory::newBlockAccessListBuilder);

    try {
      final Optional<AccessLocationTracker> preExecutionAccessLocationTracker =
          blockAccessListBuilder.map(
              b -> BlockAccessListBuilder.createPreExecutionAccessLocationTracker());
      final BlockProcessingContext blockProcessingContext =
          new BlockProcessingContext(
              blockHeader,
              worldState,
              protocolSpec,
              blockHashLookup,
              blockTracer,
              blockAccessListBuilder);
      protocolSpec
          .getPreExecutionProcessor()
          .process(blockProcessingContext, preExecutionAccessLocationTracker);

      Optional<BlockHeader> maybeParentHeader =
          blockchain.getBlockHeader(blockHeader.getParentHash());

      Wei blobGasPrice =
          maybeParentHeader
              .map(
                  parentHeader ->
                      protocolSpec
                          .getFeeMarket()
                          .blobGasPricePerGas(
                              calculateExcessBlobGasForParent(protocolSpec, parentHeader)))
              .orElse(Wei.ZERO);

      final Optional<PreprocessingContext> preProcessingContext =
          preprocessingBlockFunction.run(
              protocolContext,
              blockHeader,
              transactions,
              miningBeneficiary,
              blockHashLookup,
              blobGasPrice,
              blockAccessListBuilder,
              blockAccessList);

      boolean parallelizedTxFound = false;
      int nbParallelTx = 0;

      for (int i = 0; i < transactions.size(); i++) {
        final WorldUpdater blockUpdater = worldState.updater();
        final Transaction transaction = transactions.get(i);
        WorldUpdater transactionUpdater = blockUpdater.updater();
        if (!(transactionUpdater instanceof StackedUpdater<?, ?>)) {
          transactionUpdater = blockUpdater;
        }
        if (!hasAvailableBlockBudget(blockHeader, transaction, cumulativeBlockGasUsed)) {
          return new BlockProcessingResult(Optional.empty(), "provided gas insufficient");
        }

        final Optional<AccessLocationTracker> transactionLocationTracker =
            createTransactionAccessLocationTracker(blockAccessListBuilder, i);
        TransactionProcessingResult transactionProcessingResult =
            getTransactionProcessingResult(
                preProcessingContext,
                blockProcessingContext,
                transactionUpdater,
                blobGasPrice,
                miningBeneficiary,
                transaction,
                i,
                blockHashLookup,
                transactionLocationTracker);

        applyPartialBlockAccessView(
            transactionProcessingResult.getPartialBlockAccessView(), blockAccessListBuilder);

        if (transactionProcessingResult.isInvalid()) {
          String errorMessage =
              MessageFormat.format(
                  "Block processing error: transaction invalid {0}. Block {1} Transaction {2}",
                  transactionProcessingResult.getValidationResult().getErrorMessage(),
                  blockHeader.getHash().getBytes().toHexString(),
                  transaction.getHash().getBytes().toHexString());
          LOG.info(errorMessage);
          if (worldState instanceof BonsaiWorldState) {
            ((BonsaiWorldStateUpdateAccumulator) blockUpdater).reset();
          }
          return new BlockProcessingResult(Optional.empty(), errorMessage);
        }

        if (transactionUpdater instanceof StackedUpdater<?, ?>) {
          transactionUpdater.commit();
        }
        blockUpdater.commit();
        blockUpdater.markTransactionBoundary();

        // EIP-7778: Update both cumulative gas values
        // Block gas uses protocol-specific strategy (pre-refund for Amsterdam+)
        cumulativeBlockGasUsed +=
            protocolSpec
                .getBlockGasAccountingStrategy()
                .calculateBlockGas(transaction, transactionProcessingResult);
        // Receipt gas always uses standard post-refund calculation
        cumulativeReceiptGasUsed +=
            BlockGasAccountingStrategy.calculateReceiptGas(
                transaction, transactionProcessingResult);
        final var optionalVersionedHashes = transaction.getVersionedHashes();
        if (optionalVersionedHashes.isPresent()) {
          final var versionedHashes = optionalVersionedHashes.get();
          currentBlobGasUsed +=
              (versionedHashes.size() * protocolSpec.getGasCalculator().getBlobGasPerBlob());
        }

        final TransactionReceipt transactionReceipt =
            transactionReceiptFactory.create(
                transaction.getType(),
                transactionProcessingResult,
                worldState,
                cumulativeReceiptGasUsed);
        receipts.add(transactionReceipt);
        if (!parallelizedTxFound
            && transactionProcessingResult.getIsProcessedInParallel().isPresent()) {
          parallelizedTxFound = true;
          nbParallelTx = 1;
        } else if (transactionProcessingResult.getIsProcessedInParallel().isPresent()) {
          nbParallelTx++;
        }
      }
      final var optionalHeaderBlobGasUsed = blockHeader.getBlobGasUsed();
      if (optionalHeaderBlobGasUsed.isPresent()) {
        final long headerBlobGasUsed = optionalHeaderBlobGasUsed.get();
        if (currentBlobGasUsed != headerBlobGasUsed) {
          String errorMessage =
              String.format(
                  "block did not consume expected blob gas: header %d, transactions %d",
                  headerBlobGasUsed, currentBlobGasUsed);
          LOG.error(errorMessage);
          if (worldState instanceof BonsaiWorldState) {
            ((BonsaiWorldStateUpdateAccumulator) worldState.updater()).reset();
          }
          return new BlockProcessingResult(Optional.empty(), errorMessage);
        }
      }

      final Optional<AccessLocationTracker> postExecutionAccessLocationTracker =
          blockAccessListBuilder.map(
              b ->
                  BlockAccessListBuilder.createPostExecutionAccessLocationTracker(
                      transactions.size()));

      final Optional<WithdrawalsProcessor> maybeWithdrawalsProcessor =
          protocolSpec.getWithdrawalsProcessor();
      if (maybeWithdrawalsProcessor.isPresent() && maybeWithdrawals.isPresent()) {
        try {
          maybeWithdrawalsProcessor
              .get()
              .processWithdrawals(
                  maybeWithdrawals.get(),
                  worldState.updater(),
                  postExecutionAccessLocationTracker,
                  blockAccessListBuilder);
        } catch (final Exception e) {
          LOG.error("failed processing withdrawals", e);
          if (worldState instanceof BonsaiWorldState) {
            ((BonsaiWorldStateUpdateAccumulator) worldState.updater()).reset();
          }
          return new BlockProcessingResult(Optional.empty(), e);
        }
      }

      Optional<List<Request>> maybeRequests = Optional.empty();
      try {
        // EIP-7685: process EL requests
        final Optional<RequestProcessorCoordinator> requestProcessor =
            protocolSpec.getRequestProcessorCoordinator();
        if (requestProcessor.isPresent()) {
          RequestProcessingContext requestProcessingContext =
              new RequestProcessingContext(blockProcessingContext, receipts);
          maybeRequests =
              Optional.of(
                  requestProcessor
                      .get()
                      .process(requestProcessingContext, postExecutionAccessLocationTracker));
        }
      } catch (final Exception e) {
        LOG.error("failed processing requests", e);
        if (worldState instanceof BonsaiWorldState) {
          ((BonsaiWorldStateUpdateAccumulator) worldState.updater()).reset();
        }
        return new BlockProcessingResult(Optional.empty(), e);
      }

      applyAccessLocationTracker(
          postExecutionAccessLocationTracker,
          blockAccessListBuilder,
          worldState.updater().updater());

      final var optionalRequestsHash = blockHeader.getRequestsHash();
      if (maybeRequests.isPresent() && optionalRequestsHash.isPresent()) {
        final List<Request> requests = maybeRequests.get();
        final Hash headerRequestsHash = optionalRequestsHash.get();
        Hash calculatedRequestHash = BodyValidation.requestsHash(requests);
        if (!calculatedRequestHash.equals(headerRequestsHash)) {
          String errorMessage =
              String.format(
                  "Requests hash mismatch, calculated: %s header: %s",
                  calculatedRequestHash.getBytes().toHexString(),
                  headerRequestsHash.getBytes().toHexString());
          LOG.error(errorMessage);
          if (worldState instanceof BonsaiWorldState) {
            ((BonsaiWorldStateUpdateAccumulator) worldState.updater()).reset();
          }
          return new BlockProcessingResult(Optional.empty(), errorMessage);
        }
      }

      if (!rewardCoinbase(worldState, blockHeader, ommers, skipZeroBlockRewards)) {
        // no need to log, rewardCoinbase logs the error.
        if (worldState instanceof BonsaiWorldState) {
          ((BonsaiWorldStateUpdateAccumulator) worldState.updater()).reset();
        }
        return new BlockProcessingResult(Optional.empty(), "ommer too old");
      }

      final Optional<BlockAccessList> maybeBlockAccessList;
      try {
        if (blockAccessListBuilder.isPresent()) {
          final BlockAccessList bal = blockAccessListBuilder.get().build();
          final Optional<Hash> headerBalHash = block.getHeader().getBalHash();
          if (headerBalHash.isPresent()) {
            final Hash expectedHash = BodyValidation.balHash(bal);
            if (!headerBalHash.get().equals(expectedHash)) {
              final String errorMessage =
                  String.format(
                      "Block access list hash mismatch, calculated: %s header: %s",
                      expectedHash.getBytes().toHexString(),
                      headerBalHash.get().getBytes().toHexString());
              LOG.error(errorMessage);

              if (balConfiguration.shouldLogBalsOnMismatch()) {
                final String constructedBalStr = bal.toString();
                final String blockBalStr =
                    blockAccessList.map(Object::toString).orElse("<no BAL present for block>");
                LOG.error(
                    "--- BAL constructed during execution ---\n{}\n"
                        + "--- BAL supplied for block ---\n{}",
                    constructedBalStr,
                    blockBalStr);
              }

              if (worldState instanceof BonsaiWorldState) {
                ((BonsaiWorldStateUpdateAccumulator) worldState.updater()).reset();
              }
              return new BlockProcessingResult(
                  Optional.empty(), errorMessage, false, Optional.of(bal));
            }
          }
          maybeBlockAccessList = Optional.of(bal);
          blockProcessingMetrics.recordBlockAccessListMetrics(bal);
        } else {
          maybeBlockAccessList = Optional.empty();
        }
      } catch (Exception e) {
        LOG.error("Error validating BAL hash", e);
        if (worldState instanceof BonsaiWorldState) {
          ((BonsaiWorldStateUpdateAccumulator) worldState.updater()).reset();
        }
        return new BlockProcessingResult(Optional.empty(), e);
      }

      LOG.trace("traceEndBlock for {}", blockHeader.getNumber());
      blockTracer.traceEndBlock(blockHeader, blockBody);

      try {
        worldState.persist(blockHeader, stateRootCommitter);
      } catch (MerkleTrieException e) {
        LOG.trace("Merkle trie exception during Transaction processing ", e);
        if (worldState instanceof BonsaiWorldState) {
          ((BonsaiWorldStateUpdateAccumulator) worldState.updater()).reset();
        }
        @SuppressWarnings(
            "java:S2139") // Exception is logged and rethrown to preserve original behavior
        RuntimeException rethrown = e;
        throw rethrown;
      } catch (StateRootMismatchException ex) {
        LOG.error(
            "failed persisting block due to stateroot mismatch; expected {}, actual {}",
            ex.getExpectedRoot().getBytes().toHexString(),
            ex.getActualRoot().getBytes().toHexString());
        return new BlockProcessingResult(Optional.empty(), ex.getMessage());
      } catch (Exception e) {
        LOG.error("failed persisting block", e);
        return new BlockProcessingResult(Optional.empty(), e);
      }

      return new BlockProcessingResult(
          Optional.of(
              new BlockProcessingOutputs(
                  worldState,
                  receipts,
                  maybeRequests,
                  maybeBlockAccessList,
                  cumulativeBlockGasUsed)),
          parallelizedTxFound ? Optional.of(nbParallelTx) : Optional.empty());
    } finally {
      stateRootCommitter.cancel();
    }
  }

  @SuppressWarnings("unused") // preProcessingContext and location are used by subclasses
  protected TransactionProcessingResult getTransactionProcessingResult(
      final Optional<PreprocessingContext> preProcessingContext,
      final BlockProcessingContext blockProcessingContext,
      final WorldUpdater transactionUpdater,
      final Wei blobGasPrice,
      final Address miningBeneficiary,
      final Transaction transaction,
      final int location,
      final BlockHashLookup blockHashLookup,
      final Optional<AccessLocationTracker> accessLocationTracker) {
    return transactionProcessor.processTransaction(
        transactionUpdater,
        blockProcessingContext.getBlockHeader(),
        transaction,
        miningBeneficiary,
        blockProcessingContext.getOperationTracer(),
        blockHashLookup,
        TransactionValidationParams.processingBlock(),
        blobGasPrice,
        accessLocationTracker);
  }

  @SuppressWarnings(
      "java:S2629") // INFO level logging rarely disabled in this project per maintainer feedback
  protected boolean hasAvailableBlockBudget(
      final BlockHeader blockHeader, final Transaction transaction, final long currentGasUsed) {
    final long remainingGasBudget = blockHeader.getGasLimit() - currentGasUsed;
    if (Long.compareUnsigned(transaction.getGasLimit(), remainingGasBudget) > 0) {
      LOG.info(
          "Block processing error: transaction gas limit {} exceeds available block budget"
              + " remaining {}. Block {} Transaction {}",
          transaction.getGasLimit(),
          remainingGasBudget,
          blockHeader.getHash().getBytes().toHexString(),
          transaction.getHash().getBytes().toHexString());
      return false;
    }

    return true;
  }

  private Optional<AccessLocationTracker> createTransactionAccessLocationTracker(
      final Optional<BlockAccessListBuilder> blockAccessListBuilder,
      final int transactionLocation) {
    return blockAccessListBuilder.map(
        b -> BlockAccessListBuilder.createTransactionAccessLocationTracker(transactionLocation));
  }

  private void applyAccessLocationTracker(
      final Optional<AccessLocationTracker> accessLocationTracker,
      final Optional<BlockAccessListBuilder> blockAccessListBuilder,
      final WorldUpdater updater) {
    accessLocationTracker.ifPresent(
        tracker ->
            blockAccessListBuilder.ifPresent(
                builder -> builder.apply(tracker.createPartialBlockAccessView(updater))));
  }

  private void applyPartialBlockAccessView(
      final Optional<PartialBlockAccessView> partialBlockAccessView,
      final Optional<BlockAccessListBuilder> blockAccessListBuilder) {
    partialBlockAccessView.ifPresent(
        view -> blockAccessListBuilder.ifPresent(builder -> builder.apply(view)));
  }

  protected MiningBeneficiaryCalculator getMiningBeneficiaryCalculator() {
    return miningBeneficiaryCalculator;
  }

  abstract boolean rewardCoinbase(
      final MutableWorldState worldState,
      final BlockHeader header,
      final List<BlockHeader> ommers,
      final boolean skipZeroBlockRewards);

  public interface PreprocessingFunction {
    Optional<PreprocessingContext> run(
        final ProtocolContext protocolContext,
        final BlockHeader blockHeader,
        final List<Transaction> transactions,
        final Address miningBeneficiary,
        final BlockHashLookup blockHashLookup,
        final Wei blobGasPrice,
        final Optional<BlockAccessListBuilder> blockAccessListBuilder,
        final Optional<BlockAccessList> maybeBlockBal);

    class NoPreprocessing implements PreprocessingFunction {

      @Override
      public Optional<PreprocessingContext> run(
          final ProtocolContext protocolContext,
          final BlockHeader blockHeader,
          final List<Transaction> transactions,
          final Address miningBeneficiary,
          final BlockHashLookup blockHashLookup,
          final Wei blobGasPrice,
          final Optional<BlockAccessListBuilder> blockAccessListBuilder,
          final Optional<BlockAccessList> maybeBlockBal) {
        return Optional.empty();
      }
    }
  }
}
