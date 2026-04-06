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
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.AccessLocationTracker;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BlockAccessListBuilder;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.evm.worldstate.WorldView;
import org.hyperledger.besu.plugin.services.metrics.Counter;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.google.common.annotations.VisibleForTesting;

/**
 * Optimizes transaction processing by executing transactions in parallel within a given block.
 * Transactions are executed optimistically in a non-blocking manner. After execution, the class
 * checks for potential conflicts among transactions to ensure data integrity before applying the
 * results to the world state.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class ParallelizedConcurrentTransactionProcessor extends ParallelBlockTransactionProcessor {

  private final MainnetTransactionProcessor transactionProcessor;

  private final TransactionCollisionDetector transactionCollisionDetector;

  /**
   * Constructs a PreloadConcurrentTransactionProcessor with a specified transaction processor. This
   * processor is responsible for the individual processing of transactions.
   *
   * @param transactionProcessor The transaction processor for processing individual transactions.
   */
  public ParallelizedConcurrentTransactionProcessor(
      final MainnetTransactionProcessor transactionProcessor) {
    this.transactionProcessor = transactionProcessor;
    this.transactionCollisionDetector = new TransactionCollisionDetector();
  }

  @VisibleForTesting
  public ParallelizedConcurrentTransactionProcessor(
      final MainnetTransactionProcessor transactionProcessor,
      final TransactionCollisionDetector transactionCollisionDetector) {
    this.transactionProcessor = transactionProcessor;
    this.transactionCollisionDetector = transactionCollisionDetector;
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
      final Optional<BlockAccessListBuilder> blockAccessListBuilder) {

    final BonsaiWorldState ws = getWorldState(protocolContext, blockHeader);
    if (ws == null) return null;

    try {
      ws.disableCacheMerkleTrieLoader();
      final ParallelizedTransactionContext.Builder contextBuilder =
          new ParallelizedTransactionContext.Builder();
      final PathBasedWorldStateUpdateAccumulator<?> roundWorldStateUpdater =
          (PathBasedWorldStateUpdateAccumulator<?>) ws.updater();
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
              blockHashLookup,
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
         * we remove it from the accumulator to avoid a false positive collision.
         * The balance will be increased during the sequential processing.
         */
        roundWorldStateUpdater.getAccountsToUpdate().remove(miningBeneficiary);
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

    final CompletableFuture<ParallelizedTransactionContext> future = futures[transactionLocation];

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
      final boolean hasCollision =
          transactionCollisionDetector.hasCollision(
              transaction, miningBeneficiary, parallelizedTransactionContext, blockAccumulator);
      if (transactionProcessingResult.isSuccessful() && !hasCollision) {
        final MutableAccount miningBeneficiaryAccount =
            blockAccumulator.getOrCreate(miningBeneficiary);
        Wei reward = parallelizedTransactionContext.miningBeneficiaryReward();
        if (!reward.isZero() || !transactionProcessor.getClearEmptyAccounts()) {
          miningBeneficiaryAccount.incrementBalance(reward);
        }

        final Wei miningBeneficiaryPostBalance = miningBeneficiaryAccount.getBalance();
        transactionProcessingResult
            .getPartialBlockAccessView()
            .ifPresent(
                partialBlockAccessView ->
                    partialBlockAccessView.accountChanges().stream()
                        .filter(
                            accountChanges -> accountChanges.getAddress().equals(miningBeneficiary))
                        .findFirst()
                        .ifPresent(
                            accountChanges ->
                                accountChanges.setPostBalance(miningBeneficiaryPostBalance)));

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
