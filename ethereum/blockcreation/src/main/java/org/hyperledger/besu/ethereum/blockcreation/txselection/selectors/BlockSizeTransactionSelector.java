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
package org.hyperledger.besu.ethereum.blockcreation.txselection.selectors;

import org.hyperledger.besu.ethereum.blockcreation.txselection.BlockSelectionContext;
import org.hyperledger.besu.ethereum.blockcreation.txselection.TransactionEvaluationContext;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.BlockGasAccountingStrategy;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.plugin.data.TransactionSelectionResult;
import org.hyperledger.besu.plugin.services.txselection.SelectorsStateManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class extends AbstractTransactionSelector and provides a specific implementation for
 * evaluating transactions based on block size. It checks if a transaction is too large for the
 * block and determines the selection result accordingly.
 *
 * <p>For EIP-8037 multidimensional gas, this selector tracks both regular and state gas dimensions.
 * A transaction fits if its gasLimit does not exceed the remaining capacity of each dimension
 * independently. Post-processing verifies the actual gas metered stays within limits.
 */
public class BlockSizeTransactionSelector extends AbstractStatefulTransactionSelector<GasState> {
  private static final Logger LOG = LoggerFactory.getLogger(BlockSizeTransactionSelector.class);

  private final long blockGasLimit;
  private final BlockGasAccountingStrategy gasAccountingStrategy;

  public BlockSizeTransactionSelector(
      final BlockSelectionContext context, final SelectorsStateManager selectorsStateManager) {
    super(context, selectorsStateManager, GasState.ZERO, GasState::duplicate);
    this.blockGasLimit = context.pendingBlockHeader().getGasLimit();
    this.gasAccountingStrategy = context.protocolSpec().getBlockGasAccountingStrategy();
  }

  /**
   * Evaluates a transaction considering other transactions in the same block. If the transaction is
   * too large for the block returns a selection result based on block occupancy.
   *
   * @param evaluationContext The current selection session data.
   * @return The result of the transaction selection.
   */
  @Override
  public TransactionSelectionResult evaluateTransactionPreProcessing(
      final TransactionEvaluationContext evaluationContext) {

    final GasState state = getWorkingState();

    if (transactionTooLargeForBlock(evaluationContext.getTransaction(), state)) {
      LOG.atTrace()
          .setMessage("Transaction {} too large to select for block creation")
          .addArgument(evaluationContext.getPendingTransaction()::toTraceLog)
          .log();
      if (blockFull(state)) {
        LOG.trace("Block full, completing operation");
        return TransactionSelectionResult.BLOCK_FULL;
      } else {
        return TransactionSelectionResult.TX_TOO_LARGE_FOR_REMAINING_GAS;
      }
    }
    return TransactionSelectionResult.SELECTED;
  }

  @Override
  public TransactionSelectionResult evaluateTransactionPostProcessing(
      final TransactionEvaluationContext evaluationContext,
      final TransactionProcessingResult processingResult) {
    final long txRegularGasUsed =
        gasAccountingStrategy.calculateTransactionRegularGas(
            evaluationContext.getTransaction(), processingResult);
    final long stateGasUsed = processingResult.getStateGasUsed();

    final GasState state = getWorkingState();
    final GasState newState =
        new GasState(state.regularGas() + txRegularGasUsed, state.stateGas() + stateGasUsed);
    setWorkingState(newState);

    final long gasMetered =
        gasAccountingStrategy.effectiveGasUsed(newState.regularGas(), newState.stateGas());
    if (gasMetered > blockGasLimit) {
      LOG.atTrace()
          .setMessage(
              "Transaction {} exceeds block gas limit post-processing:"
                  + " regularGas={}, stateGas={}, gasMetered={}, blockGasLimit={}")
          .addArgument(evaluationContext.getPendingTransaction()::toTraceLog)
          .addArgument(newState.regularGas())
          .addArgument(newState.stateGas())
          .addArgument(gasMetered)
          .addArgument(blockGasLimit)
          .log();
      return TransactionSelectionResult.TX_TOO_LARGE_FOR_REMAINING_GAS;
    }
    return TransactionSelectionResult.SELECTED;
  }

  /**
   * Checks if the transaction is too large for the block using the gas accounting strategy. For 1D
   * gas, this checks regular gas only. For 2D gas (EIP-8037), the tx gas limit must fit within the
   * remaining capacity of each dimension independently.
   *
   * <p>The post-processing check verifies that gas metered (max of regular, state) stays within the
   * block gas limit after processing reveals the actual regular/state gas split.
   *
   * @param transaction The transaction to be checked.
   * @param state The current gas state with regular and state gas.
   * @return True if the transaction is too large for the block, false otherwise.
   */
  private boolean transactionTooLargeForBlock(final Transaction transaction, final GasState state) {
    return !gasAccountingStrategy.hasBlockCapacity(
        transaction.getGasLimit(), state.regularGas(), state.stateGas(), blockGasLimit);
  }

  /**
   * Checks if the block is full.
   *
   * @param state The current gas state.
   * @return True if the block is full, false otherwise.
   */
  private boolean blockFull(final GasState state) {
    final long gasUsed =
        gasAccountingStrategy.effectiveGasUsed(state.regularGas(), state.stateGas());
    final long gasRemaining = blockGasLimit - gasUsed;

    if (gasRemaining < context.gasCalculator().getMinimumTransactionCost()) {
      LOG.trace(
          "Block full, remaining gas {} is less than minimum transaction gas cost {}",
          gasRemaining,
          context.gasCalculator().getMinimumTransactionCost());
      return true;
    }
    return false;
  }
}
