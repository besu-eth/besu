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
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.plugin.data.TransactionSelectionResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class extends AbstractTransactionSelector and provides a specific implementation for
 * evaluating transactions based on processing results. It checks if a transaction is invalid and
 * determines the selection result accordingly.
 */
public class ProcessingResultTransactionSelector extends AbstractTransactionSelector {
  private static final Logger LOG =
      LoggerFactory.getLogger(ProcessingResultTransactionSelector.class);

  public ProcessingResultTransactionSelector(final BlockSelectionContext context) {
    super(context);
  }

  @Override
  public TransactionSelectionResult evaluateTransactionPreProcessing(
      final TransactionEvaluationContext evaluationContext) {
    // All checks depend on processingResult and will be done in the post-processing method, so
    // nothing to do here.
    return TransactionSelectionResult.SELECTED;
  }

  /**
   * Evaluates a transaction considering other transactions in the same block and a processing
   * result. If the processing result is invalid, it determines the selection result for the invalid
   * result.
   *
   * @param evaluationContext The current selection session data.
   * @param processingResult The processing result of the transaction.
   * @return The result of the transaction selection.
   */
  @Override
  public TransactionSelectionResult evaluateTransactionPostProcessing(
      final TransactionEvaluationContext evaluationContext,
      final TransactionProcessingResult processingResult) {

    if (processingResult.isInvalid()) {
      return transactionSelectionResultForInvalidResult(
          evaluationContext.getTransaction(), processingResult.getValidationResult());
    }
    return TransactionSelectionResult.SELECTED;
  }

  /**
   * Determines the transaction selection result for an invalid result. If the invalid reason is
   * transient, returns an invalid transient result. If the invalid reason is not transient, returns
   * an invalid result.
   *
   * @param transaction The invalid transaction.
   * @param invalidReasonValidationResult The validation result containing the invalid reason.
   * @return The transaction selection result.
   */
  private TransactionSelectionResult transactionSelectionResultForInvalidResult(
      final Transaction transaction,
      final ValidationResult<TransactionInvalidReason> invalidReasonValidationResult) {

    final TransactionInvalidReason invalidReason = invalidReasonValidationResult.getInvalidReason();
    // If the invalid reason is transient, then penalize but leave the transaction in the pool and
    // continue
    if (isTransientValidationError(invalidReason)) {
      LOG.atTrace()
          .setMessage("Transient validation error {} for transaction {}, penalize it in the pool")
          .addArgument(invalidReason)
          .addArgument(transaction::toTraceLog)
          .log();
      return TransactionSelectionResult.invalidPenalized(invalidReason.name());
    }
    // If the transaction was invalid for any other reason, delete it, and continue.
    LOG.atTrace()
        .setMessage("Delete invalid transaction {}, reason {}")
        .addArgument(transaction::toTraceLog)
        .addArgument(invalidReason)
        .log();
    return TransactionSelectionResult.invalid(invalidReason.name());
  }

  /**
   * Checks if the invalid reason is a transient validation error. When transient, the transaction
   * is penalised (score reduced) but retained in the pool so it can be retried in a later block.
   * When not transient, the transaction is dropped from the pool permanently.
   *
   * <p>UPFRONT_COST_EXCEEDS_BALANCE is only treated as transient when {@code noLateFunding} is
   * false (the default). When {@code noLateFunding} is true the sender's insufficient balance is
   * treated as a permanent failure and the transaction is removed from the pool immediately.
   *
   * @param invalidReason The invalid reason.
   * @return True if the invalid reason is transient, false otherwise.
   */
  private boolean isTransientValidationError(final TransactionInvalidReason invalidReason) {
    if (invalidReason.equals(TransactionInvalidReason.UPFRONT_COST_EXCEEDS_BALANCE)) {
      return !context.transactionPool().getConfiguration().getNoLateFunding();
    }
    return invalidReason.equals(TransactionInvalidReason.GAS_PRICE_BELOW_CURRENT_BASE_FEE)
        || invalidReason.equals(TransactionInvalidReason.NONCE_TOO_HIGH)
        || invalidReason.equals(TransactionInvalidReason.EXECUTION_INTERRUPTED);
  }
}
