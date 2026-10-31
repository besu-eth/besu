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
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.plugin.data.TransactionSelectionResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class extends AbstractTransactionSelector and provides a specific implementation for
 * evaluating transactions based on Blob price. It checks if a transaction's current blob price is
 * below the minimum and determines the selection result accordingly.
 */
public class BlobPriceTransactionSelector extends AbstractTransactionSelector {
  private static final Logger LOG = LoggerFactory.getLogger(BlobPriceTransactionSelector.class);

  public BlobPriceTransactionSelector(final BlockSelectionContext context) {
    super(context);
  }

  /**
   * Evaluates a transaction considering its blob price.
   *
   * @param evaluationContext The current selection session data.
   * @return The result of the transaction selection.
   */
  @Override
  public TransactionSelectionResult evaluateTransactionPreProcessing(
      final TransactionEvaluationContext evaluationContext) {
    if (transactionBlobPriceBelowMin(evaluationContext.getTransaction())) {
      return TransactionSelectionResult.BLOB_PRICE_BELOW_CURRENT_MIN;
    }
    return TransactionSelectionResult.SELECTED;
  }

  @Override
  public TransactionSelectionResult evaluateTransactionPostProcessing(
      final TransactionEvaluationContext evaluationContext,
      final TransactionProcessingResult processingResult) {
    // All necessary checks were done in the pre-processing method, so nothing to do here.
    return TransactionSelectionResult.SELECTED;
  }

  /**
   * Checks if the transaction's blob price is below the minimum.
   *
   * @param transaction The transaction to be checked.
   * @return True if the transaction's data price is below the minimum, false otherwise.
   */
  private boolean transactionBlobPriceBelowMin(final Transaction transaction) {
    // Checked on the blobs carried rather than the type: an EIP-8141 frame transaction may carry
    // blobs but usually does not, and then declares a zero blob fee cap.
    if (transaction.getBlobCount() > 0) {
      if (transaction.getMaxFeePerBlobGas().orElseThrow().lessThan(context.blobGasPrice())) {
        LOG.trace(
            "Max fee per Blob Gas {} below {}",
            transaction.getMaxFeePerBlobGas(),
            context.blobGasPrice());
        return true;
      }
    }
    return false;
  }
}
