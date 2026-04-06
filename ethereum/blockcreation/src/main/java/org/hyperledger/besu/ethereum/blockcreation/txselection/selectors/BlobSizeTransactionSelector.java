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
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.plugin.data.TransactionSelectionResult;
import org.hyperledger.besu.plugin.services.txselection.SelectorsStateManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class extends AbstractTransactionSelector and provides a specific implementation for
 * evaluating transactions based on blobs size. It checks if a transaction supports blobs, and if
 * so, checks that there is enough remaining blob gas in the block to fit the blobs of the tx.
 */
public class BlobSizeTransactionSelector extends AbstractStatefulTransactionSelector<Long> {
  private static final Logger LOG = LoggerFactory.getLogger(BlobSizeTransactionSelector.class);

  public BlobSizeTransactionSelector(
      final BlockSelectionContext context, final SelectorsStateManager selectorsStateManager) {
    super(context, selectorsStateManager, 0L, SelectorsStateManager.StateDuplicator::duplicateLong);
  }

  /**
   * Evaluates a transaction considering other transactions in the same block. If the tx does not
   * support blobs, no check is performed, and SELECTED is returned, otherwise SELECTED is returned
   * only if there is enough remaining blob gas to fit the blobs of the tx, otherwise a specific not
   * selected result is returned, depending on the fact that the block already contains the max
   * number of blobs or not.
   *
   * @param evaluationContext The current selection session data.
   * @return The result of the transaction selection.
   */
  @Override
  public TransactionSelectionResult evaluateTransactionPreProcessing(
      final TransactionEvaluationContext evaluationContext) {

    final var tx = evaluationContext.getTransaction();
    if (tx.getType().supportsBlob()) {

      final var cumulativeBlobGasUsed = getWorkingState();

      final var remainingBlobGas =
          context.gasLimitCalculator().blockBuilderBlobGasLimit() - cumulativeBlobGasUsed;

      if (remainingBlobGas == 0) {
        LOG.atTrace()
            .setMessage(
                "The block already contains the max number of allowed blobs, pending tx: {}")
            .addArgument(evaluationContext.getPendingTransaction()::toTraceLog)
            .log();
        return TransactionSelectionResult.BLOBS_FULL;
      }

      final long requestedBlobGas = context.gasCalculator().blobGasCost(tx.getBlobCount());

      if (requestedBlobGas > remainingBlobGas) {
        LOG.atTrace()
            .setMessage(
                "There is not enough blob gas available to fit the blobs of the transaction {} in the block."
                    + " Available {} / Requested {}")
            .addArgument(evaluationContext.getPendingTransaction()::toTraceLog)
            .addArgument(remainingBlobGas)
            .addArgument(requestedBlobGas)
            .log();
        return TransactionSelectionResult.TX_TOO_LARGE_FOR_REMAINING_BLOB_GAS;
      }
    }
    return TransactionSelectionResult.SELECTED;
  }

  @Override
  public TransactionSelectionResult evaluateTransactionPostProcessing(
      final TransactionEvaluationContext evaluationContext,
      final TransactionProcessingResult processingResult) {
    final var tx = evaluationContext.getTransaction();
    if (tx.getType().supportsBlob()) {
      setWorkingState(getWorkingState() + context.gasCalculator().blobGasCost(tx.getBlobCount()));
    }
    return TransactionSelectionResult.SELECTED;
  }
}
