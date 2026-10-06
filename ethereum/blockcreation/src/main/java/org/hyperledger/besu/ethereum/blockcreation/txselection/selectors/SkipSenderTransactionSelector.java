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
package org.hyperledger.besu.ethereum.blockcreation.txselection.selectors;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.blockcreation.txselection.BlockSelectionContext;
import org.hyperledger.besu.ethereum.blockcreation.txselection.TransactionEvaluationContext;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.plugin.data.TransactionSelectionResult;

import java.util.HashMap;
import java.util.Map;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SkipSenderTransactionSelector extends AbstractTransactionSelector {
  private static final Logger LOG = LoggerFactory.getLogger(SkipSenderTransactionSelector.class);
  private final Map<Address, Long> skippedSenderNonceMap = new HashMap<>();

  public SkipSenderTransactionSelector(final BlockSelectionContext context) {
    super(context);
  }

  /**
   * Check if this pending tx belongs to a sender with a previous pending tx not selected, in which
   * case we can safely skip the evaluation due to the nonce gap
   *
   * @param evaluationContext The current selection session data.
   * @return SENDER_WITH_PREVIOUS_TX_NOT_SELECTED if there is a nonce gas for this sender
   */
  @Override
  public TransactionSelectionResult evaluateTransactionPreProcessing(
      final TransactionEvaluationContext evaluationContext) {
    final var sender = evaluationContext.getTransaction().getSender();
    final var skippedNonce = skippedSenderNonceMap.get(sender);
    if (nonceGap(evaluationContext, skippedNonce)) {
      LOG.atTrace()
          .setMessage("Not selecting tx {} since its sender {} is in the skip list with nonce {}")
          .addArgument(() -> evaluationContext.getPendingTransaction().toTraceLog())
          .addArgument(sender)
          .addArgument(skippedNonce)
          .log();

      return TransactionSelectionResult.SENDER_WITH_PREVIOUS_TX_NOT_SELECTED;
    }
    return TransactionSelectionResult.SELECTED;
  }

  private static boolean nonceGap(
      final TransactionEvaluationContext evaluationContext, final @Nullable Long skippedNonce) {
    return skippedNonce != null && evaluationContext.getTransaction().getNonce() > skippedNonce;
  }

  @Override
  public TransactionSelectionResult evaluateTransactionPostProcessing(
      final TransactionEvaluationContext evaluationContext,
      final TransactionProcessingResult processingResult) {
    // All necessary checks were done in the pre-processing method, so nothing to do here.
    return TransactionSelectionResult.SELECTED;
  }

  /**
   * When a transaction is not selected we can skip processing any following transaction from the
   * same sender, since it will never be selected due to the nonce gap, so we add the sender to the
   * skip list.
   *
   * @param evaluationContext The current evaluation context
   * @param transactionSelectionResult The transaction selection result
   */
  @Override
  public void onTransactionNotSelected(
      final TransactionEvaluationContext evaluationContext,
      final TransactionSelectionResult transactionSelectionResult) {
    final var sender = evaluationContext.getTransaction().getSender();
    final var nonce = evaluationContext.getTransaction().getNonce();
    skippedSenderNonceMap.put(sender, nonce);
    LOG.trace("Sender {} added to the skip list with nonce {}", sender, nonce);
  }

  /**
   * When a transaction is selected we can remove it from the list. This could happen when the same
   * pending tx is present both in the internal pool and the plugin pool, and for example it is not
   * selected by the plugin but could be later selected from the internal pool.
   *
   * @param evaluationContext The current evaluation context
   * @param processingResult The transaction processing result
   */
  @Override
  public void onTransactionSelected(
      final TransactionEvaluationContext evaluationContext,
      final TransactionProcessingResult processingResult) {
    final var sender = evaluationContext.getTransaction().getSender();
    final var skippedNonce = skippedSenderNonceMap.remove(sender);
    if (skippedNonce != null) {
      LOG.trace("Sender {} removed from the skip list, skipped nonce was {}", sender, skippedNonce);
    }
  }
}
