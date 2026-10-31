/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.eth.transactions;

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.feemarket.TransactionPriceCalculator;
import org.hyperledger.besu.util.number.Percentage;

import java.util.Optional;

public class TransactionReplacementByFeeMarketRule implements TransactionPoolReplacementRule {

  private static final TransactionPriceCalculator FRONTIER_CALCULATOR =
      TransactionPriceCalculator.frontier();
  private static final TransactionPriceCalculator EIP1559_CALCULATOR =
      TransactionPriceCalculator.eip1559();
  private final Percentage priceBump;
  private final Percentage blobPriceBump;

  public TransactionReplacementByFeeMarketRule(
      final Percentage priceBump, final Percentage blobPriceBump) {
    this.priceBump = priceBump;
    this.blobPriceBump = blobPriceBump;
  }

  @Override
  public boolean shouldReplace(
      final PendingTransaction existingPendingTransaction,
      final PendingTransaction newPendingTransaction,
      final Optional<Wei> maybeBaseFee) {

    return validExecutionPriceReplacement(
            existingPendingTransaction, newPendingTransaction, maybeBaseFee)
        && validBlobPriceReplacement(existingPendingTransaction, newPendingTransaction);
  }

  private boolean validExecutionPriceReplacement(
      final PendingTransaction existingPendingTransaction,
      final PendingTransaction newPendingTransaction,
      final Optional<Wei> maybeBaseFee) {

    // bail early if basefee is absent or neither transaction supports 1559 fee market
    if (maybeBaseFee.isEmpty()
        || !(isNotGasPriced(existingPendingTransaction) || isNotGasPriced(newPendingTransaction))) {
      return false;
    }

    Wei newEffPrice = priceOf(newPendingTransaction.getTransaction(), maybeBaseFee);
    Wei newEffPriority =
        newPendingTransaction.getTransaction().getEffectivePriorityFeePerGas(maybeBaseFee);

    Wei curEffPrice = priceOf(existingPendingTransaction.getTransaction(), maybeBaseFee);
    Wei curEffPriority =
        existingPendingTransaction.getTransaction().getEffectivePriorityFeePerGas(maybeBaseFee);

    if (isBumpedBy(curEffPrice, newEffPrice, priceBump)) {
      // if effective price is bumped by percent:
      // replace if new effective priority is >= current effective priority
      return newEffPriority.compareTo(curEffPriority) >= 0;
    } else if (curEffPrice.equals(newEffPrice)) {
      // elsif new effective price is equal to current effective price:
      // replace if the new effective priority is bumped by priceBump relative to current priority
      return isBumpedBy(curEffPriority, newEffPriority, priceBump);
    }
    return false;
  }

  private boolean validBlobPriceReplacement(
      final PendingTransaction existingPendingTransaction,
      final PendingTransaction newPendingTransaction) {

    // Keyed on the blobs carried rather than the type: an EIP-8141 frame transaction's type
    // supports blobs, but one that carries none is priced like any other non-blob transaction.
    final boolean existingCarriesBlobs =
        existingPendingTransaction.getTransaction().getBlobCount() > 0;
    final boolean newCarriesBlobs = newPendingTransaction.getTransaction().getBlobCount() > 0;

    if (existingCarriesBlobs || newCarriesBlobs) {
      if (existingCarriesBlobs && newCarriesBlobs) {
        final Wei replacementThreshold =
            existingPendingTransaction
                .getTransaction()
                .getMaxFeePerBlobGas()
                .orElseThrow()
                .multiply(100 + blobPriceBump.getValue())
                .divide(100);
        return newPendingTransaction
                .getTransaction()
                .getMaxFeePerBlobGas()
                .orElseThrow()
                .compareTo(replacementThreshold)
            >= 0;
      }
      // blob tx can only replace and be replaced by blob tx
      return false;
    }

    // in case no blob tx, then we are fine
    return true;
  }

  private Wei priceOf(final Transaction transaction, final Optional<Wei> maybeBaseFee) {
    final TransactionPriceCalculator transactionPriceCalculator =
        transaction.getType().supports1559FeeMarket() ? EIP1559_CALCULATOR : FRONTIER_CALCULATOR;
    return transactionPriceCalculator.price(transaction, maybeBaseFee);
  }

  private boolean isBumpedBy(final Wei val, final Wei bumpVal, final Percentage percent) {
    return val.multiply(percent.getValue() + 100L).compareTo(bumpVal.multiply(100L)) <= 0;
  }
}
