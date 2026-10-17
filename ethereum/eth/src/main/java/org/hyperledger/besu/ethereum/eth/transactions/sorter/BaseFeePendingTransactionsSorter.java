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
package org.hyperledger.besu.ethereum.eth.transactions.sorter;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toUnmodifiableList;

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionEventNotifier;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.time.Clock;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.TreeSet;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds the current set of pending transactions with the ability to iterate them based on priority
 * for mining or look-up by hash.
 *
 * <p>This class is safe for use across multiple threads.
 */
public class BaseFeePendingTransactionsSorter extends AbstractPendingTransactionsSorter {

  private static final Logger LOG = LoggerFactory.getLogger(BaseFeePendingTransactionsSorter.class);

  private Optional<Wei> baseFee;

  public BaseFeePendingTransactionsSorter(
      final TransactionPoolConfiguration poolConfig,
      final Clock clock,
      final MetricsSystem metricsSystem,
      final Supplier<BlockHeader> chainHeadHeaderSupplier,
      final TransactionEventNotifier eventNotifier) {
    super(poolConfig, clock, metricsSystem, chainHeadHeaderSupplier, eventNotifier);
    this.baseFee = chainHeadHeaderSupplier.get().getBaseFee();
  }

  /**
   * See this post for an explainer about these data structures:
   * https://hackmd.io/@adietrichs/1559-transaction-sorting
   */
  private final NavigableSet<PendingTransaction> prioritizedTransactionsStaticRange =
      new TreeSet<>(
          comparing(PendingTransaction::hasPriority)
              .thenComparing(
                  pendingTx ->
                      pendingTx
                          .getTransaction()
                          .getMaxPriorityFeePerGas()
                          // just in case we attempt to compare non-1559 transaction
                          .orElse(Wei.ZERO)
                          .getAsBigInteger()
                          .longValue())
              .thenComparing(PendingTransaction::getSequence, Comparator.reverseOrder())
              .reversed());

  private final NavigableSet<PendingTransaction> prioritizedTransactionsDynamicRange =
      new TreeSet<>(
          comparing(PendingTransaction::hasPriority)
              .thenComparing(
                  pendingTx ->
                      pendingTx
                          .getTransaction()
                          .getMaxFeePerGas()
                          .map(maxFeePerGas -> maxFeePerGas.getAsBigInteger().longValue())
                          .orElse(pendingTx.getGasPrice().toLong()))
              .thenComparing(PendingTransaction::getSequence, Comparator.reverseOrder())
              .reversed());

  @Override
  public void reset() {
    super.reset();
    prioritizedTransactionsStaticRange.clear();
    prioritizedTransactionsDynamicRange.clear();
  }

  @Override
  public void manageBlockAdded(final BlockHeader blockHeader) {
    blockHeader.getBaseFee().ifPresent(this::updateBaseFee);
  }

  @Override
  protected void removePrioritizedTransaction(final PendingTransaction removedPendingTx) {
    if (prioritizedTransactionsDynamicRange.remove(removedPendingTx)) {
      LOG.atTrace()
          .setMessage("Removed dynamic range transaction {}")
          .addArgument(removedPendingTx::toTraceLog)
          .log();
    } else {
      removedPendingTx
          .getTransaction()
          .getMaxPriorityFeePerGas()
          .ifPresent(
              __ -> {
                if (prioritizedTransactionsStaticRange.remove(removedPendingTx)) {
                  LOG.atTrace()
                      .setMessage("Removed static range transaction {}")
                      .addArgument(removedPendingTx::toTraceLog)
                      .log();
                }
              });
    }
  }

  @Override
  protected Iterator<PendingTransaction> prioritizedTransactions() {
    return new Iterator<>() {
      final Iterator<PendingTransaction> staticRangeIterable =
          prioritizedTransactionsStaticRange.iterator();
      final Iterator<PendingTransaction> dynamicRangeIterable =
          prioritizedTransactionsDynamicRange.iterator();

      Optional<PendingTransaction> currentStaticRangeTransaction =
          getNextOptional(staticRangeIterable);
      Optional<PendingTransaction> currentDynamicRangeTransaction =
          getNextOptional(dynamicRangeIterable);

      @Override
      public boolean hasNext() {
        return currentStaticRangeTransaction.isPresent()
            || currentDynamicRangeTransaction.isPresent();
      }

      @Override
      public PendingTransaction next() {
        if (currentStaticRangeTransaction.isEmpty() && currentDynamicRangeTransaction.isEmpty()) {
          throw new NoSuchElementException("Tried to iterate past end of iterator.");
        } else if (currentStaticRangeTransaction.isEmpty()) {
          // only dynamic range txs left
          final PendingTransaction best = currentDynamicRangeTransaction.get();
          currentDynamicRangeTransaction = getNextOptional(dynamicRangeIterable);
          return best;
        } else if (currentDynamicRangeTransaction.isEmpty()) {
          // only static range txs left
          final PendingTransaction best = currentStaticRangeTransaction.get();
          currentStaticRangeTransaction = getNextOptional(staticRangeIterable);
          return best;
        } else {
          // there are both static and dynamic txs remaining, so we need to compare them by their
          // effective priority fees
          final Wei dynamicRangeEffectivePriorityFee =
              currentDynamicRangeTransaction
                  .get()
                  .getTransaction()
                  .getEffectivePriorityFeePerGas(baseFee);
          final Wei staticRangeEffectivePriorityFee =
              currentStaticRangeTransaction
                  .get()
                  .getTransaction()
                  .getEffectivePriorityFeePerGas(baseFee);
          final PendingTransaction best;
          if (dynamicRangeEffectivePriorityFee.compareTo(staticRangeEffectivePriorityFee) > 0) {
            best = currentDynamicRangeTransaction.get();
            currentDynamicRangeTransaction = getNextOptional(dynamicRangeIterable);
          } else {
            best = currentStaticRangeTransaction.get();
            currentStaticRangeTransaction = getNextOptional(staticRangeIterable);
          }
          return best;
        }
      }

      private Optional<PendingTransaction> getNextOptional(
          final Iterator<PendingTransaction> pendingTxsIterator) {
        return pendingTxsIterator.hasNext()
            ? Optional.of(pendingTxsIterator.next())
            : Optional.empty();
      }
    };
  }

  @Override
  protected void prioritizeTransaction(final PendingTransaction pendingTransaction) {
    // check if it's in static or dynamic range
    final String kind;
    if (isInStaticRange(pendingTransaction.getTransaction(), baseFee)) {
      kind = "static";
      prioritizedTransactionsStaticRange.add(pendingTransaction);
    } else {
      kind = "dynamic";
      prioritizedTransactionsDynamicRange.add(pendingTransaction);
    }
    LOG.atTrace()
        .setMessage("Adding {} to pending transactions, range type {}")
        .addArgument(pendingTransaction::toTraceLog)
        .addArgument(kind)
        .log();
  }

  @Override
  protected PendingTransaction getLeastPriorityTransaction() {
    final var lastStatic =
        prioritizedTransactionsStaticRange.isEmpty()
            ? null
            : prioritizedTransactionsStaticRange.last();
    final var lastDynamic =
        prioritizedTransactionsDynamicRange.isEmpty()
            ? null
            : prioritizedTransactionsDynamicRange.last();

    if (lastDynamic == null) {
      return lastStatic;
    }
    if (lastStatic == null) {
      return lastDynamic;
    }

    final Comparator<PendingTransaction> compareByValue =
        Comparator.comparing(
            txInfo ->
                txInfo.getTransaction().getEffectivePriorityFeePerGas(baseFee).getAsBigInteger());

    return compareByValue.compare(lastStatic, lastDynamic) < 0 ? lastStatic : lastDynamic;
  }

  private boolean isInStaticRange(final Transaction transaction, final Optional<Wei> baseFee) {
    return transaction
        .getMaxPriorityFeePerGas()
        .map(
            maxPriorityFeePerGas ->
                transaction.getEffectivePriorityFeePerGas(baseFee).compareTo(maxPriorityFeePerGas)
                    >= 0)
        .orElse(
            // non-eip-1559 txs can't be in static range
            false);
  }

  public void updateBaseFee(final Wei newBaseFee) {
    LOG.atTrace()
        .setMessage("Updating base fee from {} to {}")
        .addArgument(this.baseFee)
        .addArgument(newBaseFee::toShortHexString)
        .log();
    if (this.baseFee.orElse(Wei.ZERO).equals(newBaseFee)) {
      return;
    }
    synchronized (lock) {
      final boolean baseFeeIncreased = newBaseFee.compareTo(this.baseFee.orElse(Wei.ZERO)) > 0;
      this.baseFee = Optional.of(newBaseFee);
      if (baseFeeIncreased) {
        // base fee increases can only cause transactions to go from static to dynamic range
        prioritizedTransactionsStaticRange.stream()
            .filter(
                // these are the transactions whose effective priority fee have now dropped
                // below their max priority fee
                pendingTx -> !isInStaticRange(pendingTx.getTransaction(), baseFee))
            .collect(toUnmodifiableList())
            .forEach(
                pendingTx -> {
                  LOG.atTrace()
                      .setMessage("Moving {} from static to dynamic gas fee paradigm")
                      .addArgument(pendingTx::toTraceLog)
                      .log();
                  prioritizedTransactionsStaticRange.remove(pendingTx);
                  prioritizedTransactionsDynamicRange.add(pendingTx);
                });
      } else {
        // base fee decreases can only cause transactions to go from dynamic to static range
        prioritizedTransactionsDynamicRange.stream()
            .filter(
                // these are the transactions whose effective priority fee are now above their
                // max priority fee
                pendingTx -> isInStaticRange(pendingTx.getTransaction(), baseFee))
            .collect(toUnmodifiableList())
            .forEach(
                pendingTx -> {
                  LOG.atTrace()
                      .setMessage("Moving {} from dynamic to static gas fee paradigm")
                      .addArgument(pendingTx::toTraceLog)
                      .log();
                  prioritizedTransactionsDynamicRange.remove(pendingTx);
                  prioritizedTransactionsStaticRange.add(pendingTx);
                });
      }
    }
  }
}
