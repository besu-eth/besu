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
package org.hyperledger.besu.ethereum.eth.transactions;

import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.util.Subscribers;

import java.time.Duration;

/**
 * Notifies the listeners of the transactions added to and dropped from the pending transactions.
 * Listeners are notified synchronously on the caller thread, unless subscribed with the async
 * variants.
 */
public class TransactionEventNotifier {
  private final Subscribers<PendingTransactionAddedListener> onAddedListeners =
      Subscribers.create(true);
  private final Subscribers<PendingTransactionDroppedListener> onDroppedListeners =
      Subscribers.create(true);
  private final EthScheduler.OrderedProcessor<Runnable> orderedProcessor;

  public TransactionEventNotifier(final EthScheduler ethScheduler) {
    this.orderedProcessor = ethScheduler.createOrderedProcessor(Runnable::run);
  }

  /**
   * Notify the listeners that a transaction has been added.
   *
   * @param pendingTransaction the added transaction
   */
  public void notifyPendingTransactionAdded(final PendingTransaction pendingTransaction) {
    onAddedListeners.forEach(
        listener -> listener.onTransactionAdded(pendingTransaction.getTransaction()));
  }

  /**
   * Notify the listeners that a transaction has been dropped.
   *
   * @param pendingTransaction the dropped transaction
   * @param reason the reason the transaction has been dropped
   */
  public void notifyPendingTransactionDropped(
      final PendingTransaction pendingTransaction, final RemovalReason reason) {
    onDroppedListeners.forEach(
        listener -> listener.onTransactionDropped(pendingTransaction.getTransaction(), reason));
  }

  /**
   * Subscribe a listener that is notified on the caller thread.
   *
   * @param listener the listener
   * @return the id to use to unsubscribe the listener
   */
  public long subscribePendingTransactionAdded(final PendingTransactionAddedListener listener) {
    return onAddedListeners.subscribe(listener);
  }

  /**
   * Subscribe a listener that is notified on the caller thread.
   *
   * @param listener the listener
   * @return the id to use to unsubscribe the listener
   */
  public long subscribePendingTransactionDropped(final PendingTransactionDroppedListener listener) {
    return onDroppedListeners.subscribe(listener);
  }

  /**
   * Subscribe a listener that is notified on a services thread instead of the caller thread. Async
   * listeners receive the events in the same order they are notified.
   *
   * @param listener the listener
   * @return the id to use to unsubscribe the listener
   */
  public long subscribePendingTransactionAddedAsync(
      final PendingTransactionAddedListener listener) {
    return onAddedListeners.subscribe(
        transaction -> orderedProcessor.submit(() -> listener.onTransactionAdded(transaction)));
  }

  /**
   * Subscribe a listener that is notified on a services thread instead of the caller thread. Async
   * listeners receive the events in the same order they are notified.
   *
   * @param listener the listener
   * @return the id to use to unsubscribe the listener
   */
  public long subscribePendingTransactionDroppedAsync(
      final PendingTransactionDroppedListener listener) {
    return onDroppedListeners.subscribe(
        (transaction, reason) ->
            orderedProcessor.submit(() -> listener.onTransactionDropped(transaction, reason)));
  }

  /**
   * Unsubscribe a sync or async transaction added listener.
   *
   * @param id the id returned when the listener was subscribed
   */
  public void unsubscribePendingTransactionAdded(final long id) {
    onAddedListeners.unsubscribe(id);
  }

  /**
   * Unsubscribe a sync or async transaction dropped listener.
   *
   * @param id the id returned when the listener was subscribed
   */
  public void unsubscribePendingTransactionDropped(final long id) {
    onDroppedListeners.unsubscribe(id);
  }

  /**
   * Stop notifying async listeners, dropping the notifications not yet delivered, and wait for the
   * one in progress, if any, to complete. Sync listeners are not affected.
   *
   * @param timeout the maximum time to wait for the notification in progress to complete
   * @return {@code true} if no async notification is in progress, {@code false} if the timeout was
   *     reached or the wait was interrupted
   */
  public boolean awaitDisabled(final Duration timeout) {
    return orderedProcessor.awaitDrainedAndDisabled(timeout);
  }

  /** Start notifying async listeners again, after they were disabled. */
  public void enable() {
    orderedProcessor.enable();
  }
}
