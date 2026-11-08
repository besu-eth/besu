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

import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.util.Subscribers;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Notifies the listeners of the transactions added to and dropped from the pending transactions.
 * Listeners are notified synchronously on the caller thread, unless subscribed with the async
 * variants.
 *
 * <p>Events are notified through an {@link Emitter}, created for each pending transactions
 * instance. When the pending transactions instance is discarded, {@link #invalidateEmitters()}
 * ensures that the events it could still notify, and the async notifications not yet delivered, are
 * ignored.
 */
public class TransactionEventNotifier {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionEventNotifier.class);

  private final Subscribers<AddedDispatcher> onAddedListeners = Subscribers.create(true);
  private final Subscribers<DroppedDispatcher> onDroppedListeners = Subscribers.create(true);
  private final EthScheduler.OrderedProcessor<Runnable> orderedProcessor;
  private final ReadWriteLock generationLock = new ReentrantReadWriteLock();
  private final AtomicLong currentGeneration = new AtomicLong();

  public TransactionEventNotifier(final EthScheduler ethScheduler) {
    this.orderedProcessor = ethScheduler.createOrderedProcessor(Runnable::run);
  }

  /**
   * Create an emitter to notify the events of a pending transactions instance. The emitter is valid
   * until {@link #invalidateEmitters()} is called.
   *
   * @return a new emitter
   */
  public Emitter newEmitter() {
    return new Emitter(currentGeneration.get());
  }

  /**
   * Invalidate all the emitters created so far, so their events are no longer notified, including
   * the async notifications already queued but not yet delivered. This waits for the sync
   * notifications in progress to complete, but not for the async ones.
   */
  public void invalidateEmitters() {
    final Lock writeLock = generationLock.writeLock();
    writeLock.lock();
    try {
      currentGeneration.incrementAndGet();
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Subscribe a listener that is notified on the caller thread.
   *
   * @param listener the listener
   * @return the id to use to unsubscribe the listener
   */
  public long subscribePendingTransactionAdded(final PendingTransactionAddedListener listener) {
    return onAddedListeners.subscribe(
        (generation, transaction) -> listener.onTransactionAdded(transaction));
  }

  /**
   * Subscribe a listener that is notified on the caller thread.
   *
   * @param listener the listener
   * @return the id to use to unsubscribe the listener
   */
  public long subscribePendingTransactionDropped(final PendingTransactionDroppedListener listener) {
    return onDroppedListeners.subscribe(
        (generation, transaction, reason) -> listener.onTransactionDropped(transaction, reason));
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
    return subscribePendingTransactionAddedAsync(listener, listener.getClass().getName());
  }

  /**
   * Subscribe a listener that is notified on a services thread instead of the caller thread. Async
   * listeners receive the events in the same order they are notified.
   *
   * @param listener the listener
   * @param listenerName the name used to identify the listener in the logs
   * @return the id to use to unsubscribe the listener
   */
  public long subscribePendingTransactionAddedAsync(
      final PendingTransactionAddedListener listener, final String listenerName) {
    return onAddedListeners.subscribe(
        (generation, transaction) ->
            notifyAsync(
                generation,
                listenerName,
                transaction,
                () -> listener.onTransactionAdded(transaction)));
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
    return subscribePendingTransactionDroppedAsync(listener, listener.getClass().getName());
  }

  /**
   * Subscribe a listener that is notified on a services thread instead of the caller thread. Async
   * listeners receive the events in the same order they are notified.
   *
   * @param listener the listener
   * @param listenerName the name used to identify the listener in the logs
   * @return the id to use to unsubscribe the listener
   */
  public long subscribePendingTransactionDroppedAsync(
      final PendingTransactionDroppedListener listener, final String listenerName) {
    return onDroppedListeners.subscribe(
        (generation, transaction, reason) ->
            notifyAsync(
                generation,
                listenerName,
                transaction,
                () -> listener.onTransactionDropped(transaction, reason)));
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

  private void notifyAsync(
      final long generation,
      final String listenerName,
      final Transaction transaction,
      final Runnable notification) {
    orderedProcessor.submit(
        () -> {
          // skip the notification if its emitter has been invalidated in the meantime
          if (generation == currentGeneration.get()) {
            try {
              notification.run();
            } catch (final VirtualMachineError e) {
              throw e;
            } catch (final Throwable t) {
              LOG.warn(
                  "Async transaction listener {} failed to process the notification for transaction {}: {}",
                  listenerName,
                  transaction.getHash(),
                  t.toString());
              LOG.debug("Async transaction listener {} failure stack trace", listenerName, t);
            }
          }
        });
  }

  /**
   * Notifies the events of a pending transactions instance, until {@link #invalidateEmitters()} is
   * called.
   */
  public final class Emitter {
    private final long generation;

    private Emitter(final long generation) {
      this.generation = generation;
    }

    /**
     * Notify the listeners that a transaction has been added.
     *
     * @param pendingTransaction the added transaction
     */
    public void notifyPendingTransactionAdded(final PendingTransaction pendingTransaction) {
      final Lock readLock = generationLock.readLock();
      readLock.lock();
      try {
        if (generation == currentGeneration.get()) {
          onAddedListeners.forEach(
              dispatcher -> dispatcher.dispatch(generation, pendingTransaction.getTransaction()));
        }
      } finally {
        readLock.unlock();
      }
    }

    /**
     * Notify the listeners that a transaction has been dropped.
     *
     * @param pendingTransaction the dropped transaction
     * @param reason the reason the transaction has been dropped
     */
    public void notifyPendingTransactionDropped(
        final PendingTransaction pendingTransaction, final RemovalReason reason) {
      final Lock readLock = generationLock.readLock();
      readLock.lock();
      try {
        if (generation == currentGeneration.get()) {
          onDroppedListeners.forEach(
              dispatcher ->
                  dispatcher.dispatch(generation, pendingTransaction.getTransaction(), reason));
        }
      } finally {
        readLock.unlock();
      }
    }
  }

  @FunctionalInterface
  private interface AddedDispatcher {
    void dispatch(long generation, Transaction transaction);
  }

  @FunctionalInterface
  private interface DroppedDispatcher {
    void dispatch(long generation, Transaction transaction, RemovalReason reason);
  }
}
