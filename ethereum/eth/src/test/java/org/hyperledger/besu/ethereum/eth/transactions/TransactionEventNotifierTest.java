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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

public class TransactionEventNotifierTest {
  private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(30);
  private static final KeyPair KEY_PAIR = SignatureAlgorithmFactory.getInstance().generateKeyPair();
  private static final RemovalReason DROPPED = new TestRemovalReason("dropped");
  private static final RemovalReason REPLACED = new TestRemovalReason("replaced");

  private final PendingTransactionAddedListener addedListener =
      mock(PendingTransactionAddedListener.class);
  private final PendingTransactionDroppedListener droppedListener =
      mock(PendingTransactionDroppedListener.class);

  private final PendingTransaction pendingTx0 = createPendingTransaction(0);
  private final PendingTransaction pendingTx1 = createPendingTransaction(1);

  private DeterministicEthScheduler deterministicEthScheduler;
  private EthScheduler realEthScheduler;

  @BeforeEach
  public void setUp() {
    deterministicEthScheduler = new DeterministicEthScheduler();
  }

  @AfterEach
  public void tearDown() throws InterruptedException {
    if (realEthScheduler != null) {
      realEthScheduler.stop();
      realEthScheduler.awaitStop();
    }
  }

  @Test
  public void addedListenersAreNotified() {
    final var notifier = new TransactionEventNotifier(deterministicEthScheduler);
    final var emitter = notifier.newEmitter();
    final var otherAddedListener = mock(PendingTransactionAddedListener.class);
    notifier.subscribePendingTransactionAdded(addedListener);
    notifier.subscribePendingTransactionAdded(otherAddedListener);
    notifier.subscribePendingTransactionDropped(droppedListener);

    emitter.notifyPendingTransactionAdded(pendingTx0);

    verify(addedListener).onTransactionAdded(pendingTx0.getTransaction());
    verify(otherAddedListener).onTransactionAdded(pendingTx0.getTransaction());
    verifyNoInteractions(droppedListener);
  }

  @Test
  public void droppedListenersAreNotifiedWithTheRemovalReason() {
    final var notifier = new TransactionEventNotifier(deterministicEthScheduler);
    final var emitter = notifier.newEmitter();
    final var otherDroppedListener = mock(PendingTransactionDroppedListener.class);
    notifier.subscribePendingTransactionAdded(addedListener);
    notifier.subscribePendingTransactionDropped(droppedListener);
    notifier.subscribePendingTransactionDropped(otherDroppedListener);

    emitter.notifyPendingTransactionDropped(pendingTx0, DROPPED);

    verify(droppedListener).onTransactionDropped(pendingTx0.getTransaction(), DROPPED);
    verify(otherDroppedListener).onTransactionDropped(pendingTx0.getTransaction(), DROPPED);
    verifyNoInteractions(addedListener);
  }

  @Test
  public void addedListenerIsNotNotifiedAfterUnsubscribe() {
    final var notifier = new TransactionEventNotifier(deterministicEthScheduler);
    final var emitter = notifier.newEmitter();
    final long id = notifier.subscribePendingTransactionAdded(addedListener);

    emitter.notifyPendingTransactionAdded(pendingTx0);
    verify(addedListener).onTransactionAdded(pendingTx0.getTransaction());

    notifier.unsubscribePendingTransactionAdded(id);
    emitter.notifyPendingTransactionAdded(pendingTx1);

    verifyNoMoreInteractions(addedListener);
  }

  @Test
  public void droppedListenerIsNotNotifiedAfterUnsubscribe() {
    final var notifier = new TransactionEventNotifier(deterministicEthScheduler);
    final var emitter = notifier.newEmitter();
    final long id = notifier.subscribePendingTransactionDropped(droppedListener);

    emitter.notifyPendingTransactionDropped(pendingTx0, DROPPED);
    verify(droppedListener).onTransactionDropped(pendingTx0.getTransaction(), DROPPED);

    notifier.unsubscribePendingTransactionDropped(id);
    emitter.notifyPendingTransactionDropped(pendingTx1, DROPPED);

    verifyNoMoreInteractions(droppedListener);
  }

  @Test
  public void unsubscribingAnAddedListenerDoesNotAffectDroppedListeners() {
    final var notifier = new TransactionEventNotifier(deterministicEthScheduler);
    final var emitter = notifier.newEmitter();
    final long addedId = notifier.subscribePendingTransactionAdded(addedListener);
    notifier.subscribePendingTransactionDropped(droppedListener);

    notifier.unsubscribePendingTransactionAdded(addedId);
    emitter.notifyPendingTransactionAdded(pendingTx0);
    emitter.notifyPendingTransactionDropped(pendingTx0, DROPPED);

    verifyNoInteractions(addedListener);
    verify(droppedListener).onTransactionDropped(pendingTx0.getTransaction(), DROPPED);
  }

  @Test
  public void syncListenersAreNotifiedOnTheCallerThread() {
    deterministicEthScheduler.disableAutoRun();
    final var notifier = new TransactionEventNotifier(deterministicEthScheduler);
    final var emitter = notifier.newEmitter();
    notifier.subscribePendingTransactionAdded(addedListener);
    notifier.subscribePendingTransactionDropped(droppedListener);

    emitter.notifyPendingTransactionAdded(pendingTx0);
    emitter.notifyPendingTransactionDropped(pendingTx1, DROPPED);

    final InOrder inOrder = inOrder(addedListener, droppedListener);
    inOrder.verify(addedListener).onTransactionAdded(pendingTx0.getTransaction());
    inOrder.verify(droppedListener).onTransactionDropped(pendingTx1.getTransaction(), DROPPED);
    inOrder.verifyNoMoreInteractions();
    // without async listeners nothing is scheduled
    assertThat(deterministicEthScheduler.getPendingFuturesCount()).isZero();
  }

  @Test
  public void asyncListenersAreNotNotifiedOnTheCallerThread() {
    deterministicEthScheduler.disableAutoRun();
    final var notifier = new TransactionEventNotifier(deterministicEthScheduler);
    final var emitter = notifier.newEmitter();
    notifier.subscribePendingTransactionAddedAsync(addedListener);
    notifier.subscribePendingTransactionDroppedAsync(droppedListener);

    emitter.notifyPendingTransactionAdded(pendingTx0);
    emitter.notifyPendingTransactionDropped(pendingTx1, DROPPED);

    // nothing delivered until the services executor runs the queued work
    verifyNoInteractions(addedListener, droppedListener);
    assertThat(deterministicEthScheduler.getPendingFuturesCount()).isPositive();

    deterministicEthScheduler.runPendingFutures();

    final InOrder inOrder = inOrder(addedListener, droppedListener);
    inOrder.verify(addedListener).onTransactionAdded(pendingTx0.getTransaction());
    inOrder.verify(droppedListener).onTransactionDropped(pendingTx1.getTransaction(), DROPPED);
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void syncAndAsyncListenersAreNotifiedOfTheSameEvents() {
    deterministicEthScheduler.disableAutoRun();
    final var notifier = new TransactionEventNotifier(deterministicEthScheduler);
    final var emitter = notifier.newEmitter();
    final var asyncAddedListener = mock(PendingTransactionAddedListener.class);
    final var asyncDroppedListener = mock(PendingTransactionDroppedListener.class);
    notifier.subscribePendingTransactionAddedAsync(asyncAddedListener);
    notifier.subscribePendingTransactionAdded(addedListener);
    notifier.subscribePendingTransactionDroppedAsync(asyncDroppedListener);
    notifier.subscribePendingTransactionDropped(droppedListener);

    emitter.notifyPendingTransactionAdded(pendingTx0);
    emitter.notifyPendingTransactionDropped(pendingTx0, REPLACED);
    emitter.notifyPendingTransactionAdded(pendingTx1);

    final InOrder syncInOrder = inOrder(addedListener, droppedListener);
    syncInOrder.verify(addedListener).onTransactionAdded(pendingTx0.getTransaction());
    syncInOrder.verify(droppedListener).onTransactionDropped(pendingTx0.getTransaction(), REPLACED);
    syncInOrder.verify(addedListener).onTransactionAdded(pendingTx1.getTransaction());
    syncInOrder.verifyNoMoreInteractions();
    verifyNoInteractions(asyncAddedListener, asyncDroppedListener);

    deterministicEthScheduler.runPendingFutures();

    final InOrder asyncInOrder = inOrder(asyncAddedListener, asyncDroppedListener);
    asyncInOrder.verify(asyncAddedListener).onTransactionAdded(pendingTx0.getTransaction());
    asyncInOrder
        .verify(asyncDroppedListener)
        .onTransactionDropped(pendingTx0.getTransaction(), REPLACED);
    asyncInOrder.verify(asyncAddedListener).onTransactionAdded(pendingTx1.getTransaction());
    asyncInOrder.verifyNoMoreInteractions();
  }

  @Test
  public void asyncListenersAreNotNotifiedAfterUnsubscribe() {
    final var notifier = new TransactionEventNotifier(deterministicEthScheduler);
    final var emitter = notifier.newEmitter();
    final long addedId = notifier.subscribePendingTransactionAddedAsync(addedListener);
    final long droppedId = notifier.subscribePendingTransactionDroppedAsync(droppedListener);

    emitter.notifyPendingTransactionAdded(pendingTx0);
    emitter.notifyPendingTransactionDropped(pendingTx0, DROPPED);
    verify(addedListener).onTransactionAdded(pendingTx0.getTransaction());
    verify(droppedListener).onTransactionDropped(pendingTx0.getTransaction(), DROPPED);

    notifier.unsubscribePendingTransactionAdded(addedId);
    notifier.unsubscribePendingTransactionDropped(droppedId);
    emitter.notifyPendingTransactionAdded(pendingTx1);
    emitter.notifyPendingTransactionDropped(pendingTx1, DROPPED);

    verifyNoMoreInteractions(addedListener, droppedListener);
  }

  @Test
  public void unsubscribingAnAsyncListenerDoesNotUnsubscribeSyncListeners() {
    final var notifier = new TransactionEventNotifier(deterministicEthScheduler);
    final var emitter = notifier.newEmitter();
    final var asyncAddedListener = mock(PendingTransactionAddedListener.class);
    final var asyncDroppedListener = mock(PendingTransactionDroppedListener.class);
    final long syncAddedId = notifier.subscribePendingTransactionAdded(addedListener);
    final long syncDroppedId = notifier.subscribePendingTransactionDropped(droppedListener);
    final long asyncAddedId = notifier.subscribePendingTransactionAddedAsync(asyncAddedListener);
    final long asyncDroppedId =
        notifier.subscribePendingTransactionDroppedAsync(asyncDroppedListener);

    assertThat(asyncAddedId).isNotEqualTo(syncAddedId);
    assertThat(asyncDroppedId).isNotEqualTo(syncDroppedId);

    notifier.unsubscribePendingTransactionAdded(asyncAddedId);
    notifier.unsubscribePendingTransactionDropped(asyncDroppedId);
    emitter.notifyPendingTransactionAdded(pendingTx0);
    emitter.notifyPendingTransactionDropped(pendingTx0, DROPPED);

    verify(addedListener).onTransactionAdded(pendingTx0.getTransaction());
    verify(droppedListener).onTransactionDropped(pendingTx0.getTransaction(), DROPPED);
    verifyNoInteractions(asyncAddedListener, asyncDroppedListener);
  }

  @Test
  public void unsubscribingASyncListenerDoesNotUnsubscribeAsyncListeners() {
    final var notifier = new TransactionEventNotifier(deterministicEthScheduler);
    final var emitter = notifier.newEmitter();
    final var asyncAddedListener = mock(PendingTransactionAddedListener.class);
    final var asyncDroppedListener = mock(PendingTransactionDroppedListener.class);
    final long syncAddedId = notifier.subscribePendingTransactionAdded(addedListener);
    final long syncDroppedId = notifier.subscribePendingTransactionDropped(droppedListener);
    notifier.subscribePendingTransactionAddedAsync(asyncAddedListener);
    notifier.subscribePendingTransactionDroppedAsync(asyncDroppedListener);

    notifier.unsubscribePendingTransactionAdded(syncAddedId);
    notifier.unsubscribePendingTransactionDropped(syncDroppedId);
    emitter.notifyPendingTransactionAdded(pendingTx0);
    emitter.notifyPendingTransactionDropped(pendingTx0, DROPPED);

    verify(asyncAddedListener).onTransactionAdded(pendingTx0.getTransaction());
    verify(asyncDroppedListener).onTransactionDropped(pendingTx0.getTransaction(), DROPPED);
    verifyNoInteractions(addedListener, droppedListener);
  }

  @Test
  public void invalidatedEmittersDoNotNotifySyncAndAsyncListeners() {
    deterministicEthScheduler.disableAutoRun();
    final var notifier = new TransactionEventNotifier(deterministicEthScheduler);
    final var emitter = notifier.newEmitter();
    final var otherEmitter = notifier.newEmitter();
    final var asyncAddedListener = mock(PendingTransactionAddedListener.class);
    final var asyncDroppedListener = mock(PendingTransactionDroppedListener.class);
    notifier.subscribePendingTransactionAdded(addedListener);
    notifier.subscribePendingTransactionDropped(droppedListener);
    notifier.subscribePendingTransactionAddedAsync(asyncAddedListener);
    notifier.subscribePendingTransactionDroppedAsync(asyncDroppedListener);
    final PendingTransaction pendingTx2 = createPendingTransaction(2);

    // notified before the invalidation, async notification still queued
    emitter.notifyPendingTransactionAdded(pendingTx0);
    verify(addedListener).onTransactionAdded(pendingTx0.getTransaction());

    notifier.invalidateEmitters();

    // the queued async notification is skipped
    deterministicEthScheduler.runPendingFutures();
    verifyNoInteractions(asyncAddedListener);

    // events from the invalidated emitters are ignored
    emitter.notifyPendingTransactionAdded(pendingTx1);
    otherEmitter.notifyPendingTransactionDropped(pendingTx1, DROPPED);
    deterministicEthScheduler.runPendingFutures();
    verifyNoMoreInteractions(addedListener);
    verifyNoInteractions(droppedListener, asyncAddedListener, asyncDroppedListener);

    // a new emitter is valid
    final var newEmitter = notifier.newEmitter();
    newEmitter.notifyPendingTransactionAdded(pendingTx2);
    newEmitter.notifyPendingTransactionDropped(pendingTx2, DROPPED);
    deterministicEthScheduler.runPendingFutures();
    verify(addedListener).onTransactionAdded(pendingTx2.getTransaction());
    verify(droppedListener).onTransactionDropped(pendingTx2.getTransaction(), DROPPED);
    verify(asyncAddedListener).onTransactionAdded(pendingTx2.getTransaction());
    verify(asyncDroppedListener).onTransactionDropped(pendingTx2.getTransaction(), DROPPED);
    verifyNoMoreInteractions(
        addedListener, droppedListener, asyncAddedListener, asyncDroppedListener);
  }

  @Test
  public void invalidateEmittersWaitsForTheSyncNotificationInProgress()
      throws InterruptedException {
    final var notifier = new TransactionEventNotifier(deterministicEthScheduler);
    final var emitter = notifier.newEmitter();
    final CountDownLatch notificationStarted = new CountDownLatch(1);
    final CountDownLatch releaseNotification = new CountDownLatch(1);
    notifier.subscribePendingTransactionAdded(
        tx -> {
          notificationStarted.countDown();
          awaitLatch(releaseNotification);
        });

    final Thread notifying = new Thread(() -> emitter.notifyPendingTransactionAdded(pendingTx0));
    notifying.start();
    notificationStarted.await();

    final Thread invalidating = new Thread(notifier::invalidateEmitters);
    invalidating.start();
    invalidating.join(200);
    assertThat(invalidating.isAlive()).isTrue();

    releaseNotification.countDown();
    invalidating.join(AWAIT_TIMEOUT.toMillis());
    assertThat(invalidating.isAlive()).isFalse();
    notifying.join(AWAIT_TIMEOUT.toMillis());
  }

  @Test
  public void invalidateEmittersDoesNotWaitForTheAsyncNotificationInProgress()
      throws InterruptedException {
    realEthScheduler = new EthScheduler(1, 1, 1, new NoOpMetricsSystem());
    final var notifier = new TransactionEventNotifier(realEthScheduler);
    final var emitter = notifier.newEmitter();
    final CountDownLatch notificationStarted = new CountDownLatch(1);
    final CountDownLatch releaseNotification = new CountDownLatch(1);
    notifier.subscribePendingTransactionAddedAsync(
        tx -> {
          notificationStarted.countDown();
          awaitLatch(releaseNotification);
        });

    emitter.notifyPendingTransactionAdded(pendingTx0);
    notificationStarted.await();

    try {
      final Thread invalidating = new Thread(notifier::invalidateEmitters);
      invalidating.start();
      invalidating.join(AWAIT_TIMEOUT.toMillis());
      assertThat(invalidating.isAlive()).isFalse();
    } finally {
      releaseNotification.countDown();
    }
  }

  @Test
  public void failingSyncListenerDoesNotPreventOtherListenersAndDoesNotPropagate() {
    final var notifier = new TransactionEventNotifier(deterministicEthScheduler);
    final var emitter = notifier.newEmitter();
    notifier.subscribePendingTransactionAdded(
        tx -> {
          throw new IllegalStateException("listener failure");
        });
    notifier.subscribePendingTransactionAdded(addedListener);
    notifier.subscribePendingTransactionDropped(
        (tx, reason) -> {
          throw new IllegalStateException("listener failure");
        });
    notifier.subscribePendingTransactionDropped(droppedListener);

    emitter.notifyPendingTransactionAdded(pendingTx0);
    emitter.notifyPendingTransactionDropped(pendingTx0, DROPPED);

    verify(addedListener).onTransactionAdded(pendingTx0.getTransaction());
    verify(droppedListener).onTransactionDropped(pendingTx0.getTransaction(), DROPPED);
  }

  @Test
  public void failingAsyncListenerDoesNotPreventOtherNotifications() {
    final var notifier = new TransactionEventNotifier(deterministicEthScheduler);
    final var emitter = notifier.newEmitter();
    final List<Transaction> failingListenerCalls = new ArrayList<>();
    notifier.subscribePendingTransactionAddedAsync(
        tx -> {
          failingListenerCalls.add(tx);
          throw new IllegalStateException("listener failure");
        });
    notifier.subscribePendingTransactionAddedAsync(addedListener);

    emitter.notifyPendingTransactionAdded(pendingTx0);
    emitter.notifyPendingTransactionAdded(pendingTx1);

    assertThat(failingListenerCalls)
        .containsExactly(pendingTx0.getTransaction(), pendingTx1.getTransaction());
    final InOrder inOrder = inOrder(addedListener);
    inOrder.verify(addedListener).onTransactionAdded(pendingTx0.getTransaction());
    inOrder.verify(addedListener).onTransactionAdded(pendingTx1.getTransaction());
  }

  @Test
  public void addedAndDroppedNotificationsAreDeliveredInSubmissionOrder() {
    final var notifier = new TransactionEventNotifier(deterministicEthScheduler);
    final var emitter = notifier.newEmitter();
    notifier.subscribePendingTransactionAdded(addedListener);
    notifier.subscribePendingTransactionDropped(droppedListener);

    emitter.notifyPendingTransactionAdded(pendingTx0);
    emitter.notifyPendingTransactionDropped(pendingTx0, REPLACED);
    emitter.notifyPendingTransactionAdded(pendingTx1);
    emitter.notifyPendingTransactionDropped(pendingTx1, DROPPED);

    final InOrder inOrder = inOrder(addedListener, droppedListener);
    inOrder.verify(addedListener).onTransactionAdded(pendingTx0.getTransaction());
    inOrder.verify(droppedListener).onTransactionDropped(pendingTx0.getTransaction(), REPLACED);
    inOrder.verify(addedListener).onTransactionAdded(pendingTx1.getTransaction());
    inOrder.verify(droppedListener).onTransactionDropped(pendingTx1.getTransaction(), DROPPED);
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void asyncNotificationsAreDeliveredInSubmissionOrderWithRealScheduler() {
    realEthScheduler = new EthScheduler(1, 1, 1, new NoOpMetricsSystem());
    final var notifier = new TransactionEventNotifier(realEthScheduler);
    final var emitter = notifier.newEmitter();

    final int numOfTxs = 200;
    final List<PendingTransaction> pendingTxs =
        IntStream.range(0, numOfTxs).mapToObj(this::createPendingTransaction).toList();

    final List<String> events = new CopyOnWriteArrayList<>();
    notifier.subscribePendingTransactionAddedAsync(
        tx -> {
          events.add("added:" + tx.getNonce());
          // slow listener, so notifications pile up in the queue
          sleep(1);
        });
    notifier.subscribePendingTransactionDroppedAsync(
        (tx, reason) -> events.add("dropped:" + tx.getNonce()));

    final List<String> expectedEvents = new ArrayList<>(numOfTxs * 2);
    for (final PendingTransaction pendingTx : pendingTxs) {
      emitter.notifyPendingTransactionAdded(pendingTx);
      expectedEvents.add("added:" + pendingTx.getNonce());
      if (pendingTx.getNonce() % 3 == 0) {
        emitter.notifyPendingTransactionDropped(pendingTx, DROPPED);
        expectedEvents.add("dropped:" + pendingTx.getNonce());
      }
    }

    await().atMost(AWAIT_TIMEOUT).until(() -> events.size() == expectedEvents.size());
    assertThat(events).containsExactlyElementsOf(expectedEvents);
  }

  @Test
  public void asyncNotificationsFromConcurrentProducersAreDeliveredOneAtATimeKeepingOrder()
      throws InterruptedException {
    realEthScheduler = new EthScheduler(1, 1, 1, new NoOpMetricsSystem());
    final var notifier = new TransactionEventNotifier(realEthScheduler);
    final var emitter = notifier.newEmitter();

    final int numOfProducers = 4;
    final int numOfTxsPerProducer = 100;

    final AtomicInteger concurrentDeliveries = new AtomicInteger();
    final AtomicInteger maxConcurrentDeliveries = new AtomicInteger();
    final List<Transaction> delivered = new CopyOnWriteArrayList<>();
    notifier.subscribePendingTransactionAddedAsync(
        tx -> {
          maxConcurrentDeliveries.accumulateAndGet(
              concurrentDeliveries.incrementAndGet(), Math::max);
          delivered.add(tx);
          sleep(1);
          concurrentDeliveries.decrementAndGet();
        });

    // each producer uses a distinct gas limit to be able to tell their txs apart
    final List<List<PendingTransaction>> txsByProducer =
        IntStream.range(0, numOfProducers)
            .mapToObj(
                p ->
                    IntStream.range(0, numOfTxsPerProducer)
                        .mapToObj(n -> createPendingTransaction(n, 21_000L + p))
                        .toList())
            .toList();

    final CountDownLatch start = new CountDownLatch(1);
    final List<Thread> producers =
        txsByProducer.stream()
            .map(
                txs ->
                    new Thread(
                        () -> {
                          try {
                            start.await();
                          } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                          }
                          txs.forEach(emitter::notifyPendingTransactionAdded);
                        }))
            .toList();
    producers.forEach(Thread::start);
    start.countDown();
    for (final Thread producer : producers) {
      producer.join();
    }

    await()
        .atMost(AWAIT_TIMEOUT)
        .until(() -> delivered.size() == numOfProducers * numOfTxsPerProducer);

    assertThat(maxConcurrentDeliveries.get()).isEqualTo(1);
    for (int p = 0; p < numOfProducers; p++) {
      final long gasLimit = 21_000L + p;
      assertThat(delivered.stream().filter(tx -> tx.getGasLimit() == gasLimit))
          .describedAs("notifications of producer %d", p)
          .containsExactlyElementsOf(
              txsByProducer.get(p).stream().map(PendingTransaction::getTransaction).toList());
    }
  }

  private static void awaitLatch(final CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private static void sleep(final long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private PendingTransaction createPendingTransaction(final int nonce) {
    return createPendingTransaction(nonce, 21_000);
  }

  private PendingTransaction createPendingTransaction(final int nonce, final long gasLimit) {
    final Transaction tx =
        new TransactionTestFixture().nonce(nonce).gasLimit(gasLimit).createTransaction(KEY_PAIR);
    return new PendingTransaction.Remote(tx);
  }

  private record TestRemovalReason(String label) implements RemovalReason {
    @Override
    public boolean stopTracking() {
      return false;
    }

    @Override
    public boolean stopBroadcasting() {
      return false;
    }
  }
}
