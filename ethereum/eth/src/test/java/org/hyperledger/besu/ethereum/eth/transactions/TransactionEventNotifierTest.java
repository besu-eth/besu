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
    final var otherAddedListener = mock(PendingTransactionAddedListener.class);
    notifier.subscribePendingTransactionAdded(addedListener);
    notifier.subscribePendingTransactionAdded(otherAddedListener);
    notifier.subscribePendingTransactionDropped(droppedListener);

    notifier.notifyPendingTransactionAdded(pendingTx0);

    verify(addedListener).onTransactionAdded(pendingTx0.getTransaction());
    verify(otherAddedListener).onTransactionAdded(pendingTx0.getTransaction());
    verifyNoInteractions(droppedListener);
  }

  @Test
  public void droppedListenersAreNotifiedWithTheRemovalReason() {
    final var notifier = new TransactionEventNotifier(deterministicEthScheduler);
    final var otherDroppedListener = mock(PendingTransactionDroppedListener.class);
    notifier.subscribePendingTransactionAdded(addedListener);
    notifier.subscribePendingTransactionDropped(droppedListener);
    notifier.subscribePendingTransactionDropped(otherDroppedListener);

    notifier.notifyPendingTransactionDropped(pendingTx0, DROPPED);

    verify(droppedListener).onTransactionDropped(pendingTx0.getTransaction(), DROPPED);
    verify(otherDroppedListener).onTransactionDropped(pendingTx0.getTransaction(), DROPPED);
    verifyNoInteractions(addedListener);
  }

  @Test
  public void addedListenerIsNotNotifiedAfterUnsubscribe() {
    final var notifier = new TransactionEventNotifier(deterministicEthScheduler);
    final long id = notifier.subscribePendingTransactionAdded(addedListener);

    notifier.notifyPendingTransactionAdded(pendingTx0);
    verify(addedListener).onTransactionAdded(pendingTx0.getTransaction());

    notifier.unsubscribePendingTransactionAdded(id);
    notifier.notifyPendingTransactionAdded(pendingTx1);

    verifyNoMoreInteractions(addedListener);
  }

  @Test
  public void droppedListenerIsNotNotifiedAfterUnsubscribe() {
    final var notifier = new TransactionEventNotifier(deterministicEthScheduler);
    final long id = notifier.subscribePendingTransactionDropped(droppedListener);

    notifier.notifyPendingTransactionDropped(pendingTx0, DROPPED);
    verify(droppedListener).onTransactionDropped(pendingTx0.getTransaction(), DROPPED);

    notifier.unsubscribePendingTransactionDropped(id);
    notifier.notifyPendingTransactionDropped(pendingTx1, DROPPED);

    verifyNoMoreInteractions(droppedListener);
  }

  @Test
  public void unsubscribingAnAddedListenerDoesNotAffectDroppedListeners() {
    final var notifier = new TransactionEventNotifier(deterministicEthScheduler);
    final long addedId = notifier.subscribePendingTransactionAdded(addedListener);
    notifier.subscribePendingTransactionDropped(droppedListener);

    notifier.unsubscribePendingTransactionAdded(addedId);
    notifier.notifyPendingTransactionAdded(pendingTx0);
    notifier.notifyPendingTransactionDropped(pendingTx0, DROPPED);

    verifyNoInteractions(addedListener);
    verify(droppedListener).onTransactionDropped(pendingTx0.getTransaction(), DROPPED);
  }

  @Test
  public void syncListenersAreNotifiedOnTheCallerThread() {
    deterministicEthScheduler.disableAutoRun();
    final var notifier = new TransactionEventNotifier(deterministicEthScheduler);
    notifier.subscribePendingTransactionAdded(addedListener);
    notifier.subscribePendingTransactionDropped(droppedListener);

    notifier.notifyPendingTransactionAdded(pendingTx0);
    notifier.notifyPendingTransactionDropped(pendingTx1, DROPPED);

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
    notifier.subscribePendingTransactionAddedAsync(addedListener);
    notifier.subscribePendingTransactionDroppedAsync(droppedListener);

    notifier.notifyPendingTransactionAdded(pendingTx0);
    notifier.notifyPendingTransactionDropped(pendingTx1, DROPPED);

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
    final var asyncAddedListener = mock(PendingTransactionAddedListener.class);
    final var asyncDroppedListener = mock(PendingTransactionDroppedListener.class);
    notifier.subscribePendingTransactionAddedAsync(asyncAddedListener);
    notifier.subscribePendingTransactionAdded(addedListener);
    notifier.subscribePendingTransactionDroppedAsync(asyncDroppedListener);
    notifier.subscribePendingTransactionDropped(droppedListener);

    notifier.notifyPendingTransactionAdded(pendingTx0);
    notifier.notifyPendingTransactionDropped(pendingTx0, REPLACED);
    notifier.notifyPendingTransactionAdded(pendingTx1);

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
    final long addedId = notifier.subscribePendingTransactionAddedAsync(addedListener);
    final long droppedId = notifier.subscribePendingTransactionDroppedAsync(droppedListener);

    notifier.notifyPendingTransactionAdded(pendingTx0);
    notifier.notifyPendingTransactionDropped(pendingTx0, DROPPED);
    verify(addedListener).onTransactionAdded(pendingTx0.getTransaction());
    verify(droppedListener).onTransactionDropped(pendingTx0.getTransaction(), DROPPED);

    notifier.unsubscribePendingTransactionAdded(addedId);
    notifier.unsubscribePendingTransactionDropped(droppedId);
    notifier.notifyPendingTransactionAdded(pendingTx1);
    notifier.notifyPendingTransactionDropped(pendingTx1, DROPPED);

    verifyNoMoreInteractions(addedListener, droppedListener);
  }

  @Test
  public void unsubscribingAnAsyncListenerDoesNotUnsubscribeSyncListeners() {
    final var notifier = new TransactionEventNotifier(deterministicEthScheduler);
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
    notifier.notifyPendingTransactionAdded(pendingTx0);
    notifier.notifyPendingTransactionDropped(pendingTx0, DROPPED);

    verify(addedListener).onTransactionAdded(pendingTx0.getTransaction());
    verify(droppedListener).onTransactionDropped(pendingTx0.getTransaction(), DROPPED);
    verifyNoInteractions(asyncAddedListener, asyncDroppedListener);
  }

  @Test
  public void unsubscribingASyncListenerDoesNotUnsubscribeAsyncListeners() {
    final var notifier = new TransactionEventNotifier(deterministicEthScheduler);
    final var asyncAddedListener = mock(PendingTransactionAddedListener.class);
    final var asyncDroppedListener = mock(PendingTransactionDroppedListener.class);
    final long syncAddedId = notifier.subscribePendingTransactionAdded(addedListener);
    final long syncDroppedId = notifier.subscribePendingTransactionDropped(droppedListener);
    notifier.subscribePendingTransactionAddedAsync(asyncAddedListener);
    notifier.subscribePendingTransactionDroppedAsync(asyncDroppedListener);

    notifier.unsubscribePendingTransactionAdded(syncAddedId);
    notifier.unsubscribePendingTransactionDropped(syncDroppedId);
    notifier.notifyPendingTransactionAdded(pendingTx0);
    notifier.notifyPendingTransactionDropped(pendingTx0, DROPPED);

    verify(asyncAddedListener).onTransactionAdded(pendingTx0.getTransaction());
    verify(asyncDroppedListener).onTransactionDropped(pendingTx0.getTransaction(), DROPPED);
    verifyNoInteractions(addedListener, droppedListener);
  }

  @Test
  public void asyncListenersAreNotNotifiedWhileDisabled() {
    deterministicEthScheduler.disableAutoRun();
    final var notifier = new TransactionEventNotifier(deterministicEthScheduler);
    final var asyncAddedListener = mock(PendingTransactionAddedListener.class);
    notifier.subscribePendingTransactionAdded(addedListener);
    notifier.subscribePendingTransactionAddedAsync(asyncAddedListener);
    final PendingTransaction pendingTx2 = createPendingTransaction(2);

    // queued before disabling, then dropped
    notifier.notifyPendingTransactionAdded(pendingTx0);
    assertThat(notifier.awaitDisabled(Duration.ofSeconds(1))).isTrue();
    // ignored while disabled
    notifier.notifyPendingTransactionAdded(pendingTx1);
    deterministicEthScheduler.runPendingFutures();
    verifyNoInteractions(asyncAddedListener);

    notifier.enable();
    notifier.notifyPendingTransactionAdded(pendingTx2);
    deterministicEthScheduler.runPendingFutures();
    verify(asyncAddedListener).onTransactionAdded(pendingTx2.getTransaction());
    verifyNoMoreInteractions(asyncAddedListener);

    // sync listeners are not affected
    final InOrder inOrder = inOrder(addedListener);
    inOrder.verify(addedListener).onTransactionAdded(pendingTx0.getTransaction());
    inOrder.verify(addedListener).onTransactionAdded(pendingTx1.getTransaction());
    inOrder.verify(addedListener).onTransactionAdded(pendingTx2.getTransaction());
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void failingSyncListenerDoesNotPreventOtherListenersAndDoesNotPropagate() {
    final var notifier = new TransactionEventNotifier(deterministicEthScheduler);
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

    notifier.notifyPendingTransactionAdded(pendingTx0);
    notifier.notifyPendingTransactionDropped(pendingTx0, DROPPED);

    verify(addedListener).onTransactionAdded(pendingTx0.getTransaction());
    verify(droppedListener).onTransactionDropped(pendingTx0.getTransaction(), DROPPED);
  }

  @Test
  public void failingAsyncListenerDoesNotPreventOtherNotifications() {
    final var notifier = new TransactionEventNotifier(deterministicEthScheduler);
    final List<Transaction> failingListenerCalls = new ArrayList<>();
    notifier.subscribePendingTransactionAddedAsync(
        tx -> {
          failingListenerCalls.add(tx);
          throw new IllegalStateException("listener failure");
        });
    notifier.subscribePendingTransactionAddedAsync(addedListener);

    notifier.notifyPendingTransactionAdded(pendingTx0);
    notifier.notifyPendingTransactionAdded(pendingTx1);

    assertThat(failingListenerCalls)
        .containsExactly(pendingTx0.getTransaction(), pendingTx1.getTransaction());
    final InOrder inOrder = inOrder(addedListener);
    inOrder.verify(addedListener).onTransactionAdded(pendingTx0.getTransaction());
    inOrder.verify(addedListener).onTransactionAdded(pendingTx1.getTransaction());
  }

  @Test
  public void addedAndDroppedNotificationsAreDeliveredInSubmissionOrder() {
    final var notifier = new TransactionEventNotifier(deterministicEthScheduler);
    notifier.subscribePendingTransactionAdded(addedListener);
    notifier.subscribePendingTransactionDropped(droppedListener);

    notifier.notifyPendingTransactionAdded(pendingTx0);
    notifier.notifyPendingTransactionDropped(pendingTx0, REPLACED);
    notifier.notifyPendingTransactionAdded(pendingTx1);
    notifier.notifyPendingTransactionDropped(pendingTx1, DROPPED);

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
      notifier.notifyPendingTransactionAdded(pendingTx);
      expectedEvents.add("added:" + pendingTx.getNonce());
      if (pendingTx.getNonce() % 3 == 0) {
        notifier.notifyPendingTransactionDropped(pendingTx, DROPPED);
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
                          txs.forEach(notifier::notifyPendingTransactionAdded);
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
