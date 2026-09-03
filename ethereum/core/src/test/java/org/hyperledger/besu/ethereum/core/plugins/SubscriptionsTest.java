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
package org.hyperledger.besu.ethereum.core.plugins;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.plugin.services.Subscription;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

class SubscriptionsTest {

  @Test
  void closeRunsTheUnsubscribeAction() {
    final AtomicInteger runs = new AtomicInteger();
    final Subscription subscription = Subscriptions.unsubscribeOnClose(runs::incrementAndGet);

    assertThat(runs.get()).isZero();
    subscription.close();
    assertThat(runs.get()).isEqualTo(1);
  }

  @Test
  void secondCloseIsANoOp() {
    final AtomicInteger runs = new AtomicInteger();
    final Subscription subscription = Subscriptions.unsubscribeOnClose(runs::incrementAndGet);

    subscription.close();
    subscription.close();
    subscription.close();
    assertThat(runs.get()).isEqualTo(1);
  }

  @Test
  void concurrentClosesRunTheActionOnce() throws InterruptedException {
    final AtomicInteger runs = new AtomicInteger();
    final Subscription subscription = Subscriptions.unsubscribeOnClose(runs::incrementAndGet);
    final int threads = 16;
    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(threads);
    final List<Thread> workers = new ArrayList<>();
    for (int i = 0; i < threads; i++) {
      final Thread t =
          new Thread(
              () -> {
                try {
                  start.await();
                } catch (final InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
                subscription.close();
                done.countDown();
              });
      workers.add(t);
      t.start();
    }
    start.countDown();
    done.await();
    for (final Thread t : workers) {
      t.join();
    }

    assertThat(runs.get()).isEqualTo(1);
  }

  @Test
  void eachHandleIsIndependent() {
    final AtomicInteger a = new AtomicInteger();
    final AtomicInteger b = new AtomicInteger();
    final Subscription subA = Subscriptions.unsubscribeOnClose(a::incrementAndGet);
    final Subscription subB = Subscriptions.unsubscribeOnClose(b::incrementAndGet);

    subA.close();
    assertThat(a.get()).isEqualTo(1);
    assertThat(b.get()).isZero();

    subB.close();
    assertThat(a.get()).isEqualTo(1);
    assertThat(b.get()).isEqualTo(1);
  }
}
