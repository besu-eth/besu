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
package org.hyperledger.besu.ethereum.worldstate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

class WorldStateRepairBudgetTest {

  @Test
  void expiresAtTenSecondsOfMonotonicTime() {
    final AtomicLong nanoTime = new AtomicLong(123L);
    final WorldStateRepairBudget budget = new WorldStateRepairBudget(nanoTime::get);

    assertThat(budget.remainingTime()).isEqualTo(Duration.ofSeconds(10));
    assertThat(budget.isExhausted()).isFalse();

    nanoTime.addAndGet(Duration.ofSeconds(10).toNanos());

    assertThat(budget.remainingTime()).isZero();
    assertThat(budget.isExhausted()).isTrue();
    assertThat(budget.consumeRepairObject()).isFalse();
  }

  @Test
  void permitsExactlyThirtyTwoObjectsAndLatchesExhaustion() {
    final WorldStateRepairBudget budget = new WorldStateRepairBudget(() -> 0L);
    for (int i = 0; i < 32; i++) {
      assertThat(budget.consumeRepairObject()).isTrue();
    }
    assertThat(budget.isExhausted()).isFalse();
    assertThat(budget.consumeRepairObject()).isFalse();
    assertThat(budget.isExhausted()).isTrue();
    assertThat(budget.consumeReceivedBytes(1)).isFalse();
  }

  @Test
  void chargesTransitionsCumulativelyWithoutOverflow() {
    final WorldStateRepairBudget budget = new WorldStateRepairBudget(() -> 0L);
    assertThat(budget.consumeTransitions(40)).isTrue();
    assertThat(budget.consumeTransitions(24)).isTrue();
    assertThat(budget.consumeTransitions(Integer.MAX_VALUE)).isFalse();
    assertThat(budget.isExhausted()).isTrue();
  }

  @Test
  void chargesReceivedBytesCumulativelyAtEightMebibytes() {
    final WorldStateRepairBudget budget = new WorldStateRepairBudget(() -> 0L);
    assertThat(budget.consumeReceivedBytes(3L * 1024 * 1024)).isTrue();
    assertThat(budget.consumeReceivedBytes(5L * 1024 * 1024)).isTrue();
    assertThat(budget.consumeReceivedBytes(1)).isFalse();
    assertThat(budget.isExhausted()).isTrue();
  }

  @Test
  void chargesStagedBytesAtSixteenMebibytesAndRejectsOverflow() {
    final WorldStateRepairBudget budget = new WorldStateRepairBudget(() -> 0L);
    assertThat(budget.consumeStagedBytes(16L * 1024 * 1024)).isTrue();
    assertThat(budget.consumeStagedBytes(Long.MAX_VALUE)).isFalse();
    assertThat(budget.isExhausted()).isTrue();
  }

  @Test
  void cancellationRejectsEveryChargeWithoutPretendingResourcesWereExhausted() {
    final WorldStateRepairBudget budget = new WorldStateRepairBudget(() -> 0L);
    budget.cancel();
    budget.cancel();
    assertThat(budget.isCancelled()).isTrue();
    assertThat(budget.isExhausted()).isFalse();
    assertThat(budget.remainingTime()).isZero();
    assertThat(budget.consumeRepairObject()).isFalse();
    assertThat(budget.consumeTransitions(0)).isFalse();
    assertThat(budget.consumeReceivedBytes(0)).isFalse();
    assertThat(budget.consumeStagedBytes(0)).isFalse();
  }

  @Test
  void negativeChargesCannotRefundTheBudget() {
    final WorldStateRepairBudget budget = new WorldStateRepairBudget(() -> 0L);
    assertThatThrownBy(() -> budget.consumeReceivedBytes(-1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> budget.consumeStagedBytes(-1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> budget.consumeTransitions(-1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(budget.consumeTransitions(64)).isTrue();
  }

  @Test
  void monotonicClockWrapDoesNotExtendTheDeadline() {
    final AtomicLong nanoTime = new AtomicLong(Long.MAX_VALUE - Duration.ofSeconds(5).toNanos());
    final WorldStateRepairBudget budget = new WorldStateRepairBudget(nanoTime::get);
    nanoTime.addAndGet(Duration.ofSeconds(9).toNanos());
    assertThat(budget.remainingTime()).isEqualTo(Duration.ofSeconds(1));
    nanoTime.addAndGet(Duration.ofSeconds(1).toNanos());
    assertThat(budget.isExhausted()).isTrue();
  }

  @Test
  void concurrentChargesCannotOverspendTheObjectLimit() throws Exception {
    final WorldStateRepairBudget budget = new WorldStateRepairBudget(() -> 0L);
    final ExecutorService workers = Executors.newFixedThreadPool(2);
    try {
      final List<Callable<Boolean>> charges = Collections.nCopies(128, budget::consumeRepairObject);
      long accepted = 0;
      for (final Future<Boolean> charge : workers.invokeAll(charges)) {
        if (charge.get()) {
          accepted++;
        }
      }
      assertThat(accepted).isEqualTo(32);
      assertThat(budget.isExhausted()).isTrue();
    } finally {
      workers.shutdownNow();
    }
  }
}
