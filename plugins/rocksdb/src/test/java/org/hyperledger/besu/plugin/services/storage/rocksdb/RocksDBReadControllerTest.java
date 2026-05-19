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
package org.hyperledger.besu.plugin.services.storage.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.plugin.services.storage.StorageReadPriority;
import org.hyperledger.besu.plugin.services.storage.StorageReadPriorityContext;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

public class RocksDBReadControllerTest {

  @Test
  public void shouldBypassHighPriorityReadsAndPreferMediumOverLowPriorityBackgroundReads()
      throws Exception {
    final RocksDBReadController controller = new RocksDBReadController(1);
    final ExecutorService executor = Executors.newFixedThreadPool(4);
    final CountDownLatch firstReadStarted = new CountDownLatch(1);
    final CountDownLatch releaseFirstRead = new CountDownLatch(1);
    final List<StorageReadPriority> completionOrder = new CopyOnWriteArrayList<>();

    try {
      final Future<?> firstLowRead =
          executor.submit(
              () ->
                  StorageReadPriorityContext.withPriority(
                      StorageReadPriority.LOW,
                      () ->
                          controller.executeUnchecked(
                              () -> {
                                firstReadStarted.countDown();
                                completionOrder.add(StorageReadPriority.LOW);
                                await(releaseFirstRead);
                                return null;
                              })));
      assertThat(firstReadStarted.await(5, TimeUnit.SECONDS)).isTrue();

      final Future<?> queuedLowRead =
          executor.submit(
              () ->
                  StorageReadPriorityContext.withPriority(
                      StorageReadPriority.LOW,
                      () ->
                          controller.executeUnchecked(
                              () -> {
                                completionOrder.add(StorageReadPriority.LOW);
                                return null;
                              })));
      awaitWaitingReads(controller, StorageReadPriority.LOW, 1);

      final Future<?> queuedHighRead =
          executor.submit(
              () ->
                  StorageReadPriorityContext.withPriority(
                      StorageReadPriority.HIGH,
                      () ->
                          controller.executeUnchecked(
                              () -> {
                                completionOrder.add(StorageReadPriority.HIGH);
                                return null;
                              })));
      queuedHighRead.get(5, TimeUnit.SECONDS);
      assertThat(completionOrder)
          .containsExactly(StorageReadPriority.LOW, StorageReadPriority.HIGH);

      final Future<?> queuedMediumRead =
          executor.submit(
              () ->
                  StorageReadPriorityContext.withPriority(
                      StorageReadPriority.MEDIUM,
                      () ->
                          controller.executeUnchecked(
                              () -> {
                                completionOrder.add(StorageReadPriority.MEDIUM);
                                return null;
                              })));
      awaitWaitingReads(controller, StorageReadPriority.MEDIUM, 1);

      releaseFirstRead.countDown();

      firstLowRead.get(5, TimeUnit.SECONDS);
      queuedMediumRead.get(5, TimeUnit.SECONDS);
      queuedLowRead.get(5, TimeUnit.SECONDS);

      assertThat(completionOrder)
          .containsExactly(
              StorageReadPriority.LOW,
              StorageReadPriority.HIGH,
              StorageReadPriority.MEDIUM,
              StorageReadPriority.LOW);
      assertThat(controller.metricsSummaryAndReset())
          .isPresent()
          .get()
          .asString()
          .contains(
              "maxConcurrentBackgroundReads=1",
              "maxInFlightBackgroundReads=1",
              "HIGH{gated=false}",
              "HIGH/high{reads=1, queued=0",
              "MEDIUM/medium{reads=1, queued=1",
              "LOW/low{reads=2, queued=1");
      assertThat(controller.metricsSummaryAndReset()).isEmpty();
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void shouldAllowMoreLowPriorityReadsWhenHighPriorityReadsAreNotActive() throws Exception {
    final RocksDBReadController controller =
        new RocksDBReadController(4, 3, 2, TimeUnit.SECONDS.toNanos(30));
    final ExecutorService executor = Executors.newFixedThreadPool(5);
    final CountDownLatch releaseLowReads = new CountDownLatch(1);

    try {
      final List<Future<?>> lowReads = new CopyOnWriteArrayList<>();
      for (int i = 0; i < 4; i++) {
        lowReads.add(
            submitBlockingRead(executor, controller, StorageReadPriority.LOW, releaseLowReads));
      }

      awaitInFlightReads(controller, StorageReadPriority.LOW, 4);
      assertThat(controller.waitingReads(StorageReadPriority.LOW)).isZero();

      releaseLowReads.countDown();
      for (final Future<?> lowRead : lowReads) {
        lowRead.get(5, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void shouldReduceLowPriorityReadConcurrencyWhileHighPriorityReadsAreActive()
      throws Exception {
    final RocksDBReadController controller =
        new RocksDBReadController(4, 3, 2, TimeUnit.SECONDS.toNanos(30));
    final ExecutorService executor = Executors.newFixedThreadPool(4);
    final CountDownLatch highReadStarted = new CountDownLatch(1);
    final CountDownLatch releaseHighRead = new CountDownLatch(1);
    final CountDownLatch releaseLowReads = new CountDownLatch(1);

    try {
      final Future<?> highRead =
          executor.submit(
              () ->
                  StorageReadPriorityContext.withPriority(
                      StorageReadPriority.HIGH,
                      () ->
                          controller.executeUnchecked(
                              () -> {
                                highReadStarted.countDown();
                                await(releaseHighRead);
                                return null;
                              })));
      assertThat(highReadStarted.await(5, TimeUnit.SECONDS)).isTrue();

      final Future<?> firstLowRead =
          submitBlockingRead(executor, controller, StorageReadPriority.LOW, releaseLowReads);
      final Future<?> secondLowRead =
          submitBlockingRead(executor, controller, StorageReadPriority.LOW, releaseLowReads);
      final Future<?> queuedLowRead =
          submitBlockingRead(executor, controller, StorageReadPriority.LOW, releaseLowReads);

      awaitInFlightReads(controller, StorageReadPriority.LOW, 2);
      awaitWaitingReads(controller, StorageReadPriority.LOW, 1);

      releaseHighRead.countDown();
      highRead.get(5, TimeUnit.SECONDS);
      assertThat(controller.waitingReads(StorageReadPriority.LOW)).isEqualTo(1);

      releaseLowReads.countDown();
      firstLowRead.get(5, TimeUnit.SECONDS);
      secondLowRead.get(5, TimeUnit.SECONDS);
      queuedLowRead.get(5, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void shouldReduceLowPriorityReadConcurrencyWhileHighPriorityScopeIsActive()
      throws Exception {
    final RocksDBReadController controller =
        new RocksDBReadController(4, 3, 2, TimeUnit.SECONDS.toNanos(30));
    final ExecutorService executor = Executors.newFixedThreadPool(4);
    final CountDownLatch highScopeStarted = new CountDownLatch(1);
    final CountDownLatch releaseHighScope = new CountDownLatch(1);
    final CountDownLatch releaseLowReads = new CountDownLatch(1);

    try {
      final Future<?> highScope =
          executor.submit(
              () ->
                  StorageReadPriorityContext.withPriority(
                      StorageReadPriority.HIGH,
                      () -> {
                        highScopeStarted.countDown();
                        await(releaseHighScope);
                        return null;
                      }));
      assertThat(highScopeStarted.await(5, TimeUnit.SECONDS)).isTrue();

      final Future<?> firstLowRead =
          submitBlockingRead(executor, controller, StorageReadPriority.LOW, releaseLowReads);
      final Future<?> secondLowRead =
          submitBlockingRead(executor, controller, StorageReadPriority.LOW, releaseLowReads);
      final Future<?> queuedLowRead =
          submitBlockingRead(executor, controller, StorageReadPriority.LOW, releaseLowReads);

      awaitInFlightReads(controller, StorageReadPriority.LOW, 2);
      awaitWaitingReads(controller, StorageReadPriority.LOW, 1);

      releaseHighScope.countDown();
      highScope.get(5, TimeUnit.SECONDS);
      awaitInFlightReads(controller, StorageReadPriority.LOW, 3);

      releaseLowReads.countDown();
      firstLowRead.get(5, TimeUnit.SECONDS);
      secondLowRead.get(5, TimeUnit.SECONDS);
      queuedLowRead.get(5, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void shouldAllowMediumPriorityReadsWhenLowPriorityReadsAreAtHighPressureLimit()
      throws Exception {
    final RocksDBReadController controller =
        new RocksDBReadController(4, 4, 2, TimeUnit.SECONDS.toNanos(30));
    final ExecutorService executor = Executors.newFixedThreadPool(4);
    final CountDownLatch highReadStarted = new CountDownLatch(1);
    final CountDownLatch releaseHighRead = new CountDownLatch(1);
    final CountDownLatch releaseLowReads = new CountDownLatch(1);
    final CountDownLatch mediumReadStarted = new CountDownLatch(1);
    final CountDownLatch releaseMediumRead = new CountDownLatch(1);

    try {
      final Future<?> highRead =
          executor.submit(
              () ->
                  StorageReadPriorityContext.withPriority(
                      StorageReadPriority.HIGH,
                      () ->
                          controller.executeUnchecked(
                              () -> {
                                highReadStarted.countDown();
                                await(releaseHighRead);
                                return null;
                              })));
      assertThat(highReadStarted.await(5, TimeUnit.SECONDS)).isTrue();

      final Future<?> firstLowRead =
          submitBlockingRead(executor, controller, StorageReadPriority.LOW, releaseLowReads);
      final Future<?> secondLowRead =
          submitBlockingRead(executor, controller, StorageReadPriority.LOW, releaseLowReads);
      awaitInFlightReads(controller, StorageReadPriority.LOW, 2);

      final Future<?> mediumRead =
          executor.submit(
              () ->
                  StorageReadPriorityContext.withPriority(
                      StorageReadPriority.MEDIUM,
                      () ->
                          controller.executeUnchecked(
                              () -> {
                                mediumReadStarted.countDown();
                                await(releaseMediumRead);
                                return null;
                              })));

      assertThat(mediumReadStarted.await(5, TimeUnit.SECONDS)).isTrue();
      awaitInFlightReads(controller, StorageReadPriority.MEDIUM, 1);

      releaseHighRead.countDown();
      releaseMediumRead.countDown();
      releaseLowReads.countDown();
      highRead.get(5, TimeUnit.SECONDS);
      mediumRead.get(5, TimeUnit.SECONDS);
      firstLowRead.get(5, TimeUnit.SECONDS);
      secondLowRead.get(5, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }
  }

  private static void awaitWaitingReads(
      final RocksDBReadController controller,
      final StorageReadPriority priority,
      final int expectedWaitingReads)
      throws InterruptedException {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (System.nanoTime() < deadline) {
      if (controller.waitingReads(priority) == expectedWaitingReads) {
        return;
      }
      Thread.sleep(10);
    }
    assertThat(controller.waitingReads(priority)).isEqualTo(expectedWaitingReads);
  }

  private static void awaitInFlightReads(
      final RocksDBReadController controller,
      final StorageReadPriority priority,
      final int expectedInFlightReads)
      throws InterruptedException {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (System.nanoTime() < deadline) {
      if (controller.inFlightReads(priority) == expectedInFlightReads) {
        return;
      }
      Thread.sleep(10);
    }
    assertThat(controller.inFlightReads(priority)).isEqualTo(expectedInFlightReads);
  }

  private static Future<?> submitBlockingRead(
      final ExecutorService executor,
      final RocksDBReadController controller,
      final StorageReadPriority priority,
      final CountDownLatch releaseRead) {
    return executor.submit(
        () ->
            StorageReadPriorityContext.withPriority(
                priority,
                () ->
                    controller.executeUnchecked(
                        () -> {
                          await(releaseRead);
                          return null;
                        })));
  }

  private static void await(final CountDownLatch latch) {
    try {
      assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError("Interrupted while waiting for latch", e);
    }
  }
}
