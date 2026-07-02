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
package org.hyperledger.besu.consensus.common.bft.blockcreation;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.consensus.common.bft.BftEventQueue;
import org.hyperledger.besu.consensus.common.bft.BftExecutors;
import org.hyperledger.besu.consensus.common.bft.BftProcessor;
import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.EventMultiplexer;
import org.hyperledger.besu.consensus.common.bft.events.BftEvent;
import org.hyperledger.besu.consensus.common.bft.events.BlockTimerExpiry;
import org.hyperledger.besu.consensus.common.bft.statemachine.BftEventHandler;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Exercises the merge transition path with a real {@link BftProcessor} running on real {@link
 * BftExecutors}: the TTD watcher in TransitionBesuControllerBuilder calls {@code disable()} then
 * {@code stop()} on the coordinator, and does so synchronously from the BFT event thread itself
 * (QBFT imports its own sealed blocks inline, which fires the block-added observers that drive the
 * merge state callback). After that, no further queued events may be dispatched — each dispatched
 * BlockTimerExpiry would seal another block past TTD.
 */
@ExtendWith(MockitoExtension.class)
public class BftMiningCoordinatorTransitionTest {

  @Mock private BftEventHandler eventHandler;
  @Mock private BftBlockCreatorFactory<?> blockCreatorFactory;
  @Mock private Blockchain blockchain;

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  public void stopFromEventThreadHaltsEventProcessingWithoutDeadlock() throws InterruptedException {
    final BftEventQueue eventQueue = new BftEventQueue(1000);
    eventQueue.start();

    final BftExecutors bftExecutors =
        BftExecutors.create(new NoOpMetricsSystem(), BftExecutors.ConsensusType.QBFT);

    final AtomicInteger handledEvents = new AtomicInteger(0);
    final AtomicReference<BftMiningCoordinator> coordinatorRef = new AtomicReference<>();

    // Simulates exactly what the TTD watcher does when the terminal block is imported:
    // disable() then stop(), invoked on the BFT event thread while it is mid-dispatch.
    final EventMultiplexer eventMultiplexer =
        new EventMultiplexer(eventHandler) {
          @Override
          public void handleBftEvent(final BftEvent bftEvent) {
            if (handledEvents.incrementAndGet() == 1) {
              coordinatorRef.get().disable();
              coordinatorRef.get().stop();
            }
          }
        };

    final BftProcessor bftProcessor = new BftProcessor(eventQueue, eventMultiplexer);
    final BftMiningCoordinator coordinator =
        new BftMiningCoordinator(
            bftExecutors, eventHandler, bftProcessor, blockCreatorFactory, blockchain, eventQueue);
    coordinatorRef.set(coordinator);

    coordinator.enable();
    coordinator.start();

    // Two queued events: the first triggers the merge transition stop; the second must
    // never be dispatched (it would be "the extra QBFT block sealed past TTD").
    eventQueue.add(new BlockTimerExpiry(new ConsensusRoundIdentifier(1, 0)));
    eventQueue.add(new BlockTimerExpiry(new ConsensusRoundIdentifier(1, 0)));

    Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> !coordinator.isMining());

    // Allow time for a wrongly-dispatched second event to surface before asserting.
    Awaitility.await()
        .pollDelay(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(() -> assertThat(handledEvents.get()).isEqualTo(1));

    // stop() offloads its blocking teardown to a separate thread when invoked from the event
    // thread; wait for that teardown to finish so the BftProcessorExecutor/BftTimerExecutor
    // threads it shuts down don't leak into later tests.
    bftExecutors.awaitStop();
  }
}
