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
package org.hyperledger.besu.consensus.common.bft;

import org.hyperledger.besu.consensus.common.bft.events.BftEvent;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Execution context for draining queued bft events and applying them to a maintained state */
public class BftProcessor implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(BftProcessor.class);

  private final BftEventQueue incomingQueue;
  private volatile boolean shutdown = false;
  private final EventMultiplexer eventMultiplexer;
  private volatile CountDownLatch shutdownLatch = new CountDownLatch(1);
  private volatile Thread processorThread;

  /**
   * Construct a new BftProcessor
   *
   * @param incomingQueue The event queue from which to drain new events
   * @param eventMultiplexer an object capable of handling any/all BFT events
   */
  public BftProcessor(final BftEventQueue incomingQueue, final EventMultiplexer eventMultiplexer) {
    this.incomingQueue = incomingQueue;
    this.eventMultiplexer = eventMultiplexer;
  }

  /** Indicate to the processor that it can be started */
  public synchronized void start() {
    shutdown = false;
    // A fresh latch per start/stop cycle: BftMiningCoordinator reuses this processor across
    // multiple start/stop cycles (e.g. pausing/resuming for sync), and the previous cycle's
    // latch is already counted down to zero, which would make awaitStop() return immediately
    // without actually waiting for this cycle's event loop to exit.
    shutdownLatch = new CountDownLatch(1);
  }

  /** Indicate to the processor that it should gracefully stop at its next opportunity */
  public synchronized void stop() {
    shutdown = true;
  }

  /**
   * Await stop. Returns immediately if {@link #run()} has never executed (no thread was ever
   * scheduled), since there is then nothing to wait for and {@link #shutdownLatch} would never be
   * counted down.
   *
   * @throws InterruptedException the interrupted exception
   */
  public void awaitStop() throws InterruptedException {
    if (processorThread == null) {
      return;
    }
    shutdownLatch.await();
  }

  /**
   * Returns whether the calling thread is the thread currently executing this processor's event
   * loop. Callers performing a blocking shutdown (e.g. waiting on {@link #awaitStop()}) must not do
   * so from the event thread itself.
   *
   * @return true if the calling thread is the BFT event processing thread
   */
  public boolean isEventThread() {
    return Thread.currentThread() == processorThread;
  }

  @Override
  public void run() {
    processorThread = Thread.currentThread();
    // Snapshot the latch for this run cycle: start() may install a new one for a subsequent
    // cycle as soon as this thread begins, and this cycle must count down the latch that
    // callers of awaitStop() are (or will be) waiting on for it, not a later cycle's latch.
    final CountDownLatch currentShutdownLatch = shutdownLatch;
    try {
      // Start the event queue. Until it is started it won't accept new events from peers
      incomingQueue.start();

      while (!shutdown) {
        final Optional<BftEvent> event = nextEvent();
        // Re-check the shutdown flag after polling: stop() may have been requested while
        // this event was being polled, in which case it must not be dispatched (e.g. a
        // queued BlockTimerExpiry would otherwise seal one more block after shutdown).
        if (!shutdown) {
          event.ifPresent(eventMultiplexer::handleBftEvent);
        }
      }

      incomingQueue.stop();
    } catch (final Throwable t) {
      LOG.error("BFT Mining thread has suffered a fatal error, mining has been halted", t);
    }
    // Clean up the executor service the round timer has been utilising
    LOG.info("Shutting down BFT event processor");
    currentShutdownLatch.countDown();
  }

  private Optional<BftEvent> nextEvent() {
    try {
      return Optional.ofNullable(incomingQueue.poll(500, TimeUnit.MILLISECONDS));
    } catch (final InterruptedException interrupt) {
      // If the queue was interrupted propagate it and spin to check our shutdown status
      Thread.currentThread().interrupt();
      return Optional.empty();
    }
  }
}
